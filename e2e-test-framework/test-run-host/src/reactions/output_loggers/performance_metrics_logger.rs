// Copyright 2025 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Performance metrics output logger for measuring reaction throughput
//!
//! This logger tracks timing information and record counts to calculate
//! performance metrics like records per second. It writes a summary file
//! when the test run ends with detailed performance statistics.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use test_data_store::test_run_storage::{TestRunReactionId, TestRunReactionStorage};

use crate::common::HandlerRecord;

use super::{OutputLogger, OutputLoggerResult};

/// Metrics for a single phase of a run (e.g. bootstrap or steady-state).
#[derive(Debug, Serialize, Deserialize)]
pub struct PhaseMetrics {
    /// Timestamp in nanoseconds when the first record of this phase was received
    pub start_time_ns: u64,
    /// Timestamp in nanoseconds when the last record of this phase was received
    pub end_time_ns: u64,
    /// Duration of this phase in nanoseconds
    pub duration_ns: u64,
    /// Number of records processed during this phase
    pub record_count: u64,
    /// Records processed per second during this phase
    pub records_per_second: f64,
}

/// Performance metrics data structure
#[derive(Debug, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    /// Timestamp in nanoseconds when first record was received
    pub start_time_ns: u64,
    /// Timestamp in nanoseconds when test run ended
    pub end_time_ns: u64,
    /// Total duration in nanoseconds
    pub duration_ns: u64,
    /// Total number of records processed
    pub record_count: u64,
    /// Records processed per second
    pub records_per_second: f64,
    /// Bootstrap-phase metrics (records 1..=bootstrap_record_count). Present
    /// only when `bootstrap_record_count` is configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bootstrap: Option<PhaseMetrics>,
    /// Steady-state-phase metrics (records after bootstrap_record_count).
    /// Present only when `bootstrap_record_count` is configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub steady_state: Option<PhaseMetrics>,
    /// Test run reaction identifier
    pub test_run_reaction_id: String,
    /// Timestamp when metrics were written
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl std::fmt::Display for PerformanceMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Performance Metrics for {}: {} records in {:.3}s ({:.2} records/sec)",
            self.test_run_reaction_id,
            self.record_count,
            self.duration_ns as f64 / 1_000_000_000.0,
            self.records_per_second
        )
    }
}

/// Configuration for the performance metrics output logger
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerformanceMetricsOutputLoggerConfig {
    /// Optional custom filename for the metrics output
    pub filename: Option<String>,
    /// Optional number of leading records that constitute the bootstrap phase.
    /// When set, the logger reports separate `bootstrap` and `steady_state`
    /// metrics: the first `bootstrap_record_count` records are attributed to
    /// the bootstrap (initial-load) phase and the remainder to steady-state.
    #[serde(default)]
    pub bootstrap_record_count: Option<u64>,
}

/// Performance metrics output logger implementation
pub struct PerformanceMetricsOutputLogger {
    /// Timestamp in nanoseconds when first record was received
    start_time_ns: Option<u64>,
    /// Timestamp in nanoseconds when test run ended
    end_time_ns: u64,
    /// Total number of records received
    record_count: u64,
    /// Optional number of leading records treated as the bootstrap phase
    bootstrap_record_count: Option<u64>,
    /// Timestamp in nanoseconds when the bootstrap phase completed (i.e. when
    /// the `bootstrap_record_count`-th record was received)
    bootstrap_end_time_ns: Option<u64>,
    /// Test run reaction identifier
    test_run_reaction_id: TestRunReactionId,
    /// Storage abstraction for writing output files
    output_storage: TestRunReactionStorage,
    /// Path where metrics file will be written
    output_path: PathBuf,
}

impl PerformanceMetricsOutputLogger {
    /// Create a new performance metrics output logger
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(
        test_run_reaction_id: TestRunReactionId,
        config: &PerformanceMetricsOutputLoggerConfig,
        output_storage: &TestRunReactionStorage,
    ) -> anyhow::Result<Box<dyn OutputLogger + Send + Sync>> {
        log::info!(
            "PerformanceMetricsOutputLogger::new() called for {test_run_reaction_id} with config {config:?}"
        );

        // Generate output filename
        let filename = config.filename.clone().unwrap_or_else(|| {
            format!(
                "performance_metrics_{}.json",
                chrono::Utc::now().format("%Y%m%d_%H%M%S")
            )
        });

        // Create output directory
        let output_dir = output_storage
            .reaction_output_path
            .join("performance_metrics");
        log::info!("PerformanceMetricsOutputLogger checking/creating directory: {output_dir:?}");

        if !output_dir.exists() {
            log::info!("Creating directory: {output_dir:?}");
            tokio::fs::create_dir_all(&output_dir).await?;
        } else {
            log::info!("Directory already exists: {output_dir:?}");
        }

        // Set the output path
        let output_path = output_dir.join(&filename);

        log::info!("PerformanceMetricsOutputLogger created with output path: {output_path:?}");

        Ok(Box::new(Self {
            start_time_ns: None,
            end_time_ns: 0,
            record_count: 0,
            bootstrap_record_count: config.bootstrap_record_count,
            bootstrap_end_time_ns: None,
            test_run_reaction_id,
            output_storage: output_storage.clone(),
            output_path,
        }))
    }

    /// Get current time in nanoseconds since UNIX epoch
    fn get_current_time_ns() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as u64
    }

    /// Compute bootstrap and steady-state phase metrics when a
    /// `bootstrap_record_count` is configured. Returns `(None, None)` when it
    /// is not configured, so the overall metrics behave exactly as before.
    ///
    /// `start_time` is the timestamp of the first record received. The
    /// bootstrap phase spans `[start_time, bootstrap_end]` and covers the first
    /// `k` records; the steady-state phase spans `[bootstrap_end, end_time_ns]`
    /// and covers the remaining records.
    fn compute_phase_metrics(
        &self,
        start_time: u64,
    ) -> (Option<PhaseMetrics>, Option<PhaseMetrics>) {
        let k = match self.bootstrap_record_count {
            Some(k) if k > 0 => k,
            _ => return (None, None),
        };

        let rps = |count: u64, duration_ns: u64| -> f64 {
            let secs = duration_ns as f64 / 1_000_000_000.0;
            if secs > 0.0 {
                count as f64 / secs
            } else {
                0.0
            }
        };

        // The bootstrap phase ends when the k-th record arrives. If it was never
        // reached (fewer than k records total), treat the run end as the
        // boundary and report no steady-state phase.
        let bootstrap_reached = self.record_count >= k;
        let bootstrap_end = self.bootstrap_end_time_ns.unwrap_or(self.end_time_ns);

        let bootstrap_count = self.record_count.min(k);
        let bootstrap_duration_ns = bootstrap_end.saturating_sub(start_time);
        let bootstrap = PhaseMetrics {
            start_time_ns: start_time,
            end_time_ns: bootstrap_end,
            duration_ns: bootstrap_duration_ns,
            record_count: bootstrap_count,
            records_per_second: rps(bootstrap_count, bootstrap_duration_ns),
        };

        let steady_state = if bootstrap_reached {
            let steady_count = self.record_count - k;
            let steady_duration_ns = self.end_time_ns.saturating_sub(bootstrap_end);
            Some(PhaseMetrics {
                start_time_ns: bootstrap_end,
                end_time_ns: self.end_time_ns,
                duration_ns: steady_duration_ns,
                record_count: steady_count,
                records_per_second: rps(steady_count, steady_duration_ns),
            })
        } else {
            None
        };

        (Some(bootstrap), steady_state)
    }
}

#[async_trait]
impl OutputLogger for PerformanceMetricsOutputLogger {
    async fn log_handler_record(&mut self, _record: &HandlerRecord) -> anyhow::Result<()> {
        // Set start time on first record
        if self.start_time_ns.is_none() {
            self.start_time_ns = Some(Self::get_current_time_ns());
            log::debug!(
                "PerformanceMetricsOutputLogger: First record received at {} ns",
                self.start_time_ns.unwrap()
            );
        }

        // Increment record count
        self.record_count += 1;

        // Capture the bootstrap phase boundary: the moment the
        // bootstrap_record_count-th record is received marks the end of the
        // bootstrap (initial-load) phase and the start of steady-state.
        if let Some(k) = self.bootstrap_record_count {
            if k > 0 && self.record_count == k && self.bootstrap_end_time_ns.is_none() {
                self.bootstrap_end_time_ns = Some(Self::get_current_time_ns());
                log::debug!(
                    "PerformanceMetricsOutputLogger: Bootstrap phase complete at {} records",
                    self.record_count
                );
            }
        }

        // Log every 1000 records for debugging
        if self.record_count % 1000 == 0 {
            log::debug!(
                "PerformanceMetricsOutputLogger: Processed {} records",
                self.record_count
            );
        }

        Ok(())
    }

    async fn end_test_run(&mut self) -> anyhow::Result<OutputLoggerResult> {
        log::error!(
            "PerformanceMetricsOutputLogger: Ending test run for {} with {} records",
            self.test_run_reaction_id,
            self.record_count
        );

        // Capture end time
        self.end_time_ns = Self::get_current_time_ns();

        // Calculate metrics
        let start_time = self.start_time_ns.unwrap_or(self.end_time_ns);
        let duration_ns = if self.start_time_ns.is_some() {
            self.end_time_ns - start_time
        } else {
            0
        };

        let duration_seconds = duration_ns as f64 / 1_000_000_000.0;
        let records_per_second = if duration_seconds > 0.0 {
            self.record_count as f64 / duration_seconds
        } else {
            0.0
        };

        // Compute per-phase (bootstrap / steady-state) metrics when a
        // bootstrap_record_count is configured.
        let (bootstrap, steady_state) = self.compute_phase_metrics(start_time);

        // Create metrics struct
        let metrics = PerformanceMetrics {
            start_time_ns: start_time,
            end_time_ns: self.end_time_ns,
            duration_ns,
            record_count: self.record_count,
            records_per_second,
            bootstrap,
            steady_state,
            test_run_reaction_id: self.test_run_reaction_id.to_string(),
            timestamp: chrono::Utc::now(),
        };

        log::error!("{metrics}");

        // Write metrics to file
        let metrics_json = serde_json::to_string_pretty(&metrics)?;
        log::info!(
            "PerformanceMetricsOutputLogger writing {} bytes to {:?}",
            metrics_json.len(),
            self.output_path
        );

        match tokio::fs::write(&self.output_path, metrics_json.as_bytes()).await {
            Ok(_) => log::info!("Successfully wrote metrics to {:?}", self.output_path),
            Err(e) => {
                log::error!("Failed to write metrics to {:?}: {}", self.output_path, e);
                return Err(e.into());
            }
        }

        // Get the parent directory for the output folder path
        let output_folder = self
            .output_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| self.output_storage.reaction_output_path.clone());

        Ok(OutputLoggerResult {
            has_output: true,
            logger_name: "PerformanceMetrics".to_string(),
            output_folder_path: Some(output_folder),
            summary: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::HandlerPayload;
    use tempfile::TempDir;
    use test_data_store::test_run_storage::{TestRunId, TestRunReactionStorage};

    async fn create_test_logger() -> (PerformanceMetricsOutputLogger, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let test_run_id = TestRunId::new("test_repo", "test_id", "test_run_001");
        let test_run_reaction_id = TestRunReactionId::new(&test_run_id, "reaction_001");

        let reaction_storage = TestRunReactionStorage {
            id: test_run_reaction_id.clone(),
            path: temp_dir.path().to_path_buf(),
            reaction_output_path: temp_dir.path().join("output"),
        };

        let _config = PerformanceMetricsOutputLoggerConfig {
            filename: Some("test_metrics.json".to_string()),
            bootstrap_record_count: None,
        };

        // Create output directory
        let output_dir = reaction_storage
            .reaction_output_path
            .join("performance_metrics");
        tokio::fs::create_dir_all(&output_dir).await.unwrap();

        let logger = PerformanceMetricsOutputLogger {
            start_time_ns: None,
            end_time_ns: 0,
            record_count: 0,
            bootstrap_record_count: None,
            bootstrap_end_time_ns: None,
            test_run_reaction_id,
            output_storage: reaction_storage,
            output_path: output_dir.join("test_metrics.json"),
        };

        (logger, temp_dir)
    }

    #[tokio::test]
    async fn test_first_record_sets_start_time() {
        let (mut logger, _temp_dir) = create_test_logger().await;

        assert!(logger.start_time_ns.is_none());

        let record = HandlerRecord {
            id: "test_id".to_string(),
            sequence: 1,
            created_time_ns: 1000,
            processed_time_ns: 2000,
            traceparent: None,
            tracestate: None,
            payload: HandlerPayload::ReactionOutput {
                reaction_output: serde_json::json!({"test": "data"}),
            },
        };

        logger.log_handler_record(&record).await.unwrap();

        assert!(logger.start_time_ns.is_some());
        assert_eq!(logger.record_count, 1);
    }

    #[tokio::test]
    async fn test_multiple_records_increment_count() {
        let (mut logger, _temp_dir) = create_test_logger().await;

        let record = HandlerRecord {
            id: "test_id".to_string(),
            sequence: 1,
            created_time_ns: 1000,
            processed_time_ns: 2000,
            traceparent: None,
            tracestate: None,
            payload: HandlerPayload::ReactionOutput {
                reaction_output: serde_json::json!({"test": "data"}),
            },
        };

        for i in 0..5 {
            let mut r = record.clone();
            r.sequence = i;
            logger.log_handler_record(&r).await.unwrap();
        }

        assert_eq!(logger.record_count, 5);
        assert!(logger.start_time_ns.is_some());
    }

    #[tokio::test]
    async fn test_end_test_run_produces_metrics() {
        let (mut logger, temp_dir) = create_test_logger().await;

        // Simulate some records
        let record = HandlerRecord {
            id: "test_id".to_string(),
            sequence: 1,
            created_time_ns: 1000,
            processed_time_ns: 2000,
            traceparent: None,
            tracestate: None,
            payload: HandlerPayload::ReactionOutput {
                reaction_output: serde_json::json!({"test": "data"}),
            },
        };

        // Add some records
        for _ in 0..100 {
            logger.log_handler_record(&record).await.unwrap();
        }

        // Force a specific start time for predictable testing
        logger.start_time_ns = Some(1_000_000_000);

        // Sleep briefly to ensure some time passes
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let result = logger.end_test_run().await.unwrap();

        assert!(result.has_output);
        assert_eq!(result.logger_name, "PerformanceMetrics");
        assert!(result.output_folder_path.is_some());

        // Verify the metrics file was created
        let metrics_path = temp_dir
            .path()
            .join("output")
            .join("performance_metrics")
            .join("test_metrics.json");
        assert!(metrics_path.exists());

        // Read and verify metrics content
        let metrics_content = std::fs::read_to_string(metrics_path).unwrap();
        let metrics: PerformanceMetrics = serde_json::from_str(&metrics_content).unwrap();

        assert_eq!(metrics.record_count, 100);
        assert!(metrics.duration_ns > 0);
        assert!(metrics.records_per_second > 0.0);
    }

    #[tokio::test]
    async fn test_no_records_case() {
        let (mut logger, _temp_dir) = create_test_logger().await;

        let result = logger.end_test_run().await.unwrap();

        assert!(result.has_output);
        assert_eq!(result.logger_name, "PerformanceMetrics");

        // Even with no records, metrics should be written
        assert_eq!(logger.record_count, 0);
    }

    #[tokio::test]
    async fn test_bootstrap_phase_split() {
        let (mut logger, temp_dir) = create_test_logger().await;

        // Configure a bootstrap boundary of 10 records.
        logger.bootstrap_record_count = Some(10);

        let record = HandlerRecord {
            id: "test_id".to_string(),
            sequence: 1,
            created_time_ns: 1000,
            processed_time_ns: 2000,
            traceparent: None,
            tracestate: None,
            payload: HandlerPayload::ReactionOutput {
                reaction_output: serde_json::json!({"test": "data"}),
            },
        };

        // First 10 records = bootstrap phase.
        for _ in 0..10 {
            logger.log_handler_record(&record).await.unwrap();
        }
        // The bootstrap boundary should have been captured.
        assert!(logger.bootstrap_end_time_ns.is_some());

        // Ensure measurable time passes before steady-state records.
        tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;

        // Remaining 15 records = steady-state phase.
        for _ in 0..15 {
            logger.log_handler_record(&record).await.unwrap();
        }

        let result = logger.end_test_run().await.unwrap();
        assert!(result.has_output);

        let metrics_path = temp_dir
            .path()
            .join("output")
            .join("performance_metrics")
            .join("test_metrics.json");
        let metrics_content = std::fs::read_to_string(metrics_path).unwrap();
        let metrics: PerformanceMetrics = serde_json::from_str(&metrics_content).unwrap();

        assert_eq!(metrics.record_count, 25);

        let bootstrap = metrics.bootstrap.expect("bootstrap phase present");
        assert_eq!(bootstrap.record_count, 10);

        let steady_state = metrics.steady_state.expect("steady_state phase present");
        assert_eq!(steady_state.record_count, 15);
    }

    #[tokio::test]
    async fn test_no_bootstrap_config_omits_phases() {
        let (mut logger, temp_dir) = create_test_logger().await;

        let record = HandlerRecord {
            id: "test_id".to_string(),
            sequence: 1,
            created_time_ns: 1000,
            processed_time_ns: 2000,
            traceparent: None,
            tracestate: None,
            payload: HandlerPayload::ReactionOutput {
                reaction_output: serde_json::json!({"test": "data"}),
            },
        };

        for _ in 0..5 {
            logger.log_handler_record(&record).await.unwrap();
        }

        logger.end_test_run().await.unwrap();

        let metrics_path = temp_dir
            .path()
            .join("output")
            .join("performance_metrics")
            .join("test_metrics.json");
        let metrics_content = std::fs::read_to_string(metrics_path).unwrap();
        let metrics: PerformanceMetrics = serde_json::from_str(&metrics_content).unwrap();

        // Without a configured bootstrap_record_count, no phase metrics emitted.
        assert!(metrics.bootstrap.is_none());
        assert!(metrics.steady_state.is_none());
    }
}
