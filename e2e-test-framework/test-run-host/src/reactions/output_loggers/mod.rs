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

use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use console_logger::{ConsoleOutputLogger, ConsoleOutputLoggerConfig};
pub use determinism_hash_logger::{DeterminismHashOutputLogger, DeterminismHashOutputLoggerConfig};
pub use jsonl_file_logger::{JsonlFileOutputLogger, JsonlFileOutputLoggerConfig};
pub use performance_metrics_logger::{
    PerformanceMetricsOutputLogger, PerformanceMetricsOutputLoggerConfig,
};
use test_data_store::test_run_storage::{TestRunReactionId, TestRunReactionStorage};

use crate::common::HandlerRecord;

pub mod console_logger;
pub mod determinism_hash_logger;
pub(crate) mod final_state;
pub mod jsonl_file_logger;
pub mod performance_metrics_logger;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum OutputLoggerConfig {
    Console(ConsoleOutputLoggerConfig),
    DeterminismHash(DeterminismHashOutputLoggerConfig),
    JsonlFile(JsonlFileOutputLoggerConfig),
    PerformanceMetrics(PerformanceMetricsOutputLoggerConfig),
}

#[derive(Debug, thiserror::Error)]
pub enum OutputLoggerError {
    Io(#[from] std::io::Error),
    Serde(#[from] serde_json::Error),
}

impl std::fmt::Display for OutputLoggerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {e}"),
            Self::Serde(e) => write!(f, "Serde error: {e}"),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct OutputLoggerResult {
    pub has_output: bool,
    pub logger_name: String,
    pub output_folder_path: Option<PathBuf>,
    /// Free-form per-logger summary made available to completion handlers via
    /// the test-service reaction-state API. `None` for loggers that don't
    /// publish a structured summary (e.g. `Console`, `JsonlFile`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<serde_json::Value>,
}

#[async_trait]
pub trait OutputLogger: Send + Sync {
    async fn end_test_run(&mut self) -> anyhow::Result<OutputLoggerResult>;
    async fn log_handler_record(&mut self, record: &HandlerRecord) -> anyhow::Result<()>;
}

#[async_trait]
impl OutputLogger for Box<dyn OutputLogger + Send + Sync> {
    async fn end_test_run(&mut self) -> anyhow::Result<OutputLoggerResult> {
        (**self).end_test_run().await
    }
    async fn log_handler_record(&mut self, record: &HandlerRecord) -> anyhow::Result<()> {
        (**self).log_handler_record(record).await
    }
}

pub async fn create_output_logger(
    test_run_reaction_id: TestRunReactionId,
    config: &OutputLoggerConfig,
    output_storage: &TestRunReactionStorage,
) -> anyhow::Result<Box<dyn OutputLogger + Send + Sync>> {
    log::info!("create_output_logger called for {test_run_reaction_id} with config: {config:?}");
    match config {
        OutputLoggerConfig::Console(cfg) => ConsoleOutputLogger::new(test_run_reaction_id, cfg),
        OutputLoggerConfig::DeterminismHash(cfg) => {
            DeterminismHashOutputLogger::new(test_run_reaction_id, cfg)
        }
        OutputLoggerConfig::JsonlFile(cfg) => {
            JsonlFileOutputLogger::new(test_run_reaction_id, cfg, output_storage).await
        }
        OutputLoggerConfig::PerformanceMetrics(cfg) => {
            PerformanceMetricsOutputLogger::new(test_run_reaction_id, cfg, output_storage).await
        }
    }
}

pub async fn create_output_loggers(
    test_run_reaction_id: TestRunReactionId,
    configs: &Vec<OutputLoggerConfig>,
    output_storage: &TestRunReactionStorage,
) -> anyhow::Result<Vec<Box<dyn OutputLogger + Send + Sync>>> {
    let mut result = Vec::new();
    for config in configs {
        result.push(
            create_output_logger(test_run_reaction_id.clone(), config, output_storage).await?,
        );
    }
    Ok(result)
}

#[cfg(test)]
mod tests;
