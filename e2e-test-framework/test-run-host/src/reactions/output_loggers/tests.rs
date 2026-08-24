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

use crate::common::{HandlerPayload, HandlerRecord};
use crate::reactions::output_loggers::*;
use tempfile::TempDir;
use test_data_store::test_run_storage::{TestRunId, TestRunReactionId, TestRunReactionStorage};
use tokio::fs;

#[tokio::test]
async fn test_console_logger_output() {
    let test_run_id = TestRunId::new("repo", "test", "run");
    let reaction_id = TestRunReactionId::new(&test_run_id, "reaction1");

    let config = ConsoleOutputLoggerConfig {
        date_time_format: Some("%Y-%m-%d %H:%M:%S".to_string()),
    };

    let mut logger = ConsoleOutputLogger::new(reaction_id, &config).unwrap();

    // Test logging a reaction output record
    let record = HandlerRecord {
        id: "test-1".to_string(),
        sequence: 1,
        created_time_ns: 1000000,
        processed_time_ns: 2000000,
        traceparent: None,
        tracestate: None,
        payload: HandlerPayload::ReactionOutput {
            reaction_output: serde_json::json!({"status": "completed", "data": {"value": 42}}),
        },
    };

    assert!(logger.log_handler_record(&record).await.is_ok());

    // Test end_test_run
    let result = logger.end_test_run().await.unwrap();
    assert!(!result.has_output);
    assert_eq!(result.logger_name, "Console");
    assert!(result.output_folder_path.is_none());
}

#[tokio::test]
async fn test_jsonl_file_logger_rotation() {
    let temp_dir = TempDir::new().unwrap();
    let test_run_id = TestRunId::new("repo", "test", "run");
    let reaction_id = TestRunReactionId::new(&test_run_id, "reaction1");

    let storage = TestRunReactionStorage {
        id: reaction_id.clone(),
        path: temp_dir.path().to_path_buf(),
        reaction_output_path: temp_dir.path().join("outputs"),
    };

    let config = JsonlFileOutputLoggerConfig {
        max_lines_per_file: Some(2),
    };

    let mut logger = JsonlFileOutputLogger::new(reaction_id, &config, &storage)
        .await
        .unwrap();

    // Log 5 records to test file rotation (should create 3 files)
    for i in 0..5 {
        let record = HandlerRecord {
            id: format!("test-{i}"),
            sequence: i as u64,
            created_time_ns: i as u64 * 1000000,
            processed_time_ns: (i + 1) as u64 * 1000000,
            traceparent: None,
            tracestate: None,
            payload: HandlerPayload::ReactionOutput {
                reaction_output: serde_json::json!({"iteration": i, "data": format!("test data {}", i)}),
            },
        };
        assert!(logger.log_handler_record(&record).await.is_ok());
    }

    let result = logger.end_test_run().await.unwrap();
    assert!(result.has_output);
    assert_eq!(result.logger_name, "JsonlFile");
    assert!(result.output_folder_path.is_some());

    // Verify files were created with correct naming
    let output_dir = temp_dir.path().join("outputs").join("jsonl_file");
    let mut entries = fs::read_dir(&output_dir).await.unwrap();
    let mut files = Vec::new();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        files.push(entry.file_name());
    }
    files.sort();

    assert_eq!(files.len(), 3);
    assert_eq!(files[0].to_str().unwrap(), "outputs_00000.jsonl");
    assert_eq!(files[1].to_str().unwrap(), "outputs_00001.jsonl");
    assert_eq!(files[2].to_str().unwrap(), "outputs_00002.jsonl");

    // Verify file contents
    let first_file_content = fs::read_to_string(output_dir.join(&files[0]))
        .await
        .unwrap();
    let lines: Vec<&str> = first_file_content.trim().split('\n').collect();
    assert_eq!(lines.len(), 2); // max_lines_per_file = 2
}

#[tokio::test]
async fn test_output_logger_factory() {
    let temp_dir = TempDir::new().unwrap();
    let test_run_id = TestRunId::new("repo", "test", "run");
    let reaction_id = TestRunReactionId::new(&test_run_id, "reaction1");

    let storage = TestRunReactionStorage {
        id: reaction_id.clone(),
        path: temp_dir.path().to_path_buf(),
        reaction_output_path: temp_dir.path().join("outputs"),
    };

    // Test creating console logger via factory
    let console_config = OutputLoggerConfig::Console(ConsoleOutputLoggerConfig {
        date_time_format: None,
    });
    let console_logger = create_output_logger(reaction_id.clone(), &console_config, &storage).await;
    assert!(console_logger.is_ok());

    // Test creating JSONL file logger via factory
    let jsonl_config = OutputLoggerConfig::JsonlFile(JsonlFileOutputLoggerConfig {
        max_lines_per_file: Some(100),
    });
    let jsonl_logger = create_output_logger(reaction_id.clone(), &jsonl_config, &storage).await;
    assert!(jsonl_logger.is_ok());

    // Test creating multiple loggers
    let configs = vec![console_config, jsonl_config];
    let loggers = create_output_loggers(reaction_id, &configs, &storage).await;
    assert!(loggers.is_ok());
    assert_eq!(loggers.unwrap().len(), 2);
}

#[tokio::test]
async fn test_performance_metrics_logger() {
    let temp_dir = TempDir::new().unwrap();
    let test_run_id = TestRunId::new("test_repo", "test_id", "test_run_001");
    let reaction_id = TestRunReactionId::new(&test_run_id, "reaction_001");

    let reaction_storage = TestRunReactionStorage {
        id: reaction_id.clone(),
        path: temp_dir.path().to_path_buf(),
        reaction_output_path: temp_dir.path().join("outputs"),
    };

    let config = OutputLoggerConfig::PerformanceMetrics(PerformanceMetricsOutputLoggerConfig {
        filename: Some("test_performance.json".to_string()),
        bootstrap_record_count: None,
    });

    let mut logger = create_output_logger(reaction_id, &config, &reaction_storage)
        .await
        .unwrap();

    // Create test records
    for i in 0..50 {
        let record = HandlerRecord {
            id: format!("test_{i}"),
            sequence: i,
            created_time_ns: 1000 + i,
            processed_time_ns: 2000 + i,
            traceparent: None,
            tracestate: None,
            payload: HandlerPayload::ReactionOutput {
                reaction_output: serde_json::json!({"count": i}),
            },
        };
        assert!(logger.log_handler_record(&record).await.is_ok());
    }

    let result = logger.end_test_run().await.unwrap();
    assert!(result.has_output);
    assert_eq!(result.logger_name, "PerformanceMetrics");
    assert!(result.output_folder_path.is_some());

    // Verify file exists
    let metrics_path = temp_dir
        .path()
        .join("outputs")
        .join("performance_metrics")
        .join("test_performance.json");
    assert!(metrics_path.exists());

    // Verify content
    let content = std::fs::read_to_string(metrics_path).unwrap();
    assert!(content.contains("\"record_count\": 50"));
    assert!(content.contains("\"records_per_second\""));
}
