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

//! Verifies that each reaction's `DeterminismHash` output-logger SHA matches
//! the baseline declared in the test definition.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use test_data_store::{
    test_repo_storage::models::{MissingBaselinePolicy, Sha256DeterminismHandlerConfig},
    test_run_storage::TestRunId,
    TestDataStore,
};

use super::CompletionHandler;
use crate::test_run_completion::types::ComponentCompletionSummary;

const DETERMINISM_VERDICT_FILENAME: &str = "determinism_verdict.json";

pub struct Sha256DeterminismCompletionHandler {
    config: Sha256DeterminismHandlerConfig,
    data_store: Arc<TestDataStore>,
    test_run_id: TestRunId,
}

impl Sha256DeterminismCompletionHandler {
    pub fn new(
        config: &Sha256DeterminismHandlerConfig,
        data_store: Arc<TestDataStore>,
        test_run_id: TestRunId,
    ) -> Self {
        Self {
            config: config.clone(),
            data_store,
            test_run_id,
        }
    }
}

#[async_trait]
impl CompletionHandler for Sha256DeterminismCompletionHandler {
    async fn handle_completion(
        &self,
        test_run_id: &str,
        completion_summary: &ComponentCompletionSummary,
    ) -> anyhow::Result<()> {
        // Walk every reaction the tracker saw and pick out the
        // `DeterminismHash` logger's summary, if present.
        let mut per_reaction: Vec<ReactionVerdict> = Vec::new();
        let mut mismatched: Vec<String> = Vec::new();
        let mut missing_baseline_failures: Vec<String> = Vec::new();

        for (reaction_id, logger_results) in &completion_summary.reaction_logger_outputs {
            let reaction_id_str = reaction_id.test_reaction_id.clone();
            let actual = logger_results
                .iter()
                .find(|r| r.logger_name == "DeterminismHash")
                .and_then(|r| r.summary.as_ref())
                .and_then(|s| s.get("sha256").and_then(|v| v.as_str()))
                .map(str::to_string);

            let expected = self.config.expected.get(&reaction_id_str).cloned();

            let passed = match (&actual, &expected) {
                (Some(a), Some(e)) => {
                    let ok = a == e;
                    if !ok {
                        mismatched.push(reaction_id_str.clone());
                    }
                    ok
                }
                (Some(a), None) => match self.config.missing_baseline {
                    MissingBaselinePolicy::Warn => {
                        log::warn!(
                            "[{test_run_id}] reaction {reaction_id_str}: no expected SHA-256 \
                             baseline; actual={a} (treating as pass)"
                        );
                        true
                    }
                    MissingBaselinePolicy::Fail => {
                        missing_baseline_failures.push(reaction_id_str.clone());
                        false
                    }
                },
                (None, _) => {
                    log::warn!(
                        "[{test_run_id}] reaction {reaction_id_str}: no DeterminismHash \
                         logger output found; nothing to compare"
                    );
                    // No logger output to compare; only fail if a baseline was
                    // declared (i.e. the user expected to verify this reaction).
                    if expected.is_some() {
                        mismatched.push(reaction_id_str.clone());
                        false
                    } else {
                        true
                    }
                }
            };

            per_reaction.push(ReactionVerdict {
                reaction_id: reaction_id_str,
                actual,
                expected,
                passed,
            });
        }

        // Sorted for stable verdict ordering.
        per_reaction.sort_by(|a, b| a.reaction_id.cmp(&b.reaction_id));

        // Best-effort verdict file. Failure to write is logged but does not
        // override the comparison verdict.
        if let Err(e) = self.write_verdict_file(&per_reaction).await {
            log::error!("[{test_run_id}] failed to write determinism verdict file: {e}");
        }

        if !mismatched.is_empty() || !missing_baseline_failures.is_empty() {
            let mut msg = String::from("Determinism mismatch for reactions: [");
            msg.push_str(&mismatched.join(", "));
            msg.push(']');
            if !missing_baseline_failures.is_empty() {
                msg.push_str("; missing baseline (policy=Fail) for: [");
                msg.push_str(&missing_baseline_failures.join(", "));
                msg.push(']');
            }
            return Err(anyhow::anyhow!(msg));
        }

        log::info!(
            "[{test_run_id}] determinism verification passed for {} reaction(s)",
            per_reaction.len()
        );
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ReactionVerdict {
    reaction_id: String,
    actual: Option<String>,
    expected: Option<String>,
    passed: bool,
}

impl Sha256DeterminismCompletionHandler {
    async fn write_verdict_file(&self, verdicts: &[ReactionVerdict]) -> anyhow::Result<()> {
        let storage = self
            .data_store
            .get_test_run_storage(&self.test_run_id)
            .await?;
        let path = storage.path.join(DETERMINISM_VERDICT_FILENAME);

        let body = json!({
            "test_run_id": self.test_run_id.to_string(),
            "results": verdicts
                .iter()
                .map(|v| {
                    (
                        v.reaction_id.clone(),
                        json!({
                            "actual": v.actual,
                            "expected": v.expected,
                            "passed": v.passed,
                        }),
                    )
                })
                .collect::<serde_json::Map<_, _>>(),
        });

        tokio::fs::write(&path, serde_json::to_vec_pretty(&body)?).await?;
        log::debug!("Wrote determinism verdict to {}", path.display());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use test_data_store::{
        test_repo_storage::models::{MissingBaselinePolicy, Sha256DeterminismHandlerConfig},
        test_run_storage::{TestRunId, TestRunReactionId},
        TestDataStore, TestDataStoreConfig,
    };

    use crate::reactions::output_loggers::OutputLoggerResult;
    use crate::test_run_completion::types::ComponentCompletionSummary;

    async fn data_store() -> (Arc<TestDataStore>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = TestDataStoreConfig {
            data_store_path: Some(tmp.path().to_string_lossy().to_string()),
            delete_on_start: Some(false),
            delete_on_stop: Some(false),
            ..Default::default()
        };
        let store = Arc::new(TestDataStore::new(cfg).await.unwrap());
        (store, tmp)
    }

    fn summary_with(
        reactions: Vec<(&str, Option<&str>)>,
        run: &TestRunId,
    ) -> ComponentCompletionSummary {
        let mut outputs: HashMap<TestRunReactionId, Vec<OutputLoggerResult>> = HashMap::new();
        for (rid, sha_hex) in reactions {
            let id = TestRunReactionId::new(run, rid);
            let entry = match sha_hex {
                Some(s) => vec![OutputLoggerResult {
                    has_output: true,
                    logger_name: "DeterminismHash".to_string(),
                    output_folder_path: None,
                    summary: Some(json!({ "sha256": s, "record_count": 1 })),
                }],
                None => vec![],
            };
            outputs.insert(id, entry);
        }
        ComponentCompletionSummary {
            drasi_lib_instances_stopped: 0,
            drasi_lib_instances_error: 0,
            sources_finished: 0,
            sources_stopped: 0,
            sources_error: 0,
            queries_stopped: 0,
            queries_error: 0,
            reactions_stopped: outputs.len(),
            reactions_error: 0,
            component_finish_times: HashMap::new(),
            reaction_logger_outputs: outputs,
        }
    }

    #[tokio::test]
    async fn passes_when_all_reactions_match_expected() {
        let (store, _tmp) = data_store().await;
        let run = TestRunId::new("repo", "test", "run");
        let mut expected = std::collections::BTreeMap::new();
        expected.insert("r1".to_string(), "aa".to_string());
        let cfg = Sha256DeterminismHandlerConfig {
            expected,
            missing_baseline: MissingBaselinePolicy::Warn,
        };
        let handler = Sha256DeterminismCompletionHandler::new(&cfg, store, run.clone());
        let summary = summary_with(vec![("r1", Some("aa"))], &run);
        handler
            .handle_completion(&run.to_string(), &summary)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fails_when_actual_differs_from_expected() {
        let (store, _tmp) = data_store().await;
        let run = TestRunId::new("repo", "test", "run");
        let mut expected = std::collections::BTreeMap::new();
        expected.insert("r1".to_string(), "aa".to_string());
        let cfg = Sha256DeterminismHandlerConfig {
            expected,
            missing_baseline: MissingBaselinePolicy::Warn,
        };
        let handler = Sha256DeterminismCompletionHandler::new(&cfg, store, run.clone());
        let summary = summary_with(vec![("r1", Some("bb"))], &run);
        let err = handler
            .handle_completion(&run.to_string(), &summary)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("r1"), "error: {err}");
    }

    #[tokio::test]
    async fn warn_policy_passes_on_missing_baseline() {
        let (store, _tmp) = data_store().await;
        let run = TestRunId::new("repo", "test", "run");
        let cfg = Sha256DeterminismHandlerConfig {
            expected: std::collections::BTreeMap::new(),
            missing_baseline: MissingBaselinePolicy::Warn,
        };
        let handler = Sha256DeterminismCompletionHandler::new(&cfg, store, run.clone());
        let summary = summary_with(vec![("r1", Some("aa"))], &run);
        handler
            .handle_completion(&run.to_string(), &summary)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fail_policy_errors_on_missing_baseline() {
        let (store, _tmp) = data_store().await;
        let run = TestRunId::new("repo", "test", "run");
        let cfg = Sha256DeterminismHandlerConfig {
            expected: std::collections::BTreeMap::new(),
            missing_baseline: MissingBaselinePolicy::Fail,
        };
        let handler = Sha256DeterminismCompletionHandler::new(&cfg, store, run.clone());
        let summary = summary_with(vec![("r1", Some("aa"))], &run);
        let err = handler
            .handle_completion(&run.to_string(), &summary)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("missing baseline"), "error: {err}");
    }
}
