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

//! Verifies ordered-stream hashes or final materialized state against a stored
//! golden manifest.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use test_data_store::{
    test_repo_storage::models::{MissingBaselinePolicy, Sha256DeterminismHandlerConfig},
    test_run_storage::TestRunId,
    TestDataStore,
};

use super::CompletionHandler;
use crate::reactions::output_loggers::final_state::canonical_row;
use crate::test_run_completion::types::ComponentCompletionSummary;

const DETERMINISM_VERDICT_FILENAME: &str = "determinism_verdict.json";
const FINAL_STATE_CANDIDATE_FILENAME: &str = "final_state_candidate.json";
const FINAL_STATE_SCHEMA_VERSION: u32 = 1;

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
        if self.config.golden_file.is_some() {
            self.handle_final_state(test_run_id, completion_summary)
                .await
        } else {
            self.handle_ordered_stream(test_run_id, completion_summary)
                .await
        }
    }
}

impl Sha256DeterminismCompletionHandler {
    async fn handle_ordered_stream(
        &self,
        test_run_id: &str,
        completion_summary: &ComponentCompletionSummary,
    ) -> anyhow::Result<()> {
        let mut per_reaction: Vec<OrderedReactionVerdict> = Vec::new();
        let mut mismatched: Vec<String> = Vec::new();
        let mut missing_baseline_failures: Vec<String> = Vec::new();

        for (reaction_id, logger_results) in &completion_summary.reaction_logger_outputs {
            let reaction_id_str = reaction_id.test_reaction_id.clone();
            let actual = logger_results
                .iter()
                .find(|r| r.logger_name == "DeterminismHash")
                .and_then(|r| r.summary.as_ref())
                .and_then(|s| s.get("sha256").and_then(Value::as_str))
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
                    if expected.is_some() {
                        mismatched.push(reaction_id_str.clone());
                        false
                    } else {
                        true
                    }
                }
            };

            per_reaction.push(OrderedReactionVerdict {
                reaction_id: reaction_id_str,
                actual,
                expected,
                passed,
            });
        }
        per_reaction.sort_by(|a, b| a.reaction_id.cmp(&b.reaction_id));

        let body = json!({
            "mode": "ordered_stream",
            "test_run_id": self.test_run_id.to_string(),
            "results": per_reaction
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
        if let Err(error) = self
            .write_json_artifact(DETERMINISM_VERDICT_FILENAME, &body)
            .await
        {
            log::error!("[{test_run_id}] failed to write determinism verdict file: {error}");
        }

        if !mismatched.is_empty() || !missing_baseline_failures.is_empty() {
            let mut msg = format!(
                "Determinism mismatch for reactions: [{}]",
                mismatched.join(", ")
            );
            if !missing_baseline_failures.is_empty() {
                msg.push_str(&format!(
                    "; missing baseline (policy=Fail) for: [{}]",
                    missing_baseline_failures.join(", ")
                ));
            }
            anyhow::bail!(msg);
        }

        log::info!(
            "[{test_run_id}] determinism verification passed for {} reaction(s)",
            per_reaction.len()
        );
        Ok(())
    }

    async fn handle_final_state(
        &self,
        test_run_id: &str,
        completion_summary: &ComponentCompletionSummary,
    ) -> anyhow::Result<()> {
        let actual = FinalStateManifest::from_completion_summary(completion_summary)?;
        self.write_json_artifact(FINAL_STATE_CANDIDATE_FILENAME, &actual)
            .await?;
        let normalization_failures: Vec<String> = actual
            .reactions
            .iter()
            .filter(|(_, reaction)| reaction.normalization_error_count > 0)
            .map(|(reaction_id, reaction)| {
                format!("{reaction_id} ({})", reaction.normalization_error_count)
            })
            .collect();
        if !normalization_failures.is_empty() {
            anyhow::bail!(
                "final-state materialization failed to normalize records for reactions: [{}]",
                normalization_failures.join(", ")
            );
        }

        let golden_path = self.resolve_golden_path().await?;
        let golden_bytes = match tokio::fs::read(&golden_path).await {
            Ok(bytes) => bytes,
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    && self.config.missing_baseline == MissingBaselinePolicy::Warn =>
            {
                log::warn!(
                    "[{test_run_id}] final-state golden '{}' is missing; candidate written to {} \
                     (treating as pass because missing_baseline=Warn)",
                    golden_path.display(),
                    FINAL_STATE_CANDIDATE_FILENAME
                );
                return Ok(());
            }
            Err(error) => {
                return Err(anyhow::anyhow!(
                    "failed to read final-state golden '{}': {error}",
                    golden_path.display()
                ));
            }
        };
        let golden: FinalStateManifest =
            serde_json::from_slice(&golden_bytes).map_err(|error| {
                anyhow::anyhow!(
                    "failed to parse final-state golden '{}': {error}",
                    golden_path.display()
                )
            })?;
        golden.validate(&golden_path)?;

        let verdict = compare_final_state(
            &golden,
            &actual,
            self.config.diagnostic_limit,
            &self.test_run_id.to_string(),
        );
        self.write_json_artifact(DETERMINISM_VERDICT_FILENAME, &verdict)
            .await?;

        let failed: Vec<&str> = verdict
            .results
            .iter()
            .filter_map(|(reaction_id, result)| (!result.passed).then_some(reaction_id.as_str()))
            .collect();
        if !failed.is_empty() {
            anyhow::bail!(
                "Final-state mismatch for reactions: [{}]; see {}",
                failed.join(", "),
                DETERMINISM_VERDICT_FILENAME
            );
        }

        log::info!(
            "[{test_run_id}] final-state verification passed for {} reaction(s)",
            verdict.results.len()
        );
        Ok(())
    }

    async fn resolve_golden_path(&self) -> anyhow::Result<PathBuf> {
        let configured = self
            .config
            .golden_file
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("final-state golden file is not configured"))?;
        let configured = Path::new(configured);
        if configured.is_absolute() {
            anyhow::bail!("final-state golden_file must be relative to the test data folder");
        }

        let repo = self
            .data_store
            .get_test_repo_storage(&self.test_run_id.test_repo_id)
            .await?;
        let test = repo.get_test_storage(&self.test_run_id.test_id).await?;
        Ok(test.path.join(configured))
    }

    async fn write_json_artifact<T: Serialize>(
        &self,
        filename: &str,
        value: &T,
    ) -> anyhow::Result<()> {
        let storage = self
            .data_store
            .get_test_run_storage(&self.test_run_id)
            .await?;
        let path = storage.path.join(filename);
        tokio::fs::write(&path, serde_json::to_vec_pretty(value)?).await?;
        log::debug!("Wrote determinism artifact to {}", path.display());
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct OrderedReactionVerdict {
    reaction_id: String,
    actual: Option<String>,
    expected: Option<String>,
    passed: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct FinalStateManifest {
    schema_version: u32,
    reactions: BTreeMap<String, FinalStateReaction>,
}

impl FinalStateManifest {
    fn from_completion_summary(summary: &ComponentCompletionSummary) -> anyhow::Result<Self> {
        let mut reactions = BTreeMap::new();
        for (reaction_id, logger_results) in &summary.reaction_logger_outputs {
            let logger_summary = logger_results
                .iter()
                .find(|result| result.logger_name == "DeterminismHash")
                .and_then(|result| result.summary.as_ref())
                .filter(|summary| {
                    summary.get("mode").and_then(Value::as_str) == Some("final_state")
                })
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "reaction '{}' has no final-state DeterminismHash output",
                        reaction_id.test_reaction_id
                    )
                })?;
            let reaction: FinalStateReaction = serde_json::from_value(logger_summary.clone())
                .map_err(|error| {
                    anyhow::anyhow!(
                        "invalid final-state output for reaction '{}': {error}",
                        reaction_id.test_reaction_id
                    )
                })?;
            reactions.insert(reaction_id.test_reaction_id.clone(), reaction);
        }
        Ok(Self {
            schema_version: FINAL_STATE_SCHEMA_VERSION,
            reactions,
        })
    }

    fn validate(&self, path: &Path) -> anyhow::Result<()> {
        if self.schema_version != FINAL_STATE_SCHEMA_VERSION {
            anyhow::bail!(
                "unsupported final-state golden schema_version {} in '{}' (expected {})",
                self.schema_version,
                path.display(),
                FINAL_STATE_SCHEMA_VERSION
            );
        }
        for (reaction_id, reaction) in &self.reactions {
            if reaction.mode != "final_state" {
                anyhow::bail!(
                    "final-state golden '{}' reaction '{}' has unsupported mode '{}'",
                    path.display(),
                    reaction_id,
                    reaction.mode
                );
            }
            if reaction.normalization_error_count > 0 {
                anyhow::bail!(
                    "final-state golden '{}' reaction '{}' was captured with {} normalization errors",
                    path.display(),
                    reaction_id,
                    reaction.normalization_error_count
                );
            }
            if reaction.row_count != reaction.rows.len() {
                anyhow::bail!(
                    "final-state golden '{}' reaction '{}' declares row_count={} but contains {} rows",
                    path.display(),
                    reaction_id,
                    reaction.row_count,
                    reaction.rows.len()
                );
            }
            for row in &reaction.rows {
                let (canonical_bytes, _) = canonical_row(row.value.clone())?;
                let actual_row_hash = hex::encode(Sha256::digest(&canonical_bytes));
                if row.sha256 != actual_row_hash {
                    anyhow::bail!(
                        "final-state golden '{}' reaction '{}' contains an invalid row hash: expected {}, calculated {}",
                        path.display(),
                        reaction_id,
                        row.sha256,
                        actual_row_hash
                    );
                }
            }
            let actual_aggregate = aggregate_row_hash(&reaction.rows)?;
            if reaction.sha256 != actual_aggregate {
                anyhow::bail!(
                    "final-state golden '{}' reaction '{}' contains an invalid aggregate hash: expected {}, calculated {}",
                    path.display(),
                    reaction_id,
                    reaction.sha256,
                    actual_aggregate
                );
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct FinalStateReaction {
    mode: String,
    sha256: String,
    row_count: usize,
    #[serde(default)]
    change_count: u64,
    #[serde(default)]
    duplicate_count: u64,
    #[serde(default)]
    missing_before_count: u64,
    #[serde(default)]
    skipped_record_count: u64,
    #[serde(default)]
    normalization_error_count: usize,
    #[serde(default)]
    normalization_errors: Vec<String>,
    #[serde(default)]
    query_ids: Vec<String>,
    rows: Vec<FinalStateRow>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct FinalStateRow {
    query_id: String,
    sha256: String,
    value: Value,
}

#[derive(Debug, Serialize)]
struct FinalStateVerdict {
    mode: &'static str,
    schema_version: u32,
    test_run_id: String,
    diagnostic_limit: usize,
    results: BTreeMap<String, FinalStateReactionVerdict>,
}

#[derive(Debug, Serialize)]
struct FinalStateReactionVerdict {
    passed: bool,
    expected_sha256: Option<String>,
    actual_sha256: Option<String>,
    expected_row_count: Option<usize>,
    actual_row_count: Option<usize>,
    missing_rows: Vec<FinalStateRow>,
    unexpected_rows: Vec<FinalStateRow>,
    missing_row_count: usize,
    unexpected_row_count: usize,
    actual_duplicate_count: Option<u64>,
    actual_missing_before_count: Option<u64>,
}

fn compare_final_state(
    expected: &FinalStateManifest,
    actual: &FinalStateManifest,
    diagnostic_limit: usize,
    test_run_id: &str,
) -> FinalStateVerdict {
    let reaction_ids: BTreeSet<&String> = expected
        .reactions
        .keys()
        .chain(actual.reactions.keys())
        .collect();
    let mut results = BTreeMap::new();

    for reaction_id in reaction_ids {
        let expected_reaction = expected.reactions.get(reaction_id);
        let actual_reaction = actual.reactions.get(reaction_id);
        let expected_rows = rows_by_identity(expected_reaction);
        let actual_rows = rows_by_identity(actual_reaction);
        let missing_hashes: Vec<&String> = expected_rows
            .keys()
            .filter(|hash| !actual_rows.contains_key(*hash))
            .collect();
        let unexpected_hashes: Vec<&String> = actual_rows
            .keys()
            .filter(|hash| !expected_rows.contains_key(*hash))
            .collect();
        let passed = expected_reaction
            .zip(actual_reaction)
            .is_some_and(|(expected, actual)| {
                expected.sha256 == actual.sha256
                    && expected.row_count == actual.row_count
                    && missing_hashes.is_empty()
                    && unexpected_hashes.is_empty()
            });

        results.insert(
            reaction_id.clone(),
            FinalStateReactionVerdict {
                passed,
                expected_sha256: expected_reaction.map(|value| value.sha256.clone()),
                actual_sha256: actual_reaction.map(|value| value.sha256.clone()),
                expected_row_count: expected_reaction.map(|value| value.row_count),
                actual_row_count: actual_reaction.map(|value| value.row_count),
                missing_rows: missing_hashes
                    .iter()
                    .take(diagnostic_limit)
                    .filter_map(|hash| expected_rows.get(*hash).cloned())
                    .collect(),
                unexpected_rows: unexpected_hashes
                    .iter()
                    .take(diagnostic_limit)
                    .filter_map(|hash| actual_rows.get(*hash).cloned())
                    .collect(),
                missing_row_count: missing_hashes.len(),
                unexpected_row_count: unexpected_hashes.len(),
                actual_duplicate_count: actual_reaction.map(|value| value.duplicate_count),
                actual_missing_before_count: actual_reaction
                    .map(|value| value.missing_before_count),
            },
        );
    }

    FinalStateVerdict {
        mode: "final_state",
        schema_version: FINAL_STATE_SCHEMA_VERSION,
        test_run_id: test_run_id.to_string(),
        diagnostic_limit,
        results,
    }
}

fn rows_by_identity(reaction: Option<&FinalStateReaction>) -> BTreeMap<String, FinalStateRow> {
    reaction
        .map(|reaction| {
            reaction
                .rows
                .iter()
                .cloned()
                .map(|row| (format!("{}\0{}", row.query_id, row.sha256), row))
                .collect()
        })
        .unwrap_or_default()
}

fn aggregate_row_hash(rows: &[FinalStateRow]) -> anyhow::Result<String> {
    let mut canonical_rows = rows
        .iter()
        .map(|row| {
            let (canonical, _) = canonical_row(row.value.clone())?;
            Ok((row.query_id.as_str(), canonical, row.sha256.as_str()))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    canonical_rows.sort_by(|(left_query, left, _), (right_query, right, _)| {
        left_query.cmp(right_query).then_with(|| left.cmp(right))
    });

    let mut aggregate = Sha256::new();
    for (query_id, _, row_hash) in canonical_rows {
        aggregate.update(query_id.as_bytes());
        aggregate.update(b"\0");
        aggregate.update(row_hash.as_bytes());
        aggregate.update(b"\n");
    }
    Ok(hex::encode(aggregate.finalize()))
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
                    summary: Some(json!({
                        "mode": "ordered_stream",
                        "sha256": s,
                        "record_count": 1
                    })),
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

    fn config(
        expected: BTreeMap<String, String>,
        policy: MissingBaselinePolicy,
    ) -> Sha256DeterminismHandlerConfig {
        Sha256DeterminismHandlerConfig {
            expected,
            missing_baseline: policy,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn passes_when_all_reactions_match_expected() {
        let (store, _tmp) = data_store().await;
        let run = TestRunId::new("repo", "test", "run");
        let expected = BTreeMap::from([("r1".to_string(), "aa".to_string())]);
        let handler = Sha256DeterminismCompletionHandler::new(
            &config(expected, MissingBaselinePolicy::Warn),
            store,
            run.clone(),
        );
        handler
            .handle_completion(
                &run.to_string(),
                &summary_with(vec![("r1", Some("aa"))], &run),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fails_when_actual_differs_from_expected() {
        let (store, _tmp) = data_store().await;
        let run = TestRunId::new("repo", "test", "run");
        let expected = BTreeMap::from([("r1".to_string(), "aa".to_string())]);
        let handler = Sha256DeterminismCompletionHandler::new(
            &config(expected, MissingBaselinePolicy::Warn),
            store,
            run.clone(),
        );
        let error = handler
            .handle_completion(
                &run.to_string(),
                &summary_with(vec![("r1", Some("bb"))], &run),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("r1"));
    }

    #[tokio::test]
    async fn warn_policy_passes_on_missing_baseline() {
        let (store, _tmp) = data_store().await;
        let run = TestRunId::new("repo", "test", "run");
        let handler = Sha256DeterminismCompletionHandler::new(
            &config(BTreeMap::new(), MissingBaselinePolicy::Warn),
            store,
            run.clone(),
        );
        handler
            .handle_completion(
                &run.to_string(),
                &summary_with(vec![("r1", Some("aa"))], &run),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fail_policy_errors_on_missing_baseline() {
        let (store, _tmp) = data_store().await;
        let run = TestRunId::new("repo", "test", "run");
        let handler = Sha256DeterminismCompletionHandler::new(
            &config(BTreeMap::new(), MissingBaselinePolicy::Fail),
            store,
            run.clone(),
        );
        let error = handler
            .handle_completion(
                &run.to_string(),
                &summary_with(vec![("r1", Some("aa"))], &run),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("missing baseline"));
    }

    #[test]
    fn final_state_comparison_localizes_rows_and_limits_diagnostics() {
        let expected = manifest(vec![
            row("q1", "a", json!({"id": 1})),
            row("q1", "b", json!({"id": 2})),
            row("q1", "c", json!({"id": 3})),
        ]);
        let actual = manifest(vec![
            row("q1", "a", json!({"id": 1})),
            row("q1", "x", json!({"id": 20})),
            row("q1", "y", json!({"id": 30})),
        ]);

        let verdict = compare_final_state(&expected, &actual, 1, "repo.test.run");
        let reaction = &verdict.results["reaction"];
        assert!(!reaction.passed);
        assert_eq!(reaction.missing_row_count, 2);
        assert_eq!(reaction.unexpected_row_count, 2);
        assert_eq!(reaction.missing_rows.len(), 1);
        assert_eq!(reaction.unexpected_rows.len(), 1);
        assert_eq!(reaction.missing_rows[0].value, json!({"id": 2}));
        assert_eq!(reaction.unexpected_rows[0].value, json!({"id": 20}));
    }

    #[test]
    fn final_state_comparison_ignores_row_order() {
        let expected = manifest(vec![
            row("q1", "a", json!({"id": 1})),
            row("q1", "b", json!({"id": 2})),
        ]);
        let actual = manifest(vec![
            row("q1", "b", json!({"id": 2})),
            row("q1", "a", json!({"id": 1})),
        ]);

        assert!(
            compare_final_state(&expected, &actual, 20, "repo.test.run").results["reaction"].passed
        );
    }

    fn row(query_id: &str, sha256: &str, value: Value) -> FinalStateRow {
        FinalStateRow {
            query_id: query_id.to_string(),
            sha256: sha256.to_string(),
            value,
        }
    }

    fn manifest(rows: Vec<FinalStateRow>) -> FinalStateManifest {
        let row_count = rows.len();
        let sha256 = aggregate_row_hash(&rows).unwrap();
        FinalStateManifest {
            schema_version: FINAL_STATE_SCHEMA_VERSION,
            reactions: BTreeMap::from([(
                "reaction".to_string(),
                FinalStateReaction {
                    mode: "final_state".to_string(),
                    sha256,
                    row_count,
                    change_count: 0,
                    duplicate_count: 0,
                    missing_before_count: 0,
                    skipped_record_count: 0,
                    normalization_error_count: 0,
                    normalization_errors: Vec::new(),
                    query_ids: vec!["q1".to_string()],
                    rows,
                },
            )]),
        }
    }
}
