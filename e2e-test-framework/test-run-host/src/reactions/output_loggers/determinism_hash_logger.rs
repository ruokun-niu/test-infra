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

//! SHA-256 verification data over reaction output.
//!
//! The default `OrderedStream` mode preserves the historical order-sensitive
//! hash. `FinalState` materializes add/update/delete changes and hashes the
//! sorted set of rows that remain when the reaction stops.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use test_data_store::test_run_storage::TestRunReactionId;

use crate::common::{HandlerPayload, HandlerRecord};

use super::final_state::FinalStateMaterializer;
use super::{OutputLogger, OutputLoggerResult};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeterminismHashMode {
    #[default]
    OrderedStream,
    FinalState,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DeterminismHashOutputLoggerConfig {
    #[serde(default)]
    pub mode: DeterminismHashMode,
}

pub struct DeterminismHashOutputLogger {
    test_run_reaction_id: TestRunReactionId,
    hasher: Sha256,
    mode: DeterminismHashMode,
    final_state: FinalStateMaterializer,
    record_count: u64,
}

impl DeterminismHashOutputLogger {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        test_run_reaction_id: TestRunReactionId,
        config: &DeterminismHashOutputLoggerConfig,
    ) -> anyhow::Result<Box<dyn OutputLogger + Send + Sync>> {
        log::debug!("Creating DeterminismHashOutputLogger for {test_run_reaction_id}");
        Ok(Box::new(Self {
            test_run_reaction_id,
            hasher: Sha256::new(),
            mode: config.mode,
            final_state: FinalStateMaterializer::default(),
            record_count: 0,
        }))
    }
}

#[async_trait]
impl OutputLogger for DeterminismHashOutputLogger {
    async fn end_test_run(&mut self) -> anyhow::Result<OutputLoggerResult> {
        if self.mode == DeterminismHashMode::FinalState {
            let summary = self.final_state.summary()?;
            log::info!(
                "DeterminismHashOutputLogger finalised final state for {}: sha256={} row_count={}",
                self.test_run_reaction_id,
                summary.sha256,
                summary.row_count
            );
            return Ok(OutputLoggerResult {
                has_output: true,
                logger_name: "DeterminismHash".to_string(),
                output_folder_path: None,
                summary: Some(serde_json::to_value(summary)?),
            });
        }

        let digest = std::mem::replace(&mut self.hasher, Sha256::new()).finalize();
        let hex_sha = hex::encode(digest);
        log::info!(
            "DeterminismHashOutputLogger finalised for {}: sha256={} record_count={}",
            self.test_run_reaction_id,
            hex_sha,
            self.record_count
        );
        Ok(OutputLoggerResult {
            has_output: true,
            logger_name: "DeterminismHash".to_string(),
            output_folder_path: None,
            summary: Some(json!({
                "mode": "ordered_stream",
                "sha256": hex_sha,
                "record_count": self.record_count,
            })),
        })
    }

    async fn log_handler_record(&mut self, record: &HandlerRecord) -> anyhow::Result<()> {
        if self.mode == DeterminismHashMode::FinalState {
            self.final_state.apply_record(record)?;
            self.record_count += 1;
            return Ok(());
        }

        // Skip empty-results heartbeats: drasi-lib re-evaluations that coalesce
        // to zero rows still produce a HandlerRecord (`{query_id, results: []}`),
        // and how many of those land between two real records is decided by
        // tokio's scheduler. Hashing them makes the SHA host-dependent. Real
        // records use the singular `result` key, so the filter is precise.
        let Some(canonical) = canonical_payload_bytes(record)? else {
            return Ok(());
        };
        self.hasher.update(&canonical);
        self.hasher.update(b"\n");
        self.record_count += 1;
        Ok(())
    }
}

/// Project a `HandlerRecord` to the canonical payload bytes that participate
/// in the determinism hash, or `Ok(None)` to skip the record entirely.
///
/// Matches the historical bash pipeline:
///   - `ReactionInvocation` -> `request_body`
///   - `ReactionOutput`     -> `reaction_output`
///   - anything else        -> the entire `payload` object (serde-tagged form)
///
/// The chosen value is re-serialised to compact JSON with recursively sorted
/// keys (equivalent of `jq -cS`) so logically identical objects always hash
/// to the same bytes regardless of in-memory field order. Volatile keys
/// (see [`VOLATILE_KEYS`]) are stripped first so wall-clock timestamps and
/// per-emission sequence ids do not make the hash run-dependent.
///
/// Records whose projected payload is an object containing `results: []`
/// (the drasi-lib in-process "empty re-evaluation" heartbeat shape) are
/// excluded from the hash — see the comment in `log_handler_record`.
pub(crate) fn canonical_payload_bytes(record: &HandlerRecord) -> anyhow::Result<Option<Vec<u8>>> {
    let projected: Value = match &record.payload {
        HandlerPayload::ReactionInvocation { request_body, .. } => request_body.clone(),
        HandlerPayload::ReactionOutput { reaction_output } => reaction_output.clone(),
        _ => serde_json::to_value(&record.payload)?,
    };
    if is_empty_results_heartbeat(&projected) {
        return Ok(None);
    }
    let canonical = sort_json_keys(strip_volatile_keys(projected));
    Ok(Some(serde_json::to_vec(&canonical)?))
}

/// Keys whose values are not stable across runs and therefore must not
/// participate in the determinism hash:
///   - `timestamp`  — the wall-clock time of the originating query emission.
///   - `sequenceId` — the monotonic per-emission sequence id.
///
/// Reaction payloads that forward the raw Drasi notification verbatim carry
/// these fields — notably the HTTP `DefaultChangeNotification` envelope. The
/// gRPC converter already projects items down to `{type, before, after}` and
/// never emits them, so stripping here keeps the two transports comparable
/// and makes the HTTP hash stable run-to-run.
const VOLATILE_KEYS: &[&str] = &["timestamp", "sequenceId"];

/// Recursively remove [`VOLATILE_KEYS`] from every object in `value`.
pub(crate) fn strip_volatile_keys(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .filter(|(k, _)| !VOLATILE_KEYS.contains(&k.as_str()))
                .map(|(k, v)| (k, strip_volatile_keys(v)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.into_iter().map(strip_volatile_keys).collect()),
        other => other,
    }
}

/// True for the in-process drasi-lib heartbeat shape
/// `{ "query_id": ..., "results": [] }` (and any superset with `results: []`).
/// HTTP/gRPC reaction payloads use different field names
/// (`addedResults`, `updatedResults`, ...) so they are never matched.
fn is_empty_results_heartbeat(value: &Value) -> bool {
    match value {
        Value::Object(map) => match map.get("results") {
            Some(Value::Array(items)) => items.is_empty(),
            _ => false,
        },
        _ => false,
    }
}

pub(crate) fn sort_json_keys(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut sorted: std::collections::BTreeMap<String, Value> =
                std::collections::BTreeMap::new();
            for (k, v) in map {
                sorted.insert(k, sort_json_keys(v));
            }
            Value::Object(sorted.into_iter().collect())
        }
        Value::Array(items) => Value::Array(items.into_iter().map(sort_json_keys).collect()),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{HandlerPayload, HandlerRecord};
    use serde_json::json;
    use test_data_store::test_run_storage::{TestRunId, TestRunReactionId};

    fn make_record(seq: u64, payload: HandlerPayload) -> HandlerRecord {
        HandlerRecord {
            id: format!("rec-{seq}"),
            sequence: seq,
            created_time_ns: seq * 1_000_000,
            processed_time_ns: seq * 1_000_000 + 500,
            traceparent: None,
            tracestate: None,
            payload,
        }
    }

    #[test]
    fn canonical_payload_uses_request_body_for_invocations() {
        let rec = make_record(
            1,
            HandlerPayload::ReactionInvocation {
                reaction_type: "http".into(),
                query_id: "q1".into(),
                request_method: "POST".into(),
                request_path: "/r".into(),
                request_body: json!({ "b": 2, "a": 1 }),
                headers: Default::default(),
            },
        );
        let bytes = canonical_payload_bytes(&rec).unwrap().unwrap();
        // Keys recursively sorted -> a before b.
        assert_eq!(std::str::from_utf8(&bytes).unwrap(), r#"{"a":1,"b":2}"#);
    }

    #[test]
    fn canonical_payload_uses_reaction_output_for_outputs() {
        let rec = make_record(
            2,
            HandlerPayload::ReactionOutput {
                reaction_output: json!({ "z": [3, 2, 1], "a": { "y": 1, "x": 2 } }),
            },
        );
        let bytes = canonical_payload_bytes(&rec).unwrap().unwrap();
        // Array order is preserved; nested object keys are sorted.
        assert_eq!(
            std::str::from_utf8(&bytes).unwrap(),
            r#"{"a":{"x":2,"y":1},"z":[3,2,1]}"#
        );
    }

    #[test]
    fn canonical_payload_skips_empty_results_heartbeat() {
        let rec = make_record(
            3,
            HandlerPayload::ReactionOutput {
                reaction_output: json!({ "query_id": "q1", "results": [] }),
            },
        );
        assert!(canonical_payload_bytes(&rec).unwrap().is_none());
    }

    #[test]
    fn canonical_payload_keeps_non_empty_results() {
        let rec = make_record(
            4,
            HandlerPayload::ReactionOutput {
                reaction_output: json!({ "query_id": "q1", "results": [{"a": 1}] }),
            },
        );
        assert!(canonical_payload_bytes(&rec).unwrap().is_some());
    }

    #[test]
    fn canonical_payload_strips_volatile_keys() {
        // Mirrors the HTTP `DefaultChangeNotification` envelope the handler
        // forwards verbatim: only `operation`/`before`/`after` are stable,
        // while `timestamp` and `sequenceId` vary run-to-run.
        let with_volatile = make_record(
            5,
            HandlerPayload::ReactionInvocation {
                reaction_type: "http".into(),
                query_id: "q1".into(),
                request_method: "POST".into(),
                request_path: "/reaction".into(),
                request_body: json!({
                    "operation": "ADD",
                    "queryId": "q1",
                    "sequenceId": 42,
                    "timestamp": "2026-01-01T00:00:00+00:00",
                    "after": { "id": 1 }
                }),
                headers: Default::default(),
            },
        );
        let without_volatile = make_record(
            6,
            HandlerPayload::ReactionInvocation {
                reaction_type: "http".into(),
                query_id: "q1".into(),
                request_method: "POST".into(),
                request_path: "/reaction".into(),
                request_body: json!({
                    "operation": "ADD",
                    "queryId": "q1",
                    "sequenceId": 99,
                    "timestamp": "2027-06-06T12:34:56+00:00",
                    "after": { "id": 1 }
                }),
                headers: Default::default(),
            },
        );
        let a = canonical_payload_bytes(&with_volatile).unwrap().unwrap();
        let b = canonical_payload_bytes(&without_volatile).unwrap().unwrap();
        // Differing timestamp / sequenceId must not change the hashed bytes.
        assert_eq!(a, b);
        assert_eq!(
            std::str::from_utf8(&a).unwrap(),
            r#"{"after":{"id":1},"operation":"ADD","queryId":"q1"}"#
        );
    }

    #[tokio::test]
    async fn streaming_hash_ignores_interleaved_heartbeats() {
        let test_run_id = TestRunId::new("repo", "test", "run");
        let reaction_id = TestRunReactionId::new(&test_run_id, "r");

        let data1 = make_record(
            0,
            HandlerPayload::ReactionOutput {
                reaction_output: json!({ "query_id": "q1", "result": { "v": 1 } }),
            },
        );
        let data2 = make_record(
            1,
            HandlerPayload::ReactionOutput {
                reaction_output: json!({ "query_id": "q1", "result": { "v": 2 } }),
            },
        );
        let beat = make_record(
            2,
            HandlerPayload::ReactionOutput {
                reaction_output: json!({ "query_id": "q1", "results": [] }),
            },
        );

        // Stream A: just the two data records.
        let mut a =
            DeterminismHashOutputLogger::new(reaction_id.clone(), &Default::default()).unwrap();
        a.log_handler_record(&data1).await.unwrap();
        a.log_handler_record(&data2).await.unwrap();
        let a_sum = a.end_test_run().await.unwrap();

        // Stream B: same two data records with five heartbeats sprinkled in.
        let mut b = DeterminismHashOutputLogger::new(reaction_id, &Default::default()).unwrap();
        b.log_handler_record(&beat).await.unwrap();
        b.log_handler_record(&data1).await.unwrap();
        b.log_handler_record(&beat).await.unwrap();
        b.log_handler_record(&beat).await.unwrap();
        b.log_handler_record(&data2).await.unwrap();
        b.log_handler_record(&beat).await.unwrap();
        b.log_handler_record(&beat).await.unwrap();
        let b_sum = b.end_test_run().await.unwrap();

        assert_eq!(a_sum.summary, b_sum.summary);
        assert_eq!(
            a_sum.summary.as_ref().unwrap()["record_count"].as_u64(),
            Some(2)
        );
    }

    #[tokio::test]
    async fn streaming_hash_is_stable_for_same_record_sequence() {
        let test_run_id = TestRunId::new("repo", "test", "run");
        let reaction_id = TestRunReactionId::new(&test_run_id, "r");

        let mut logger_a =
            DeterminismHashOutputLogger::new(reaction_id.clone(), &Default::default()).unwrap();
        let mut logger_b =
            DeterminismHashOutputLogger::new(reaction_id, &Default::default()).unwrap();

        let records = vec![
            make_record(
                0,
                HandlerPayload::ReactionOutput {
                    reaction_output: json!({ "value": 1 }),
                },
            ),
            make_record(
                1,
                HandlerPayload::ReactionOutput {
                    reaction_output: json!({ "value": 2 }),
                },
            ),
            make_record(
                2,
                HandlerPayload::ReactionOutput {
                    reaction_output: json!({ "value": 3 }),
                },
            ),
        ];

        for r in &records {
            logger_a.log_handler_record(r).await.unwrap();
            logger_b.log_handler_record(r).await.unwrap();
        }
        let a = logger_a.end_test_run().await.unwrap();
        let b = logger_b.end_test_run().await.unwrap();
        assert_eq!(a.summary, b.summary);
        let sha = a.summary.as_ref().unwrap()["sha256"].as_str().unwrap();
        assert_eq!(sha.len(), 64);
        assert_eq!(
            a.summary.as_ref().unwrap()["record_count"].as_u64(),
            Some(3)
        );
    }

    #[tokio::test]
    async fn streaming_hash_differs_when_record_order_changes() {
        let test_run_id = TestRunId::new("repo", "test", "run");
        let reaction_id = TestRunReactionId::new(&test_run_id, "r");

        let r1 = make_record(
            0,
            HandlerPayload::ReactionOutput {
                reaction_output: json!({ "value": 1 }),
            },
        );
        let r2 = make_record(
            1,
            HandlerPayload::ReactionOutput {
                reaction_output: json!({ "value": 2 }),
            },
        );

        let mut forward =
            DeterminismHashOutputLogger::new(reaction_id.clone(), &Default::default()).unwrap();
        forward.log_handler_record(&r1).await.unwrap();
        forward.log_handler_record(&r2).await.unwrap();
        let f = forward.end_test_run().await.unwrap();

        let mut reverse =
            DeterminismHashOutputLogger::new(reaction_id, &Default::default()).unwrap();
        reverse.log_handler_record(&r2).await.unwrap();
        reverse.log_handler_record(&r1).await.unwrap();
        let r = reverse.end_test_run().await.unwrap();

        assert_ne!(f.summary, r.summary);
    }

    #[tokio::test]
    async fn final_state_hash_ignores_independent_row_order_and_duplicate_replay() {
        let test_run_id = TestRunId::new("repo", "test", "run");
        let reaction_id = TestRunReactionId::new(&test_run_id, "r");
        let config = DeterminismHashOutputLoggerConfig {
            mode: DeterminismHashMode::FinalState,
        };
        let row_one = make_record(
            1,
            HandlerPayload::ReactionInvocation {
                reaction_type: "Grpc".into(),
                query_id: "q1".into(),
                request_method: "POST".into(),
                request_path: "/".into(),
                request_body: json!({
                    "query_id": "q1",
                    "result": {"type": "ADD", "after": {"id": 1}}
                }),
                headers: Default::default(),
            },
        );
        let row_two = make_record(
            2,
            HandlerPayload::ReactionInvocation {
                reaction_type: "Grpc".into(),
                query_id: "q1".into(),
                request_method: "POST".into(),
                request_path: "/".into(),
                request_body: json!({
                    "query_id": "q1",
                    "result": {"type": "ADD", "after": {"id": 2}}
                }),
                headers: Default::default(),
            },
        );

        let mut first = DeterminismHashOutputLogger::new(reaction_id.clone(), &config).unwrap();
        first.log_handler_record(&row_one).await.unwrap();
        first.log_handler_record(&row_two).await.unwrap();
        let first = first.end_test_run().await.unwrap();

        let mut second = DeterminismHashOutputLogger::new(reaction_id, &config).unwrap();
        second.log_handler_record(&row_two).await.unwrap();
        second.log_handler_record(&row_one).await.unwrap();
        second.log_handler_record(&row_one).await.unwrap();
        let second = second.end_test_run().await.unwrap();

        assert_eq!(
            first.summary.as_ref().unwrap()["sha256"],
            second.summary.as_ref().unwrap()["sha256"]
        );
        assert_eq!(second.summary.as_ref().unwrap()["row_count"], 2);
        assert_eq!(second.summary.as_ref().unwrap()["duplicate_count"], 1);
    }
}
