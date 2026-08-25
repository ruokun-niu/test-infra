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

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::common::{HandlerPayload, HandlerRecord};

use super::determinism_hash_logger::sort_json_keys;

#[derive(Debug)]
enum RowChange {
    Add(Value),
    Update { before: Value, after: Value },
    Delete(Value),
}

#[derive(Debug, Default)]
pub(super) struct FinalStateMaterializer {
    rows: BTreeMap<(String, Vec<u8>), MaterializedRow>,
    key_fields: Vec<String>,
    query_ids: BTreeSet<String>,
    change_count: u64,
    duplicate_count: u64,
    upsert_count: u64,
    missing_before_count: u64,
    skipped_record_count: u64,
    normalization_error_count: usize,
    normalization_errors: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(super) struct FinalStateSummary {
    pub mode: &'static str,
    pub sha256: String,
    pub row_count: usize,
    pub change_count: u64,
    pub duplicate_count: u64,
    pub upsert_count: u64,
    pub missing_before_count: u64,
    pub skipped_record_count: u64,
    pub normalization_error_count: usize,
    pub normalization_errors: Vec<String>,
    pub key_fields: Vec<String>,
    pub query_ids: Vec<String>,
    pub rows: Vec<FinalStateRow>,
}

#[derive(Debug, Serialize)]
pub(super) struct FinalStateRow {
    pub query_id: String,
    pub sha256: String,
    pub value: Value,
}

impl FinalStateMaterializer {
    pub fn new(key_fields: Vec<String>) -> Self {
        Self {
            key_fields,
            ..Default::default()
        }
    }

    pub fn apply_record(&mut self, record: &HandlerRecord) -> anyhow::Result<()> {
        let (query_id, payload) = match &record.payload {
            HandlerPayload::ReactionInvocation {
                query_id,
                request_body,
                ..
            } => (query_id.as_str(), request_body),
            HandlerPayload::ReactionOutput { reaction_output } => ("", reaction_output),
            HandlerPayload::ResultStream { query_result } => {
                let value = serde_json::to_value(query_result)?;
                return self.apply_payload("", &value);
            }
        };
        self.apply_payload(query_id, payload)
    }

    fn apply_payload(&mut self, fallback_query_id: &str, payload: &Value) -> anyhow::Result<()> {
        let query_id = payload
            .get("query_id")
            .or_else(|| payload.get("queryId"))
            .and_then(Value::as_str)
            .unwrap_or(fallback_query_id);
        let changes = match normalize_changes(payload) {
            Ok(changes) => changes,
            Err(error) => {
                self.normalization_error_count += 1;
                if self.normalization_errors.len() < 20 {
                    self.normalization_errors.push(error.to_string());
                }
                return Ok(());
            }
        };
        if changes.is_empty() {
            self.skipped_record_count += 1;
            return Ok(());
        }
        let query_id = if query_id.is_empty() || query_id == "unknown" {
            "unknown"
        } else {
            query_id
        };
        self.query_ids.insert(query_id.to_string());
        for change in changes {
            self.apply_change(query_id, change)?;
        }
        Ok(())
    }

    fn apply_change(&mut self, query_id: &str, change: RowChange) -> anyhow::Result<()> {
        self.change_count += 1;
        match change {
            RowChange::Add(after) => {
                let row = materialized_row(after, &self.key_fields)?;
                if let Some(previous) = self
                    .rows
                    .insert((query_id.to_string(), row.identity.clone()), row.clone())
                {
                    if previous.canonical == row.canonical {
                        self.duplicate_count += 1;
                    } else {
                        self.upsert_count += 1;
                    }
                }
            }
            RowChange::Update { before, after } => {
                let before = materialized_row(before, &self.key_fields)?;
                let after = materialized_row(after, &self.key_fields)?;
                let before_key = (query_id.to_string(), before.identity);
                let after_key = (query_id.to_string(), after.identity.clone());
                if self.rows.remove(&before_key).is_none() {
                    if self.rows.contains_key(&after_key) {
                        self.duplicate_count += 1;
                        return Ok(());
                    }
                    self.missing_before_count += 1;
                }
                self.rows.insert(after_key, after);
            }
            RowChange::Delete(before) => {
                let row = materialized_row(before, &self.key_fields)?;
                if self
                    .rows
                    .remove(&(query_id.to_string(), row.identity))
                    .is_none()
                {
                    self.duplicate_count += 1;
                }
            }
        }
        Ok(())
    }

    pub fn summary(&self) -> anyhow::Result<FinalStateSummary> {
        let mut aggregate = Sha256::new();
        let mut rows = Vec::with_capacity(self.rows.len());
        for ((query_id, _), row) in &self.rows {
            let row_sha = hex::encode(Sha256::digest(&row.canonical));
            aggregate.update(query_id.as_bytes());
            aggregate.update(b"\0");
            aggregate.update(row_sha.as_bytes());
            aggregate.update(b"\n");
            rows.push(FinalStateRow {
                query_id: query_id.clone(),
                sha256: row_sha,
                value: row.value.clone(),
            });
        }

        Ok(FinalStateSummary {
            mode: "final_state",
            sha256: hex::encode(aggregate.finalize()),
            row_count: rows.len(),
            change_count: self.change_count,
            duplicate_count: self.duplicate_count,
            upsert_count: self.upsert_count,
            missing_before_count: self.missing_before_count,
            skipped_record_count: self.skipped_record_count,
            normalization_error_count: self.normalization_error_count,
            normalization_errors: self.normalization_errors.clone(),
            key_fields: self.key_fields.clone(),
            query_ids: self.query_ids.iter().cloned().collect(),
            rows,
        })
    }
}

fn normalize_changes(payload: &Value) -> anyhow::Result<Vec<RowChange>> {
    let Some(object) = payload.as_object() else {
        anyhow::bail!("final-state payload must be a JSON object");
    };

    if object.get("kind").and_then(Value::as_str) == Some("control") {
        return Ok(Vec::new());
    }
    if let Some(result) = object.get("result") {
        return Ok(vec![normalize_change(result)?]);
    }
    if let Some(results) = object.get("results").and_then(Value::as_array) {
        return results.iter().map(normalize_change).collect();
    }

    let mut changes = Vec::new();
    if let Some(added) = object.get("addedResults").and_then(Value::as_array) {
        changes.extend(added.iter().cloned().map(RowChange::Add));
    }
    if let Some(updated) = object.get("updatedResults").and_then(Value::as_array) {
        for update in updated {
            changes.push(normalize_change(update)?);
        }
    }
    if let Some(deleted) = object.get("deletedResults").and_then(Value::as_array) {
        changes.extend(deleted.iter().cloned().map(RowChange::Delete));
    }
    if !changes.is_empty()
        || object.contains_key("addedResults")
        || object.contains_key("updatedResults")
        || object.contains_key("deletedResults")
    {
        return Ok(changes);
    }

    if let Some(request_body) = object.get("request_body") {
        let operation = request_body
            .get("type")
            .or_else(|| request_body.get("operation"))
            .and_then(Value::as_str)
            .or_else(|| object.get("reaction_type").and_then(Value::as_str));
        return Ok(vec![normalize_change_with_operation(
            request_body,
            operation,
        )?]);
    }

    if has_change_shape(payload) {
        return Ok(vec![normalize_change(payload)?]);
    }

    anyhow::bail!("unsupported final-state reaction payload shape")
}

fn normalize_change(value: &Value) -> anyhow::Result<RowChange> {
    let operation = value
        .get("type")
        .or_else(|| value.get("operation"))
        .and_then(Value::as_str);
    normalize_change_with_operation(value, operation)
}

fn normalize_change_with_operation(
    value: &Value,
    operation: Option<&str>,
) -> anyhow::Result<RowChange> {
    let before = value.get("before").filter(|v| !v.is_null()).cloned();
    let after = value.get("after").filter(|v| !v.is_null()).cloned();
    let operation = operation.map(|op| op.to_ascii_lowercase());

    match operation.as_deref() {
        Some("add" | "added" | "insert") => after
            .map(RowChange::Add)
            .ok_or_else(|| anyhow::anyhow!("add change is missing an 'after' row")),
        Some("update" | "updated") => match (before, after) {
            (Some(before), Some(after)) => Ok(RowChange::Update { before, after }),
            _ => anyhow::bail!("update change requires both 'before' and 'after' rows"),
        },
        Some("delete" | "deleted") => before
            .map(RowChange::Delete)
            .ok_or_else(|| anyhow::anyhow!("delete change is missing a 'before' row")),
        Some(other) => anyhow::bail!("unsupported final-state operation '{other}'"),
        None => match (before, after) {
            (None, Some(after)) => Ok(RowChange::Add(after)),
            (Some(before), Some(after)) => Ok(RowChange::Update { before, after }),
            (Some(before), None) => Ok(RowChange::Delete(before)),
            (None, None) => anyhow::bail!("change has neither a 'before' nor an 'after' row"),
        },
    }
}

fn has_change_shape(value: &Value) -> bool {
    value.get("type").is_some()
        || value.get("operation").is_some()
        || value.get("before").is_some()
        || value.get("after").is_some()
}

pub(crate) fn canonical_row(value: Value) -> anyhow::Result<(Vec<u8>, Value)> {
    let value = sort_json_keys(normalize_json_numbers(value));
    let bytes = serde_json::to_vec(&value)?;
    Ok((bytes, value))
}

#[derive(Clone, Debug)]
struct MaterializedRow {
    identity: Vec<u8>,
    canonical: Vec<u8>,
    value: Value,
}

fn materialized_row(value: Value, key_fields: &[String]) -> anyhow::Result<MaterializedRow> {
    let (canonical, value) = canonical_row(value)?;
    let identity = if key_fields.is_empty() {
        canonical.clone()
    } else {
        let object = value.as_object().ok_or_else(|| {
            anyhow::anyhow!("key_fields require each final-state row to be a JSON object")
        })?;
        let mut key = serde_json::Map::new();
        for field in key_fields {
            let field_value = object.get(field).ok_or_else(|| {
                anyhow::anyhow!("final-state row is missing configured key field '{field}'")
            })?;
            key.insert(field.clone(), field_value.clone());
        }
        serde_json::to_vec(&Value::Object(key))?
    };
    Ok(MaterializedRow {
        identity,
        canonical,
        value,
    })
}

fn normalize_json_numbers(value: Value) -> Value {
    const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

    match value {
        Value::Number(number) if number.is_f64() => {
            let Some(value) = number.as_f64() else {
                return Value::Number(number);
            };
            if value.fract() != 0.0 || !(-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&value) {
                return Value::Number(number);
            }
            if value >= 0.0 {
                Value::Number(serde_json::Number::from(value as u64))
            } else {
                Value::Number(serde_json::Number::from(value as i64))
            }
        }
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, value)| (key, normalize_json_numbers(value)))
                .collect(),
        ),
        Value::Array(values) => {
            Value::Array(values.into_iter().map(normalize_json_numbers).collect())
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn apply(materializer: &mut FinalStateMaterializer, payload: Value) {
        materializer.apply_payload("", &payload).unwrap();
    }

    #[test]
    fn materializes_add_update_delete() {
        let mut materializer = FinalStateMaterializer::default();
        apply(
            &mut materializer,
            json!({"result": {"type": "ADD", "after": {"id": 1, "value": "a"}}}),
        );
        apply(
            &mut materializer,
            json!({"result": {
                "type": "UPDATE",
                "before": {"id": 1, "value": "a"},
                "after": {"id": 1, "value": "b"}
            }}),
        );
        apply(
            &mut materializer,
            json!({"result": {"type": "ADD", "after": {"id": 2}}}),
        );
        apply(
            &mut materializer,
            json!({"result": {"type": "DELETE", "before": {"id": 2}}}),
        );

        let summary = materializer.summary().unwrap();
        assert_eq!(summary.row_count, 1);
        assert_eq!(summary.rows[0].value, json!({"id": 1, "value": "b"}));
    }

    #[test]
    fn grpc_and_http_shapes_produce_same_state() {
        let mut grpc = FinalStateMaterializer::default();
        apply(
            &mut grpc,
            json!({"query_id": "q1", "result": {
                "type": "ADD",
                "after": {"id": 1.0, "value": "ok"}
            }}),
        );

        let mut http = FinalStateMaterializer::default();
        apply(
            &mut http,
            json!({
                "query_id": "q1",
                "reaction_type": "added",
                "request_body": {"after": {"value": "ok", "id": 1}}
            }),
        );

        assert_eq!(
            grpc.summary().unwrap().sha256,
            http.summary().unwrap().sha256
        );
    }

    #[test]
    fn keyed_adds_replace_prior_aggregate_snapshots() {
        let mut materializer = FinalStateMaterializer::new(vec!["FloorId".to_string()]);
        apply(
            &mut materializer,
            json!({"result": {"type": "ADD", "after": {
                "FloorId": "F1", "RoomCount": 1
            }}}),
        );
        apply(
            &mut materializer,
            json!({"result": {"type": "ADD", "after": {
                "FloorId": "F1", "RoomCount": 2
            }}}),
        );

        let summary = materializer.summary().unwrap();
        assert_eq!(summary.row_count, 1);
        assert_eq!(summary.upsert_count, 1);
        assert_eq!(summary.duplicate_count, 0);
        assert_eq!(
            summary.rows[0].value,
            json!({"FloorId": "F1", "RoomCount": 2})
        );
    }

    #[test]
    fn keyed_grpc_snapshots_match_http_updates() {
        let key_fields = vec!["FloorId".to_string()];
        let mut grpc = FinalStateMaterializer::new(key_fields.clone());
        apply(
            &mut grpc,
            json!({"result": {"type": "ADD", "after": {
                "FloorId": "F1", "RoomCount": 1.0
            }}}),
        );
        apply(
            &mut grpc,
            json!({"result": {"type": "ADD", "after": {
                "FloorId": "F1", "RoomCount": 2.0
            }}}),
        );

        let mut http = FinalStateMaterializer::new(key_fields);
        apply(
            &mut http,
            json!({"reaction_type": "added", "request_body": {
                "operation": "ADD",
                "after": {"FloorId": "F1", "RoomCount": 1}
            }}),
        );
        apply(
            &mut http,
            json!({"reaction_type": "updated", "request_body": {
                "operation": "UPDATE",
                "before": {"FloorId": "F1", "RoomCount": 1},
                "after": {"FloorId": "F1", "RoomCount": 2}
            }}),
        );

        let grpc = grpc.summary().unwrap();
        let http = http.summary().unwrap();
        assert_eq!(grpc.sha256, http.sha256);
        assert_eq!(grpc.rows[0].value, http.rows[0].value);
    }

    #[test]
    fn records_normalization_errors_instead_of_silently_dropping_them() {
        let mut materializer = FinalStateMaterializer::default();
        apply(
            &mut materializer,
            json!({"result": {"type": "UNKNOWN", "after": {"id": 1}}}),
        );

        let summary = materializer.summary().unwrap();
        assert_eq!(summary.normalization_error_count, 1);
        assert_eq!(summary.row_count, 0);
        assert!(summary.normalization_errors[0].contains("unsupported"));
    }

    #[test]
    fn skips_control_records_without_treating_them_as_errors() {
        let mut materializer = FinalStateMaterializer::default();
        apply(
            &mut materializer,
            json!({"kind": "control", "queryId": "q1", "controlSignal": {"kind": "running"}}),
        );

        let summary = materializer.summary().unwrap();
        assert_eq!(summary.normalization_error_count, 0);
        assert_eq!(summary.skipped_record_count, 1);
    }

    #[test]
    fn independent_row_order_does_not_change_hash() {
        let mut first = FinalStateMaterializer::default();
        let mut second = FinalStateMaterializer::default();
        apply(
            &mut first,
            json!({"results": [
                {"type": "ADD", "after": {"id": 1}},
                {"type": "ADD", "after": {"id": 2}}
            ]}),
        );
        apply(
            &mut second,
            json!({"results": [
                {"type": "ADD", "after": {"id": 2}},
                {"type": "ADD", "after": {"id": 1}}
            ]}),
        );

        assert_eq!(
            first.summary().unwrap().sha256,
            second.summary().unwrap().sha256
        );
    }

    #[test]
    fn duplicate_replay_is_idempotent() {
        let mut materializer = FinalStateMaterializer::default();
        let add = json!({"result": {"type": "ADD", "after": {"id": 1}}});
        apply(&mut materializer, add.clone());
        apply(&mut materializer, add);

        let summary = materializer.summary().unwrap();
        assert_eq!(summary.row_count, 1);
        assert_eq!(summary.duplicate_count, 1);
    }

    #[test]
    fn supports_in_process_change_batches() {
        let mut materializer = FinalStateMaterializer::default();
        apply(
            &mut materializer,
            json!({
                "queryId": "q1",
                "addedResults": [{"id": 1}, {"id": 2}],
                "updatedResults": [{
                    "before": {"id": 2},
                    "after": {"id": 2, "value": "updated"}
                }],
                "deletedResults": [{"id": 1}]
            }),
        );

        let summary = materializer.summary().unwrap();
        assert_eq!(summary.row_count, 1);
        assert_eq!(summary.rows[0].value, json!({"id": 2, "value": "updated"}));
    }

    #[test]
    fn rejects_malformed_changes() {
        let mut materializer = FinalStateMaterializer::default();
        materializer
            .apply_payload(
                "",
                &json!({"result": {"type": "UPDATE", "after": {"id": 1}}}),
            )
            .unwrap();
        let summary = materializer.summary().unwrap();
        assert_eq!(summary.normalization_error_count, 1);
        assert!(summary.normalization_errors[0].contains("requires both"));
    }
}
