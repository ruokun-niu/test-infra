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

use anyhow::Result;
use prost_types::Struct;
use serde_json::{Map, Value as JsonValue};
use std::collections::BTreeMap;
use test_data_store::scripts::SourceChangeEvent;

// Include the generated Drasi proto code
pub mod drasi {
    pub mod v1 {
        tonic::include_proto!("drasi.v1");
    }
}

use drasi::v1::{
    ChangeType, Element, ElementMetadata, ElementReference, Node, QueryResult, Relation,
    SourceChange,
};

/// Convert a test framework SourceChangeEvent to Drasi SourceChange
pub fn convert_to_drasi_source_change(
    event: &SourceChangeEvent,
    source_id: &str,
) -> Result<SourceChange> {
    let change_type = match event.op.as_str() {
        "i" | "insert" => ChangeType::Insert,
        "u" | "update" => ChangeType::Update,
        "d" | "delete" => ChangeType::Delete,
        _ => ChangeType::Unspecified,
    };

    let timestamp = prost_types::Timestamp {
        seconds: (event.payload.source.ts_ns / 1_000_000_000) as i64,
        nanos: (event.payload.source.ts_ns % 1_000_000_000) as i32,
    };

    let mut source_change = SourceChange {
        r#type: change_type as i32,
        change: None,
        timestamp: Some(timestamp),
        source_id: source_id.to_string(),
    };

    // Handle different change types
    match change_type {
        ChangeType::Delete => {
            // CDC-style deletes carry the prior state in `before`; some test
            // generators put it in `after`. Accept either so we always have
            // labels + id to send drasi-server's metadata-only delete event.
            let obj = event
                .payload
                .before
                .as_object()
                .or_else(|| event.payload.after.as_object());
            if let Some(obj) = obj {
                let element_id = obj
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                let metadata = ElementMetadata {
                    reference: Some(ElementReference {
                        source_id: source_id.to_string(),
                        element_id,
                    }),
                    labels: extract_labels(obj),
                    effective_from: event.payload.source.ts_ns,
                };

                source_change.change = Some(drasi::v1::source_change::Change::Metadata(metadata));
            }
        }
        ChangeType::Insert | ChangeType::Update => {
            // For insert/update, we need the full element
            if let Some(after_obj) = event.payload.after.as_object() {
                let element = create_element_from_json(after_obj, source_id, event)?;
                source_change.change = Some(drasi::v1::source_change::Change::Element(element));
            }
        }
        _ => {}
    }

    Ok(source_change)
}

/// Create an Element from JSON data
fn create_element_from_json(
    obj: &Map<String, JsonValue>,
    source_id: &str,
    event: &SourceChangeEvent,
) -> Result<Element> {
    let element_id = obj
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let labels = extract_labels(obj);
    let properties = extract_properties(obj)?;

    let metadata = ElementMetadata {
        reference: Some(ElementReference {
            source_id: source_id.to_string(),
            element_id: element_id.clone(),
        }),
        labels: labels.clone(),
        effective_from: event.payload.source.ts_ns,
    };

    // Check if this is a relationship (has start/end endpoints). Accept both
    // the camelCase keys used by some clients and the snake_case keys used by
    // the test framework's script source / RelationRecord serialization.
    if let (Some(start_id), Some(end_id)) = (
        obj.get("startId")
            .or_else(|| obj.get("start_id"))
            .and_then(|v| v.as_str()),
        obj.get("endId")
            .or_else(|| obj.get("end_id"))
            .and_then(|v| v.as_str()),
    ) {
        // Optional cross-source endpoints: each side may live in a different
        // source. When the key is absent, fall back to the dispatcher's own
        // source_id (the in-source case).
        let start_source = obj
            .get("startSourceId")
            .or_else(|| obj.get("start_source_id"))
            .and_then(|v| v.as_str())
            .unwrap_or(source_id);
        let end_source = obj
            .get("endSourceId")
            .or_else(|| obj.get("end_source_id"))
            .and_then(|v| v.as_str())
            .unwrap_or(source_id);

        // This is a Relation
        let relation = Relation {
            metadata: Some(metadata),
            in_node: Some(ElementReference {
                source_id: start_source.to_string(),
                element_id: start_id.to_string(),
            }),
            out_node: Some(ElementReference {
                source_id: end_source.to_string(),
                element_id: end_id.to_string(),
            }),
            properties: Some(properties),
        };

        Ok(Element {
            element: Some(drasi::v1::element::Element::Relation(relation)),
        })
    } else {
        // This is a Node
        let node = Node {
            metadata: Some(metadata),
            properties: Some(properties),
        };

        Ok(Element {
            element: Some(drasi::v1::element::Element::Node(node)),
        })
    }
}

/// Extract labels from JSON object
fn extract_labels(obj: &Map<String, JsonValue>) -> Vec<String> {
    obj.get("labels")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

/// Extract properties from JSON object as protobuf Struct
fn extract_properties(obj: &Map<String, JsonValue>) -> Result<Struct> {
    let mut fields = BTreeMap::new();

    // Check if there's a nested "properties" field (common in test data)
    if let Some(JsonValue::Object(props)) = obj.get("properties") {
        // Extract from nested properties object
        for (key, value) in props {
            if let Some(proto_value) = json_to_proto_value(value) {
                fields.insert(key.clone(), proto_value);
            }
        }
    } else {
        // Fall back to extracting from top-level fields (excluding special fields)
        for (key, value) in obj {
            // Skip special fields
            if key == "id"
                || key == "labels"
                || key == "startId"
                || key == "start_id"
                || key == "endId"
                || key == "end_id"
                || key == "startSourceId"
                || key == "start_source_id"
                || key == "endSourceId"
                || key == "end_source_id"
                || key == "properties"
            {
                continue;
            }

            if let Some(proto_value) = json_to_proto_value(value) {
                fields.insert(key.clone(), proto_value);
            }
        }
    }

    Ok(Struct { fields })
}

/// Convert JSON value to protobuf Value
fn json_to_proto_value(json_val: &JsonValue) -> Option<prost_types::Value> {
    match json_val {
        JsonValue::Null => Some(prost_types::Value {
            kind: Some(prost_types::value::Kind::NullValue(0)),
        }),
        JsonValue::Bool(b) => Some(prost_types::Value {
            kind: Some(prost_types::value::Kind::BoolValue(*b)),
        }),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(prost_types::Value {
                    kind: Some(prost_types::value::Kind::NumberValue(i as f64)),
                })
            } else {
                n.as_f64().map(|f| prost_types::Value {
                    kind: Some(prost_types::value::Kind::NumberValue(f)),
                })
            }
        }
        JsonValue::String(s) => Some(prost_types::Value {
            kind: Some(prost_types::value::Kind::StringValue(s.clone())),
        }),
        JsonValue::Array(arr) => {
            let values: Vec<prost_types::Value> =
                arr.iter().filter_map(json_to_proto_value).collect();
            Some(prost_types::Value {
                kind: Some(prost_types::value::Kind::ListValue(
                    prost_types::ListValue { values },
                )),
            })
        }
        JsonValue::Object(obj) => {
            let mut fields = BTreeMap::new();
            for (k, v) in obj {
                if let Some(proto_val) = json_to_proto_value(v) {
                    fields.insert(k.clone(), proto_val);
                }
            }
            Some(prost_types::Value {
                kind: Some(prost_types::value::Kind::StructValue(Struct { fields })),
            })
        }
    }
}

/// Convert Drasi QueryResult to internal reaction format
pub fn convert_from_drasi_query_result(result: QueryResult) -> Result<Vec<JsonValue>> {
    let mut output = Vec::new();

    for item in result.results {
        let mut json_obj = Map::new();

        // Set the change type
        json_obj.insert("type".to_string(), JsonValue::String(item.r#type.clone()));

        // Add data fields
        if let Some(data) = item.data {
            if let Some(obj) = proto_struct_to_json(&data)?.as_object() {
                for (k, v) in obj {
                    json_obj.insert(k.clone(), v.clone());
                }
            }
        }

        // For UPDATE, add before and after
        if item.r#type == "UPDATE" {
            if let Some(before) = item.before {
                json_obj.insert("before".to_string(), proto_struct_to_json(&before)?);
            }
            if let Some(after) = item.after {
                json_obj.insert("after".to_string(), proto_struct_to_json(&after)?);
            }
        }

        output.push(JsonValue::Object(json_obj));
    }

    Ok(output)
}

/// Convert protobuf Struct to JSON
fn proto_struct_to_json(s: &Struct) -> Result<JsonValue> {
    let mut map = Map::new();
    for (k, v) in &s.fields {
        if let Some(json_val) = proto_value_to_json(v) {
            map.insert(k.clone(), json_val);
        }
    }
    Ok(JsonValue::Object(map))
}

/// Convert protobuf Value to JSON
fn proto_value_to_json(val: &prost_types::Value) -> Option<JsonValue> {
    match &val.kind {
        Some(prost_types::value::Kind::NullValue(_)) => Some(JsonValue::Null),
        Some(prost_types::value::Kind::BoolValue(b)) => Some(JsonValue::Bool(*b)),
        Some(prost_types::value::Kind::NumberValue(n)) => {
            Some(JsonValue::Number(serde_json::Number::from_f64(*n)?))
        }
        Some(prost_types::value::Kind::StringValue(s)) => Some(JsonValue::String(s.clone())),
        Some(prost_types::value::Kind::ListValue(list)) => {
            let arr: Vec<JsonValue> = list.values.iter().filter_map(proto_value_to_json).collect();
            Some(JsonValue::Array(arr))
        }
        Some(prost_types::value::Kind::StructValue(s)) => {
            let mut map = Map::new();
            for (k, v) in &s.fields {
                if let Some(json_val) = proto_value_to_json(v) {
                    map.insert(k.clone(), json_val);
                }
            }
            Some(JsonValue::Object(map))
        }
        None => None,
    }
}
