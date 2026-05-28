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

use async_trait::async_trait;

use test_data_store::{
    scripts::SourceChangeEvent, test_repo_storage::models::HttpSourceChangeDispatcherDefinition,
    test_run_storage::TestRunSourceStorage,
};

use super::SourceChangeDispatcher;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use tracing::{debug, error, trace};

/// HTTP change event format matching Drasi Server's Direct Format
#[derive(Debug, Serialize, Deserialize, Clone)]
struct HttpChangeEvent {
    operation: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    element: Option<DirectElement>,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    labels: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<i64>,
}

/// Direct element format for Drasi Server
#[derive(Debug, Serialize, Deserialize, Clone)]
struct DirectElement {
    #[serde(rename = "type")]
    element_type: String,
    id: String,
    labels: Vec<String>,
    properties: serde_json::Map<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    to: Option<String>,
}

/// Batch event request that wraps multiple events
#[derive(Debug, Serialize, Deserialize)]
struct BatchEventRequest {
    events: Vec<HttpChangeEvent>,
}

/// Convert SourceChangeEvent to Direct Format HttpChangeEvent
fn convert_to_direct_format(event: &SourceChangeEvent) -> Option<HttpChangeEvent> {
    // Parse the payload to extract element data
    let payload = serde_json::to_value(&event.payload).ok()?;

    // Determine operation type
    let operation = match event.op.as_str() {
        "i" => "insert",
        "u" => "update",
        "d" => "delete",
        _ => {
            error!("Unknown operation type: {}", event.op);
            return None;
        }
    };

    // Get timestamp in nanoseconds
    let timestamp = Some(event.reactivator_start_ns as i64);

    // For delete operations, we need to extract the ID from "before"
    if operation == "delete" {
        if let Some(before) = payload.get("before") {
            if let Some(id) = before.get("id").and_then(|v| v.as_str()) {
                let labels = before.get("labels").and_then(|v| v.as_array()).map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect::<Vec<String>>()
                });

                return Some(HttpChangeEvent {
                    operation: operation.to_string(),
                    element: None,
                    id: Some(id.to_string()),
                    labels,
                    timestamp,
                });
            }
        }
        error!("Delete operation missing 'before' data");
        return None;
    }

    // For insert/update, extract element from "after"
    let element_data = if operation == "update" || operation == "insert" {
        payload.get("after")
    } else {
        None
    };

    if let Some(elem_value) = element_data {
        // Extract element fields
        let id = elem_value
            .get("id")
            .and_then(|v| v.as_str())
            .map(String::from)?;

        let labels = elem_value
            .get("labels")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();

        let properties = elem_value
            .get("properties")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();

        // Determine element type based on table in source info
        let element_type = payload
            .get("source")
            .and_then(|s| s.get("table"))
            .and_then(|t| t.as_str())
            .unwrap_or("node");

        let element_type = match element_type {
            "relation" | "edge" => "relation",
            _ => "node",
        };

        // Internal SourceChangeEvent uses the CDC-style start_id/end_id.
        // Translate to from/to which is drasi-server's HTTP source plugin
        // DirectElement schema.
        let from = elem_value
            .get("start_id")
            .and_then(|v| v.as_str())
            .map(String::from);
        let to = elem_value
            .get("end_id")
            .and_then(|v| v.as_str())
            .map(String::from);

        let element = DirectElement {
            element_type: if from.is_some() && to.is_some() {
                "relation".to_string()
            } else {
                element_type.to_string()
            },
            id,
            labels,
            properties,
            from,
            to,
        };

        Some(HttpChangeEvent {
            operation: operation.to_string(),
            element: Some(element),
            id: None,
            labels: None,
            timestamp,
        })
    } else {
        error!("Insert/Update operation missing 'after' data");
        None
    }
}

#[derive(Debug)]
pub struct HttpSourceChangeDispatcherSettings {
    pub url: String,
    pub port: u16,
    pub endpoint: String,
    pub timeout_seconds: u64,
    pub batch_events: bool,
    pub source_id: String,
}

impl HttpSourceChangeDispatcherSettings {
    pub fn new(
        definition: &HttpSourceChangeDispatcherDefinition,
        source_id: String,
    ) -> anyhow::Result<Self> {
        // If endpoint is provided, use it as-is, otherwise construct from source_id
        let endpoint = if let Some(ep) = &definition.endpoint {
            ep.clone()
        } else {
            format!("/sources/{source_id}/events")
        };

        Ok(Self {
            url: definition.url.clone(),
            port: definition.port,
            endpoint,
            timeout_seconds: definition.timeout_seconds.unwrap_or(30),
            batch_events: definition.batch_events.unwrap_or(true),
            source_id,
        })
    }

    pub fn full_url(&self) -> String {
        format!("{}:{}{}", self.url, self.port, self.endpoint)
    }
}

pub struct HttpSourceChangeDispatcher {
    settings: HttpSourceChangeDispatcherSettings,
    client: Client,
}

impl HttpSourceChangeDispatcher {
    pub fn new(
        definition: &HttpSourceChangeDispatcherDefinition,
        storage: TestRunSourceStorage,
    ) -> anyhow::Result<Self> {
        log::debug!("Creating HttpSourceChangeDispatcher from {definition:?}, ");

        let source_id = storage.id.test_source_id.clone();
        let settings = HttpSourceChangeDispatcherSettings::new(definition, source_id)?;
        trace!(
            "Creating HttpSourceChangeDispatcher with settings {:?}, ",
            settings
        );

        let client = Client::builder()
            .timeout(Duration::from_secs(settings.timeout_seconds))
            .build()?;

        Ok(Self { settings, client })
    }
}

#[async_trait]
impl SourceChangeDispatcher for HttpSourceChangeDispatcher {
    async fn close(&mut self) -> anyhow::Result<()> {
        debug!("Closing HTTP source change dispatcher");
        Ok(())
    }

    async fn dispatch_source_change_events(
        &mut self,
        events: Vec<&SourceChangeEvent>,
    ) -> anyhow::Result<()> {
        trace!("Dispatching {} events to HTTP endpoint", events.len());

        if events.is_empty() {
            return Ok(());
        }

        let url = self.settings.full_url();

        log::info!(
            "HTTP dispatcher sending {} events to {} (source_id: {}, batch: {})",
            events.len(),
            url,
            self.settings.source_id,
            self.settings.batch_events
        );

        if self.settings.batch_events {
            // Convert events to Direct Format
            let http_events: Vec<HttpChangeEvent> = events
                .iter()
                .filter_map(|e| convert_to_direct_format(e))
                .collect();

            if http_events.is_empty() {
                error!("Failed to convert any events to Direct Format");
                return Err(anyhow::anyhow!("Failed to convert events to Direct Format"));
            }

            // Use batch endpoint for batch mode
            let batch_url = format!("{url}/batch");
            let batch_request = BatchEventRequest {
                events: http_events,
            };

            // Log request body at debug level
            debug!(
                "HTTP dispatcher sending batch request to {}: {}",
                batch_url,
                serde_json::to_string_pretty(&batch_request)
                    .unwrap_or_else(|e| format!("Failed to serialize: {e}"))
            );

            let response = match self
                .client
                .post(&batch_url)
                .json(&batch_request)
                .send()
                .await
            {
                Ok(resp) => resp,
                Err(e) => {
                    error!("Failed to connect to {}: {}", batch_url, e);
                    return Err(e.into());
                }
            };

            let status = response.status();
            let response_body = response.text().await.unwrap_or_default();

            // Log response at debug level
            debug!(
                "HTTP dispatcher received response from {}: Status: {}, Body: {}",
                batch_url, status, response_body
            );

            if !status.is_success() {
                error!(
                    "Failed to dispatch events batch to {}: {} - {}",
                    batch_url, status, response_body
                );
                anyhow::bail!("HTTP request failed with status: {status}");
            }

            log::info!(
                "Successfully dispatched batch of {} events to {} - Status: {}",
                events.len(),
                batch_url,
                status
            );
        } else {
            let event_count = events.len();
            for event in &events {
                // Convert to Direct Format
                let http_event = match convert_to_direct_format(event) {
                    Some(e) => e,
                    None => {
                        error!("Failed to convert event to Direct Format");
                        continue;
                    }
                };

                // Log request body at debug level
                debug!(
                    "HTTP dispatcher sending individual event to {}: {}",
                    url,
                    serde_json::to_string_pretty(&http_event)
                        .unwrap_or_else(|e| format!("Failed to serialize: {e}"))
                );

                let response = self.client.post(&url).json(&http_event).send().await?;

                let status = response.status();
                let response_body = response.text().await.unwrap_or_default();

                // Log response at debug level
                debug!(
                    "HTTP dispatcher received response from {}: Status: {}, Body: {}",
                    url, status, response_body
                );

                if !status.is_success() {
                    error!(
                        "Failed to dispatch event to {}: {} - {}",
                        url, status, response_body
                    );
                    anyhow::bail!("HTTP request failed with status: {status}");
                }
            }

            trace!(
                "Successfully dispatched {} individual events to {}",
                event_count,
                url
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_settings_with_defaults() {
        let definition = HttpSourceChangeDispatcherDefinition {
            url: "http://localhost".to_string(),
            port: 8080,
            endpoint: None,
            timeout_seconds: None,
            batch_events: None,
            adaptive_enabled: None,
            batch_size: None,
            batch_timeout_ms: None,
            source_id: None,
        };

        let source_id = "test-source".to_string();
        let settings = HttpSourceChangeDispatcherSettings::new(&definition, source_id).unwrap();

        assert_eq!(settings.url, "http://localhost");
        assert_eq!(settings.port, 8080);
        assert_eq!(settings.endpoint, "/sources/test-source/events");
        assert_eq!(settings.timeout_seconds, 30);
        assert!(settings.batch_events);
        assert_eq!(
            settings.full_url(),
            "http://localhost:8080/sources/test-source/events"
        );
    }

    #[test]
    fn test_settings_with_custom_values() {
        let definition = HttpSourceChangeDispatcherDefinition {
            url: "https://api.example.com".to_string(),
            port: 443,
            endpoint: Some("/webhooks/changes".to_string()),
            timeout_seconds: Some(60),
            batch_events: Some(false),
            adaptive_enabled: None,
            batch_size: None,
            batch_timeout_ms: None,
            source_id: None,
        };

        let source_id = "test-source".to_string();
        let settings = HttpSourceChangeDispatcherSettings::new(&definition, source_id).unwrap();

        assert_eq!(settings.url, "https://api.example.com");
        assert_eq!(settings.port, 443);
        assert_eq!(settings.endpoint, "/webhooks/changes");
        assert_eq!(settings.timeout_seconds, 60);
        assert!(!settings.batch_events);
        assert_eq!(
            settings.full_url(),
            "https://api.example.com:443/webhooks/changes"
        );
    }
}
