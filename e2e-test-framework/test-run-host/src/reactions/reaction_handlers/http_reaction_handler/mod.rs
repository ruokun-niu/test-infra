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

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::SystemTime};

use async_trait::async_trait;
use axum::{
    extract::State,
    http::{HeaderMap, Method, StatusCode},
    response::IntoResponse,
    routing::any,
    Router, Server,
};
use test_data_store::{
    test_repo_storage::models::HttpReactionHandlerDefinition, test_run_storage::TestRunQueryId,
};
use tokio::sync::{
    mpsc::{Receiver, Sender},
    Notify, RwLock,
};

use crate::reactions::reaction_output_handler::{
    ReactionControlSignal, ReactionHandlerError, ReactionHandlerMessage, ReactionHandlerPayload,
    ReactionHandlerStatus, ReactionHandlerType, ReactionInvocation, ReactionOutputHandler,
};

#[derive(Clone, Debug)]
pub struct HttpReactionHandlerSettings {
    pub host: String,
    pub port: u16,
    pub path: String,
    pub correlation_header: Option<String>,
    pub test_run_query_id: TestRunQueryId,
}

impl HttpReactionHandlerSettings {
    pub fn new(
        id: TestRunQueryId,
        definition: HttpReactionHandlerDefinition,
    ) -> anyhow::Result<Self> {
        Ok(HttpReactionHandlerSettings {
            host: definition
                .host
                .clone()
                .unwrap_or_else(|| "0.0.0.0".to_string()),
            port: definition.port.unwrap_or(8081),
            path: definition
                .path
                .clone()
                .unwrap_or_else(|| "/reaction".to_string()),
            correlation_header: definition.correlation_header,
            test_run_query_id: id,
        })
    }
}

#[derive(Clone)]
struct HttpInstanceState {
    tx: Sender<ReactionHandlerMessage>,
    settings: HttpReactionHandlerSettings,
}

pub struct HttpReactionHandler {
    notifier: Arc<Notify>,
    settings: HttpReactionHandlerSettings,
    status: Arc<RwLock<ReactionHandlerStatus>>,
    shutdown_notify: Arc<Notify>,
}

impl HttpReactionHandler {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(
        id: TestRunQueryId,
        definition: HttpReactionHandlerDefinition,
    ) -> anyhow::Result<Box<dyn ReactionOutputHandler + Send + Sync>> {
        let settings = HttpReactionHandlerSettings::new(id, definition)?;
        log::trace!("Creating HttpReactionHandler with settings {settings:?}");

        let notifier = Arc::new(Notify::new());
        let status = Arc::new(RwLock::new(ReactionHandlerStatus::Uninitialized));
        let shutdown_notify = Arc::new(Notify::new());

        Ok(Box::new(Self {
            notifier,
            settings,
            status,
            shutdown_notify,
        }))
    }
}

#[async_trait]
impl ReactionOutputHandler for HttpReactionHandler {
    async fn init(&self) -> anyhow::Result<Receiver<ReactionHandlerMessage>> {
        log::debug!("Initializing HttpReactionHandler");

        if let Ok(mut status) = self.status.try_write() {
            match *status {
                ReactionHandlerStatus::Uninitialized => {
                    let (handler_tx_channel, handler_rx_channel) = tokio::sync::mpsc::channel(100);

                    *status = ReactionHandlerStatus::Paused;

                    tokio::spawn(http_instance_thread(
                        self.settings.clone(),
                        self.status.clone(),
                        self.notifier.clone(),
                        self.shutdown_notify.clone(),
                        handler_tx_channel,
                    ));

                    Ok(handler_rx_channel)
                }
                ReactionHandlerStatus::Running => {
                    anyhow::bail!("Can't Init Handler, Handler currently Running");
                }
                ReactionHandlerStatus::Paused => {
                    anyhow::bail!("Can't Init Handler, Handler currently Paused");
                }
                ReactionHandlerStatus::Stopped => {
                    anyhow::bail!("Can't Init Handler, Handler currently Stopped");
                }
                ReactionHandlerStatus::Error => {
                    anyhow::bail!("Handler in Error state");
                }
            }
        } else {
            anyhow::bail!("Could not acquire status lock");
        }
    }

    async fn start(&self) -> anyhow::Result<()> {
        log::debug!("Starting HttpReactionHandler");

        if let Ok(mut status) = self.status.try_write() {
            match *status {
                ReactionHandlerStatus::Uninitialized => {
                    anyhow::bail!("Can't Start Handler, Handler Uninitialized");
                }
                ReactionHandlerStatus::Running => Ok(()),
                ReactionHandlerStatus::Paused => {
                    *status = ReactionHandlerStatus::Running;
                    self.notifier.notify_one();
                    Ok(())
                }
                ReactionHandlerStatus::Stopped => {
                    anyhow::bail!("Can't Start Handler, Handler already Stopped");
                }
                ReactionHandlerStatus::Error => {
                    anyhow::bail!("Handler in Error state");
                }
            }
        } else {
            anyhow::bail!("Could not acquire status lock");
        }
    }

    async fn pause(&self) -> anyhow::Result<()> {
        log::debug!("Pausing HttpReactionHandler");

        if let Ok(mut status) = self.status.try_write() {
            match *status {
                ReactionHandlerStatus::Uninitialized => {
                    anyhow::bail!("Can't Pause Handler, Handler Uninitialized");
                }
                ReactionHandlerStatus::Running => {
                    *status = ReactionHandlerStatus::Paused;
                    Ok(())
                }
                ReactionHandlerStatus::Paused => Ok(()),
                ReactionHandlerStatus::Stopped => {
                    anyhow::bail!("Can't Pause Handler, Handler already Stopped");
                }
                ReactionHandlerStatus::Error => {
                    anyhow::bail!("Handler in Error state");
                }
            }
        } else {
            anyhow::bail!("Could not acquire status lock");
        }
    }

    async fn stop(&self) -> anyhow::Result<()> {
        log::debug!("Stopping HttpReactionHandler");

        if let Ok(mut status) = self.status.try_write() {
            match *status {
                ReactionHandlerStatus::Uninitialized => {
                    anyhow::bail!("Handler not initialized, current status: Uninitialized");
                }
                ReactionHandlerStatus::Running | ReactionHandlerStatus::Paused => {
                    *status = ReactionHandlerStatus::Stopped;
                    self.shutdown_notify.notify_one();
                    Ok(())
                }
                ReactionHandlerStatus::Stopped => Ok(()),
                ReactionHandlerStatus::Error => {
                    anyhow::bail!("Handler in Error state");
                }
            }
        } else {
            anyhow::bail!("Could not acquire status lock");
        }
    }

    async fn status(&self) -> ReactionHandlerStatus {
        *self.status.read().await
    }

    async fn metrics(&self) -> Option<serde_json::Value> {
        None
    }
}

async fn http_instance_thread(
    settings: HttpReactionHandlerSettings,
    status: Arc<RwLock<ReactionHandlerStatus>>,
    notify: Arc<Notify>,
    shutdown_notify: Arc<Notify>,
    result_handler_tx_channel: Sender<ReactionHandlerMessage>,
) {
    log::debug!("Starting HttpReactionHandler Instance Thread");

    // Wait for the handler to be started
    loop {
        let current_status = {
            if let Ok(status) = status.try_read() {
                *status
            } else {
                log::warn!("Could not acquire status lock while waiting to start");
                continue;
            }
        };

        match current_status {
            ReactionHandlerStatus::Running => break,
            ReactionHandlerStatus::Paused => {
                log::debug!("HTTP instance waiting to be started");
                notify.notified().await;
            }
            ReactionHandlerStatus::Stopped => {
                log::debug!("Handler stopped before instance could start");
                return;
            }
            _ => {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
    }

    let state = HttpInstanceState {
        tx: result_handler_tx_channel.clone(),
        settings: settings.clone(),
    };

    let app = Router::new()
        .route(&settings.path, any(handle_reaction))
        .route(&format!("{}/*path", &settings.path), any(handle_reaction))
        .route("/batch", any(handle_reaction))
        // Accept the reaction callback on ANY path. Different drasi-server /
        // reaction-plugin versions construct the callback URL differently (e.g.
        // posting to the query id or the base URL root instead of the configured
        // path), which otherwise 404s and silently drops every result.
        // `handle_reaction` derives batch-vs-single from the URI, so serving all
        // paths is safe and version-tolerant.
        .fallback(handle_reaction)
        .with_state(state);

    let addr = match format!("{}:{}", settings.host, settings.port).parse::<SocketAddr>() {
        Ok(addr) => addr,
        Err(e) => {
            log::error!("Failed to parse instance address: {e}");
            *status.write().await = ReactionHandlerStatus::Error;
            let _ = result_handler_tx_channel
                .send(ReactionHandlerMessage::Error(ReactionHandlerError::new(
                    format!("Invalid HTTP instance address: {e}"),
                    false,
                )))
                .await;
            return;
        }
    };

    log::info!(
        "HTTP Reaction Handler listening on http://{} with path {} and batch support",
        addr,
        settings.path
    );

    let instance = Server::bind(&addr)
        .serve(app.into_make_service())
        .with_graceful_shutdown(async move {
            shutdown_notify.notified().await;
            log::debug!("HTTP instance received shutdown signal");
        });

    if let Err(e) = instance.await {
        log::error!("HTTP instance error: {e}");
        *status.write().await = ReactionHandlerStatus::Error;
        let _ = result_handler_tx_channel
            .send(ReactionHandlerMessage::Error(ReactionHandlerError::new(
                format!("HTTP instance error: {e}"),
                false,
            )))
            .await;
    }

    log::debug!("HTTP instance thread shutting down, sending HandlerStopping message");
    let _ = result_handler_tx_channel
        .send(ReactionHandlerMessage::Control(ReactionControlSignal::Stop))
        .await;
}

async fn handle_reaction(
    State(state): State<HttpInstanceState>,
    method: Method,
    headers: HeaderMap,
    uri: axum::http::Uri,
    body: String,
) -> impl IntoResponse {
    let invocation_time_ns = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    // Parse request body as JSON
    let request_body: serde_json::Value = match serde_json::from_str(&body) {
        Ok(json) => json,
        Err(_) => serde_json::json!({ "raw": body }),
    };

    // Extract headers
    let mut header_map = HashMap::new();
    for (name, value) in headers.iter() {
        if let Ok(value_str) = value.to_str() {
            header_map.insert(name.as_str().to_string(), value_str.to_string());
        }
    }

    // Extract trace context
    let traceparent = header_map.get("traceparent").cloned();
    let tracestate = header_map.get("tracestate").cloned();

    // Check if this is a batch request (array of batch results or single batch result)
    let is_batch = uri.path().contains("/batch")
        || request_body.is_array()
        || (request_body.is_object() && request_body.get("results").is_some());

    log::debug!(
        "HTTP Reaction Handler received {} request to {} with body type: {}",
        method,
        uri.path(),
        if is_batch { "batch" } else { "single" }
    );

    if is_batch {
        // Handle batch of events
        let batch_items = if request_body.is_array() {
            // Direct array of batch results
            request_body.as_array().unwrap().clone()
        } else if let Some(results) = request_body.get("results") {
            // Single batch result with results array
            if let Some(_arr) = results.as_array() {
                vec![request_body.clone()]
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        log::info!(
            "Processing batch with {} items at path {}",
            batch_items.len(),
            uri.path()
        );

        // Process each batch item
        for (idx, batch_item) in batch_items.iter().enumerate() {
            let query_id = batch_item
                .get("query_id")
                .and_then(|v| v.as_str())
                .unwrap_or(&state.settings.test_run_query_id.test_query_id)
                .to_string();

            let results = batch_item
                .get("results")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            log::debug!(
                "Batch item {} has {} results for query {}",
                idx,
                results.len(),
                query_id
            );

            // Process each result in the batch
            for (result_idx, result) in results.iter().enumerate() {
                // Determine reaction type from the result
                let reaction_type =
                    if result.get("before").is_some() && result.get("after").is_some() {
                        "updated"
                    } else if result.get("after").is_some() {
                        "added"
                    } else if result.get("before").is_some() {
                        "deleted"
                    } else {
                        "unknown"
                    }
                    .to_string();

                let sequence = (idx * 1000 + result_idx) as u64; // Generate sequence for batch items

                // Create reaction data as JSON
                let reaction_data = serde_json::json!({
                    "query_id": query_id,
                    "reaction_type": reaction_type,
                    "request_body": result,
                    "batch_index": idx,
                    "result_index": result_idx,
                });

                // Create metadata with HTTP-specific information
                let metadata = serde_json::json!({
                    "request_method": method.to_string(),
                    "request_path": uri.path().to_string(),
                    "headers": header_map.clone(),
                    "traceparent": traceparent.clone(),
                    "tracestate": tracestate.clone(),
                    "is_batch": true,
                });

                let invocation = ReactionInvocation {
                    handler_type: ReactionHandlerType::Http,
                    payload: ReactionHandlerPayload {
                        value: reaction_data,
                        timestamp: chrono::DateTime::from_timestamp_nanos(
                            invocation_time_ns as i64,
                        ),
                        invocation_id: Some(format!("{query_id}-{sequence}")),
                        metadata: Some(metadata),
                    },
                };

                if let Err(e) = state
                    .tx
                    .send(ReactionHandlerMessage::Invocation(invocation))
                    .await
                {
                    log::error!("Failed to send batch reaction message: {e}");
                }
            }
        }

        (StatusCode::OK, "Batch processed")
    } else {
        // Handle single event (original logic)
        // Extract sequence from correlation header or request body
        let sequence = if let Some(correlation_header) = &state.settings.correlation_header {
            header_map
                .get(correlation_header)
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0)
        } else {
            request_body
                .get("sequence")
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
        };

        // Determine reaction type from path or request body
        let reaction_type = if uri.path().contains("/added") {
            "added".to_string()
        } else if uri.path().contains("/updated") {
            "updated".to_string()
        } else if uri.path().contains("/deleted") {
            "deleted".to_string()
        } else {
            request_body
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string()
        };

        let query_id = state.settings.test_run_query_id.test_query_id.clone();

        // Create reaction data as JSON
        let reaction_data = serde_json::json!({
            "query_id": query_id,
            "reaction_type": reaction_type,
            "request_body": request_body,
        });

        // Create metadata with HTTP-specific information
        let metadata = serde_json::json!({
            "request_method": method.to_string(),
            "request_path": uri.path().to_string(),
            "headers": header_map,
            "traceparent": traceparent,
            "tracestate": tracestate,
            "is_batch": false,
        });

        let invocation = ReactionInvocation {
            handler_type: ReactionHandlerType::Http,
            payload: ReactionHandlerPayload {
                value: reaction_data,
                timestamp: chrono::DateTime::from_timestamp_nanos(invocation_time_ns as i64),
                invocation_id: Some(format!("{query_id}-{sequence}")),
                metadata: Some(metadata),
            },
        };

        log::debug!(
            "Received single reaction invocation: {} {} (sequence: {})",
            method,
            uri.path(),
            sequence
        );

        match state
            .tx
            .send(ReactionHandlerMessage::Invocation(invocation))
            .await
        {
            Ok(_) => (StatusCode::OK, "OK"),
            Err(e) => {
                log::error!("Failed to send reaction message: {e}");
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal Instance Error")
            }
        }
    }
}
