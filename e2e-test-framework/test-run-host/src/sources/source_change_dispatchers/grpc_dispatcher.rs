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
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;
use tracing::{debug, error, trace};

use test_data_store::{
    scripts::SourceChangeEvent, test_repo_storage::models::GrpcSourceChangeDispatcherDefinition,
    test_run_storage::TestRunSourceStorage,
};

use super::SourceChangeDispatcher;
use crate::grpc_converters::{convert_to_drasi_source_change, drasi};

use drasi::v1::source_service_client::SourceServiceClient;
use drasi::v1::SubmitEventRequest;

#[derive(Debug, Clone)]
pub struct GrpcSourceChangeDispatcherSettings {
    pub host: String,
    pub port: u16,
    pub timeout_seconds: u64,
    pub batch_events: bool,
    pub source_id: String,
    pub tls: bool,
}

impl GrpcSourceChangeDispatcherSettings {
    pub fn new(definition: &GrpcSourceChangeDispatcherDefinition) -> anyhow::Result<Self> {
        Ok(Self {
            host: definition.host.clone(),
            port: definition.port,
            timeout_seconds: definition.timeout_seconds.unwrap_or(30),
            batch_events: definition.batch_events.unwrap_or(true),
            source_id: definition.source_id.clone(),
            tls: definition.tls.unwrap_or(false),
        })
    }

    pub fn endpoint_url(&self) -> String {
        let scheme = if self.tls { "https" } else { "http" };
        format!("{}://{}:{}", scheme, self.host, self.port)
    }
}

pub struct GrpcSourceChangeDispatcher {
    settings: GrpcSourceChangeDispatcherSettings,
    client: Option<SourceServiceClient<Channel>>,
    channel: Option<Channel>,
}

struct BatchAcknowledgments {
    expected: u64,
    processed: u64,
}

impl BatchAcknowledgments {
    fn observe(&mut self, success: bool, processed: u64, error: &str) -> anyhow::Result<()> {
        anyhow::ensure!(success, "Batch dispatch failed: {error}");
        anyhow::ensure!(
            processed >= self.processed && processed <= self.expected,
            "Invalid cumulative acknowledgment: previous {}, received {}, expected {}",
            self.processed,
            processed,
            self.expected
        );
        self.processed = processed;
        Ok(())
    }

    fn finish(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.processed == self.expected,
            "Batch acknowledgment mismatch: sent {}, acknowledged {}",
            self.expected,
            self.processed
        );
        Ok(())
    }
}

impl GrpcSourceChangeDispatcher {
    pub async fn new(
        definition: &GrpcSourceChangeDispatcherDefinition,
        _storage: TestRunSourceStorage,
    ) -> anyhow::Result<Self> {
        log::debug!("Creating GrpcSourceChangeDispatcher from {definition:?}");

        let settings = GrpcSourceChangeDispatcherSettings::new(definition)?;
        trace!(
            "Creating GrpcSourceChangeDispatcher with settings {:?}",
            settings
        );

        // Don't connect immediately - will connect on first use
        Ok(Self {
            settings,
            client: None,
            channel: None,
        })
    }

    async fn ensure_connected(&mut self) -> anyhow::Result<()> {
        if self.client.is_some() {
            return Ok(());
        }

        debug!(
            "Attempting to connect to Drasi SourceService at: {}",
            self.settings.endpoint_url()
        );

        let endpoint = Endpoint::new(self.settings.endpoint_url())?
            .timeout(Duration::from_secs(self.settings.timeout_seconds))
            .connect_timeout(Duration::from_secs(5));

        match endpoint.connect().await {
            Ok(channel) => {
                let client = SourceServiceClient::new(channel.clone());
                self.channel = Some(channel);
                self.client = Some(client);
                debug!("Successfully connected to Drasi SourceService");
                Ok(())
            }
            Err(e) => {
                error!(
                    "Failed to connect to Drasi SourceService at {}: {}",
                    self.settings.endpoint_url(),
                    e
                );
                Err(anyhow::anyhow!(
                    "Failed to connect to Drasi SourceService at {}: {}",
                    self.settings.endpoint_url(),
                    e
                ))
            }
        }
    }

    #[allow(dead_code)]
    async fn health_check(&mut self) -> anyhow::Result<()> {
        self.ensure_connected().await?;

        let client = self
            .client
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Drasi SourceService client not initialized"))?;

        let request = Request::new(());
        match client.health_check(request).await {
            Ok(response) => {
                let health = response.into_inner();
                debug!("Drasi SourceService health: {:?}", health.status);
                Ok(())
            }
            Err(e) => {
                error!("Drasi SourceService health check failed: {}", e);
                Err(anyhow::anyhow!("Health check failed: {e}"))
            }
        }
    }
}

#[async_trait]
impl SourceChangeDispatcher for GrpcSourceChangeDispatcher {
    async fn close(&mut self) -> anyhow::Result<()> {
        debug!("Closing Drasi SourceService dispatcher");
        self.client = None;
        self.channel = None;
        Ok(())
    }

    async fn dispatch_source_change_events(
        &mut self,
        events: Vec<&SourceChangeEvent>,
    ) -> anyhow::Result<()> {
        trace!("Dispatching {} events to Drasi SourceService", events.len());

        if events.is_empty() {
            return Ok(());
        }

        // Ensure we're connected
        self.ensure_connected().await?;

        let client = self
            .client
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Drasi SourceService client not initialized"))?;

        if self.settings.batch_events {
            // Use StreamEvents for batch dispatch
            let source_changes: Result<Vec<_>, _> = events
                .iter()
                .map(|e| convert_to_drasi_source_change(e, &self.settings.source_id))
                .collect();

            let source_changes = source_changes?;

            debug!(
                "Sending batch of {} events to Drasi SourceService via StreamEvents",
                source_changes.len()
            );

            // Create a stream of source changes
            let stream = tokio_stream::iter(source_changes);
            let request = Request::new(stream);

            let mut response_stream = client.stream_events(request).await?.into_inner();

            let mut acknowledgments = BatchAcknowledgments {
                expected: events.len() as u64,
                processed: 0,
            };
            while let Some(response) = response_stream
                .message()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to receive stream response: {e}"))?
            {
                acknowledgments.observe(
                    response.success,
                    response.events_processed,
                    &response.error,
                )?;
            }

            acknowledgments.finish()?;

            trace!(
                "Successfully dispatched {} events to Drasi SourceService",
                acknowledgments.processed
            );
        } else {
            // Use SubmitEvent for individual dispatch
            let event_count = events.len();
            for event in &events {
                let source_change =
                    convert_to_drasi_source_change(event, &self.settings.source_id)?;

                debug!("Sending individual event to Drasi SourceService");

                let request = Request::new(SubmitEventRequest {
                    event: Some(source_change),
                });

                let response = client.submit_event(request).await.map_err(|e| {
                    error!("Failed to submit event to Drasi SourceService: {}", e);
                    anyhow::anyhow!("Event submission failed: {e}")
                })?;

                let resp = response.into_inner();
                if !resp.success {
                    error!(
                        "Drasi SourceService event submission failed: {}",
                        resp.error
                    );
                    anyhow::bail!("Event submission failed: {}", resp.error);
                }
            }

            trace!(
                "Successfully dispatched {} individual events to Drasi SourceService",
                event_count
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cumulative_acknowledgments_accept_repeated_final_total() {
        let mut acknowledgments = BatchAcknowledgments {
            expected: 200,
            processed: 0,
        };
        for processed in [100, 200, 200] {
            acknowledgments.observe(true, processed, "").unwrap();
        }
        acknowledgments.finish().unwrap();
    }

    #[test]
    fn cumulative_acknowledgments_reject_short_or_empty_stream() {
        let mut acknowledgments = BatchAcknowledgments {
            expected: 200,
            processed: 0,
        };
        assert!(acknowledgments.finish().is_err());
        acknowledgments.observe(true, 100, "").unwrap();
        assert!(acknowledgments.finish().is_err());
    }

    #[test]
    fn cumulative_acknowledgments_reject_failures_and_invalid_counts() {
        let mut acknowledgments = BatchAcknowledgments {
            expected: 200,
            processed: 0,
        };
        assert!(acknowledgments.observe(false, 0, "").is_err());
        acknowledgments.observe(true, 100, "").unwrap();
        assert!(acknowledgments.observe(true, 99, "").is_err());
        assert!(acknowledgments.observe(true, 201, "").is_err());
    }

    #[test]
    fn test_settings_creation() {
        let definition = GrpcSourceChangeDispatcherDefinition {
            host: "localhost".to_string(),
            port: 50051,
            timeout_seconds: Some(60),
            batch_events: Some(true),
            tls: Some(false),
            source_id: "test-source".to_string(),
            adaptive_enabled: None,
            batch_size: None,
            batch_timeout_ms: None,
        };

        let settings = GrpcSourceChangeDispatcherSettings::new(&definition).unwrap();

        assert_eq!(settings.host, "localhost");
        assert_eq!(settings.port, 50051);
        assert_eq!(settings.timeout_seconds, 60);
        assert!(settings.batch_events);
        assert_eq!(settings.source_id, "test-source");
        assert!(!settings.tls);
        assert_eq!(settings.endpoint_url(), "http://localhost:50051");
    }

    #[test]
    fn test_settings_with_tls() {
        let definition = GrpcSourceChangeDispatcherDefinition {
            host: "example.com".to_string(),
            port: 443,
            timeout_seconds: None,
            batch_events: None,
            tls: Some(true),
            source_id: "test-source".to_string(),
            adaptive_enabled: None,
            batch_size: None,
            batch_timeout_ms: None,
        };

        let settings = GrpcSourceChangeDispatcherSettings::new(&definition).unwrap();

        assert_eq!(settings.timeout_seconds, 30); // default
        assert!(settings.batch_events); // default
        assert!(settings.tls);
        assert_eq!(settings.endpoint_url(), "https://example.com:443");
    }
}
