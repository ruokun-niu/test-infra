use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;
use tracing::{debug, error, info, trace, warn};

use test_data_store::{
    scripts::SourceChangeEvent, test_repo_storage::models::GrpcSourceChangeDispatcherDefinition,
    test_run_storage::TestRunSourceStorage,
};

use super::SourceChangeDispatcher;
use crate::grpc_converters::{convert_to_drasi_source_change, drasi};
use crate::utils::{AdaptiveBatchConfig, AdaptiveBatcher};

use drasi::v1::source_service_client::SourceServiceClient;

/// Adaptive gRPC source dispatcher that dynamically adjusts batching
pub struct AdaptiveGrpcSourceChangeDispatcher {
    host: String,
    port: u16,
    source_id: String,
    tls: bool,
    timeout_seconds: u64,
    adaptive_config: AdaptiveBatchConfig,
    // Channel for sending events to the batcher
    event_tx: Option<mpsc::Sender<SourceChangeEvent>>,
    // Handle to the background batcher task
    batcher_handle: Option<JoinHandle<anyhow::Result<()>>>,
    client: Arc<Mutex<Option<SourceServiceClient<Channel>>>>,
    failure: Option<String>,
}

impl AdaptiveGrpcSourceChangeDispatcher {
    pub async fn new(
        definition: &GrpcSourceChangeDispatcherDefinition,
        _storage: TestRunSourceStorage,
    ) -> anyhow::Result<Self> {
        info!("Creating AdaptiveGrpcSourceChangeDispatcher");

        // Configure adaptive batching
        let mut adaptive_config = AdaptiveBatchConfig::default();

        // Allow overriding from definition if we add fields later
        if let Some(batch_size) = definition.batch_size {
            adaptive_config.max_batch_size = batch_size as usize;
        }
        if let Some(timeout_ms) = definition.batch_timeout_ms {
            adaptive_config.max_wait_time = Duration::from_millis(timeout_ms);
        }

        Ok(Self {
            host: definition.host.clone(),
            port: definition.port,
            source_id: definition.source_id.clone(),
            tls: definition.tls.unwrap_or(false),
            timeout_seconds: definition.timeout_seconds.unwrap_or(30),
            adaptive_config,
            event_tx: None,
            batcher_handle: None,
            client: Arc::new(Mutex::new(None)),
            failure: None,
        })
    }

    fn endpoint_url(&self) -> String {
        let scheme = if self.tls { "https" } else { "http" };
        format!("{}://{}:{}", scheme, self.host, self.port)
    }

    async fn ensure_connected(
        client: Arc<Mutex<Option<SourceServiceClient<Channel>>>>,
        endpoint_url: String,
        timeout_seconds: u64,
    ) -> anyhow::Result<()> {
        let mut client_guard = client.lock().await;
        if client_guard.is_some() {
            return Ok(());
        }

        debug!("Connecting to Drasi SourceService at: {}", endpoint_url);

        let endpoint = Endpoint::new(endpoint_url.clone())?
            .timeout(Duration::from_secs(timeout_seconds))
            .connect_timeout(Duration::from_secs(5));

        match endpoint.connect().await {
            Ok(channel) => {
                *client_guard = Some(SourceServiceClient::new(channel));
                info!("Successfully connected to Drasi SourceService");
                Ok(())
            }
            Err(e) => {
                error!("Failed to connect to Drasi SourceService: {}", e);
                Err(anyhow::anyhow!("Connection failed: {e}"))
            }
        }
    }

    async fn send_batch(
        client: Arc<Mutex<Option<SourceServiceClient<Channel>>>>,
        batch: Vec<SourceChangeEvent>,
        source_id: String,
        endpoint_url: String,
        timeout_seconds: u64,
    ) -> anyhow::Result<()> {
        // Ensure connected
        Self::ensure_connected(client.clone(), endpoint_url, timeout_seconds).await?;

        let mut client = client
            .lock()
            .await
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Client not available"))?;

        // Convert events to protobuf format
        let source_changes: Result<Vec<_>, _> = batch
            .iter()
            .map(|e| convert_to_drasi_source_change(e, &source_id))
            .collect();

        let source_changes = source_changes?;

        debug!(
            "Sending adaptive batch of {} events via StreamEvents",
            source_changes.len()
        );

        // Create a stream and send
        let stream = tokio_stream::iter(source_changes);
        let request = Request::new(stream);

        let mut response_stream = client.stream_events(request).await?.into_inner();

        let mut total_processed = 0u64;
        while let Some(response) = response_stream.message().await? {
            if !response.success && !response.error.is_empty() {
                anyhow::bail!("Batch dispatch failed: {}", response.error);
            }
            total_processed += response.events_processed;
        }

        trace!("Successfully dispatched {} events", total_processed);
        Ok(())
    }

    fn start_batcher(&mut self) -> anyhow::Result<()> {
        if self.batcher_handle.is_some() {
            return Ok(()); // Already started
        }

        let (tx, rx) = mpsc::channel(1000);
        self.event_tx = Some(tx);

        let client = self.client.clone();
        let source_id = self.source_id.clone();
        let endpoint_url = self.endpoint_url();
        let timeout_seconds = self.timeout_seconds;
        let adaptive_config = self.adaptive_config.clone();

        // Spawn the batcher task
        let handle = tokio::spawn(async move {
            let mut batcher = AdaptiveBatcher::new(rx, adaptive_config);
            let mut successful_batches = 0u64;

            info!("Adaptive batcher started for gRPC source dispatcher");

            while let Some(batch) = batcher.next_batch().await {
                if batch.is_empty() {
                    continue;
                }

                debug!("Adaptive batch ready with {} events", batch.len());

                // Send the batch with retries
                let mut retries = 0;
                const MAX_RETRIES: u32 = 3;

                loop {
                    match Self::send_batch(
                        client.clone(),
                        batch.clone(),
                        source_id.clone(),
                        endpoint_url.clone(),
                        timeout_seconds,
                    )
                    .await
                    {
                        Ok(_) => {
                            successful_batches += 1;
                            if successful_batches % 100 == 0 {
                                info!(
                                    "Adaptive dispatcher metrics - Successful: {}",
                                    successful_batches
                                );
                            }
                            break;
                        }
                        Err(e) => {
                            retries += 1;
                            if retries > MAX_RETRIES {
                                anyhow::bail!(
                                    "Failed to send adaptive gRPC batch after {MAX_RETRIES} retries: {e}"
                                );
                            }
                            warn!(
                                "Batch send failed (retry {}/{}): {}",
                                retries, MAX_RETRIES, e
                            );

                            // Exponential backoff
                            let backoff = Duration::from_millis(100 * 2u64.pow(retries));
                            tokio::time::sleep(backoff).await;

                            // Clear client to force reconnection
                            let mut client_guard = client.lock().await;
                            *client_guard = None;
                        }
                    }
                }
            }

            info!("Adaptive batcher completed - Successful: {successful_batches}");
            Ok(())
        });

        self.batcher_handle = Some(handle);
        Ok(())
    }

    async fn await_batcher(&mut self) -> anyhow::Result<()> {
        let Some(handle) = self.batcher_handle.take() else {
            return match &self.failure {
                Some(failure) => Err(anyhow::anyhow!(failure.clone())),
                None => Ok(()),
            };
        };

        let result = handle
            .await
            .map_err(|e| anyhow::anyhow!("Adaptive gRPC batcher task failed: {e}"))?;

        if let Err(error) = &result {
            self.failure = Some(error.to_string());
        }

        result
    }
}

#[async_trait]
impl SourceChangeDispatcher for AdaptiveGrpcSourceChangeDispatcher {
    async fn close(&mut self) -> anyhow::Result<()> {
        debug!("Closing adaptive gRPC source dispatcher");

        // Close the event channel to signal batcher to stop
        self.event_tx = None;

        let batcher_result = if let Some(mut handle) = self.batcher_handle.take() {
            match tokio::time::timeout(Duration::from_secs(5), &mut handle).await {
                Ok(Ok(result)) => result,
                Ok(Err(error)) => Err(anyhow::anyhow!(
                    "Adaptive gRPC batcher task failed: {error}"
                )),
                Err(_) => {
                    handle.abort();
                    let _ = handle.await;
                    Err(anyhow::anyhow!(
                        "Timed out waiting for adaptive gRPC batcher to stop"
                    ))
                }
            }
        } else {
            Ok(())
        };

        // Clear the client
        let mut client_guard = self.client.lock().await;
        *client_guard = None;
        drop(client_guard);

        if let Err(error) = &batcher_result {
            self.failure = Some(error.to_string());
        }

        batcher_result
    }

    async fn dispatch_source_change_events(
        &mut self,
        events: Vec<&SourceChangeEvent>,
    ) -> anyhow::Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        if let Some(failure) = &self.failure {
            anyhow::bail!(failure.clone());
        }

        if self
            .batcher_handle
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
        {
            self.event_tx = None;
            self.await_batcher().await?;
            anyhow::bail!("Adaptive gRPC batcher stopped unexpectedly");
        }

        // Start batcher if not running
        self.start_batcher()?;

        // Send events to the batcher
        if let Some(tx) = &self.event_tx {
            for event in events {
                if tx.send(event.clone()).await.is_err() {
                    self.event_tx = None;
                    return match self.await_batcher().await {
                        Ok(()) => Err(anyhow::anyhow!(
                            "Adaptive gRPC batcher channel closed unexpectedly"
                        )),
                        Err(e) => Err(e),
                    };
                }
            }
        } else {
            return Err(anyhow::anyhow!("Batcher not initialized"));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dispatcher_with_batcher(
        batcher_handle: JoinHandle<anyhow::Result<()>>,
    ) -> (
        AdaptiveGrpcSourceChangeDispatcher,
        Arc<Mutex<Option<SourceServiceClient<Channel>>>>,
    ) {
        let client = Arc::new(Mutex::new(Some(SourceServiceClient::new(
            Endpoint::from_static("http://127.0.0.1:50051").connect_lazy(),
        ))));

        (
            AdaptiveGrpcSourceChangeDispatcher {
                host: "127.0.0.1".to_string(),
                port: 50051,
                source_id: "test-source".to_string(),
                tls: false,
                timeout_seconds: 1,
                adaptive_config: AdaptiveBatchConfig::default(),
                event_tx: None,
                batcher_handle: Some(batcher_handle),
                client: client.clone(),
                failure: None,
            },
            client,
        )
    }

    #[tokio::test]
    async fn close_propagates_and_latches_batcher_failure() {
        let handle = tokio::spawn(async { Err(anyhow::anyhow!("permanent dispatch failure")) });
        let (mut dispatcher, client) = dispatcher_with_batcher(handle);

        let error = dispatcher.close().await.unwrap_err();

        assert!(error.to_string().contains("permanent dispatch failure"));
        assert_eq!(
            dispatcher.failure.as_deref(),
            Some("permanent dispatch failure")
        );
        assert!(client.lock().await.is_none());
    }

    #[tokio::test]
    async fn close_clears_client_when_batcher_panics() {
        let handle: JoinHandle<anyhow::Result<()>> =
            tokio::spawn(async { panic!("batcher panic") });
        let (mut dispatcher, client) = dispatcher_with_batcher(handle);

        let error = dispatcher.close().await.unwrap_err();

        assert!(error
            .to_string()
            .contains("Adaptive gRPC batcher task failed"));
        assert!(client.lock().await.is_none());
    }
}
