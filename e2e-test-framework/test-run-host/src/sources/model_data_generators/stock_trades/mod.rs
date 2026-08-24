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

use std::{
    collections::HashSet,
    fmt::{self, Debug, Formatter},
    num::NonZeroU32,
    sync::Arc,
    time::SystemTime,
};

use async_trait::async_trait;
use futures::future::join_all;
use governor::{
    clock::{QuantaClock, QuantaInstant},
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand_distr::{Distribution, Normal};
use serde::Serialize;
use stock_market::{ModelChange, StockMarket};
use time::{format_description, OffsetDateTime};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        oneshot, Mutex,
    },
    task::JoinHandle,
};

use test_data_store::{
    scripts::{
        NodeRecord, SourceChangeEvent, SourceChangeEventPayload, SourceChangeEventSourceInfo,
    },
    test_repo_storage::{
        models::{
            SourceChangeDispatcherDefinition, SpacingMode, StockDefinition,
            StockTradeDataGeneratorDefinition, TimeMode,
        },
        TestSourceStorage,
    },
    test_run_storage::{TestRunSourceId, TestRunSourceStorage},
};

use crate::sources::{
    bootstrap_data_generators::{BootstrapData, BootstrapDataGenerator},
    source_change_dispatchers::{create_source_change_dispatcher, SourceChangeDispatcher},
    source_change_generators::{
        SourceChangeGenerator, SourceChangeGeneratorCommandResponse, SourceChangeGeneratorState,
        SourceChangeGeneratorStatus,
    },
};

use super::ModelDataGenerator;

mod stock_market;

#[derive(Debug, thiserror::Error)]
pub enum StockTradeDataGeneratorError {
    #[error("StockTradeDataGenerator is already finished. Reset to start over.")]
    AlreadyFinished,
    #[error("StockTradeDataGenerator is already stopped. Reset to start over.")]
    AlreadyStopped,
    #[error("StockTradeDataGenerator is currently Skipping. {0} skips remaining. Pause before Skip, Step, or Reset.")]
    CurrentlySkipping(u64),
    #[error("StockTradeDataGenerator is currently Stepping. {0} steps remaining. Pause before Skip, Step, or Reset.")]
    CurrentlyStepping(u64),
    #[error("StockTradeDataGenerator is currently in an Error state - {0:?}")]
    Error(SourceChangeGeneratorStatus),
    #[error("StockTradeDataGenerator is currently Running. Pause before trying to Skip.")]
    PauseToSkip,
    #[error("StockTradeDataGenerator is currently Running. Pause before trying to Step.")]
    PauseToStep,
    #[error("StockTradeDataGenerator is currently Running. Pause before trying to Reset.")]
    PauseToReset,
}

#[derive(Clone, Debug, Serialize)]
pub struct StockTradeDataGeneratorSettings {
    pub stock_definitions: Vec<StockDefinition>,
    pub price_init: (f64, f64),
    pub price_change: (f64, f64),
    pub price_momentum: (i32, f64, f64),
    pub price_range: (f64, f64),
    pub volume_init: (i64, f64),
    pub volume_change: (i64, f64),
    pub volume_momentum: (i32, f64, f64),
    pub volume_range: (i64, i64),
    pub change_count: u64,
    pub change_interval: (u64, f64, u64, u64),
    pub dispatchers: Vec<SourceChangeDispatcherDefinition>,
    pub id: TestRunSourceId,
    pub input_storage: TestSourceStorage,
    pub output_storage: TestRunSourceStorage,
    pub seed: u64,
    pub spacing_mode: SpacingMode,
    pub time_mode: TimeMode,
    pub send_initial_inserts: bool,
}

impl StockTradeDataGeneratorSettings {
    pub async fn new(
        test_run_source_id: TestRunSourceId,
        definition: StockTradeDataGeneratorDefinition,
        input_storage: TestSourceStorage,
        output_storage: TestRunSourceStorage,
        dispatchers: Vec<SourceChangeDispatcherDefinition>,
    ) -> anyhow::Result<Self> {
        Ok(StockTradeDataGeneratorSettings {
            stock_definitions: definition.stock_definitions,
            price_init: definition.price_init.unwrap_or((100.0, 10.0)),
            price_change: definition.price_change.unwrap_or((1.0, 2.0)),
            price_momentum: definition.price_momentum.unwrap_or((5, 2.0, 0.3)),
            price_range: definition.price_range.unwrap_or((1.0, 10000.0)),
            volume_init: definition.volume_init.unwrap_or((1000000, 500000.0)),
            volume_change: definition.volume_change.unwrap_or((10000, 5000.0)),
            volume_momentum: definition.volume_momentum.unwrap_or((3, 1.0, 0.5)),
            volume_range: definition.volume_range.unwrap_or((0, 1000000000)),
            change_count: definition.common.change_count.unwrap_or(100000),
            change_interval: definition.common.change_interval.unwrap_or((
                1000000000,
                0.0,
                u64::MIN,
                u64::MAX,
            )),
            dispatchers,
            id: test_run_source_id,
            input_storage,
            output_storage,
            seed: definition.common.seed.unwrap_or(rand::rng().random()),
            spacing_mode: definition.common.spacing_mode,
            time_mode: definition.common.time_mode,
            send_initial_inserts: definition.send_initial_inserts,
        })
    }

    pub fn get_id(&self) -> TestRunSourceId {
        self.id.clone()
    }
}

#[derive(Debug)]
pub enum StockTradeDataGeneratorCommand {
    GetState,
    Pause,
    Reset,
    Skip {
        skips: u64,
        spacing_mode: Option<SpacingMode>,
    },
    Start,
    Step {
        steps: u64,
        spacing_mode: Option<SpacingMode>,
    },
    Stop,
    SetTestRunHost {
        test_run_host: std::sync::Arc<crate::TestRunHost>,
    },
}

#[derive(Debug)]
pub struct StockTradeDataGeneratorMessage {
    pub command: StockTradeDataGeneratorCommand,
    pub response_tx: Option<oneshot::Sender<StockTradeDataGeneratorMessageResponse>>,
}

#[derive(Debug)]
pub struct StockTradeDataGeneratorMessageResponse {
    pub result: anyhow::Result<()>,
    pub state: StockTradeDataGeneratorExternalState,
}

#[derive(Clone, Debug)]
pub struct ScheduledChangeEventMessage {
    pub delay_ns: u64,
    pub seq_num: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProcessedChangeEvent {
    pub dispatch_status: SourceChangeGeneratorStatus,
    pub event: SourceChangeEvent,
    pub seq: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct StockTradeDataGenerator {
    #[serde(skip_serializing)]
    stock_market: Arc<Mutex<StockMarket>>,
    settings: StockTradeDataGeneratorSettings,
    #[serde(skip_serializing)]
    model_host_tx_channel: Sender<StockTradeDataGeneratorMessage>,
    #[serde(skip_serializing)]
    _model_host_thread_handle: Arc<Mutex<JoinHandle<anyhow::Result<()>>>>,
}

impl StockTradeDataGenerator {
    pub async fn new(
        test_run_source_id: TestRunSourceId,
        definition: StockTradeDataGeneratorDefinition,
        input_storage: TestSourceStorage,
        output_storage: TestRunSourceStorage,
        dispatchers: Vec<SourceChangeDispatcherDefinition>,
    ) -> anyhow::Result<Self> {
        let settings = StockTradeDataGeneratorSettings::new(
            test_run_source_id,
            definition,
            input_storage,
            output_storage.clone(),
            dispatchers,
        )
        .await?;

        log::debug!("Creating StockTradeDataGenerator from {:?}", &settings);

        let stock_market = Arc::new(Mutex::new(StockMarket::new(&settings)?));

        let (model_host_tx_channel, model_host_rx_channel) = tokio::sync::mpsc::channel(500);
        let model_host_thread_handle = tokio::spawn(model_host_thread(
            model_host_rx_channel,
            settings.clone(),
            stock_market.clone(),
        ));

        Ok(Self {
            stock_market,
            settings,
            model_host_tx_channel,
            _model_host_thread_handle: Arc::new(Mutex::new(model_host_thread_handle)),
        })
    }

    pub fn get_id(&self) -> TestRunSourceId {
        self.settings.get_id()
    }

    pub fn get_settings(&self) -> StockTradeDataGeneratorSettings {
        self.settings.clone()
    }

    async fn send_command(
        &self,
        command: StockTradeDataGeneratorCommand,
    ) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        let (response_tx, response_rx) = oneshot::channel();

        let r = self
            .model_host_tx_channel
            .send(StockTradeDataGeneratorMessage {
                command,
                response_tx: Some(response_tx),
            })
            .await;

        match r {
            Ok(_) => {
                let player_response = response_rx.await?;

                Ok(SourceChangeGeneratorCommandResponse {
                    result: player_response.result,
                    state: SourceChangeGeneratorState {
                        status: player_response.state.status,
                        state: serde_json::to_value(player_response.state).unwrap(),
                    },
                })
            }
            Err(e) => anyhow::bail!("Error sending command to StockTradeDataGenerator: {e:?}"),
        }
    }
}

#[async_trait]
impl BootstrapDataGenerator for StockTradeDataGenerator {
    async fn get_data(
        &self,
        node_labels: &HashSet<String>,
        rel_labels: &HashSet<String>,
    ) -> anyhow::Result<BootstrapData> {
        log::debug!("Node labels: [{node_labels:?}], Rel labels: [{rel_labels:?}]");

        let mut stock_nodes = Vec::new();

        let stock_market = self.stock_market.lock().await;
        for change in stock_market.get_current_state(node_labels) {
            match change {
                ModelChange::StockAdded(stock) => {
                    let node_record = NodeRecord {
                        id: stock.id,
                        labels: stock.labels.clone(),
                        properties: serde_json::json!(stock.properties),
                    };
                    stock_nodes.push(node_record);
                }
                _ => {
                    log::debug!("Other change: {change:?}");
                }
            }
        }

        let mut bootstrap_data = BootstrapData::new();

        if !stock_nodes.is_empty() {
            bootstrap_data
                .nodes
                .insert("Stock".to_string(), stock_nodes);
        }

        Ok(bootstrap_data)
    }
}

#[async_trait]
impl SourceChangeGenerator for StockTradeDataGenerator {
    async fn get_state(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(StockTradeDataGeneratorCommand::GetState)
            .await
    }

    async fn pause(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(StockTradeDataGeneratorCommand::Pause)
            .await
    }

    async fn reset(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(StockTradeDataGeneratorCommand::Reset)
            .await
    }

    async fn skip(
        &self,
        skips: u64,
        spacing_mode: Option<SpacingMode>,
    ) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(StockTradeDataGeneratorCommand::Skip {
            skips,
            spacing_mode,
        })
        .await
    }

    async fn start(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(StockTradeDataGeneratorCommand::Start)
            .await
    }

    async fn step(
        &self,
        steps: u64,
        spacing_mode: Option<SpacingMode>,
    ) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(StockTradeDataGeneratorCommand::Step {
            steps,
            spacing_mode,
        })
        .await
    }

    async fn stop(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(StockTradeDataGeneratorCommand::Stop)
            .await
    }

    fn set_test_run_host_on_dispatchers(&self, test_run_host: std::sync::Arc<crate::TestRunHost>) {
        log::info!("StockTradeDataGenerator: Sending SetTestRunHost command to thread");

        let tx = self.model_host_tx_channel.clone();
        let command = StockTradeDataGeneratorCommand::SetTestRunHost { test_run_host };

        tokio::task::spawn(async move {
            if let Err(e) = tx
                .send(StockTradeDataGeneratorMessage {
                    command,
                    response_tx: None,
                })
                .await
            {
                log::error!("Failed to send SetTestRunHost command: {e}");
            }
        });
    }
}

struct ChangeIntervalGenerator {
    interval_dist: Normal<f64>,
    interval_range: (u64, u64),
    rng: ChaCha8Rng,
}

impl ChangeIntervalGenerator {
    fn new(seed: u64, change_interval: (u64, f64, u64, u64)) -> anyhow::Result<Self> {
        let (mean, std_dev, range_min, range_max) = change_interval;

        Ok(Self {
            interval_dist: Normal::new(mean as f64, std_dev).unwrap(),
            interval_range: (range_min, range_max),
            rng: ChaCha8Rng::seed_from_u64(seed),
        })
    }

    fn next(&mut self) -> u64 {
        let mut interval = self.interval_dist.sample(&mut self.rng) as u64;

        if interval < self.interval_range.0 {
            interval = self.interval_range.0;
        } else if interval > self.interval_range.1 {
            interval = self.interval_range.1;
        }

        interval
    }
}

#[async_trait]
impl ModelDataGenerator for StockTradeDataGenerator {}

#[derive(Debug, Serialize)]
pub struct StockTradeDataGeneratorExternalState {
    pub error_messages: Vec<String>,
    pub event_seq_num: u64,
    pub next_event: Option<SourceChangeEvent>,
    pub previous_event: Option<ProcessedChangeEvent>,
    pub skips_remaining: u64,
    pub spacing_mode: SpacingMode,
    pub stats: StockTradeDataGeneratorStats,
    pub status: SourceChangeGeneratorStatus,
    pub steps_remaining: u64,
    pub test_run_source_id: TestRunSourceId,
    pub time_mode: TimeMode,
    pub virtual_time_ns_current: u64,
    pub virtual_time_ns_next: u64,
    pub virtual_time_ns_rebase_adjustment: i64,
    pub virtual_time_ns_start: u64,
}

impl From<&mut StockTradeDataGeneratorInternalState> for StockTradeDataGeneratorExternalState {
    fn from(state: &mut StockTradeDataGeneratorInternalState) -> Self {
        Self {
            error_messages: state.error_messages.clone(),
            event_seq_num: state.event_seq_num,
            next_event: state.next_event.clone(),
            previous_event: state.previous_event.clone(),
            skips_remaining: state.skips_remaining,
            spacing_mode: state.settings.spacing_mode.clone(),
            stats: state.stats.clone(),
            status: state.status,
            steps_remaining: state.steps_remaining,
            test_run_source_id: state.settings.id.clone(),
            time_mode: state.settings.time_mode.clone(),
            virtual_time_ns_current: state.virtual_time_ns_current,
            virtual_time_ns_next: state.virtual_time_ns_next,
            virtual_time_ns_rebase_adjustment: state.virtual_time_ns_rebase_adjustment,
            virtual_time_ns_start: state.virtual_time_ns_start,
        }
    }
}

pub struct StockTradeDataGeneratorInternalState {
    stock_market: Arc<Mutex<StockMarket>>,
    change_interval_generator: ChangeIntervalGenerator,
    change_tx_channel: Sender<ScheduledChangeEventMessage>,
    dispatchers: Vec<Box<dyn SourceChangeDispatcher + Send>>,
    error_messages: Vec<String>,
    event_seq_num: u64,
    next_event: Option<SourceChangeEvent>,
    previous_event: Option<ProcessedChangeEvent>,
    rate_limiter: RateLimiter<NotKeyed, InMemoryState, QuantaClock, NoOpMiddleware<QuantaInstant>>,
    settings: StockTradeDataGeneratorSettings,
    skips_remaining: u64,
    status: SourceChangeGeneratorStatus,
    stats: StockTradeDataGeneratorStats,
    steps_remaining: u64,
    virtual_time_ns_current: u64,
    virtual_time_ns_next: u64,
    virtual_time_ns_rebase_adjustment: i64,
    virtual_time_ns_start: u64,
}

impl StockTradeDataGeneratorInternalState {
    async fn initialize(
        settings: StockTradeDataGeneratorSettings,
        stock_market: Arc<Mutex<StockMarket>>,
    ) -> anyhow::Result<(Self, Receiver<ScheduledChangeEventMessage>)> {
        log::debug!("Initializing StockTradeDataGenerator using {settings:?}");

        // Create the dispatchers
        let mut dispatchers: Vec<Box<dyn SourceChangeDispatcher + Send>> = Vec::new();
        for def in settings.dispatchers.iter() {
            match create_source_change_dispatcher(def, &settings.output_storage).await {
                Ok(dispatcher) => dispatchers.push(dispatcher),
                Err(e) => {
                    anyhow::bail!("Error creating SourceChangeDispatcher: {def:?}; Error: {e:?}");
                }
            }
        }

        let rate_limiter = match settings.spacing_mode {
            SpacingMode::Rate(rate) => RateLimiter::direct(Quota::per_second(rate)),
            _ => RateLimiter::direct(Quota::per_second(NonZeroU32::new(u32::MAX).unwrap())),
        };

        let (change_tx_channel, change_rx_channel) = tokio::sync::mpsc::channel(1000);

        let state = Self {
            stock_market,
            change_interval_generator: ChangeIntervalGenerator::new(
                settings.seed,
                settings.change_interval,
            )?,
            change_tx_channel,
            dispatchers,
            error_messages: Vec::new(),
            event_seq_num: 0,
            next_event: None,
            previous_event: None,
            rate_limiter,
            settings,
            skips_remaining: 0,
            status: SourceChangeGeneratorStatus::Paused,
            stats: StockTradeDataGeneratorStats::default(),
            steps_remaining: 0,
            virtual_time_ns_current: 0,
            virtual_time_ns_next: 0,
            virtual_time_ns_rebase_adjustment: 0,
            virtual_time_ns_start: 0,
        };

        Ok((state, change_rx_channel))
    }

    async fn close_dispatchers(&mut self) -> anyhow::Result<()> {
        let dispatchers = &mut self.dispatchers;

        log::debug!("Closing dispatchers - #dispatchers:{}", dispatchers.len());

        let futures: Vec<_> = dispatchers
            .iter_mut()
            .map(|dispatcher| dispatcher.close())
            .collect();

        join_all(futures)
            .await
            .into_iter()
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(())
    }

    async fn send_initial_inserts(&mut self) -> anyhow::Result<()> {
        log::info!(
            "Sending initial insert events for TestRunSource {}",
            self.settings.id
        );

        let now_ns = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let stock_market = self.stock_market.lock().await;
        let all_labels = HashSet::new();

        let mut insert_events = Vec::new();

        for change in stock_market.get_current_state(&all_labels) {
            let event = match change {
                ModelChange::StockAdded(stock) => Some(SourceChangeEvent {
                    op: "i".to_string(),
                    reactivator_start_ns: now_ns,
                    reactivator_end_ns: 0,
                    payload: SourceChangeEventPayload {
                        source: SourceChangeEventSourceInfo {
                            db: self.settings.id.test_source_id.to_string(),
                            lsn: self.event_seq_num,
                            table: "node".to_string(),
                            ts_ns: self.virtual_time_ns_current,
                        },
                        before: serde_json::Value::Null,
                        after: serde_json::json!({
                            "id": stock.id,
                            "labels": stock.labels,
                            "properties": stock.properties
                        }),
                    },
                }),
                _ => None,
            };

            if let Some(event) = event {
                insert_events.push(event);
                self.event_seq_num += 1;
            }
        }

        drop(stock_market);

        if !insert_events.is_empty() {
            log::info!("Dispatching {} initial insert events", insert_events.len());
            let events_refs: Vec<&SourceChangeEvent> = insert_events.iter().collect();
            self.dispatch_source_change_events(events_refs).await?;
            self.stats.num_source_change_events += insert_events.len() as u64;
        }

        Ok(())
    }

    fn set_test_run_host_on_dispatchers(
        &mut self,
        test_run_host: std::sync::Arc<crate::TestRunHost>,
    ) {
        log::info!(
            "Setting TestRunHost on {} dispatchers for source {}",
            self.dispatchers.len(),
            self.settings.id
        );

        for dispatcher in self.dispatchers.iter_mut() {
            dispatcher.set_test_run_host(test_run_host.clone());
        }
    }

    async fn dispatch_source_change_events(
        &mut self,
        events: Vec<&SourceChangeEvent>,
    ) -> anyhow::Result<()> {
        let dispatchers = &mut self.dispatchers;

        log::debug!(
            "Dispatching SourceChangeEvents - #dispatchers:{}, #events:{}",
            dispatchers.len(),
            events.len()
        );

        let futures: Vec<_> = dispatchers
            .iter_mut()
            .map(|dispatcher| {
                let events = events.clone();
                async move { dispatcher.dispatch_source_change_events(events).await }
            })
            .collect();

        join_all(futures)
            .await
            .into_iter()
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(())
    }

    fn log_state(&self, msg: &str) {
        match log::max_level() {
            log::LevelFilter::Trace => log::trace!("{msg} - {self:#?}"),
            log::LevelFilter::Debug => log::debug!("{msg} - {self:?}"),
            _ => {}
        }
    }

    async fn process_change_stream_message(
        &mut self,
        message: ScheduledChangeEventMessage,
    ) -> anyhow::Result<()> {
        log::debug!("Processing next source change event: {message:?}");

        self.virtual_time_ns_current = self.virtual_time_ns_next;

        let source_change_event = match self.next_event.as_mut() {
            Some(source_change_event) => {
                let now_ns = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;

                source_change_event.reactivator_end_ns = now_ns;
                source_change_event.clone()
            }
            None => {
                self.transition_to_error_state("No next_event to process", None);
                anyhow::bail!("No next_event to process");
            }
        };

        match &mut self.status {
            SourceChangeGeneratorStatus::Running => {
                self.dispatch_source_change_events(vec![&source_change_event])
                    .await?;

                self.previous_event = Some(ProcessedChangeEvent {
                    dispatch_status: self.status,
                    event: source_change_event,
                    seq: message.seq_num,
                });
                self.event_seq_num += 1;
                self.stats.num_source_change_events += 1;

                if self.stats.num_source_change_events >= self.settings.change_count {
                    self.transition_to_finished_state().await?;
                } else {
                    self.schedule_next_change_event().await?;
                }
            }
            SourceChangeGeneratorStatus::Stepping => {
                if self.steps_remaining > 0 {
                    self.dispatch_source_change_events(vec![&source_change_event])
                        .await?;

                    self.previous_event = Some(ProcessedChangeEvent {
                        dispatch_status: self.status,
                        event: source_change_event,
                        seq: message.seq_num,
                    });
                    self.event_seq_num += 1;
                    self.stats.num_source_change_events += 1;

                    if self.stats.num_source_change_events >= self.settings.change_count {
                        self.transition_to_finished_state().await?;
                    } else {
                        self.steps_remaining -= 1;
                        if self.steps_remaining == 0 {
                            self.status = SourceChangeGeneratorStatus::Paused;
                            self.schedule_next_change_event().await?;
                        } else {
                            self.schedule_next_change_event().await?;
                        }
                    }
                } else {
                    self.transition_to_error_state("Stepping with no steps remaining", None);
                }
            }
            SourceChangeGeneratorStatus::Skipping => {
                if self.skips_remaining > 0 {
                    log::trace!("Skipping ChangeScriptRecord: {source_change_event:?}");

                    self.previous_event = Some(ProcessedChangeEvent {
                        dispatch_status: self.status,
                        event: source_change_event,
                        seq: message.seq_num,
                    });
                    self.event_seq_num += 1;
                    self.stats.num_source_change_events += 1;
                    self.stats.num_skipped_source_change_events += 1;

                    if self.stats.num_source_change_events >= self.settings.change_count {
                        self.transition_to_finished_state().await?;
                    } else {
                        self.skips_remaining -= 1;
                        if self.skips_remaining == 0 {
                            self.status = SourceChangeGeneratorStatus::Paused;
                            self.schedule_next_change_event().await?;
                        } else {
                            self.schedule_next_change_event().await?;
                        }
                    }
                } else {
                    self.transition_to_error_state("Skipping with no skips remaining", None);
                }
            }
            _ => {
                self.transition_to_error_state(
                    "Unexpected status for SourceChange processing",
                    None,
                );
            }
        };

        Ok(())
    }

    async fn process_command_message(
        &mut self,
        message: StockTradeDataGeneratorMessage,
    ) -> anyhow::Result<()> {
        log::debug!("Received command message: {:?}", message.command);

        if let StockTradeDataGeneratorCommand::GetState = message.command {
            let message_response = StockTradeDataGeneratorMessageResponse {
                result: Ok(()),
                state: self.into(),
            };

            let r = message.response_tx.unwrap().send(message_response);
            if let Err(e) = r {
                anyhow::bail!("Error sending message response back to caller: {e:?}");
            }
        } else {
            let transition_response = match self.status {
                SourceChangeGeneratorStatus::Running => {
                    self.transition_from_running_state(&message.command).await
                }
                SourceChangeGeneratorStatus::Stepping => {
                    self.transition_from_stepping_state(&message.command).await
                }
                SourceChangeGeneratorStatus::Skipping => {
                    self.transition_from_skipping_state(&message.command).await
                }
                SourceChangeGeneratorStatus::Paused => {
                    self.transition_from_paused_state(&message.command).await
                }
                SourceChangeGeneratorStatus::Stopped => {
                    self.transition_from_stopped_state(&message.command).await
                }
                SourceChangeGeneratorStatus::Finished => {
                    self.transition_from_finished_state(&message.command).await
                }
                SourceChangeGeneratorStatus::Error => {
                    self.transition_from_error_state(&message.command).await
                }
            };

            if message.response_tx.is_some() {
                let message_response = StockTradeDataGeneratorMessageResponse {
                    result: transition_response,
                    state: self.into(),
                };

                let r = message.response_tx.unwrap().send(message_response);
                if let Err(e) = r {
                    anyhow::bail!("Error sending message response back to caller: {e:?}");
                }
            }
        }

        Ok(())
    }

    async fn reset(&mut self) -> anyhow::Result<()> {
        log::debug!("Resetting StockTradeDataGenerator");

        self.close_dispatchers().await?;
        let mut dispatchers: Vec<Box<dyn SourceChangeDispatcher + Send>> = Vec::new();
        for def in self.settings.dispatchers.iter() {
            match create_source_change_dispatcher(def, &self.settings.output_storage).await {
                Ok(dispatcher) => dispatchers.push(dispatcher),
                Err(e) => {
                    anyhow::bail!("Error creating SourceChangeDispatcher: {def:?}; Error: {e:?}");
                }
            }
        }

        self.stock_market = Arc::new(Mutex::new(StockMarket::new(&self.settings)?));
        self.change_interval_generator =
            ChangeIntervalGenerator::new(self.settings.seed, self.settings.change_interval)?;
        self.dispatchers = dispatchers;
        self.error_messages = Vec::new();
        self.event_seq_num = 0;
        self.next_event = None;
        self.previous_event = None;
        self.skips_remaining = 0;
        self.status = SourceChangeGeneratorStatus::Paused;
        self.stats = StockTradeDataGeneratorStats::default();
        self.steps_remaining = 0;
        self.virtual_time_ns_current = 0;
        self.virtual_time_ns_next = 0;
        self.virtual_time_ns_rebase_adjustment = 0;
        self.virtual_time_ns_start = 0;

        Ok(())
    }

    async fn schedule_next_change_event(&mut self) -> anyhow::Result<()> {
        log::debug!("Scheduling next change event");

        self.rate_limiter.until_ready().await;

        let now_ns = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        if self.previous_event.is_none() {
            self.stats.actual_start_time_ns = now_ns;

            match self.settings.time_mode {
                TimeMode::Live => {
                    self.virtual_time_ns_start = now_ns;
                    self.virtual_time_ns_current = now_ns;
                    self.virtual_time_ns_next = now_ns;
                    self.virtual_time_ns_rebase_adjustment = 0;
                }
                TimeMode::Rebased(base_ns) => {
                    self.virtual_time_ns_start = base_ns;
                    self.virtual_time_ns_current = base_ns;
                    self.virtual_time_ns_next = base_ns;
                    self.virtual_time_ns_rebase_adjustment = base_ns as i64 - now_ns as i64;
                }
                TimeMode::Recorded => {
                    self.virtual_time_ns_start = now_ns;
                    self.virtual_time_ns_current = now_ns;
                    self.virtual_time_ns_next = now_ns;
                    self.virtual_time_ns_rebase_adjustment = 0;
                }
            }
        } else {
            self.virtual_time_ns_next =
                self.virtual_time_ns_current + self.change_interval_generator.next();
        };

        let update = {
            let stock_market = &mut self.stock_market.lock().await;
            stock_market.generate_update(self.virtual_time_ns_next)?
        };

        let next_event = match update {
            Some(model_change) => match model_change {
                ModelChange::StockUpdated(stock_before, stock_after) => SourceChangeEvent {
                    op: "u".to_string(),
                    reactivator_start_ns: now_ns,
                    reactivator_end_ns: 0,
                    payload: SourceChangeEventPayload {
                        source: SourceChangeEventSourceInfo {
                            db: self.settings.id.test_source_id.to_string(),
                            lsn: self.event_seq_num,
                            table: "node".to_string(),
                            ts_ns: self.virtual_time_ns_next,
                        },
                        before: serde_json::json!(stock_before),
                        after: serde_json::json!(stock_after),
                    },
                },
                _ => {
                    anyhow::bail!("Unexpected model change: {model_change:?}");
                }
            },
            None => {
                anyhow::bail!("No model change generated");
            }
        };
        self.next_event = Some(next_event);

        let sch_msg = ScheduledChangeEventMessage {
            delay_ns: self.virtual_time_ns_next - self.virtual_time_ns_current,
            seq_num: self.event_seq_num,
        };

        if self.status.is_processing() {
            if let Err(e) = self.change_tx_channel.send(sch_msg).await {
                anyhow::bail!("Error sending ScheduledChangeEventMessage: {e:?}");
            }
        } else {
            log::error!("Not sending ScheduledChangeEventMessage: {sch_msg:?}");
        }

        Ok(())
    }

    async fn transition_from_error_state(
        &mut self,
        command: &StockTradeDataGeneratorCommand,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Attempting to transition from {:?} state via command: {:?}",
            self.status,
            command
        );

        match command {
            StockTradeDataGeneratorCommand::Reset => self.reset().await,
            StockTradeDataGeneratorCommand::SetTestRunHost { test_run_host } => {
                self.set_test_run_host_on_dispatchers(test_run_host.clone());
                Ok(())
            }
            _ => Err(StockTradeDataGeneratorError::Error(self.status).into()),
        }
    }

    async fn transition_from_finished_state(
        &mut self,
        command: &StockTradeDataGeneratorCommand,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Attempting to transition from {:?} state via command: {:?}",
            self.status,
            command
        );

        match command {
            StockTradeDataGeneratorCommand::Reset => self.reset().await,
            StockTradeDataGeneratorCommand::SetTestRunHost { test_run_host } => {
                self.set_test_run_host_on_dispatchers(test_run_host.clone());
                Ok(())
            }
            _ => Err(StockTradeDataGeneratorError::AlreadyFinished.into()),
        }
    }

    async fn transition_from_paused_state(
        &mut self,
        command: &StockTradeDataGeneratorCommand,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Transitioning from {:?} state via command: {:?}",
            self.status,
            command
        );

        match command {
            StockTradeDataGeneratorCommand::GetState => Ok(()),
            StockTradeDataGeneratorCommand::Pause => Ok(()),
            StockTradeDataGeneratorCommand::Reset => self.reset().await,
            StockTradeDataGeneratorCommand::Skip { skips, .. } => {
                log::info!(
                    "Script Skipping {} skips for TestRunSource {}",
                    skips,
                    self.settings.id
                );

                self.status = SourceChangeGeneratorStatus::Skipping;
                self.skips_remaining = *skips;
                self.schedule_next_change_event().await
            }
            StockTradeDataGeneratorCommand::Start => {
                log::info!("Script Started for TestRunSource {}", self.settings.id);

                self.status = SourceChangeGeneratorStatus::Running;

                if self.settings.send_initial_inserts {
                    if let Err(error) = self.send_initial_inserts().await {
                        self.transition_to_error_state(
                            "Failed to send initial inserts",
                            Some(&error),
                        );
                        return Err(error);
                    }
                }

                self.schedule_next_change_event().await
            }
            StockTradeDataGeneratorCommand::Step { steps, .. } => {
                log::info!(
                    "Script Stepping {} steps for TestRunSource {}",
                    steps,
                    self.settings.id
                );

                self.status = SourceChangeGeneratorStatus::Stepping;
                self.steps_remaining = *steps;
                self.schedule_next_change_event().await
            }
            StockTradeDataGeneratorCommand::Stop => {
                self.transition_to_stopped_state().await?;
                Ok(())
            }
            StockTradeDataGeneratorCommand::SetTestRunHost { test_run_host } => {
                self.set_test_run_host_on_dispatchers(test_run_host.clone());
                Ok(())
            }
        }
    }

    async fn transition_from_running_state(
        &mut self,
        command: &StockTradeDataGeneratorCommand,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Transitioning from {:?} state via command: {:?}",
            self.status,
            command
        );

        match command {
            StockTradeDataGeneratorCommand::GetState => Ok(()),
            StockTradeDataGeneratorCommand::Pause => {
                self.status = SourceChangeGeneratorStatus::Paused;
                Ok(())
            }
            StockTradeDataGeneratorCommand::Reset => {
                Err(StockTradeDataGeneratorError::PauseToReset.into())
            }
            StockTradeDataGeneratorCommand::Skip { .. } => {
                Err(StockTradeDataGeneratorError::PauseToSkip.into())
            }
            StockTradeDataGeneratorCommand::Start => Ok(()),
            StockTradeDataGeneratorCommand::Step { .. } => {
                Err(StockTradeDataGeneratorError::PauseToStep.into())
            }
            StockTradeDataGeneratorCommand::Stop => {
                self.transition_to_stopped_state().await?;
                Ok(())
            }
            StockTradeDataGeneratorCommand::SetTestRunHost { test_run_host } => {
                self.set_test_run_host_on_dispatchers(test_run_host.clone());
                Ok(())
            }
        }
    }

    async fn transition_from_skipping_state(
        &mut self,
        command: &StockTradeDataGeneratorCommand,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Transitioning from {:?} state via command: {:?}",
            self.status,
            command
        );

        match command {
            StockTradeDataGeneratorCommand::GetState => Ok(()),
            StockTradeDataGeneratorCommand::Pause => {
                self.status = SourceChangeGeneratorStatus::Paused;
                self.skips_remaining = 0;
                Ok(())
            }
            StockTradeDataGeneratorCommand::Stop => {
                self.transition_to_stopped_state().await?;
                Ok(())
            }
            StockTradeDataGeneratorCommand::Reset
            | StockTradeDataGeneratorCommand::Skip { .. }
            | StockTradeDataGeneratorCommand::Start
            | StockTradeDataGeneratorCommand::Step { .. } => {
                Err(StockTradeDataGeneratorError::CurrentlySkipping(self.skips_remaining).into())
            }
            StockTradeDataGeneratorCommand::SetTestRunHost { test_run_host } => {
                self.set_test_run_host_on_dispatchers(test_run_host.clone());
                Ok(())
            }
        }
    }

    async fn transition_from_stepping_state(
        &mut self,
        command: &StockTradeDataGeneratorCommand,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Transitioning from {:?} state via command: {:?}",
            self.status,
            command
        );

        match command {
            StockTradeDataGeneratorCommand::GetState => Ok(()),
            StockTradeDataGeneratorCommand::Pause => {
                self.status = SourceChangeGeneratorStatus::Paused;
                self.steps_remaining = 0;
                Ok(())
            }
            StockTradeDataGeneratorCommand::Stop => {
                self.transition_to_stopped_state().await?;
                Ok(())
            }
            StockTradeDataGeneratorCommand::Reset
            | StockTradeDataGeneratorCommand::Skip { .. }
            | StockTradeDataGeneratorCommand::Start
            | StockTradeDataGeneratorCommand::Step { .. } => {
                Err(StockTradeDataGeneratorError::CurrentlyStepping(self.steps_remaining).into())
            }
            StockTradeDataGeneratorCommand::SetTestRunHost { test_run_host } => {
                self.set_test_run_host_on_dispatchers(test_run_host.clone());
                Ok(())
            }
        }
    }

    async fn transition_from_stopped_state(
        &mut self,
        command: &StockTradeDataGeneratorCommand,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Attempting to transition from {:?} state via command: {:?}",
            self.status,
            command
        );

        match command {
            StockTradeDataGeneratorCommand::Reset => self.reset().await,
            StockTradeDataGeneratorCommand::SetTestRunHost { test_run_host } => {
                self.set_test_run_host_on_dispatchers(test_run_host.clone());
                Ok(())
            }
            _ => Err(StockTradeDataGeneratorError::AlreadyStopped.into()),
        }
    }

    async fn transition_to_finished_state(&mut self) -> anyhow::Result<()> {
        log::info!("Script Finished for TestRunSource {}", self.settings.id);

        self.status = SourceChangeGeneratorStatus::Finished;
        self.stats.actual_end_time_ns = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        self.skips_remaining = 0;
        self.steps_remaining = 0;

        let close_result = self.close_dispatchers().await;
        if let Err(error) = &close_result {
            self.transition_to_error_state("Failed to close source dispatchers", Some(error));
        }
        self.write_result_summary().await.ok();
        close_result
    }

    async fn transition_to_stopped_state(&mut self) -> anyhow::Result<()> {
        log::info!("Script Stopped for TestRunSource {}", self.settings.id);

        self.status = SourceChangeGeneratorStatus::Stopped;
        self.stats.actual_end_time_ns = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        self.skips_remaining = 0;
        self.steps_remaining = 0;

        let close_result = self.close_dispatchers().await;
        if let Err(error) = &close_result {
            self.transition_to_error_state("Failed to close source dispatchers", Some(error));
        }
        self.write_result_summary().await.ok();
        close_result
    }

    fn transition_to_error_state(&mut self, error_message: &str, error: Option<&anyhow::Error>) {
        self.status = SourceChangeGeneratorStatus::Error;

        let msg = match error {
            Some(e) => format!("{error_message}: {e:?}"),
            None => error_message.to_string(),
        };

        self.log_state(&msg);

        self.error_messages.push(msg);
    }

    pub async fn write_result_summary(&mut self) -> anyhow::Result<()> {
        let result_summary: StockTradeDataGeneratorResultSummary = self.into();
        log::info!("Stats for TestRunSource:\n{:#?}", &result_summary);

        let result_summary_value = serde_json::to_value(result_summary).unwrap();
        match self
            .settings
            .output_storage
            .write_test_run_summary(&result_summary_value)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                log::error!("Error writing result summary to output storage: {e:?}");
                Err(e)
            }
        }
    }
}

impl Debug for StockTradeDataGeneratorInternalState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("StockTradeDataGeneratorInternalState")
            .field("error_messages", &self.error_messages)
            .field("event_seq_num", &self.event_seq_num)
            .field("next_event", &self.next_event)
            .field("previous_record", &self.previous_event)
            .field("settings", &self.settings)
            .field("skips_remaining", &self.skips_remaining)
            .field("spacing_mode", &self.settings.spacing_mode)
            .field("status", &self.status)
            .field("stats", &self.stats)
            .field("steps_remaining", &self.steps_remaining)
            .field("time_mode", &self.settings.time_mode)
            .field("virtual_time_ns_current", &self.virtual_time_ns_current)
            .field("virtual_time_ns_next", &self.virtual_time_ns_next)
            .field(
                "virtual_time_ns_rebase_adjustment",
                &self.virtual_time_ns_rebase_adjustment,
            )
            .field("virtual_time_ns_start", &self.virtual_time_ns_start)
            .finish()
    }
}

#[derive(Clone, Debug, Serialize, Default)]
pub struct StockTradeDataGeneratorStats {
    pub actual_start_time_ns: u64,
    pub actual_end_time_ns: u64,
    pub num_source_change_events: u64,
    pub num_skipped_source_change_events: u64,
}

#[derive(Clone, Serialize)]
pub struct StockTradeDataGeneratorResultSummary {
    pub actual_start_time: String,
    pub actual_start_time_ns: u64,
    pub actual_end_time: String,
    pub actual_end_time_ns: u64,
    pub run_duration_ns: u64,
    pub run_duration_sec: f64,
    pub num_source_change_events: u64,
    pub num_skipped_source_events: u64,
    pub processing_rate: f64,
    pub test_run_source_id: String,
}

impl From<&mut StockTradeDataGeneratorInternalState> for StockTradeDataGeneratorResultSummary {
    fn from(state: &mut StockTradeDataGeneratorInternalState) -> Self {
        let run_duration_ns = state.stats.actual_end_time_ns - state.stats.actual_start_time_ns;
        let run_duration_sec = run_duration_ns as f64 / 1_000_000_000.0;

        Self {
            actual_start_time: OffsetDateTime::from_unix_timestamp_nanos(
                state.stats.actual_start_time_ns as i128,
            )
            .expect("Invalid timestamp")
            .format(&format_description::well_known::Rfc3339)
            .unwrap(),
            actual_start_time_ns: state.stats.actual_start_time_ns,
            actual_end_time: OffsetDateTime::from_unix_timestamp_nanos(
                state.stats.actual_end_time_ns as i128,
            )
            .expect("Invalid timestamp")
            .format(&format_description::well_known::Rfc3339)
            .unwrap(),
            actual_end_time_ns: state.stats.actual_end_time_ns,
            run_duration_ns,
            run_duration_sec,
            num_source_change_events: state.stats.num_source_change_events,
            num_skipped_source_events: state.stats.num_skipped_source_change_events,
            processing_rate: state.stats.num_source_change_events as f64 / run_duration_sec,
            test_run_source_id: state.settings.id.to_string(),
        }
    }
}

impl Debug for StockTradeDataGeneratorResultSummary {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let start_time = format!(
            "{} ({} ns)",
            self.actual_start_time, self.actual_start_time_ns
        );
        let end_time = format!("{} ({} ns)", self.actual_end_time, self.actual_end_time_ns);
        let run_duration = format!(
            "{} sec ({} ns)",
            self.run_duration_sec, self.run_duration_ns,
        );
        let source_change_events = format!(
            "{} (skipped:{})",
            self.num_source_change_events, self.num_skipped_source_events
        );
        let processing_rate = format!("{:.2} changes / sec", self.processing_rate);

        f.debug_struct("StockTradeDataGeneratorResultSummary")
            .field("test_run_source_id", &self.test_run_source_id)
            .field("start_time", &start_time)
            .field("end_time", &end_time)
            .field("run_duration", &run_duration)
            .field("source_change_events", &source_change_events)
            .field("processing_rate", &processing_rate)
            .finish()
    }
}

pub async fn model_host_thread(
    mut command_rx_channel: Receiver<StockTradeDataGeneratorMessage>,
    settings: StockTradeDataGeneratorSettings,
    stock_market: Arc<Mutex<StockMarket>>,
) -> anyhow::Result<()> {
    log::info!(
        "Script processor thread started for TestRunSource {} ...",
        settings.id
    );

    let (mut state, mut change_rx_channel) =
        match StockTradeDataGeneratorInternalState::initialize(settings, stock_market).await {
            Ok((state, change_rx_channel)) => (state, change_rx_channel),
            Err(e) => {
                let msg = format!("Error initializing StockTradeDataGenerator: {e:?}");
                log::error!("{msg}");
                anyhow::bail!(msg);
            }
        };

    loop {
        state.log_state("Top of script processor loop");

        tokio::select! {
            biased;

            command_message = command_rx_channel.recv() => {
                match command_message {
                    Some(command_message) => {
                        state.process_command_message(command_message).await
                            .inspect_err(|e| state.transition_to_error_state("Error calling process_command_message.", Some(e))).ok();
                    }
                    None => {
                        state.transition_to_error_state("Command channel closed.", None);
                        break;
                    }
                }
            },

            change_stream_message = change_rx_channel.recv() => {
                match change_stream_message {
                    Some(change_stream_message) => {
                        log::trace!("Received change stream message: {change_stream_message:?}");
                        if change_stream_message.seq_num == state.event_seq_num && state.status.is_processing() {
                            state.process_change_stream_message(change_stream_message).await
                                .inspect_err(|e| {
                                    if state.status != SourceChangeGeneratorStatus::Error {
                                        state.transition_to_error_state(
                                            "Error calling process_change_stream_message",
                                            Some(e),
                                        );
                                    }
                                }).ok();
                        }
                    }
                    None => {
                        state.transition_to_error_state("Change stream channel closed.", None);
                        break;
                    }
                }
            },

            else => {
                log::error!("Script processor loop activated for {} but no command or change to process.", state.settings.id);
            }
        }
    }

    log::info!(
        "Script processor thread exiting for TestRunSource {} ...",
        state.settings.id
    );
    Ok(())
}
