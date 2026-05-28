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
use building_graph::{BuildingGraph, GraphElementType, ModelChange};
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
        NodeRecord, RelationRecord, SourceChangeEvent, SourceChangeEventPayload,
        SourceChangeEventSourceInfo,
    },
    test_repo_storage::{
        models::{
            BuildingHierarchyDataGeneratorDefinition, SensorDefinition,
            SourceChangeDispatcherDefinition, SpacingMode, TimeMode,
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

mod building_graph;

#[derive(Debug, thiserror::Error)]
pub enum BuildingHierarchyDataGeneratorError {
    #[error("BuildingHierarchyDataGenerator is already finished. Reset to start over.")]
    AlreadyFinished,
    #[error("BuildingHierarchyDataGenerator is already stopped. Reset to start over.")]
    AlreadyStopped,
    #[error("BuildingHierarchyDataGenerator is currently Skipping. {0} skips remaining. Pause before Skip, Step, or Reset.")]
    CurrentlySkipping(u64),
    #[error("BuildingHierarchyDataGenerator is currently Stepping. {0} steps remaining. Pause before Skip, Step, or Reset.")]
    CurrentlyStepping(u64),
    #[error("BuildingHierarchyDataGenerator is currently in an Error state - {0:?}")]
    Error(SourceChangeGeneratorStatus),
    #[error("BuildingHierarchyDataGenerator is currently Running. Pause before trying to Skip.")]
    PauseToSkip,
    #[error("BuildingHierarchyDataGenerator is currently Running. Pause before trying to Step.")]
    PauseToStep,
    #[error("BuildingHierarchyDataGenerator is currently Running. Pause before trying to Reset.")]
    PauseToReset,
}

#[derive(Clone, Debug, Serialize)]
pub struct BuildingHierarchyDataGeneratorSettings {
    pub building_count: (u32, f64),
    pub floor_count: (u32, f64),
    pub room_count: (u32, f64),
    pub change_count: u64,
    pub change_interval: (u64, f64, u64, u64),
    pub dispatchers: Vec<SourceChangeDispatcherDefinition>,
    pub id: TestRunSourceId,
    pub input_storage: TestSourceStorage,
    pub output_storage: TestRunSourceStorage,
    pub room_sensors: Vec<SensorDefinition>,
    pub seed: u64,
    pub spacing_mode: SpacingMode,
    pub time_mode: TimeMode,
    pub send_initial_inserts: bool,
}

impl BuildingHierarchyDataGeneratorSettings {
    pub async fn new(
        test_run_source_id: TestRunSourceId,
        definition: BuildingHierarchyDataGeneratorDefinition,
        input_storage: TestSourceStorage,
        output_storage: TestRunSourceStorage,
        dispatchers: Vec<SourceChangeDispatcherDefinition>,
    ) -> anyhow::Result<Self> {
        Ok(BuildingHierarchyDataGeneratorSettings {
            building_count: definition.building_count.unwrap_or((1, 0.0)),
            floor_count: definition.floor_count.unwrap_or((5, 0.0)),
            room_count: definition.room_count.unwrap_or((10, 0.0)),
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
            room_sensors: definition.room_sensors,
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

// Enum of BuildingHierarchyDataGenerator commands sent from Web API handler functions.
#[derive(Debug)]
pub enum BuildingHierarchyDataGeneratorCommand {
    // Command to get the current state of the BuildingHierarchyDataGenerator.
    GetState,
    // Command to pause the BuildingHierarchyDataGenerator.
    Pause,
    // Command to reset the BuildingHierarchyDataGenerator.
    Reset,
    // Command to skip the BuildingHierarchyDataGenerator forward a specified number of ChangeScriptRecords.
    Skip {
        skips: u64,
        spacing_mode: Option<SpacingMode>,
    },
    // Command to start the BuildingHierarchyDataGenerator.
    Start,
    // Command to step the BuildingHierarchyDataGenerator forward a specified number of ChangeScriptRecords.
    Step {
        steps: u64,
        spacing_mode: Option<SpacingMode>,
    },
    // Command to stop the BuildingHierarchyDataGenerator.
    Stop,
    // Command to set TestRunHost on dispatchers
    SetTestRunHost {
        test_run_host: std::sync::Arc<crate::TestRunHost>,
    },
}

// Struct for messages sent to the BuildingHierarchyDataGenerator from the functions in the Web API.
#[derive(Debug)]
pub struct BuildingHierarchyDataGeneratorMessage {
    // Command sent to the BuildingHierarchyDataGenerator.
    pub command: BuildingHierarchyDataGeneratorCommand,
    // One-shot channel for BuildingHierarchyDataGenerator to send a response back to the caller.
    pub response_tx: Option<oneshot::Sender<BuildingHierarchyDataGeneratorMessageResponse>>,
}

// A struct for the Response sent back from the BuildingHierarchyDataGenerator to the calling Web API handler.
#[derive(Debug)]
pub struct BuildingHierarchyDataGeneratorMessageResponse {
    // Result of the command.
    pub result: anyhow::Result<()>,
    // State of the BuildingHierarchyDataGenerator after the command.
    pub state: BuildingHierarchyDataGeneratorExternalState,
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
pub struct BuildingHierarchyDataGenerator {
    #[serde(skip_serializing)]
    building_graph: Arc<Mutex<BuildingGraph>>,
    settings: BuildingHierarchyDataGeneratorSettings,
    #[serde(skip_serializing)]
    model_host_tx_channel: Sender<BuildingHierarchyDataGeneratorMessage>,
    #[serde(skip_serializing)]
    _model_host_thread_handle: Arc<Mutex<JoinHandle<anyhow::Result<()>>>>,
}

impl BuildingHierarchyDataGenerator {
    pub async fn new(
        test_run_source_id: TestRunSourceId,
        definition: BuildingHierarchyDataGeneratorDefinition,
        input_storage: TestSourceStorage,
        output_storage: TestRunSourceStorage,
        dispatchers: Vec<SourceChangeDispatcherDefinition>,
    ) -> anyhow::Result<Self> {
        let settings = BuildingHierarchyDataGeneratorSettings::new(
            test_run_source_id,
            definition,
            input_storage,
            output_storage.clone(),
            dispatchers,
        )
        .await?;
        log::debug!(
            "Creating BuildingHierarchyDataGenerator from {:?}",
            &settings
        );

        let building_graph = Arc::new(Mutex::new(BuildingGraph::new(&settings)?));

        let (model_host_tx_channel, model_host_rx_channel) = tokio::sync::mpsc::channel(500);
        let model_host_thread_handle = tokio::spawn(model_host_thread(
            model_host_rx_channel,
            settings.clone(),
            building_graph.clone(),
        ));

        Ok(Self {
            building_graph,
            settings,
            model_host_tx_channel,
            _model_host_thread_handle: Arc::new(Mutex::new(model_host_thread_handle)),
        })
    }

    pub fn get_id(&self) -> TestRunSourceId {
        self.settings.get_id()
    }

    pub fn get_settings(&self) -> BuildingHierarchyDataGeneratorSettings {
        self.settings.clone()
    }

    async fn send_command(
        &self,
        command: BuildingHierarchyDataGeneratorCommand,
    ) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        let (response_tx, response_rx) = oneshot::channel();

        let r = self
            .model_host_tx_channel
            .send(BuildingHierarchyDataGeneratorMessage {
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
            Err(e) => {
                anyhow::bail!("Error sending command to BuildingHierarchyDataGenerator: {e:?}")
            }
        }
    }
}

#[async_trait]
impl BootstrapDataGenerator for BuildingHierarchyDataGenerator {
    async fn get_data(
        &self,
        node_labels: &HashSet<String>,
        rel_labels: &HashSet<String>,
    ) -> anyhow::Result<BootstrapData> {
        log::debug!("Node labels: [{node_labels:?}], Rel labels: [{rel_labels:?}]");

        let mut building_nodes = Vec::new();
        let mut floor_nodes = Vec::new();
        let mut room_nodes = Vec::new();
        let mut building_floor_rels = Vec::new();
        let mut floor_room_rels = Vec::new();

        let building_graph = self.building_graph.lock().await;
        for change in building_graph.get_current_state(node_labels) {
            match change {
                ModelChange::BuildingAdded(building) => {
                    let node_record = NodeRecord {
                        id: building.id,
                        labels: building.labels.clone(),
                        properties: serde_json::json!({}),
                    };
                    building_nodes.push(node_record);
                }
                ModelChange::FloorAdded(floor) => {
                    let node_record = NodeRecord {
                        id: floor.id,
                        labels: floor.labels.clone(),
                        properties: serde_json::json!({}),
                    };
                    floor_nodes.push(node_record);
                }
                ModelChange::RoomAdded(room) => {
                    let node_record = NodeRecord {
                        id: room.id,
                        labels: room.labels.clone(),
                        properties: serde_json::json!(room.properties),
                    };
                    room_nodes.push(node_record);
                }
                _ => {
                    log::debug!("Other change: {change:?}");
                }
            }
        }

        for change in building_graph.get_current_state(rel_labels) {
            match change {
                ModelChange::BuildingFloorRelationAdded(relation) => {
                    let rel_record = RelationRecord {
                        id: relation.id,
                        labels: relation.labels.clone(),
                        properties: serde_json::json!({}),
                        start_id: relation.building_id,
                        start_label: Some(GraphElementType::BUILDING.to_string()),
                        end_id: relation.floor_id,
                        end_label: Some(GraphElementType::FLOOR.to_string()),
                    };
                    building_floor_rels.push(rel_record);
                }
                ModelChange::FloorRoomRelationAdded(relation) => {
                    let rel_record = RelationRecord {
                        id: relation.id,
                        labels: relation.labels.clone(),
                        properties: serde_json::json!({}),
                        start_id: relation.floor_id,
                        start_label: Some(GraphElementType::FLOOR.to_string()),
                        end_id: relation.room_id,
                        end_label: Some(GraphElementType::ROOM.to_string()),
                    };
                    floor_room_rels.push(rel_record);
                }
                _ => {
                    log::debug!("Other change: {change:?}");
                }
            }
        }

        let mut bootstrap_data = BootstrapData::new();

        if !building_nodes.is_empty() {
            bootstrap_data
                .nodes
                .insert(GraphElementType::BUILDING.to_string(), building_nodes);
        }
        if !floor_nodes.is_empty() {
            bootstrap_data
                .nodes
                .insert(GraphElementType::FLOOR.to_string(), floor_nodes);
        }
        if !room_nodes.is_empty() {
            bootstrap_data
                .nodes
                .insert(GraphElementType::ROOM.to_string(), room_nodes);
        }
        if !building_floor_rels.is_empty() {
            bootstrap_data.rels.insert(
                GraphElementType::BUILDING_FLOOR.to_string(),
                building_floor_rels,
            );
        }
        if !floor_room_rels.is_empty() {
            bootstrap_data
                .rels
                .insert(GraphElementType::FLOOR_ROOM.to_string(), floor_room_rels);
        }

        Ok(bootstrap_data)
    }
}

#[async_trait]
impl SourceChangeGenerator for BuildingHierarchyDataGenerator {
    async fn get_state(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(BuildingHierarchyDataGeneratorCommand::GetState)
            .await
    }

    async fn pause(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(BuildingHierarchyDataGeneratorCommand::Pause)
            .await
    }

    async fn reset(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(BuildingHierarchyDataGeneratorCommand::Reset)
            .await
    }

    async fn skip(
        &self,
        skips: u64,
        spacing_mode: Option<SpacingMode>,
    ) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(BuildingHierarchyDataGeneratorCommand::Skip {
            skips,
            spacing_mode,
        })
        .await
    }

    async fn start(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(BuildingHierarchyDataGeneratorCommand::Start)
            .await
    }

    async fn step(
        &self,
        steps: u64,
        spacing_mode: Option<SpacingMode>,
    ) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(BuildingHierarchyDataGeneratorCommand::Step {
            steps,
            spacing_mode,
        })
        .await
    }

    async fn stop(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(BuildingHierarchyDataGeneratorCommand::Stop)
            .await
    }

    fn set_test_run_host_on_dispatchers(&self, test_run_host: std::sync::Arc<crate::TestRunHost>) {
        // Send command to thread to set TestRunHost on dispatchers
        log::info!("BuildingHierarchyDataGenerator: Sending SetTestRunHost command to thread");

        // Use a blocking task to send the command since this is a sync function
        let tx = self.model_host_tx_channel.clone();
        let command = BuildingHierarchyDataGeneratorCommand::SetTestRunHost { test_run_host };

        tokio::task::spawn(async move {
            if let Err(e) = tx
                .send(BuildingHierarchyDataGeneratorMessage {
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
impl ModelDataGenerator for BuildingHierarchyDataGenerator {}

#[derive(Debug, Serialize)]
pub struct BuildingHierarchyDataGeneratorExternalState {
    pub error_messages: Vec<String>,
    pub event_seq_num: u64,
    pub next_event: Option<SourceChangeEvent>,
    pub previous_event: Option<ProcessedChangeEvent>,
    pub skips_remaining: u64,
    pub spacing_mode: SpacingMode,
    pub stats: BuildingHierarchyDataGeneratorStats,
    pub status: SourceChangeGeneratorStatus,
    pub steps_remaining: u64,
    pub test_run_source_id: TestRunSourceId,
    pub time_mode: TimeMode,
    pub virtual_time_ns_current: u64,
    pub virtual_time_ns_next: u64,
    pub virtual_time_ns_rebase_adjustment: i64,
    pub virtual_time_ns_start: u64,
}

impl From<&mut BuildingHierarchyDataGeneratorInternalState>
    for BuildingHierarchyDataGeneratorExternalState
{
    fn from(state: &mut BuildingHierarchyDataGeneratorInternalState) -> Self {
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

pub struct BuildingHierarchyDataGeneratorInternalState {
    building_graph: Arc<Mutex<BuildingGraph>>,
    change_interval_generator: ChangeIntervalGenerator,
    change_tx_channel: Sender<ScheduledChangeEventMessage>,
    dispatchers: Vec<Box<dyn SourceChangeDispatcher + Send>>,
    error_messages: Vec<String>,
    event_seq_num: u64,
    next_event: Option<SourceChangeEvent>,
    previous_event: Option<ProcessedChangeEvent>,
    rate_limiter: RateLimiter<NotKeyed, InMemoryState, QuantaClock, NoOpMiddleware<QuantaInstant>>,
    settings: BuildingHierarchyDataGeneratorSettings,
    skips_remaining: u64,
    status: SourceChangeGeneratorStatus,
    stats: BuildingHierarchyDataGeneratorStats,
    steps_remaining: u64,
    virtual_time_ns_current: u64,
    virtual_time_ns_next: u64,
    virtual_time_ns_rebase_adjustment: i64, // Add to current time to get rebased virtual time.
    virtual_time_ns_start: u64,
}

impl BuildingHierarchyDataGeneratorInternalState {
    async fn initialize(
        settings: BuildingHierarchyDataGeneratorSettings,
        building_graph: Arc<Mutex<BuildingGraph>>,
    ) -> anyhow::Result<(Self, Receiver<ScheduledChangeEventMessage>)> {
        log::debug!("Initializing BuildingHierarchyDataGenerator using {settings:?}");

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

        // Create the channels and threads used for message passing.
        let (change_tx_channel, change_rx_channel) = tokio::sync::mpsc::channel(1000);

        let state = Self {
            building_graph,
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
            stats: BuildingHierarchyDataGeneratorStats::default(),
            steps_remaining: 0,
            virtual_time_ns_current: 0,
            virtual_time_ns_next: 0,
            virtual_time_ns_rebase_adjustment: 0,
            virtual_time_ns_start: 0,
        };

        Ok((state, change_rx_channel))
    }

    async fn close_dispatchers(&mut self) {
        let dispatchers = &mut self.dispatchers;

        log::debug!("Closing dispatchers - #dispatchers:{}", dispatchers.len());

        let futures: Vec<_> = dispatchers
            .iter_mut()
            .map(|dispatcher| async move {
                let _ = dispatcher.close().await;
            })
            .collect();

        // Wait for all of them to complete
        // TODO - Handle errors properly.
        let _ = join_all(futures).await;
    }

    async fn send_initial_inserts(&mut self) -> anyhow::Result<()> {
        log::info!(
            "Sending initial insert events for TestRunSource {}",
            self.settings.id
        );

        // Get current time
        let now_ns = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        // Get all nodes and relations from current state. The iterator filters
        // by included_types.contains(label), so we must list every label
        // explicitly here — an empty HashSet means "include none", not "all".
        let building_graph = self.building_graph.lock().await;
        let all_labels: HashSet<String> = [
            GraphElementType::BUILDING,
            GraphElementType::FLOOR,
            GraphElementType::ROOM,
            GraphElementType::BUILDING_FLOOR,
            GraphElementType::FLOOR_ROOM,
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();

        // Collect all insert events
        let mut insert_events = Vec::new();

        // Process nodes
        for change in building_graph.get_current_state(&all_labels) {
            let event = match change {
                ModelChange::BuildingAdded(building) => Some(SourceChangeEvent {
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
                            "id": building.id,
                            "labels": building.labels,
                            "properties": {}
                        }),
                    },
                }),
                ModelChange::FloorAdded(floor) => Some(SourceChangeEvent {
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
                            "id": floor.id,
                            "labels": floor.labels,
                            "properties": {}
                        }),
                    },
                }),
                ModelChange::RoomAdded(room) => Some(SourceChangeEvent {
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
                            "id": room.id,
                            "labels": room.labels,
                            "properties": room.properties
                        }),
                    },
                }),
                ModelChange::BuildingFloorRelationAdded(relation) => Some(SourceChangeEvent {
                    op: "i".to_string(),
                    reactivator_start_ns: now_ns,
                    reactivator_end_ns: 0,
                    payload: SourceChangeEventPayload {
                        source: SourceChangeEventSourceInfo {
                            db: self.settings.id.test_source_id.to_string(),
                            lsn: self.event_seq_num,
                            table: "relation".to_string(),
                            ts_ns: self.virtual_time_ns_current,
                        },
                        before: serde_json::Value::Null,
                        after: serde_json::json!({
                            "id": relation.id,
                            "labels": relation.labels,
                            "properties": {},
                            "start_id": relation.building_id,
                            "end_id": relation.floor_id
                        }),
                    },
                }),
                ModelChange::FloorRoomRelationAdded(relation) => Some(SourceChangeEvent {
                    op: "i".to_string(),
                    reactivator_start_ns: now_ns,
                    reactivator_end_ns: 0,
                    payload: SourceChangeEventPayload {
                        source: SourceChangeEventSourceInfo {
                            db: self.settings.id.test_source_id.to_string(),
                            lsn: self.event_seq_num,
                            table: "relation".to_string(),
                            ts_ns: self.virtual_time_ns_current,
                        },
                        before: serde_json::Value::Null,
                        after: serde_json::json!({
                            "id": relation.id,
                            "labels": relation.labels,
                            "properties": {},
                            "start_id": relation.floor_id,
                            "end_id": relation.room_id
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

        drop(building_graph);

        // Dispatch all insert events
        if !insert_events.is_empty() {
            log::info!("Dispatching {} initial insert events", insert_events.len());
            let events_refs: Vec<&SourceChangeEvent> = insert_events.iter().collect();
            self.dispatch_source_change_events(events_refs).await;
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

    async fn dispatch_source_change_events(&mut self, events: Vec<&SourceChangeEvent>) {
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
                async move {
                    let _ = dispatcher.dispatch_source_change_events(events).await;
                }
            })
            .collect();

        // Wait for all of them to complete
        // TODO - Handle errors properly.
        let _ = join_all(futures).await;
    }

    // Function to log the Player State at varying levels of detail.
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

        // Update times
        self.virtual_time_ns_current = self.virtual_time_ns_next;

        let source_change_event = match self.next_event.as_mut() {
            Some(source_change_event) => {
                let now_ns = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;

                source_change_event.reactivator_end_ns = now_ns;

                // match self.settings.time_mode {
                //     TimeMode::Live => {
                //         source_change_event.payload.source.ts_ns = now_ns;
                //     },
                //     TimeMode::Rebased(_) => {
                //         source_change_event.payload.source.ts_ns = now_ns + self.virtual_time_ns_rebase_adjustment as u64;
                //     },
                //     TimeMode::Recorded => {}
                // }

                source_change_event.clone()
            }
            None => {
                self.transition_to_error_state("No next_event to process", None);
                anyhow::bail!("No next_event to process");
            }
        };

        match &mut self.status {
            SourceChangeGeneratorStatus::Running => {
                // Dispatch the SourceChangeEvent.
                self.dispatch_source_change_events(vec![&source_change_event])
                    .await;

                self.previous_event = Some(ProcessedChangeEvent {
                    dispatch_status: self.status,
                    event: source_change_event,
                    seq: message.seq_num,
                });
                self.event_seq_num += 1;
                self.stats.num_source_change_events += 1;

                if self.stats.num_source_change_events >= self.settings.change_count {
                    self.transition_to_finished_state().await;
                } else {
                    self.schedule_next_change_event().await?;
                }
            }
            SourceChangeGeneratorStatus::Stepping => {
                if self.steps_remaining > 0 {
                    // Dispatch the SourceChangeEvent.
                    self.dispatch_source_change_events(vec![&source_change_event])
                        .await;

                    self.previous_event = Some(ProcessedChangeEvent {
                        dispatch_status: self.status,
                        event: source_change_event,
                        seq: message.seq_num,
                    });
                    self.event_seq_num += 1;
                    self.stats.num_source_change_events += 1;

                    if self.stats.num_source_change_events >= self.settings.change_count {
                        self.transition_to_finished_state().await;
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
                    // Transition to an error state.
                    self.transition_to_error_state("Stepping with no steps remaining", None);
                }
            }
            SourceChangeGeneratorStatus::Skipping => {
                if self.skips_remaining > 0 {
                    // DON'T dispatch the SourceChangeEvent.
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
                        self.transition_to_finished_state().await;
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
                    // Transition to an error state.
                    self.transition_to_error_state("Skipping with no skips remaining", None);
                }
            }
            _ => {
                // Transition to an error state.
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
        message: BuildingHierarchyDataGeneratorMessage,
    ) -> anyhow::Result<()> {
        log::debug!("Received command message: {:?}", message.command);

        if let BuildingHierarchyDataGeneratorCommand::GetState = message.command {
            let message_response = BuildingHierarchyDataGeneratorMessageResponse {
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
                let message_response = BuildingHierarchyDataGeneratorMessageResponse {
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
        log::debug!("Resetting BuildingHierarchyDataGenerator");

        // Create the new dispatchers
        self.close_dispatchers().await;
        let mut dispatchers: Vec<Box<dyn SourceChangeDispatcher + Send>> = Vec::new();
        for def in self.settings.dispatchers.iter() {
            match create_source_change_dispatcher(def, &self.settings.output_storage).await {
                Ok(dispatcher) => dispatchers.push(dispatcher),
                Err(e) => {
                    anyhow::bail!("Error creating SourceChangeDispatcher: {def:?}; Error: {e:?}");
                }
            }
        }
        // These fields do not get reset:
        //   change_tx_channel
        //   delayer_tx_channel
        //   rate_limiter
        //   rate_limiter_tx_channel
        //   settings

        self.building_graph = Arc::new(Mutex::new(BuildingGraph::new(&self.settings)?));
        self.change_interval_generator =
            ChangeIntervalGenerator::new(self.settings.seed, self.settings.change_interval)?;
        self.dispatchers = dispatchers;
        self.error_messages = Vec::new();
        self.event_seq_num = 0;
        self.next_event = None;
        self.previous_event = None;
        self.skips_remaining = 0;
        self.status = SourceChangeGeneratorStatus::Paused;
        self.stats = BuildingHierarchyDataGeneratorStats::default();
        self.steps_remaining = 0;
        self.virtual_time_ns_current = 0;
        self.virtual_time_ns_next = 0;
        self.virtual_time_ns_rebase_adjustment = 0;
        self.virtual_time_ns_start = 0;

        Ok(())
    }

    async fn schedule_next_change_event(&mut self) -> anyhow::Result<()> {
        log::debug!("Scheduling next change event");

        // Throttle the event generation to the configured rate.
        self.rate_limiter.until_ready().await;

        // Calculate times
        let now_ns = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        if self.previous_event.is_none() {
            // First event after start, initialize times.
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
            // Calculate the next event time based on the current time and the configured event interval.
            self.virtual_time_ns_next =
                self.virtual_time_ns_current + self.change_interval_generator.next();
        };

        let update = {
            let building_graph = &mut self.building_graph.lock().await;
            building_graph.generate_update(self.virtual_time_ns_next)?
        };

        let next_event = match update {
            Some(model_change) => {
                match model_change {
                    ModelChange::RoomUpdated(room_before, room_after) => {
                        SourceChangeEvent {
                            op: "u".to_string(),
                            reactivator_start_ns: now_ns,
                            reactivator_end_ns: 0, // Will be set in process_change_stream_message.
                            payload: SourceChangeEventPayload {
                                source: SourceChangeEventSourceInfo {
                                    db: self.settings.id.test_source_id.to_string(),
                                    lsn: self.event_seq_num,
                                    table: "node".to_string(),
                                    ts_ns: self.virtual_time_ns_next,
                                },
                                before: serde_json::json!(room_before),
                                after: serde_json::json!(room_after),
                            },
                        }
                    }
                    _ => {
                        anyhow::bail!("Unexpected model change: {model_change:?}");
                    }
                }
            }
            None => {
                anyhow::bail!("No model change generated");
            }
        };
        self.next_event = Some(next_event);

        let sch_msg = ScheduledChangeEventMessage {
            delay_ns: self.virtual_time_ns_next - self.virtual_time_ns_current,
            seq_num: self.event_seq_num,
        };

        // if the status is Running, Skipping, or Stepping, send the message to the change_tx_channel.
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
        command: &BuildingHierarchyDataGeneratorCommand,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Attempting to transition from {:?} state via command: {:?}",
            self.status,
            command
        );

        match command {
            BuildingHierarchyDataGeneratorCommand::Reset => self.reset().await,
            BuildingHierarchyDataGeneratorCommand::SetTestRunHost { test_run_host } => {
                self.set_test_run_host_on_dispatchers(test_run_host.clone());
                Ok(())
            }
            _ => Err(BuildingHierarchyDataGeneratorError::Error(self.status).into()),
        }
    }

    async fn transition_from_finished_state(
        &mut self,
        command: &BuildingHierarchyDataGeneratorCommand,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Attempting to transition from {:?} state via command: {:?}",
            self.status,
            command
        );

        match command {
            BuildingHierarchyDataGeneratorCommand::Reset => self.reset().await,
            BuildingHierarchyDataGeneratorCommand::SetTestRunHost { test_run_host } => {
                self.set_test_run_host_on_dispatchers(test_run_host.clone());
                Ok(())
            }
            _ => Err(BuildingHierarchyDataGeneratorError::AlreadyFinished.into()),
        }
    }

    async fn transition_from_paused_state(
        &mut self,
        command: &BuildingHierarchyDataGeneratorCommand,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Transitioning from {:?} state via command: {:?}",
            self.status,
            command
        );

        match command {
            BuildingHierarchyDataGeneratorCommand::GetState => Ok(()),
            BuildingHierarchyDataGeneratorCommand::Pause => Ok(()),
            BuildingHierarchyDataGeneratorCommand::Reset => self.reset().await,
            BuildingHierarchyDataGeneratorCommand::Skip { skips, .. } => {
                log::info!(
                    "Script Skipping {} skips for TestRunSource {}",
                    skips,
                    self.settings.id
                );

                self.status = SourceChangeGeneratorStatus::Skipping;
                self.skips_remaining = *skips;
                // self.skips_spacing_mode = spacing_mode.clone();
                self.schedule_next_change_event().await
            }
            BuildingHierarchyDataGeneratorCommand::Start => {
                log::info!("Script Started for TestRunSource {}", self.settings.id);

                self.status = SourceChangeGeneratorStatus::Running;

                // If send_initial_inserts is true, send insert events for all current state
                if self.settings.send_initial_inserts {
                    if let Err(e) = self.send_initial_inserts().await {
                        log::error!("Failed to send initial inserts: {e}");
                    }
                }

                self.schedule_next_change_event().await
            }
            BuildingHierarchyDataGeneratorCommand::Step { steps, .. } => {
                log::info!(
                    "Script Stepping {} steps for TestRunSource {}",
                    steps,
                    self.settings.id
                );

                self.status = SourceChangeGeneratorStatus::Stepping;
                self.steps_remaining = *steps;
                // self.steps_spacing_mode = spacing_mode.clone();
                self.schedule_next_change_event().await
            }
            BuildingHierarchyDataGeneratorCommand::Stop => {
                self.transition_to_stopped_state().await;
                Ok(())
            }
            BuildingHierarchyDataGeneratorCommand::SetTestRunHost { test_run_host } => {
                self.set_test_run_host_on_dispatchers(test_run_host.clone());
                Ok(())
            }
        }
    }

    async fn transition_from_running_state(
        &mut self,
        command: &BuildingHierarchyDataGeneratorCommand,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Transitioning from {:?} state via command: {:?}",
            self.status,
            command
        );

        match command {
            BuildingHierarchyDataGeneratorCommand::GetState => Ok(()),
            BuildingHierarchyDataGeneratorCommand::Pause => {
                self.status = SourceChangeGeneratorStatus::Paused;
                Ok(())
            }
            BuildingHierarchyDataGeneratorCommand::Reset => {
                Err(BuildingHierarchyDataGeneratorError::PauseToReset.into())
            }
            BuildingHierarchyDataGeneratorCommand::Skip { .. } => {
                Err(BuildingHierarchyDataGeneratorError::PauseToSkip.into())
            }
            BuildingHierarchyDataGeneratorCommand::Start => Ok(()),
            BuildingHierarchyDataGeneratorCommand::Step { .. } => {
                Err(BuildingHierarchyDataGeneratorError::PauseToStep.into())
            }
            BuildingHierarchyDataGeneratorCommand::Stop => {
                self.transition_to_stopped_state().await;
                Ok(())
            }
            BuildingHierarchyDataGeneratorCommand::SetTestRunHost { test_run_host } => {
                self.set_test_run_host_on_dispatchers(test_run_host.clone());
                Ok(())
            }
        }
    }

    async fn transition_from_skipping_state(
        &mut self,
        command: &BuildingHierarchyDataGeneratorCommand,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Transitioning from {:?} state via command: {:?}",
            self.status,
            command
        );

        match command {
            BuildingHierarchyDataGeneratorCommand::GetState => Ok(()),
            BuildingHierarchyDataGeneratorCommand::Pause => {
                self.status = SourceChangeGeneratorStatus::Paused;
                self.skips_remaining = 0;
                Ok(())
            }
            BuildingHierarchyDataGeneratorCommand::Stop => {
                self.transition_to_stopped_state().await;
                Ok(())
            }
            BuildingHierarchyDataGeneratorCommand::Reset
            | BuildingHierarchyDataGeneratorCommand::Skip { .. }
            | BuildingHierarchyDataGeneratorCommand::Start
            | BuildingHierarchyDataGeneratorCommand::Step { .. } => Err(
                BuildingHierarchyDataGeneratorError::CurrentlySkipping(self.skips_remaining).into(),
            ),
            BuildingHierarchyDataGeneratorCommand::SetTestRunHost { test_run_host } => {
                self.set_test_run_host_on_dispatchers(test_run_host.clone());
                Ok(())
            }
        }
    }

    async fn transition_from_stepping_state(
        &mut self,
        command: &BuildingHierarchyDataGeneratorCommand,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Transitioning from {:?} state via command: {:?}",
            self.status,
            command
        );

        match command {
            BuildingHierarchyDataGeneratorCommand::GetState => Ok(()),
            BuildingHierarchyDataGeneratorCommand::Pause => {
                self.status = SourceChangeGeneratorStatus::Paused;
                self.steps_remaining = 0;
                Ok(())
            }
            BuildingHierarchyDataGeneratorCommand::Stop => {
                self.transition_to_stopped_state().await;
                Ok(())
            }
            BuildingHierarchyDataGeneratorCommand::Reset
            | BuildingHierarchyDataGeneratorCommand::Skip { .. }
            | BuildingHierarchyDataGeneratorCommand::Start
            | BuildingHierarchyDataGeneratorCommand::Step { .. } => Err(
                BuildingHierarchyDataGeneratorError::CurrentlyStepping(self.steps_remaining).into(),
            ),
            BuildingHierarchyDataGeneratorCommand::SetTestRunHost { test_run_host } => {
                self.set_test_run_host_on_dispatchers(test_run_host.clone());
                Ok(())
            }
        }
    }

    async fn transition_from_stopped_state(
        &mut self,
        command: &BuildingHierarchyDataGeneratorCommand,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Attempting to transition from {:?} state via command: {:?}",
            self.status,
            command
        );

        match command {
            BuildingHierarchyDataGeneratorCommand::Reset => self.reset().await,
            BuildingHierarchyDataGeneratorCommand::SetTestRunHost { test_run_host } => {
                self.set_test_run_host_on_dispatchers(test_run_host.clone());
                Ok(())
            }
            _ => Err(BuildingHierarchyDataGeneratorError::AlreadyStopped.into()),
        }
    }

    async fn transition_to_finished_state(&mut self) {
        log::info!("Script Finished for TestRunSource {}", self.settings.id);

        self.status = SourceChangeGeneratorStatus::Finished;
        self.stats.actual_end_time_ns = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        self.skips_remaining = 0;
        self.steps_remaining = 0;

        self.close_dispatchers().await;
        self.write_result_summary().await.ok();
    }

    async fn transition_to_stopped_state(&mut self) {
        log::info!("Script Stopped for TestRunSource {}", self.settings.id);

        self.status = SourceChangeGeneratorStatus::Stopped;
        self.stats.actual_end_time_ns = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        self.skips_remaining = 0;
        self.steps_remaining = 0;

        self.close_dispatchers().await;
        self.write_result_summary().await.ok();
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
        let result_summary: BuildingHierarchyDataGeneratorResultSummary = self.into();
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

impl Debug for BuildingHierarchyDataGeneratorInternalState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BuildingHierarchyDataGeneratorInternalState")
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
pub struct BuildingHierarchyDataGeneratorStats {
    pub actual_start_time_ns: u64,
    pub actual_end_time_ns: u64,
    pub num_source_change_events: u64,
    pub num_skipped_source_change_events: u64,
}

#[derive(Clone, Serialize)]
pub struct BuildingHierarchyDataGeneratorResultSummary {
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

impl From<&mut BuildingHierarchyDataGeneratorInternalState>
    for BuildingHierarchyDataGeneratorResultSummary
{
    fn from(state: &mut BuildingHierarchyDataGeneratorInternalState) -> Self {
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

impl Debug for BuildingHierarchyDataGeneratorResultSummary {
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

        f.debug_struct("BuildingHierarchyDataGeneratorResultSummary")
            .field("test_run_source_id", &self.test_run_source_id)
            .field("start_time", &start_time)
            .field("end_time", &end_time)
            .field("run_duration", &run_duration)
            .field("source_change_events", &source_change_events)
            .field("processing_rate", &processing_rate)
            .finish()
    }
}

// Function that defines the operation of the BuildingHierarchyDataGenerator thread.
// The BuildingHierarchyDataGenerator thread processes ChangeScriptPlayerCommands sent to it from the Web API handler functions.
// The Web API function communicate via a channel and provide oneshot channels for the BuildingHierarchyDataGenerator to send responses back.
pub async fn model_host_thread(
    mut command_rx_channel: Receiver<BuildingHierarchyDataGeneratorMessage>,
    settings: BuildingHierarchyDataGeneratorSettings,
    building_graph: Arc<Mutex<BuildingGraph>>,
) -> anyhow::Result<()> {
    log::info!(
        "Script processor thread started for TestRunSource {} ...",
        settings.id
    );

    // The BuildingHierarchyDataGenerator always starts with the model initialized and Paused.
    let (mut state, mut change_rx_channel) =
        match BuildingHierarchyDataGeneratorInternalState::initialize(settings, building_graph)
            .await
        {
            Ok((state, change_rx_channel)) => (state, change_rx_channel),
            Err(e) => {
                // If initialization fails, don't dont transition to an error state, just log an error and exit the thread.
                let msg = format!("Error initializing BuildingHierarchyDataGenerator: {e:?}");
                log::error!("{msg}");
                anyhow::bail!(msg);
            }
        };

    // Loop to process commands sent to the BuildingHierarchyDataGenerator or read from the Change Stream.
    loop {
        state.log_state("Top of script processor loop");

        tokio::select! {
            // Always process all messages in the command channel and act on them first.
            biased;

            // Process messages from the command channel.
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

            // Process messages from the Change Stream.
            change_stream_message = change_rx_channel.recv() => {
                match change_stream_message {
                    Some(change_stream_message) => {
                        // Only process the message if the seq_num matches the expected one.
                        // This avoids dealing with delayed messages from the delayer thread that are no longer relevant.
                        log::trace!("Received change stream message: {change_stream_message:?}");
                        if change_stream_message.seq_num == state.event_seq_num && state.status.is_processing() {
                            state.process_change_stream_message(change_stream_message).await
                                .inspect_err(|e| state.transition_to_error_state("Error calling process_change_stream_message", Some(e))).ok();
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
