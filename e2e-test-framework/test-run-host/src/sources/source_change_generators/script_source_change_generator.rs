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
    fmt::{self, Debug, Formatter},
    num::NonZeroU32,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use futures::{future::join_all, Stream};
use governor::{Quota, RateLimiter};
use serde::Serialize;
use time::{format_description, OffsetDateTime};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        oneshot, Mutex,
    },
    task::JoinHandle,
    time::sleep,
};
use tokio_stream::StreamExt;

use test_data_store::{
    scripts::{
        change_script_file_reader::ChangeScriptReader, ChangeHeaderRecord, ChangeScriptRecord,
        SequencedChangeScriptRecord, SourceChangeEvent,
    },
    test_repo_storage::{
        models::{
            ScriptSourceChangeGeneratorDefinition, SourceChangeDispatcherDefinition, SpacingMode,
            TimeMode,
        },
        TestSourceStorage,
    },
    test_run_storage::{TestRunSourceId, TestRunSourceStorage},
};

use crate::sources::source_change_dispatchers::{
    create_source_change_dispatcher, SourceChangeDispatcher,
};

use super::{
    SourceChangeGenerator, SourceChangeGeneratorCommandResponse, SourceChangeGeneratorStatus,
};

type ChangeStream = Pin<Box<dyn Stream<Item = anyhow::Result<SequencedChangeScriptRecord>> + Send>>;

#[derive(Debug, thiserror::Error)]
pub enum ScriptSourceChangeGeneratorError {
    #[error("ScriptSourceChangeGenerator is already finished. Reset to start over.")]
    AlreadyFinished,
    #[error("ScriptSourceChangeGenerator is already stopped. Reset to start over.")]
    AlreadyStopped,
    #[error("ScriptSourceChangeGenerator is currently Skipping. {0} skips remaining. Pause before Skip, Step, or Reset.")]
    CurrentlySkipping(u64),
    #[error("ScriptSourceChangeGenerator is currently Stepping. {0} steps remaining. Pause before Skip, Step, or Reset.")]
    CurrentlyStepping(u64),
    #[error("ScriptSourceChangeGenerator is currently in an Error state - {0:?}")]
    Error(SourceChangeGeneratorStatus),
    #[error("ScriptSourceChangeGenerator is currently Running. Pause before trying to Skip.")]
    PauseToSkip,
    #[error("ScriptSourceChangeGenerator is currently Running. Pause before trying to Step.")]
    PauseToStep,
    #[error("ScriptSourceChangeGenerator is currently Running. Pause before trying to Reset.")]
    PauseToReset,
}

#[derive(Clone, Debug, Serialize)]
pub struct ScriptSourceChangeGeneratorSettings {
    pub dispatchers: Vec<SourceChangeDispatcherDefinition>,
    pub id: TestRunSourceId,
    pub ignore_scripted_pause_commands: bool,
    pub input_storage: TestSourceStorage,
    pub output_storage: TestRunSourceStorage,
    pub spacing_mode: SpacingMode,
    pub time_mode: TimeMode,
}

impl ScriptSourceChangeGeneratorSettings {
    pub async fn new(
        test_run_source_id: TestRunSourceId,
        definition: ScriptSourceChangeGeneratorDefinition,
        input_storage: TestSourceStorage,
        output_storage: TestRunSourceStorage,
        dispatchers: Vec<SourceChangeDispatcherDefinition>,
    ) -> anyhow::Result<Self> {
        Ok(ScriptSourceChangeGeneratorSettings {
            dispatchers,
            id: test_run_source_id,
            ignore_scripted_pause_commands: definition.ignore_scripted_pause_commands,
            input_storage,
            output_storage,
            spacing_mode: definition.common.spacing_mode,
            time_mode: definition.common.time_mode,
        })
    }

    pub fn get_id(&self) -> TestRunSourceId {
        self.id.clone()
    }
}

// Enum of ScriptSourceChangeGenerator commands sent from Web API handler functions.
#[derive(Debug)]
pub enum ScriptSourceChangeGeneratorCommand {
    // Command to get the current state of the ScriptSourceChangeGenerator.
    GetState,
    // Command to pause the ScriptSourceChangeGenerator.
    Pause,
    // Command to reset the ScriptSourceChangeGenerator.
    Reset,
    // Command to skip the ScriptSourceChangeGenerator forward a specified number of ChangeScriptRecords.
    Skip {
        skips: u64,
        spacing_mode: Option<SpacingMode>,
    },
    // Command to start the ScriptSourceChangeGenerator.
    Start,
    // Command to step the ScriptSourceChangeGenerator forward a specified number of ChangeScriptRecords.
    Step {
        steps: u64,
        spacing_mode: Option<SpacingMode>,
    },
    // Command to stop the ScriptSourceChangeGenerator.
    Stop,
}

// Struct for messages sent to the ScriptSourceChangeGenerator from the functions in the Web API.
#[derive(Debug)]
pub struct ScriptSourceChangeGeneratorMessage {
    // Command sent to the ScriptSourceChangeGenerator.
    pub command: ScriptSourceChangeGeneratorCommand,
    // One-shot channel for ScriptSourceChangeGenerator to send a response back to the caller.
    pub response_tx: Option<oneshot::Sender<ScriptSourceChangeGeneratorMessageResponse>>,
}

// A struct for the Response sent back from the ScriptSourceChangeGenerator to the calling Web API handler.
#[derive(Debug)]
pub struct ScriptSourceChangeGeneratorMessageResponse {
    // Result of the command.
    pub result: anyhow::Result<()>,
    // State of the ScriptSourceChangeGenerator after the command.
    pub state: ScriptSourceChangeGeneratorExternalState,
}

#[derive(Clone, Debug)]
pub struct ScheduledChangeScriptRecordMessage {
    pub delay_ns: u64,
    pub seq_num: u64,
    pub virtual_time_ns_replay: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProcessedChangeScriptRecord {
    pub dispatch_status: SourceChangeGeneratorStatus,
    pub scripted: SequencedChangeScriptRecord,
}

#[derive(Clone, Debug, Serialize)]
pub struct ScriptSourceChangeGenerator {
    settings: ScriptSourceChangeGeneratorSettings,
    #[serde(skip_serializing)]
    script_processor_tx_channel: Sender<ScriptSourceChangeGeneratorMessage>,
    #[serde(skip_serializing)]
    _script_processor_thread_handle: Arc<Mutex<JoinHandle<anyhow::Result<()>>>>,
}

impl ScriptSourceChangeGenerator {
    pub async fn new(
        test_run_source_id: TestRunSourceId,
        definition: ScriptSourceChangeGeneratorDefinition,
        input_storage: TestSourceStorage,
        output_storage: TestRunSourceStorage,
        dispatchers: Vec<SourceChangeDispatcherDefinition>,
    ) -> anyhow::Result<Self> {
        let settings = ScriptSourceChangeGeneratorSettings::new(
            test_run_source_id,
            definition,
            input_storage,
            output_storage.clone(),
            dispatchers,
        )
        .await?;
        log::debug!("Creating ScriptSourceChangeGenerator from {:?}", &settings);

        let (script_processor_tx_channel, script_processor_rx_channel) =
            tokio::sync::mpsc::channel(100);
        let script_processor_thread_handle = tokio::spawn(script_processor_thread(
            script_processor_rx_channel,
            settings.clone(),
        ));

        Ok(Self {
            settings,
            script_processor_tx_channel,
            _script_processor_thread_handle: Arc::new(Mutex::new(script_processor_thread_handle)),
        })
    }

    pub fn get_id(&self) -> TestRunSourceId {
        self.settings.get_id()
    }

    pub fn get_settings(&self) -> ScriptSourceChangeGeneratorSettings {
        self.settings.clone()
    }

    async fn send_command(
        &self,
        command: ScriptSourceChangeGeneratorCommand,
    ) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        let (response_tx, response_rx) = oneshot::channel();

        let r = self
            .script_processor_tx_channel
            .send(ScriptSourceChangeGeneratorMessage {
                command,
                response_tx: Some(response_tx),
            })
            .await;

        match r {
            Ok(_) => {
                let player_response = response_rx.await?;

                Ok(SourceChangeGeneratorCommandResponse {
                    result: player_response.result,
                    state: super::SourceChangeGeneratorState {
                        status: player_response.state.status,
                        state: serde_json::to_value(player_response.state).unwrap(),
                    },
                })
            }
            Err(e) => anyhow::bail!("Error sending command to ScriptSourceChangeGenerator: {e:?}"),
        }
    }
}

#[async_trait]
impl SourceChangeGenerator for ScriptSourceChangeGenerator {
    async fn get_state(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(ScriptSourceChangeGeneratorCommand::GetState)
            .await
    }

    async fn pause(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(ScriptSourceChangeGeneratorCommand::Pause)
            .await
    }

    async fn reset(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(ScriptSourceChangeGeneratorCommand::Reset)
            .await
    }

    async fn skip(
        &self,
        skips: u64,
        spacing_mode: Option<SpacingMode>,
    ) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(ScriptSourceChangeGeneratorCommand::Skip {
            skips,
            spacing_mode,
        })
        .await
    }

    async fn start(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(ScriptSourceChangeGeneratorCommand::Start)
            .await
    }

    async fn step(
        &self,
        steps: u64,
        spacing_mode: Option<SpacingMode>,
    ) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(ScriptSourceChangeGeneratorCommand::Step {
            steps,
            spacing_mode,
        })
        .await
    }

    async fn stop(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(ScriptSourceChangeGeneratorCommand::Stop)
            .await
    }

    fn set_test_run_host_on_dispatchers(&self, _test_run_host: std::sync::Arc<crate::TestRunHost>) {
        // This generator uses a thread-based architecture, so we can't directly access dispatchers
        // The TestRunHost will be set when the dispatchers are recreated on reset
        log::warn!("ScriptSourceChangeGenerator: set_test_run_host_on_dispatchers called but not implemented - dispatchers are in separate thread");
    }
}

#[derive(Debug, Serialize)]
pub struct ScriptSourceChangeGeneratorExternalState {
    pub error_messages: Vec<String>,
    pub ignore_scripted_pause_commands: bool,
    pub header_record: ChangeHeaderRecord,
    pub next_record: Option<SequencedChangeScriptRecord>,
    pub previous_record: Option<ProcessedChangeScriptRecord>,
    pub skips_remaining: u64,
    pub skips_spacing_mode: Option<SpacingMode>,
    pub spacing_mode: SpacingMode,
    pub status: SourceChangeGeneratorStatus,
    pub steps_remaining: u64,
    pub steps_spacing_mode: Option<SpacingMode>,
    pub test_run_source_id: TestRunSourceId,
    pub time_mode: TimeMode,
    pub virtual_time_ns_current: u64,
    pub virtual_time_ns_offset: u64,
    pub virtual_time_ns_start: u64,
}

impl From<&mut ScriptSourceChangeGeneratorInternalState>
    for ScriptSourceChangeGeneratorExternalState
{
    fn from(state: &mut ScriptSourceChangeGeneratorInternalState) -> Self {
        Self {
            error_messages: state.error_messages.clone(),
            ignore_scripted_pause_commands: state.settings.ignore_scripted_pause_commands,
            header_record: state.header_record.clone(),
            next_record: state.next_record.clone(),
            previous_record: state.previous_record.clone(),
            skips_remaining: state.skips_remaining,
            skips_spacing_mode: state.skips_spacing_mode.clone(),
            spacing_mode: state.settings.spacing_mode.clone(),
            status: state.status,
            steps_remaining: state.steps_remaining,
            steps_spacing_mode: state.steps_spacing_mode.clone(),
            test_run_source_id: state.settings.id.clone(),
            time_mode: state.settings.time_mode.clone(),
            virtual_time_ns_current: state.virtual_time_ns_current,
            virtual_time_ns_offset: state.virtual_time_ns_offset,
            virtual_time_ns_start: state.virtual_time_ns_start,
        }
    }
}

pub struct ScriptSourceChangeGeneratorInternalState {
    pub change_stream:
        Pin<Box<dyn Stream<Item = Result<SequencedChangeScriptRecord, anyhow::Error>> + Send>>,
    pub change_tx_channel: Sender<ScheduledChangeScriptRecordMessage>,
    pub delayer_tx_channel: Sender<ScheduledChangeScriptRecordMessage>,
    pub dispatchers: Vec<Box<dyn SourceChangeDispatcher + Send>>,
    pub error_messages: Vec<String>,
    pub header_record: ChangeHeaderRecord,
    pub message_seq_num: u64,
    pub next_record: Option<SequencedChangeScriptRecord>,
    pub previous_record: Option<ProcessedChangeScriptRecord>,
    pub rate_limiter_tx_channel: Sender<ScheduledChangeScriptRecordMessage>,
    pub settings: ScriptSourceChangeGeneratorSettings,
    pub skips_remaining: u64,
    pub skips_spacing_mode: Option<SpacingMode>,
    pub status: SourceChangeGeneratorStatus,
    pub stats: ScriptSourceChangeGeneratorStats,
    pub steps_remaining: u64,
    pub steps_spacing_mode: Option<SpacingMode>,
    pub virtual_time_ns_current: u64,
    pub virtual_time_ns_offset: u64,
    pub virtual_time_ns_start: u64,
}

impl ScriptSourceChangeGeneratorInternalState {
    async fn initialize(
        settings: ScriptSourceChangeGeneratorSettings,
    ) -> anyhow::Result<(Self, Receiver<ScheduledChangeScriptRecordMessage>)> {
        log::debug!("Initializing ScriptSourceChangeGenerator using {settings:?}");

        // Get the list of script files from the input storage.
        let script_files = match settings.input_storage.get_script_files().await {
            Ok(ds) => ds.source_change_script_files,
            Err(e) => {
                anyhow::bail!("Error getting script files from input storage: {e:?}");
            }
        };

        // Create the change stream.
        let reader = ChangeScriptReader::new(script_files)?;
        let header_record = reader.get_header();
        let mut change_stream = Box::pin(reader) as ChangeStream;
        let next_record = match change_stream.next().await {
            Some(Ok(seq_record)) => Some(seq_record),
            Some(Err(e)) => {
                anyhow::bail!(format!("Error reading first ChangeStream record: {e:?}"));
            }
            None => None,
        };

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

        // Create the channels and threads used for message passing.
        let (change_tx_channel, change_rx_channel) = tokio::sync::mpsc::channel(1000);

        let (delayer_tx_channel, delayer_rx_channel) = tokio::sync::mpsc::channel(1000);
        tokio::spawn(delayer_thread(
            settings.id.clone(),
            delayer_rx_channel,
            change_tx_channel.clone(),
        ));

        let (rate_limiter_tx_channel, rate_limiter_rx_channel) = tokio::sync::mpsc::channel(1000);
        tokio::spawn(rate_limiter_thread(
            settings.id.clone(),
            settings.spacing_mode.clone(),
            rate_limiter_rx_channel,
            change_tx_channel.clone(),
        ));

        let state = Self {
            change_stream,
            change_tx_channel,
            delayer_tx_channel,
            dispatchers,
            error_messages: Vec::new(),
            header_record,
            message_seq_num: 0,
            next_record,
            previous_record: None,
            rate_limiter_tx_channel,
            settings,
            skips_remaining: 0,
            skips_spacing_mode: None,
            status: SourceChangeGeneratorStatus::Paused,
            stats: ScriptSourceChangeGeneratorStats::default(),
            steps_remaining: 0,
            steps_spacing_mode: None,
            virtual_time_ns_current: 0,
            virtual_time_ns_offset: 0,
            virtual_time_ns_start: 0,
        };

        Ok((state, change_rx_channel))
    }

    // fn calculate_record_scheduling_delay(&mut self, next_record: &SequencedChangeScriptRecord) -> anyhow::Result<u64> {
    //     let delay_ns = match self.status {
    //         SourceChangeGeneratorStatus::Skipping => {
    //             match self.skips_spacing_mode {
    //                 Some(SpacingMode::None) => 0,
    //                 Some(SpacingMode::Rate(nanos)) => nanos,
    //                 Some(SpacingMode::Recorded) => if self.virtual_time_ns_offset > next_record.offset_ns { 0 } else { next_record.offset_ns - self.virtual_time_ns_offset },
    //                 None => 0,
    //             }
    //         },
    //         SourceChangeGeneratorStatus::Stepping => {
    //             match self.steps_spacing_mode {
    //                 Some(SpacingMode::None) => 0,
    //                 Some(SpacingMode::Rate(nanos)) => nanos,
    //                 Some(SpacingMode::Recorded) => if self.virtual_time_ns_offset > next_record.offset_ns { 0 } else { next_record.offset_ns - self.virtual_time_ns_offset }
    //                 None => match self.settings.spacing_mode {
    //                     SpacingMode::None => 0,
    //                     SpacingMode::Rate(nanos) => nanos,
    //                     SpacingMode::Recorded => if self.virtual_time_ns_offset > next_record.offset_ns { 0 } else { next_record.offset_ns - self.virtual_time_ns_offset }
    //                 },
    //             }
    //         },
    //         SourceChangeGeneratorStatus::Running => match self.settings.spacing_mode {
    //             SpacingMode::None => 0,
    //             SpacingMode::Rate(nanos) => nanos,
    //             SpacingMode::Recorded => if self.virtual_time_ns_offset > next_record.offset_ns { 0 } else { next_record.offset_ns - self.virtual_time_ns_offset }
    //         },
    //         _ => anyhow::bail!("Calculating record delay for unexpected status: {:?}", self.status),
    //     };

    //     Ok(delay_ns)
    // }

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

    async fn load_next_change_stream_record(&mut self) -> anyhow::Result<()> {
        match self.change_stream.next().await {
            Some(Ok(seq_record)) => {
                self.previous_record = Some(ProcessedChangeScriptRecord {
                    dispatch_status: self.status,
                    scripted: self.next_record.clone().unwrap(),
                });

                self.next_record = Some(seq_record);
            }
            Some(Err(e)) => {
                anyhow::bail!(format!("Error reading ChangeScriptRecord: {e:?}"));
            }
            None => {
                anyhow::bail!("ChangeScriptReader.next() returned None, shouldn't be seeing this.");
            }
        };

        Ok(())
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
        message: ScheduledChangeScriptRecordMessage,
    ) -> anyhow::Result<()> {
        log::trace!("Received change stream message: {message:?}");

        // Get the next record from the player state. Error if it is None.
        let next_record = match self.next_record.as_ref() {
            Some(record) => record.clone(),
            None => anyhow::bail!(
                "Received ScheduledChangeScriptRecordMessage when player_state.next_record is None"
            ),
        };

        // Time Shift.
        let shifted_record = self.time_shift(next_record)?;

        // Process the record.
        match &shifted_record.record {
            ChangeScriptRecord::SourceChange(change_record) => {
                self.stats.num_source_change_records += 1;

                match &self.status {
                    SourceChangeGeneratorStatus::Running => {
                        // Dispatch the SourceChangeEvent.
                        self.dispatch_source_change_events(vec![
                            &change_record.source_change_event,
                        ])
                        .await?;
                        self.load_next_change_stream_record().await?;
                        self.schedule_next_change_stream_record().await?;
                    }
                    SourceChangeGeneratorStatus::Stepping => {
                        if self.steps_remaining > 0 {
                            // Dispatch the SourceChangeEvent.
                            self.dispatch_source_change_events(vec![
                                &change_record.source_change_event,
                            ])
                            .await?;

                            self.load_next_change_stream_record().await?;

                            self.steps_remaining -= 1;
                            if self.steps_remaining == 0 {
                                self.status = SourceChangeGeneratorStatus::Paused;
                                self.steps_spacing_mode = None;
                            } else {
                                self.schedule_next_change_stream_record().await?;
                            }
                        } else {
                            // Transition to an error state.
                            self.transition_to_error_state(
                                "Stepping with no steps remaining",
                                None,
                            );
                        }
                    }
                    SourceChangeGeneratorStatus::Skipping => {
                        if self.skips_remaining > 0 {
                            // DON'T dispatch the SourceChangeEvent.
                            log::trace!("Skipping ChangeScriptRecord: {change_record:?}");
                            self.stats.num_skipped_source_change_records += 1;

                            self.load_next_change_stream_record().await?;

                            self.skips_remaining -= 1;
                            if self.skips_remaining == 0 {
                                self.status = SourceChangeGeneratorStatus::Paused;
                                self.skips_spacing_mode = None;
                            } else {
                                self.schedule_next_change_stream_record().await?;
                            }
                        } else {
                            // Transition to an error state.
                            self.transition_to_error_state(
                                "Skipping with no skips remaining",
                                None,
                            );
                        }
                    }
                    _ => {
                        // Transition to an error state.
                        self.transition_to_error_state(
                            "Unexpected status for SourceChange processing",
                            None,
                        );
                    }
                }
            }
            ChangeScriptRecord::PauseCommand(_) => {
                self.stats.num_pause_records += 1;

                // Process the PauseCommand only if the Player is not configured to ignore them.
                if self.settings.ignore_scripted_pause_commands {
                    log::debug!("Ignoring Change Script Pause Command: {shifted_record:?}");
                } else {
                    self.status = SourceChangeGeneratorStatus::Paused;
                }
            }
            ChangeScriptRecord::Label(label_record) => {
                self.stats.num_label_records += 1;

                log::debug!("Reached Source Change Script Label: {label_record:?}");
            }
            ChangeScriptRecord::Finish(_) => {
                self.transition_to_finished_state().await?;
            }
            ChangeScriptRecord::Header(header_record) => {
                // Transition to an error state.
                self.transition_to_error_state(
                    &format!("Unexpected Change Script Header: {header_record:?}"),
                    None,
                );
            }
            ChangeScriptRecord::Comment(comment_record) => {
                // Transition to an error state.
                self.transition_to_error_state(
                    &format!("Unexpected Change Script Comment: {comment_record:?}"),
                    None,
                );
            }
        };

        Ok(())
    }

    async fn process_command_message(
        &mut self,
        message: ScriptSourceChangeGeneratorMessage,
    ) -> anyhow::Result<()> {
        log::debug!("Received command message: {:?}", message.command);

        if let ScriptSourceChangeGeneratorCommand::GetState = message.command {
            let message_response = ScriptSourceChangeGeneratorMessageResponse {
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
                let message_response = ScriptSourceChangeGeneratorMessageResponse {
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
        // Get the list of script files from the input storage.
        let script_files = match self.settings.input_storage.get_script_files().await {
            Ok(ds) => ds.source_change_script_files,
            Err(e) => {
                anyhow::bail!("Error getting script files from input storage: {e:?}");
            }
        };

        // Create the change stream.
        let reader = ChangeScriptReader::new(script_files)?;
        let header_record = reader.get_header();
        let mut change_stream = Box::pin(reader) as ChangeStream;
        let next_record = match change_stream.next().await {
            Some(Ok(seq_record)) => Some(seq_record),
            Some(Err(e)) => {
                anyhow::bail!(format!("Error reading first ChangeStream record: {e:?}"));
            }
            None => None,
        };

        // Create the new dispatchers
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
        // These fields do not get reset:
        //   state.change_tx_channel
        //   state.delayer_tx_channel
        //   state.settings

        self.dispatchers = dispatchers;
        self.change_stream = change_stream;
        self.error_messages = Vec::new();
        self.header_record = header_record;
        self.message_seq_num = 0;
        self.next_record = next_record;
        self.previous_record = None;
        self.skips_remaining = 0;
        self.skips_spacing_mode = None;
        self.status = SourceChangeGeneratorStatus::Paused;
        self.stats = ScriptSourceChangeGeneratorStats::default();
        self.steps_remaining = 0;
        self.steps_spacing_mode = None;
        self.virtual_time_ns_current = 0;
        self.virtual_time_ns_offset = 0;
        self.virtual_time_ns_start = 0;

        Ok(())
    }

    async fn schedule_next_change_stream_record(&mut self) -> anyhow::Result<()> {
        // Get the next record from the player state. Error if it is None.
        let next_record = match self.next_record.as_ref() {
            Some(record) => record.clone(),
            None => anyhow::bail!(
                "Received ScheduledChangeScriptRecordMessage when player_state.next_record is None"
            ),
        };

        self.message_seq_num += 1;

        let mut sch_msg = ScheduledChangeScriptRecordMessage {
            delay_ns: 0,
            seq_num: self.message_seq_num,
            virtual_time_ns_replay: self.virtual_time_ns_current,
        };

        match self.status {
            SourceChangeGeneratorStatus::Skipping => {}
            SourceChangeGeneratorStatus::Stepping => match self.steps_spacing_mode {
                Some(SpacingMode::None) => {
                    if let Err(e) = self.change_tx_channel.send(sch_msg).await {
                        anyhow::bail!("Error sending ScheduledChangeScriptRecordMessage: {e:?}");
                    }
                }
                Some(SpacingMode::Rate(_)) => {
                    if let Err(e) = self.rate_limiter_tx_channel.send(sch_msg).await {
                        anyhow::bail!("Error sending ScheduledChangeScriptRecordMessage: {e:?}");
                    }
                }
                Some(SpacingMode::Recorded) => {
                    if next_record.offset_ns > self.virtual_time_ns_offset {
                        sch_msg.delay_ns = next_record.offset_ns - self.virtual_time_ns_offset;
                        sch_msg.virtual_time_ns_replay += sch_msg.delay_ns;
                    }

                    if let Err(e) = self.delayer_tx_channel.send(sch_msg).await {
                        anyhow::bail!("Error sending ScheduledChangeScriptRecordMessage: {e:?}");
                    };
                }
                None => match self.settings.spacing_mode {
                    SpacingMode::None => {
                        if let Err(e) = self.change_tx_channel.send(sch_msg).await {
                            anyhow::bail!(
                                "Error sending ScheduledChangeScriptRecordMessage: {e:?}"
                            );
                        }
                    }
                    SpacingMode::Rate(_) => {
                        if let Err(e) = self.rate_limiter_tx_channel.send(sch_msg).await {
                            anyhow::bail!(
                                "Error sending ScheduledChangeScriptRecordMessage: {e:?}"
                            );
                        }
                    }
                    SpacingMode::Recorded => {
                        if next_record.offset_ns > self.virtual_time_ns_offset {
                            sch_msg.delay_ns = next_record.offset_ns - self.virtual_time_ns_offset;
                            sch_msg.virtual_time_ns_replay += sch_msg.delay_ns;
                        }

                        if let Err(e) = self.delayer_tx_channel.send(sch_msg).await {
                            anyhow::bail!(
                                "Error sending ScheduledChangeScriptRecordMessage: {e:?}"
                            );
                        };
                    }
                },
            },
            SourceChangeGeneratorStatus::Running => match self.settings.spacing_mode {
                SpacingMode::None => {
                    if let Err(e) = self.change_tx_channel.send(sch_msg).await {
                        anyhow::bail!("Error sending ScheduledChangeScriptRecordMessage: {e:?}");
                    }
                }
                SpacingMode::Rate(_) => {
                    if let Err(e) = self.rate_limiter_tx_channel.send(sch_msg).await {
                        anyhow::bail!("Error sending ScheduledChangeScriptRecordMessage: {e:?}");
                    }
                }
                SpacingMode::Recorded => {
                    if next_record.offset_ns > self.virtual_time_ns_offset {
                        sch_msg.delay_ns = next_record.offset_ns - self.virtual_time_ns_offset;
                        sch_msg.virtual_time_ns_replay += sch_msg.delay_ns;
                    }

                    if let Err(e) = self.delayer_tx_channel.send(sch_msg).await {
                        anyhow::bail!("Error sending ScheduledChangeScriptRecordMessage: {e:?}");
                    };
                }
            },
            _ => anyhow::bail!(
                "Calculating record delay for unexpected status: {:?}",
                self.status
            ),
        };

        Ok(())
    }

    fn time_shift(
        &mut self,
        next_record: SequencedChangeScriptRecord,
    ) -> anyhow::Result<SequencedChangeScriptRecord> {
        let current_time_ns = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        // Time Mode controls how the updated virtual time is calculated.
        match self.settings.time_mode {
            TimeMode::Live => {
                // Live - Use the actual current time.
                self.virtual_time_ns_current = current_time_ns;
                self.virtual_time_ns_offset = current_time_ns - self.virtual_time_ns_start;
            }
            TimeMode::Recorded => {
                // Recorded - Use the recorded time from the script.
                self.virtual_time_ns_current = self.virtual_time_ns_start + next_record.offset_ns;
                self.virtual_time_ns_offset = next_record.offset_ns;
            }
            TimeMode::Rebased(nanos) => {
                // Rebased - Use the rebased time provided in the config.
                self.virtual_time_ns_current = nanos + next_record.offset_ns;
                self.virtual_time_ns_offset = next_record.offset_ns;
            }
        };

        let shifted_change_record = match &next_record.record {
            ChangeScriptRecord::SourceChange(change_record) => {
                let mut shifted_change_record = change_record.clone();

                shifted_change_record
                    .source_change_event
                    .reactivator_start_ns = self.virtual_time_ns_current;
                shifted_change_record.source_change_event.reactivator_end_ns =
                    self.virtual_time_ns_current + 1;
                shifted_change_record
                    .source_change_event
                    .payload
                    .source
                    .ts_ns = self.virtual_time_ns_current;

                // TODO: Modify internal date times.

                ChangeScriptRecord::SourceChange(shifted_change_record)
            }
            ChangeScriptRecord::Label(_)
            | ChangeScriptRecord::PauseCommand(_)
            | ChangeScriptRecord::Finish(_) => next_record.record.clone(),
            _ => anyhow::bail!(
                "Unexpected ChangeScriptRecord type: {:?}",
                next_record.record
            ),
        };

        let shifted_record = SequencedChangeScriptRecord {
            seq: next_record.seq,
            offset_ns: self.virtual_time_ns_current,
            record: shifted_change_record,
        };

        self.next_record = Some(shifted_record.clone());

        Ok(shifted_record)
    }

    async fn transition_from_error_state(
        &mut self,
        command: &ScriptSourceChangeGeneratorCommand,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Attempting to transition from {:?} state via command: {:?}",
            self.status,
            command
        );

        if let ScriptSourceChangeGeneratorCommand::Reset = command {
            self.reset().await
        } else {
            Err(ScriptSourceChangeGeneratorError::Error(self.status).into())
        }
    }

    async fn transition_from_finished_state(
        &mut self,
        command: &ScriptSourceChangeGeneratorCommand,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Attempting to transition from {:?} state via command: {:?}",
            self.status,
            command
        );

        if let ScriptSourceChangeGeneratorCommand::Reset = command {
            self.reset().await
        } else {
            Err(ScriptSourceChangeGeneratorError::AlreadyFinished.into())
        }
    }

    async fn transition_from_paused_state(
        &mut self,
        command: &ScriptSourceChangeGeneratorCommand,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Transitioning from {:?} state via command: {:?}",
            self.status,
            command
        );

        // If we are unpausing for the first time, we need to initialize the start times based on time_mode config.
        if self.previous_record.is_none()
            && matches!(
                command,
                ScriptSourceChangeGeneratorCommand::Start
                    | ScriptSourceChangeGeneratorCommand::Step { .. }
                    | ScriptSourceChangeGeneratorCommand::Skip { .. }
            )
        {
            self.stats.actual_start_time_ns = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;

            self.virtual_time_ns_start = match self.settings.time_mode {
                TimeMode::Live => self.stats.actual_start_time_ns,
                TimeMode::Recorded => {
                    self.header_record.start_time.timestamp_nanos_opt().unwrap() as u64
                }
                TimeMode::Rebased(nanos) => nanos,
            };

            self.virtual_time_ns_current = self.virtual_time_ns_start;
            self.virtual_time_ns_offset = 0;
        }

        match command {
            ScriptSourceChangeGeneratorCommand::GetState => Ok(()),
            ScriptSourceChangeGeneratorCommand::Pause => Ok(()),
            ScriptSourceChangeGeneratorCommand::Reset => self.reset().await,
            ScriptSourceChangeGeneratorCommand::Skip {
                skips,
                spacing_mode,
            } => {
                log::info!(
                    "Script Skipping {} skips for TestRunSource {}",
                    skips,
                    self.settings.id
                );

                self.status = SourceChangeGeneratorStatus::Skipping;
                self.skips_remaining = *skips;
                self.skips_spacing_mode = spacing_mode.clone();
                self.schedule_next_change_stream_record().await
            }
            ScriptSourceChangeGeneratorCommand::Start => {
                log::info!("Script Started for TestRunSource {}", self.settings.id);

                self.status = SourceChangeGeneratorStatus::Running;
                self.schedule_next_change_stream_record().await
            }
            ScriptSourceChangeGeneratorCommand::Step {
                steps,
                spacing_mode,
            } => {
                log::info!(
                    "Script Stepping {} steps for TestRunSource {}",
                    steps,
                    self.settings.id
                );

                self.status = SourceChangeGeneratorStatus::Stepping;
                self.steps_remaining = *steps;
                self.steps_spacing_mode = spacing_mode.clone();
                self.schedule_next_change_stream_record().await
            }
            ScriptSourceChangeGeneratorCommand::Stop => {
                self.transition_to_stopped_state().await?;
                Ok(())
            }
        }
    }

    async fn transition_from_running_state(
        &mut self,
        command: &ScriptSourceChangeGeneratorCommand,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Transitioning from {:?} state via command: {:?}",
            self.status,
            command
        );

        match command {
            ScriptSourceChangeGeneratorCommand::GetState => Ok(()),
            ScriptSourceChangeGeneratorCommand::Pause => {
                self.status = SourceChangeGeneratorStatus::Paused;
                Ok(())
            }
            ScriptSourceChangeGeneratorCommand::Reset => {
                Err(ScriptSourceChangeGeneratorError::PauseToReset.into())
            }
            ScriptSourceChangeGeneratorCommand::Skip { .. } => {
                Err(ScriptSourceChangeGeneratorError::PauseToSkip.into())
            }
            ScriptSourceChangeGeneratorCommand::Start => Ok(()),
            ScriptSourceChangeGeneratorCommand::Step { .. } => {
                Err(ScriptSourceChangeGeneratorError::PauseToStep.into())
            }
            ScriptSourceChangeGeneratorCommand::Stop => {
                self.transition_to_stopped_state().await?;
                Ok(())
            }
        }
    }

    async fn transition_from_skipping_state(
        &mut self,
        command: &ScriptSourceChangeGeneratorCommand,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Transitioning from {:?} state via command: {:?}",
            self.status,
            command
        );

        match command {
            ScriptSourceChangeGeneratorCommand::GetState => Ok(()),
            ScriptSourceChangeGeneratorCommand::Pause => {
                self.status = SourceChangeGeneratorStatus::Paused;
                self.skips_remaining = 0;
                self.skips_spacing_mode = None;
                Ok(())
            }
            ScriptSourceChangeGeneratorCommand::Stop => {
                self.transition_to_stopped_state().await?;
                Ok(())
            }
            ScriptSourceChangeGeneratorCommand::Reset
            | ScriptSourceChangeGeneratorCommand::Skip { .. }
            | ScriptSourceChangeGeneratorCommand::Start
            | ScriptSourceChangeGeneratorCommand::Step { .. } => Err(
                ScriptSourceChangeGeneratorError::CurrentlySkipping(self.skips_remaining).into(),
            ),
        }
    }

    async fn transition_from_stepping_state(
        &mut self,
        command: &ScriptSourceChangeGeneratorCommand,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Transitioning from {:?} state via command: {:?}",
            self.status,
            command
        );

        match command {
            ScriptSourceChangeGeneratorCommand::GetState => Ok(()),
            ScriptSourceChangeGeneratorCommand::Pause => {
                self.status = SourceChangeGeneratorStatus::Paused;
                self.steps_remaining = 0;
                self.steps_spacing_mode = None;
                Ok(())
            }
            ScriptSourceChangeGeneratorCommand::Stop => {
                self.transition_to_stopped_state().await?;
                Ok(())
            }
            ScriptSourceChangeGeneratorCommand::Reset
            | ScriptSourceChangeGeneratorCommand::Skip { .. }
            | ScriptSourceChangeGeneratorCommand::Start
            | ScriptSourceChangeGeneratorCommand::Step { .. } => Err(
                ScriptSourceChangeGeneratorError::CurrentlyStepping(self.steps_remaining).into(),
            ),
        }
    }

    async fn transition_from_stopped_state(
        &mut self,
        command: &ScriptSourceChangeGeneratorCommand,
    ) -> anyhow::Result<()> {
        log::debug!(
            "Attempting to transition from {:?} state via command: {:?}",
            self.status,
            command
        );

        if let ScriptSourceChangeGeneratorCommand::Reset = command {
            self.reset().await
        } else {
            Err(ScriptSourceChangeGeneratorError::AlreadyStopped.into())
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
        self.skips_spacing_mode = None;
        self.steps_remaining = 0;
        self.steps_spacing_mode = None;

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
        self.skips_spacing_mode = None;
        self.steps_remaining = 0;
        self.steps_spacing_mode = None;

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
        let result_summary: ScriptSourceChangeGeneratorResultSummary = self.into();
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

impl Debug for ScriptSourceChangeGeneratorInternalState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScriptSourceChangeGeneratorInternalState")
            .field("error_messages", &self.error_messages)
            .field(
                "ignore_scripted_pause_commands",
                &self.settings.ignore_scripted_pause_commands,
            )
            .field("header_record", &self.header_record)
            .field("next_record", &self.next_record)
            .field("previous_record", &self.previous_record)
            .field("skips_remaining", &self.skips_remaining)
            .field("skips_spacing_mode", &self.skips_spacing_mode)
            .field("spacing_mode", &self.settings.spacing_mode)
            .field("status", &self.status)
            .field("stats", &self.stats)
            .field("steps_remaining", &self.steps_remaining)
            .field("steps_spacing_mode", &self.steps_spacing_mode)
            .field("time_mode", &self.settings.time_mode)
            .field("virtual_time_ns_current", &self.virtual_time_ns_current)
            .field("virtual_time_ns_offset", &self.virtual_time_ns_offset)
            .field("virtual_time_ns_start", &self.virtual_time_ns_start)
            .finish()
    }
}

#[derive(Clone, Debug, Serialize, Default)]
pub struct ScriptSourceChangeGeneratorStats {
    pub actual_start_time_ns: u64,
    pub actual_end_time_ns: u64,
    pub num_source_change_records: u64,
    pub num_skipped_source_change_records: u64,
    pub num_label_records: u64,
    pub num_pause_records: u64,
}

#[derive(Clone, Serialize)]
pub struct ScriptSourceChangeGeneratorResultSummary {
    pub actual_start_time: String,
    pub actual_start_time_ns: u64,
    pub actual_end_time: String,
    pub actual_end_time_ns: u64,
    pub run_duration_ns: u64,
    pub run_duration_sec: f64,
    pub num_source_change_records: u64,
    pub num_skipped_source_change: u64,
    pub num_label_records: u64,
    pub num_pause_records: u64,
    pub processing_rate: f64,
    pub test_run_source_id: String,
}

impl From<&mut ScriptSourceChangeGeneratorInternalState>
    for ScriptSourceChangeGeneratorResultSummary
{
    fn from(state: &mut ScriptSourceChangeGeneratorInternalState) -> Self {
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
            num_source_change_records: state.stats.num_source_change_records,
            num_skipped_source_change: state.stats.num_skipped_source_change_records,
            num_label_records: state.stats.num_label_records,
            num_pause_records: state.stats.num_pause_records,
            processing_rate: state.stats.num_source_change_records as f64 / run_duration_sec,
            test_run_source_id: state.settings.id.to_string(),
        }
    }
}

impl Debug for ScriptSourceChangeGeneratorResultSummary {
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
        let source_change_records = format!(
            "{} (skipped:{}, label:{}, pause:{})",
            self.num_source_change_records,
            self.num_skipped_source_change,
            self.num_label_records,
            self.num_pause_records
        );
        let processing_rate = format!("{:.2} changes / sec", self.processing_rate);

        f.debug_struct("ScriptSourceChangeGeneratorResultSummary")
            .field("test_run_source_id", &self.test_run_source_id)
            .field("start_time", &start_time)
            .field("end_time", &end_time)
            .field("run_duration", &run_duration)
            .field("source_change_records", &source_change_records)
            .field("processing_rate", &processing_rate)
            .finish()
    }
}

// Function that defines the operation of the ScriptSourceChangeGenerator thread.
// The ScriptSourceChangeGenerator thread processes ChangeScriptPlayerCommands sent to it from the Web API handler functions.
// The Web API function communicate via a channel and provide oneshot channels for the ScriptSourceChangeGenerator to send responses back.
pub async fn script_processor_thread(
    mut command_rx_channel: Receiver<ScriptSourceChangeGeneratorMessage>,
    settings: ScriptSourceChangeGeneratorSettings,
) -> anyhow::Result<()> {
    log::info!(
        "Script processor thread started for TestRunSource {} ...",
        settings.id
    );

    // The ScriptSourceChangeGenerator always starts with the first script record loaded and Paused.
    let (mut state, mut change_rx_channel) =
        match ScriptSourceChangeGeneratorInternalState::initialize(settings).await {
            Ok((state, change_rx_channel)) => (state, change_rx_channel),
            Err(e) => {
                // If initialization fails, don't dont transition to an error state, just log an error and exit the thread.
                let msg = format!("Error initializing ScriptSourceChangeGenerator: {e:?}");
                log::error!("{msg}");
                anyhow::bail!(msg);
            }
        };

    // Loop to process commands sent to the ScriptSourceChangeGenerator or read from the Change Stream.
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
                        if change_stream_message.seq_num == state.message_seq_num && state.status.is_processing() {
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

pub async fn delayer_thread(
    id: TestRunSourceId,
    mut delayer_rx_channel: Receiver<ScheduledChangeScriptRecordMessage>,
    change_tx_channel: Sender<ScheduledChangeScriptRecordMessage>,
) {
    log::info!("Delayer thread started for TestRunSource {id} ...");

    loop {
        match delayer_rx_channel.recv().await {
            Some(message) => {
                // Sleep for the specified time before sending the message to the change_tx_channel.
                sleep(Duration::new(0, message.delay_ns as u32)).await;
                if let Err(e) = change_tx_channel.send(message).await {
                    log::error!("Error sending ScheduledChangeScriptRecordMessage to change_tx_channel: {e:?}");
                }
            }
            None => {
                log::error!("ChangeScriptRecord delayer channel closed.");
                break;
            }
        }
    }
}

pub async fn rate_limiter_thread(
    id: TestRunSourceId,
    spacing_mode: SpacingMode,
    mut delayer_rx_channel: Receiver<ScheduledChangeScriptRecordMessage>,
    change_tx_channel: Sender<ScheduledChangeScriptRecordMessage>,
) {
    log::info!("Rate limiter thread started for TestRunSource {id} ...");

    let limiter = match spacing_mode {
        SpacingMode::Rate(rate) => RateLimiter::direct(Quota::per_second(rate)),
        _ => RateLimiter::direct(Quota::per_second(NonZeroU32::new(u32::MAX).unwrap())),
    };

    loop {
        match delayer_rx_channel.recv().await {
            Some(message) => {
                limiter.until_ready().await;
                if let Err(e) = change_tx_channel.send(message).await {
                    log::error!("Error sending ScheduledChangeScriptRecordMessage to change_tx_channel: {e:?}");
                }
            }
            None => {
                log::error!("ChangeScriptRecord delayer channel closed.");
                break;
            }
        }
    }
}
