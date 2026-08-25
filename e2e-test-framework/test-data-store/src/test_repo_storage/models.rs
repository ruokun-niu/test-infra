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

use chrono::{DateTime, Utc};
use std::{num::NonZeroU32, str::FromStr};

use serde::{
    de::{self, Deserializer},
    Deserialize, Serialize, Serializer,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeMode {
    Live,
    Recorded,
    Rebased(u64),
}

impl Default for TimeMode {
    fn default() -> Self {
        Self::Recorded
    }
}

impl FromStr for TimeMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s.to_lowercase().as_str() {
            "live" => Ok(Self::Live),
            "recorded" => Ok(Self::Recorded),
            _ => match chrono::DateTime::parse_from_rfc3339(s) {
                Ok(t) => Ok(Self::Rebased(t.timestamp_nanos_opt().unwrap() as u64)),
                Err(e) => {
                    anyhow::bail!("Error parsing TimeMode - value:{s}, error:{e}");
                }
            },
        }
    }
}

impl std::fmt::Display for TimeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Live => write!(f, "live"),
            Self::Recorded => write!(f, "recorded"),
            Self::Rebased(time) => write!(f, "{time}"),
        }
    }
}

impl<'de> Deserialize<'de> for TimeMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value: String = Deserialize::deserialize(deserializer)?;
        value.parse::<TimeMode>().map_err(de::Error::custom)
    }
}

impl Serialize for TimeMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Live => serializer.serialize_str("live"),
            Self::Recorded => serializer.serialize_str("recorded"),
            Self::Rebased(timestamp) => {
                // Convert the timestamp to a DateTime
                let datetime = DateTime::<Utc>::from_timestamp_nanos(*timestamp as i64);

                // Format to RFC 3339 and serialize as a string
                serializer.serialize_str(&datetime.to_rfc3339())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpacingMode {
    None,
    Rate(NonZeroU32),
    Recorded,
}

impl Default for SpacingMode {
    fn default() -> Self {
        Self::Recorded
    }
}

impl FromStr for SpacingMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s.to_lowercase().as_str() {
            "none" => Ok(Self::None),
            "recorded" => Ok(Self::Recorded),
            _ => {
                // Parse the string as a NonZero<u32>.
                match s.parse::<u32>() {
                    Ok(num) => match NonZeroU32::new(num) {
                        Some(rate) => Ok(Self::Rate(rate)),
                        None => anyhow::bail!("Invalid SpacingMode: {s}"),
                    },
                    Err(e) => {
                        anyhow::bail!("Error parsing SpacingMode: {e}");
                    }
                }
            }
        }
    }
}

impl std::fmt::Display for SpacingMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Recorded => write!(f, "recorded"),
            Self::Rate(rate) => write!(f, "{rate}"),
        }
    }
}

impl<'de> Deserialize<'de> for SpacingMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value: String = Deserialize::deserialize(deserializer)?;
        value.parse::<SpacingMode>().map_err(de::Error::custom)
    }
}

impl Serialize for SpacingMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::None => serializer.serialize_str("none"),
            Self::Recorded => serializer.serialize_str("recorded"),
            Self::Rate(rate) => serializer.serialize_str(&rate.to_string()),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LocalTestDefinition {
    pub test_id: String,
    pub version: u32,
    pub description: Option<String>,
    pub test_folder: Option<String>,
    #[serde(default)]
    pub drasi_lib_instances: Vec<TestDrasiLibInstanceDefinition>,
    #[serde(default)]
    pub queries: Vec<TestQueryDefinition>,
    #[serde(default)]
    pub reactions: Vec<TestReactionDefinition>,
    #[serde(default)]
    pub sources: Vec<TestSourceDefinition>,
    /// Completion handlers that execute when all components finish
    #[serde(default)]
    pub completion_handlers: Vec<CompletionHandlerDefinition>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TestDefinition {
    #[serde(skip_deserializing)]
    #[serde(default)]
    pub test_id: String,
    pub version: u32,
    pub description: Option<String>,
    pub test_folder: Option<String>,
    #[serde(default)]
    pub drasi_lib_instances: Vec<TestDrasiLibInstanceDefinition>,
    #[serde(default)]
    pub queries: Vec<TestQueryDefinition>,
    #[serde(default)]
    pub reactions: Vec<TestReactionDefinition>,
    #[serde(default)]
    pub sources: Vec<TestSourceDefinition>,
    /// Completion handlers that execute when all components finish
    /// These define what happens when the test completes (logging, uploads, etc.)
    #[serde(default)]
    pub completion_handlers: Vec<CompletionHandlerDefinition>,
}

/// Definition of a completion handler
///
/// Completion handlers execute when all test components (sources, queries, reactions) finish.
/// They are intrinsic to the test definition and define what happens on completion.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum CompletionHandlerDefinition {
    /// Log completion summary to configured log level
    Log(LogHandlerConfig),
    /// Compare each reaction's ordered-stream SHA-256 or final materialized
    /// state to a stored baseline.
    Sha256Determinism(Sha256DeterminismHandlerConfig),
}

/// Configuration for LogCompletionHandler
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogHandlerConfig {
    /// Log level: "debug", "info", "warn", or "error"
    /// Defaults to "info" if not specified
    #[serde(default)]
    pub log_level: Option<String>,
}

/// Configuration for ordered-stream or final-state SHA-256 verification.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Sha256DeterminismHandlerConfig {
    /// Map of `test_reaction_id` -> expected SHA-256 (hex). Reactions absent
    /// from this map are treated according to `missing_baseline`.
    #[serde(default)]
    pub expected: std::collections::BTreeMap<String, String>,
    /// Behaviour when an ordered baseline or final-state golden is missing.
    #[serde(default)]
    pub missing_baseline: MissingBaselinePolicy,
    /// Path to a versioned final-state golden manifest, relative to the test's
    /// data folder. When set, final-state logger output is compared row by row.
    #[serde(default)]
    pub golden_file: Option<String>,
    /// Maximum number of missing and unexpected rows included per reaction in
    /// the verdict. Complete actual state is written to a separate candidate.
    #[serde(default = "default_diagnostic_limit")]
    pub diagnostic_limit: usize,
}

fn default_diagnostic_limit() -> usize {
    20
}

impl Default for Sha256DeterminismHandlerConfig {
    fn default() -> Self {
        Self {
            expected: std::collections::BTreeMap::new(),
            missing_baseline: MissingBaselinePolicy::default(),
            golden_file: None,
            diagnostic_limit: default_diagnostic_limit(),
        }
    }
}

/// What to do when a reaction has no entry in `expected`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum MissingBaselinePolicy {
    /// Log the actual SHA-256 and treat the reaction as passing. Useful for
    /// capturing a fresh baseline on the first run of a new test.
    #[default]
    Warn,
    /// Treat a missing baseline as a verification failure.
    Fail,
}

impl TestDefinition {
    pub fn get_test_query(&self, query_id: &str) -> anyhow::Result<TestQueryDefinition> {
        let test_query_definition = self
            .queries
            .iter()
            .find(|query| query.test_query_id == query_id)
            .ok_or_else(|| anyhow::anyhow!("Test Query with ID {query_id:?} not found"))?;

        Ok(test_query_definition.clone())
    }

    pub fn get_test_reaction(&self, reaction_id: &str) -> anyhow::Result<TestReactionDefinition> {
        let test_reaction_definition = self
            .reactions
            .iter()
            .find(|reaction| reaction.test_reaction_id == reaction_id)
            .ok_or_else(|| anyhow::anyhow!("Test Reaction with ID {reaction_id:?} not found"))?;

        Ok(test_reaction_definition.clone())
    }

    pub fn get_test_source(&self, source_id: &str) -> anyhow::Result<TestSourceDefinition> {
        let test_source_definition = self
            .sources
            .iter()
            .find(|source| match source {
                TestSourceDefinition::Model(def) => def.common.test_source_id == source_id,
                TestSourceDefinition::Script(def) => def.common.test_source_id == source_id,
            })
            .ok_or_else(|| anyhow::anyhow!("Test Source with ID {source_id:?} not found"))?;

        Ok(test_source_definition.clone())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
#[serde(tag = "kind")]
pub enum TestSourceDefinition {
    Model(ModelTestSourceDefinition),
    Script(ScriptTestSourceDefinition),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonTestSourceDefinition {
    pub test_source_id: String,
    #[serde(default)]
    pub source_change_dispatchers: Vec<SourceChangeDispatcherDefinition>,
    #[serde(default)]
    pub subscribers: Vec<QueryId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScriptTestSourceDefinition {
    pub bootstrap_data_generator: Option<BootstrapDataGeneratorDefinition>,
    #[serde(flatten)]
    pub common: CommonTestSourceDefinition,
    pub source_change_generator: Option<SourceChangeGeneratorDefinition>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelTestSourceDefinition {
    #[serde(flatten)]
    pub common: CommonTestSourceDefinition,
    pub model_data_generator: Option<ModelDataGeneratorDefinition>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum BootstrapDataGeneratorDefinition {
    Script(ScriptBootstrapDataGeneratorDefinition),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonBootstrapDataGeneratorDefinition {
    #[serde(default)]
    pub time_mode: TimeMode,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScriptBootstrapDataGeneratorDefinition {
    #[serde(flatten)]
    pub common: CommonBootstrapDataGeneratorDefinition,
    pub script_file_folder: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ModelDataGeneratorDefinition {
    BuildingHierarchy(BuildingHierarchyDataGeneratorDefinition),
    StockTrade(StockTradeDataGeneratorDefinition),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonModelDataGeneratorDefinition {
    pub change_count: Option<u64>,
    pub change_interval: Option<(u64, f64, u64, u64)>,
    pub seed: Option<u64>,
    #[serde(default)]
    pub spacing_mode: SpacingMode,
    #[serde(default)]
    pub time_mode: TimeMode,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BuildingHierarchyDataGeneratorDefinition {
    #[serde(flatten)]
    pub common: CommonModelDataGeneratorDefinition,
    pub building_count: Option<(u32, f64)>,
    pub floor_count: Option<(u32, f64)>,
    pub room_count: Option<(u32, f64)>,
    pub room_sensors: Vec<SensorDefinition>,
    #[serde(default)]
    pub send_initial_inserts: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StockTradeDataGeneratorDefinition {
    #[serde(flatten)]
    pub common: CommonModelDataGeneratorDefinition,
    pub stock_definitions: Vec<StockDefinition>,
    pub price_init: Option<(f64, f64)>,
    pub price_change: Option<(f64, f64)>,
    pub price_momentum: Option<(i32, f64, f64)>,
    pub price_range: Option<(f64, f64)>,
    pub volume_init: Option<(i64, f64)>,
    pub volume_change: Option<(i64, f64)>,
    pub volume_momentum: Option<(i32, f64, f64)>,
    pub volume_range: Option<(i64, i64)>,
    #[serde(default)]
    pub send_initial_inserts: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StockDefinition {
    pub symbol: String,
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum SensorDefinition {
    NormalFloat(FloatNormalDistSensorDefinition),
    NormalInt(IntNormalDistSensorDefinition),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FloatNormalDistSensorDefinition {
    pub id: String,
    pub momentum_init: Option<(i32, f64, f64)>, // mean, std_dev, reversal probability
    pub value_change: Option<(f64, f64)>,       // mean, std_dev
    pub value_init: Option<(f64, f64)>,         // mean, std_dev
    pub value_range: Option<(f64, f64)>,        // min, max
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IntNormalDistSensorDefinition {
    pub id: String,
    pub momentum_init: Option<(i32, f64, f64)>, // mean, std_dev, reversal probability
    pub value_change: Option<(i64, f64)>,       // mean, std_dev
    pub value_init: Option<(i64, f64)>,         // mean, std_dev
    pub value_range: Option<(i64, i64)>,        // min, max
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum SourceChangeGeneratorDefinition {
    Script(ScriptSourceChangeGeneratorDefinition),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonSourceChangeGeneratorDefinition {
    #[serde(default)]
    pub spacing_mode: SpacingMode,
    #[serde(default)]
    pub time_mode: TimeMode,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScriptSourceChangeGeneratorDefinition {
    #[serde(flatten)]
    pub common: CommonSourceChangeGeneratorDefinition,
    #[serde(default = "is_false")]
    pub ignore_scripted_pause_commands: bool,
    pub script_file_folder: String,
}
fn is_false() -> bool {
    false
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum SourceChangeDispatcherDefinition {
    Console(ConsoleSourceChangeDispatcherDefinition),
    Dapr(DaprSourceChangeDispatcherDefinition),
    Http(HttpSourceChangeDispatcherDefinition),
    Grpc(GrpcSourceChangeDispatcherDefinition),
    JsonlFile(JsonlFileSourceChangeDispatcherDefinition),
    RedisStream(RedisStreamSourceChangeDispatcherDefinition),
    DrasiLibInstanceChannel(DrasiLibInstanceChannelSourceChangeDispatcherDefinition),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConsoleSourceChangeDispatcherDefinition {
    pub date_time_format: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaprSourceChangeDispatcherDefinition {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub pubsub_name: Option<String>,
    pub pubsub_topic: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonlFileSourceChangeDispatcherDefinition {
    pub max_events_per_file: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RedisStreamSourceChangeDispatcherDefinition {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub stream_name: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HttpSourceChangeDispatcherDefinition {
    pub url: String,
    pub port: u16,
    pub endpoint: Option<String>,
    pub timeout_seconds: Option<u64>,
    pub batch_events: Option<bool>,
    pub source_id: Option<String>,
    // Adaptive batching fields
    pub adaptive_enabled: Option<bool>,
    pub batch_size: Option<u64>,
    pub batch_timeout_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GrpcSourceChangeDispatcherDefinition {
    pub host: String,
    pub port: u16,
    pub timeout_seconds: Option<u64>,
    pub batch_events: Option<bool>,
    pub tls: Option<bool>,
    pub source_id: String, // Required for Drasi SourceService
    // Adaptive batching configuration
    pub adaptive_enabled: Option<bool>,
    pub batch_size: Option<u64>,
    pub batch_timeout_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DrasiLibInstanceChannelSourceChangeDispatcherDefinition {
    pub drasi_lib_instance_id: String,
    pub source_id: String,
    pub buffer_size: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TestQueryDefinition {
    #[serde(default)]
    pub test_query_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_trigger: Option<StopTriggerDefinition>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TestReactionDefinition {
    #[serde(default)]
    pub test_reaction_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_handler: Option<ReactionHandlerDefinition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_triggers: Option<Vec<StopTriggerDefinition>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ResultStreamHandlerDefinition {
    DaprPubSub(DaprPubSubResultStreamHandlerDefinition),
    RedisStream(RedisStreamResultStreamHandlerDefinition),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaprPubSubResultStreamHandlerDefinition {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub pubsub_name: Option<String>,
    pub pubsub_topic: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RedisStreamResultStreamHandlerDefinition {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub stream_name: Option<String>,
    pub process_old_entries: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum StopTriggerDefinition {
    RecordSequenceNumber(RecordSequenceNumberStopTriggerDefinition),
    RecordCount(RecordCountStopTriggerDefinition),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordSequenceNumberStopTriggerDefinition {
    pub record_sequence_number: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordCountStopTriggerDefinition {
    pub record_count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ReactionHandlerDefinition {
    Http(HttpReactionHandlerDefinition),
    EventGrid(EventGridReactionHandlerDefinition),
    Grpc(GrpcReactionHandlerDefinition),
    DrasiLibInstanceCallback(DrasiLibInstanceCallbackReactionHandlerDefinition),
    DrasiLibInstanceChannel(DrasiLibInstanceChannelReactionHandlerDefinition),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HttpReactionHandlerDefinition {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub path: Option<String>,
    pub correlation_header: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventGridReactionHandlerDefinition {
    pub endpoint: Option<String>,
    pub access_key: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GrpcReactionHandlerDefinition {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub correlation_metadata_key: Option<String>,
    pub query_ids: Vec<String>,              // Query IDs to subscribe to
    pub include_initial_state: Option<bool>, // Whether to receive initial state
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DrasiLibInstanceCallbackReactionHandlerDefinition {
    pub drasi_lib_instance_id: String,
    pub reaction_id: String,
    pub callback_type: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DrasiLibInstanceChannelReactionHandlerDefinition {
    pub drasi_lib_instance_id: String,
    pub reaction_id: String,
    pub buffer_size: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum OutputLoggerDefinition {
    Console(ConsoleOutputLoggerDefinition),
    JsonlFile(JsonlFileOutputLoggerDefinition),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConsoleOutputLoggerDefinition {
    pub date_time_format: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonlFileOutputLoggerDefinition {
    pub max_lines_per_file: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryId {
    #[serde(default = "default_query_node_id")]
    pub node_id: String,
    #[serde(default = "default_query_id")]
    pub query_id: String,
}
fn default_query_node_id() -> String {
    "default".to_string()
}
fn default_query_id() -> String {
    "test_query".to_string()
}

#[derive(Debug, thiserror::Error)]
pub enum ParseQueryIdError {
    #[error("Invalid format for QueryId - {0}")]
    InvalidFormat(String),
    #[error("Invalid values for QueryId - {0}")]
    InvalidValues(String),
}

impl TryFrom<&str> for QueryId {
    type Error = ParseQueryIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = value.split('.').collect();
        if parts.len() == 2 {
            Ok(QueryId {
                node_id: parts[0].to_string(),
                query_id: parts[1].to_string(),
            })
        } else {
            Err(ParseQueryIdError::InvalidFormat(value.to_string()))
        }
    }
}

/// Test definition for a drasi-lib instance stored in test repositories
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestDrasiLibInstanceDefinition {
    /// Unique identifier for the drasi-lib instance.
    pub test_drasi_lib_instance_id: String,

    /// Human-readable name for the drasi-lib instance.
    pub name: Option<String>,

    /// Description of the drasi-lib instance's purpose in the test.
    pub description: Option<String>,

    /// drasi-lib instance configuration.
    pub config: DrasiLibInstanceConfig,
}

/// Runtime configuration for an embedded drasi-lib instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrasiLibInstanceConfig {
    /// Log level for the drasi-lib instance (trace, debug, info, warn, error).
    pub log_level: Option<String>,

    /// Source configurations.
    pub sources: Vec<DrasiLibSourceConfig>,

    /// Query configurations.
    pub queries: Vec<DrasiLibQueryConfig>,

    /// Reaction configurations.
    pub reactions: Vec<DrasiLibReactionConfig>,
}

fn default_true() -> bool {
    true
}

/// Source configuration for an embedded drasi-lib instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrasiLibSourceConfig {
    /// Unique identifier for the source.
    pub id: String,

    /// Source kind, such as application.
    pub kind: String,

    /// Whether to automatically start this source.
    #[serde(default = "default_true")]
    pub auto_start: bool,

    /// Kind-specific source configuration.
    #[serde(default)]
    pub config: serde_json::Value,
}

/// Query configuration for an embedded drasi-lib instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrasiLibQueryConfig {
    /// Unique identifier for the query.
    pub id: String,

    /// Cypher query string.
    pub query: String,

    /// IDs of sources this query subscribes to.
    pub sources: Vec<String>,

    /// Whether to automatically start this query.
    #[serde(default = "default_true")]
    pub auto_start: bool,

    /// Optional query-specific options.
    #[serde(default)]
    pub config: serde_json::Value,
}

/// Reaction configuration for an embedded drasi-lib instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrasiLibReactionConfig {
    /// Unique identifier for the reaction.
    pub id: String,

    /// Reaction kind, such as application.
    pub kind: String,

    /// IDs of queries this reaction subscribes to.
    pub queries: Vec<String>,

    /// Whether to automatically start this reaction.
    #[serde(default = "default_true")]
    pub auto_start: bool,

    /// Kind-specific reaction configuration.
    #[serde(default)]
    pub config: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        io::{BufReader, Write},
    };

    use tempfile::tempdir;

    use super::*;

    fn create_test_file(content: &str) -> File {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("unit_test.test.json");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "{content}").unwrap();
        file.sync_all().unwrap();
        File::open(file_path).unwrap()
    }

    #[test]
    fn test_read_bootstrap_data_generator() {
        let content = r#"
        {
            "kind": "Script",
            "script_file_folder": "bootstrap_data_scripts",
            "script_file_list": ["init*.jsonl", "deploy*.jsonl"],
            "time_mode": "recorded"
        }
        "#;
        let file = create_test_file(content);
        let reader = BufReader::new(file);
        let bootstrap_data_generator: BootstrapDataGeneratorDefinition =
            serde_json::from_reader(reader).unwrap();

        match bootstrap_data_generator {
            BootstrapDataGeneratorDefinition::Script(definition) => {
                assert_eq!(definition.common.time_mode, TimeMode::Recorded);
                assert_eq!(definition.script_file_folder, "bootstrap_data_scripts");
            }
        }
    }

    #[test]
    fn test_read_source_change_generator() {
        let content = r#"
        {
            "kind": "Script",
            "script_file_folder": "source_change_scripts",
            "script_file_list": ["change01.jsonl", "change02.jsonl"],
            "spacing_mode": "100",
            "time_mode": "recorded"
        }
        "#;
        let file = create_test_file(content);
        let reader = BufReader::new(file);
        let source_change_generator: SourceChangeGeneratorDefinition =
            serde_json::from_reader(reader).unwrap();

        match source_change_generator {
            SourceChangeGeneratorDefinition::Script(definition) => {
                assert_eq!(
                    definition.common.spacing_mode,
                    SpacingMode::Rate(NonZeroU32::new(100).unwrap())
                );
                assert_eq!(definition.common.time_mode, TimeMode::Recorded);
                assert_eq!(definition.script_file_folder, "source_change_scripts");
            }
        }
    }

    #[test]
    fn test_read_script_source() {
        let content = r#"
        {
            "test_source_id": "source1",
            "kind": "Script",
            "bootstrap_data_generator": {
                "kind": "Script",
                "script_file_folder": "bootstrap_data_scripts",
                "time_mode": "live"
            },
            "source_change_generator": {
                "kind": "Script",
                "script_file_folder": "source_change_scripts",
                "spacing_mode": "recorded",
                "time_mode": "live"
            }
        }
        "#;
        let file = create_test_file(content);
        let reader = BufReader::new(file);
        let source: TestSourceDefinition = serde_json::from_reader(reader).unwrap();

        match source {
            TestSourceDefinition::Script(source) => {
                assert_eq!(source.common.test_source_id, "source1");

                match source.bootstrap_data_generator.as_ref().unwrap() {
                    BootstrapDataGeneratorDefinition::Script(definition) => {
                        assert_eq!(definition.common.time_mode, TimeMode::Live);
                        assert_eq!(definition.script_file_folder, "bootstrap_data_scripts");
                    }
                }

                match source.source_change_generator.as_ref().unwrap() {
                    SourceChangeGeneratorDefinition::Script(definition) => {
                        assert_eq!(definition.common.spacing_mode, SpacingMode::Recorded);
                        assert_eq!(definition.common.time_mode, TimeMode::Live);
                        assert_eq!(definition.script_file_folder, "source_change_scripts");
                    }
                }
            }
            _ => panic!("Expected ScriptTestSourceDefinition"),
        }
    }

    #[test]
    fn test_read_script_source_with_no_data_generators() {
        let content = r#"
        {
            "test_source_id": "source1",
            "kind": "Script"
        }
        "#;
        let file = create_test_file(content);
        let reader = BufReader::new(file);
        let source: TestSourceDefinition = serde_json::from_reader(reader).unwrap();

        match source {
            TestSourceDefinition::Script(source) => {
                assert_eq!(source.common.test_source_id, "source1");
                assert!(source.bootstrap_data_generator.is_none());
                assert!(source.source_change_generator.is_none());
            }
            _ => panic!("Expected ScriptTestSourceDefinition"),
        }
    }

    #[test]
    fn test_read_test_definition() {
        let content = r#"
        {
            "test_id": "test1",
            "version": 1,
            "description": "A test definition",
            "test_folder": "test1",
            "sources": [
                {
                    "test_source_id": "source1",
                    "kind": "Script",
                    "bootstrap_data_generator": {
                        "kind": "Script",
                        "script_file_folder": "bootstrap_data_scripts",
                        "time_mode": "live"
                    },
                    "source_change_generator": {
                        "kind": "Script",
                        "script_file_folder": "source_change_scripts",
                        "spacing_mode": "recorded",
                        "time_mode": "live"
                    }
                }
            ],
            "queries": [],
            "reactions": [
                {
                    "test_reaction_id": "reaction1",
                    "output_handler": {
                        "kind": "Http",
                        "host": "localhost",
                        "port": 8080,
                        "path": "/webhook"
                    },
                    "output_loggers": [],
                    "stop_triggers": []
                }
            ],
            "clients": []
        }
        "#;
        let file = create_test_file(content);
        let reader = BufReader::new(file);
        let test_definition: TestDefinition = serde_json::from_reader(reader).unwrap();

        // test_id is skipped during deserialization, so it will be empty
        assert_eq!(test_definition.test_id, "");
        assert_eq!(test_definition.version, 1);
        assert_eq!(
            test_definition.description.as_ref().unwrap(),
            "A test definition"
        );
        assert_eq!(test_definition.test_folder.as_ref().unwrap(), "test1");
        assert_eq!(test_definition.sources.len(), 1);
        let source = &test_definition.sources[0];

        match source {
            TestSourceDefinition::Script(source) => {
                assert_eq!(source.common.test_source_id, "source1");

                match source.bootstrap_data_generator.as_ref().unwrap() {
                    BootstrapDataGeneratorDefinition::Script(definition) => {
                        assert_eq!(definition.common.time_mode, TimeMode::Live);
                        assert_eq!(definition.script_file_folder, "bootstrap_data_scripts");
                    }
                }

                match source.source_change_generator.as_ref().unwrap() {
                    SourceChangeGeneratorDefinition::Script(definition) => {
                        assert_eq!(definition.common.spacing_mode, SpacingMode::Recorded);
                        assert_eq!(definition.common.time_mode, TimeMode::Live);
                        assert_eq!(definition.script_file_folder, "source_change_scripts");
                    }
                }
            }
            _ => panic!("Expected ScriptTestSourceDefinition"),
        }

        // Test reactions
        assert_eq!(test_definition.reactions.len(), 1);
        let reaction = &test_definition.reactions[0];
        assert_eq!(reaction.test_reaction_id, "reaction1");

        match reaction.output_handler.as_ref().unwrap() {
            ReactionHandlerDefinition::Http(http_handler) => {
                assert_eq!(http_handler.host, Some("localhost".to_string()));
                assert_eq!(http_handler.port, Some(8080));
                assert_eq!(http_handler.path, Some("/webhook".to_string()));
            }
            _ => panic!("Expected Http reaction handler"),
        }

        // Test get_test_reaction() method
        let retrieved_reaction = test_definition.get_test_reaction("reaction1").unwrap();
        assert_eq!(retrieved_reaction.test_reaction_id, "reaction1");

        // Test error case
        assert!(test_definition.get_test_reaction("nonexistent").is_err());
    }

    #[test]
    fn test_read_test_definition_without_reactions() {
        // Test backward compatibility - old test definitions without reactions field
        let content = r#"
        {
            "test_id": "test2",
            "version": 1,
            "description": "A test without reactions",
            "test_folder": "test2",
            "sources": [],
            "queries": []
        }
        "#;
        let file = create_test_file(content);
        let reader = BufReader::new(file);
        let test_definition: TestDefinition = serde_json::from_reader(reader).unwrap();

        // test_id is skipped during deserialization, so it will be empty
        assert_eq!(test_definition.test_id, "");
        assert_eq!(test_definition.reactions.len(), 0); // Should default to empty vec
    }

    #[test]
    fn test_read_config_format_compatibility() {
        // Test the config format from the example file
        let content = r#"
        {
            "test_id": "building_comfort",
            "version": 1,
            "description": "",
            "test_folder": "building_comfort",
            "queries": [
                {
                    "test_query_id": "room-comfort-level",
                    "handler": {
                        "kind": "Http",
                        "port": 9001,
                        "path": "/reaction",
                        "correlation_header": "X-Query-Sequence"
                    },
                    "stop_trigger": {
                        "kind": "RecordCount",
                        "record_count": 90000
                    }
                }
            ],
            "reactions": [
                {
                    "test_reaction_id": "building-comfort",
                    "handler": {
                        "kind": "Http",
                        "port": 9001,
                        "path": "/reaction",
                        "correlation_header": "X-Query-Sequence"
                    },
                    "stop_trigger": {
                        "kind": "RecordCount",
                        "record_count": 90000
                    }
                }
            ],
            "sources": []
        }
        "#;
        let file = create_test_file(content);
        let reader = BufReader::new(file);
        let test_definition: TestDefinition = serde_json::from_reader(reader).unwrap();

        // Test queries - the handler field should have been parsed into the model
        assert_eq!(test_definition.queries.len(), 1);
        let query = &test_definition.queries[0];
        assert_eq!(query.test_query_id, "room-comfort-level");
        assert!(query.stop_trigger.is_some());

        match query.stop_trigger.as_ref().unwrap() {
            StopTriggerDefinition::RecordCount(trigger) => {
                assert_eq!(trigger.record_count, 90000);
            }
            _ => panic!("Expected RecordCount stop trigger"),
        }

        // Test reactions - since the JSON has "handler" field which isn't in our model,
        // it will be ignored during deserialization
        assert_eq!(test_definition.reactions.len(), 1);
        let reaction = &test_definition.reactions[0];
        assert_eq!(reaction.test_reaction_id, "building-comfort");
        // The handler field from JSON won't be parsed since it's not in the model
        assert!(reaction.output_handler.is_none());
        // The JSON has "stop_trigger" but the field is "stop_triggers", so it's None
        assert!(reaction.stop_triggers.is_none());
    }

    #[test]
    fn test_parse_actual_config_file() {
        // Test parsing the actual config file structure
        let local_test = r#"
        {
            "test_id": "building_comfort",
            "version": 1,
            "description": "",
            "test_folder": "building_comfort",
            "queries": [
                {
                    "test_query_id": "room-comfort-level",
                    "handler": {
                        "kind": "Http",
                        "port": 9001,
                        "path": "/reaction",
                        "correlation_header": "X-Query-Sequence"
                    },
                    "stop_trigger": {
                        "kind": "RecordCount",
                        "record_count": 90000
                    }
                }                      
            ],
            "sources": [
                {
                    "test_source_id": "facilities-db",
                    "kind": "Model",
                    "source_change_dispatchers": [ 
                        {
                            "kind": "Http",
                            "url": "http://localhost",
                            "port": 9000,
                            "timeout_seconds": 60,
                            "batch_events": false
                        }
                    ],
                    "model_data_generator": {
                        "kind": "BuildingHierarchy",
                        "change_interval": [2000000000, 500000000, 500000000, 4000000000],
                        "change_count": 10,
                        "seed": 123456789,
                        "spacing_mode": "none",
                        "time_mode": "2025-01-03T10:03:15.4Z",
                        "building_count": [10, 0],
                        "floor_count": [10, 0],
                        "room_count": [10, 0],                   
                        "room_sensors": []
                    },
                    "subscribers": [
                        { "node_id": "default", "query_id": "building-comfort" }
                    ]
                }
            ],
            "reactions": [
                {
                    "test_reaction_id": "building-comfort",
                    "handler": {
                        "kind": "Http",
                        "port": 9001,
                        "path": "/reaction",
                        "correlation_header": "X-Query-Sequence"
                    },
                    "stop_trigger": {
                        "kind": "RecordCount",
                        "record_count": 90000
                    }
                }
            ]
        }
        "#;

        // Parse as LocalTestDefinition (which is what's in the config)
        let result: Result<LocalTestDefinition, _> = serde_json::from_str(local_test);
        assert!(
            result.is_ok(),
            "Failed to parse LocalTestDefinition: {:?}",
            result.err()
        );

        let local_test_def = result.unwrap();
        assert_eq!(local_test_def.test_id, "building_comfort");
        assert_eq!(local_test_def.queries.len(), 1);
        assert_eq!(local_test_def.reactions.len(), 1);
        assert_eq!(local_test_def.sources.len(), 1);

        // Verify that queries and reactions are parsed correctly
        // Note: handler fields in JSON will be ignored since they're not in the model
        let query = &local_test_def.queries[0];
        assert_eq!(query.test_query_id, "room-comfort-level");
        assert!(query.stop_trigger.is_some());

        let reaction = &local_test_def.reactions[0];
        assert_eq!(reaction.test_reaction_id, "building-comfort");
        // The JSON has "stop_trigger" but the field is "stop_triggers", so it's None
        assert!(reaction.stop_triggers.is_none());
        assert!(reaction.output_handler.is_none()); // handler field is ignored
    }

    #[test]
    fn test_spacing_mode_from_str() {
        assert_eq!("none".parse::<SpacingMode>().unwrap(), SpacingMode::None);
        assert_eq!(
            "recorded".parse::<SpacingMode>().unwrap(),
            SpacingMode::Recorded
        );
        assert_eq!(
            "100".parse::<SpacingMode>().unwrap(),
            SpacingMode::Rate(NonZeroU32::new(100).unwrap())
        );
        assert_eq!(
            "1000".parse::<SpacingMode>().unwrap(),
            SpacingMode::Rate(NonZeroU32::new(1000).unwrap())
        );
    }

    #[test]
    fn test_spacing_mode_display() {
        assert_eq!(SpacingMode::None.to_string(), "none");
        assert_eq!(SpacingMode::Recorded.to_string(), "recorded");
        assert_eq!(
            SpacingMode::Rate(NonZeroU32::new(1000).unwrap()).to_string(),
            "1000"
        );
    }

    #[test]
    fn test_spacing_mode_deserialize() {
        let json = r#""none""#;
        let spacing_mode: SpacingMode = serde_json::from_str(json).unwrap();
        assert_eq!(spacing_mode, SpacingMode::None);

        let json = r#""recorded""#;
        let spacing_mode: SpacingMode = serde_json::from_str(json).unwrap();
        assert_eq!(spacing_mode, SpacingMode::Recorded);

        let json = r#""1000""#;
        let spacing_mode: SpacingMode = serde_json::from_str(json).unwrap();
        assert_eq!(
            spacing_mode,
            SpacingMode::Rate(NonZeroU32::new(1000).unwrap())
        );
    }

    #[test]
    fn test_time_mode_from_str() {
        assert_eq!("live".parse::<TimeMode>().unwrap(), TimeMode::Live);
        assert_eq!("recorded".parse::<TimeMode>().unwrap(), TimeMode::Recorded);
        let timestamp = "2021-09-14T14:12:00Z";
        let parsed_time = chrono::DateTime::parse_from_rfc3339(timestamp)
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap() as u64;
        assert_eq!(
            timestamp.parse::<TimeMode>().unwrap(),
            TimeMode::Rebased(parsed_time)
        );
    }

    #[test]
    fn test_time_mode_display() {
        assert_eq!(TimeMode::Live.to_string(), "live");
        assert_eq!(TimeMode::Recorded.to_string(), "recorded");
        assert_eq!(
            TimeMode::Rebased(1631629920000000000).to_string(),
            "1631629920000000000"
        );
    }

    #[test]
    fn test_time_mode_deserialize() {
        let json = r#""live""#;
        let time_mode: TimeMode = serde_json::from_str(json).unwrap();
        assert_eq!(time_mode, TimeMode::Live);

        let json = r#""recorded""#;
        let time_mode: TimeMode = serde_json::from_str(json).unwrap();
        assert_eq!(time_mode, TimeMode::Recorded);

        let json = r#""2021-09-14T14:12:00Z""#;
        let parsed_time = chrono::DateTime::parse_from_rfc3339("2021-09-14T14:12:00Z")
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap() as u64;
        let time_mode: TimeMode = serde_json::from_str(json).unwrap();
        assert_eq!(time_mode, TimeMode::Rebased(parsed_time));
    }
}
