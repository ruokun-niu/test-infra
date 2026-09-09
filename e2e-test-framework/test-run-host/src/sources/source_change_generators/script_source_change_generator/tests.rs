use super::*;
use std::sync::{Arc, Mutex};
use test_data_store::test_run_storage::TestRunId;

struct RecordingDispatcher(Arc<Mutex<Vec<serde_json::Value>>>);

#[async_trait]
impl SourceChangeDispatcher for RecordingDispatcher {
    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn dispatch_source_change_events(
        &mut self,
        events: Vec<&SourceChangeEvent>,
    ) -> anyhow::Result<()> {
        for event in events {
            self.0.lock().unwrap().push(serde_json::to_value(event)?);
        }
        Ok(())
    }
}

async fn fixture(
    ignore_pauses: bool,
) -> (
    ScriptSourceChangeGeneratorInternalState,
    Receiver<ScheduledChangeScriptRecordMessage>,
    Arc<Mutex<Vec<serde_json::Value>>>,
    tempfile::TempDir,
) {
    let output = tempfile::tempdir().unwrap();
    let id = TestRunSourceId::new(
        &TestRunId::new("local", "scripted_recovery", "test"),
        "script-db",
    );
    let definition = serde_json::json!({
        "script_file_folder": "source_change_scripts",
        "spacing_mode": "none",
        "time_mode": "recorded",
        "ignore_scripted_pause_commands": ignore_pauses
    });
    let input_storage = TestSourceStorage {
        id: "script-db".into(),
        path: std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../examples/scripted_recovery/dev_repo/scripted_recovery/sources/script-db"),
        repo_id: "local".into(),
        test_id: "scripted_recovery".into(),
        test_source_definition: serde_json::from_value(serde_json::json!({
            "kind": "Script", "test_source_id": "script-db",
            "source_change_dispatchers": [], "subscribers": [],
            "source_change_generator": {
                "kind": "Script", "script_file_folder": "source_change_scripts"
            }
        }))
        .unwrap(),
    };
    let output_storage = TestRunSourceStorage {
        id: id.clone(),
        path: output.path().to_path_buf(),
        source_change_path: output.path().to_path_buf(),
    };
    let settings = ScriptSourceChangeGeneratorSettings::new(
        id,
        serde_json::from_value(definition).unwrap(),
        input_storage,
        output_storage,
        vec![],
    )
    .await
    .unwrap();
    let (mut state, receiver) = ScriptSourceChangeGeneratorInternalState::initialize(settings)
        .await
        .unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    state
        .dispatchers
        .push(Box::new(RecordingDispatcher(events.clone())));
    (state, receiver, events, output)
}

async fn process_next(
    state: &mut ScriptSourceChangeGeneratorInternalState,
    receiver: &mut Receiver<ScheduledChangeScriptRecordMessage>,
) {
    let message = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("next record was not scheduled")
        .unwrap();
    state.process_change_stream_message(message).await.unwrap();
}

#[tokio::test]
async fn scripted_pause_resumes_at_next_change_exactly_once() {
    let (mut state, mut receiver, events, _output) = fixture(false).await;
    state
        .transition_from_paused_state(&ScriptSourceChangeGeneratorCommand::Start)
        .await
        .unwrap();
    for _record in 0..3 {
        process_next(&mut state, &mut receiver).await;
    }
    assert_eq!(state.status, SourceChangeGeneratorStatus::Paused);
    assert_eq!(events.lock().unwrap().len(), 2);
    assert!(receiver.try_recv().is_err());
    assert!(matches!(
        state.next_record.as_ref().unwrap().record,
        ChangeScriptRecord::SourceChange(_)
    ));
    assert!(
        matches!(&state.previous_record.as_ref().unwrap().scripted.record,
        ChangeScriptRecord::PauseCommand(record) if record.label == "before-crash")
    );

    state
        .transition_from_paused_state(&ScriptSourceChangeGeneratorCommand::Start)
        .await
        .unwrap();
    for _record in 0..3 {
        process_next(&mut state, &mut receiver).await;
    }
    assert_eq!(state.status, SourceChangeGeneratorStatus::Paused);
    assert!(receiver.try_recv().is_err());
    let ordinals: Vec<_> = events
        .lock()
        .unwrap()
        .iter()
        .map(|event| {
            event["payload"]["after"]["properties"]["ordinal"]
                .as_u64()
                .unwrap()
        })
        .collect();
    assert_eq!(ordinals, vec![1, 2, 3, 4]);
    state
        .transition_from_paused_state(&ScriptSourceChangeGeneratorCommand::Start)
        .await
        .unwrap();
    process_next(&mut state, &mut receiver).await;
    assert_eq!(state.status, SourceChangeGeneratorStatus::Finished);
    assert_eq!(state.stats.num_pause_records, 2);
}

#[tokio::test]
async fn ignored_scripted_pauses_continue_to_finish() {
    let (mut state, mut receiver, events, _output) = fixture(true).await;
    state
        .transition_from_paused_state(&ScriptSourceChangeGeneratorCommand::Start)
        .await
        .unwrap();
    for _record in 0..7 {
        process_next(&mut state, &mut receiver).await;
    }
    assert_eq!(state.status, SourceChangeGeneratorStatus::Finished);
    assert_eq!(state.stats.num_pause_records, 2);
    assert_eq!(events.lock().unwrap().len(), 4);
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn stop_at_scripted_pause_requires_reset() {
    let (mut state, mut receiver, events, _output) = fixture(false).await;
    state
        .transition_from_paused_state(&ScriptSourceChangeGeneratorCommand::Start)
        .await
        .unwrap();
    for _record in 0..3 {
        process_next(&mut state, &mut receiver).await;
    }
    state
        .transition_from_paused_state(&ScriptSourceChangeGeneratorCommand::Stop)
        .await
        .unwrap();
    assert_eq!(state.status, SourceChangeGeneratorStatus::Stopped);
    assert!(state
        .transition_from_stopped_state(&ScriptSourceChangeGeneratorCommand::Start)
        .await
        .is_err());
    assert_eq!(events.lock().unwrap().len(), 2);
    state
        .transition_from_stopped_state(&ScriptSourceChangeGeneratorCommand::Reset)
        .await
        .unwrap();
    assert_eq!(state.status, SourceChangeGeneratorStatus::Paused);
    assert!(state.previous_record.is_none());
    assert!(matches!(&state.next_record.as_ref().unwrap().record,
        ChangeScriptRecord::SourceChange(record)
        if serde_json::to_value(&record.source_change_event).unwrap()["payload"]["source"]["lsn"] == 1));
}
