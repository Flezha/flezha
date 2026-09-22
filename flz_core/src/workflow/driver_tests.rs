//! The driver's host path, served the way a C-ABI host serves it: the
//! `init_values` a bind carries land as state through the apply-state
//! command before the first message, and a message the host's entry decoder
//! rejects parks under the entry channel while the run waits for the next
//! one instead of ending, and a durable run's park releases it. A stream
//! given the same verdict parks the item as it parks any non-retryable
//! failure.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crossbeam::channel::{unbounded, Receiver};

use crate::host::{apply_result, HostPayload};
use crate::poll_core::{EnginePayload, Polled};
use crate::runner::{RunnerMessage, RunnerNode};
use crate::test_support::{capture_parks, host_class, spin_until, stream_pair};
use crate::workflow::step::StepWork;
use crate::{schema, state_sink};

/// Answers every activation the poll core hands out while it lives: an
/// apply-state command is recorded and answered applied; each item of a step
/// is answered with `verdict` over the host payload's bytes, an `Ok` item
/// echoing its bytes to the plugin channel `out`.
struct FakeHost {
    stop: Arc<AtomicBool>,
    applied: Arc<Mutex<Vec<String>>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl FakeHost {
    fn serve(verdict: impl Fn(&[u8]) -> schema::Verdict + Send + 'static) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let applied = Arc::new(Mutex::new(Vec::new()));
        let thread = {
            let stop = Arc::clone(&stop);
            let applied = Arc::clone(&applied);
            thread::spawn(move || {
                let core = crate::poll_core::global();
                while !stop.load(Ordering::Acquire) {
                    match core.poll(Duration::from_millis(20)) {
                        Polled::Activation(act) => {
                            let outcome = answer(&act.payload, &applied, &verdict);
                            let token = act.token;
                            apply_result(core, act, schema::ActivationResult { token, outcome })
                                .expect("the poll core takes the answer");
                        }
                        Polled::Timeout => {}
                        Polled::ShuttingDown => return,
                    }
                }
            })
        };
        Self {
            stop,
            applied,
            thread: Some(thread),
        }
    }

    fn applied(&self) -> Vec<String> {
        self.applied.lock().expect("applied poisoned").clone()
    }
}

impl Drop for FakeHost {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn answer(
    payload: &EnginePayload,
    applied: &Mutex<Vec<String>>,
    verdict: &(impl Fn(&[u8]) -> schema::Verdict + Send + 'static),
) -> schema::ActivationOutcome {
    match payload {
        EnginePayload::Command {
            kind: schema::ActivationKind::ApplyState { state_json },
            ..
        } => {
            applied
                .lock()
                .expect("applied poisoned")
                .push(String::from_utf8_lossy(state_json).to_string());
            schema::ActivationOutcome::StateApplied
        }
        EnginePayload::WorkflowStep(step) => {
            let bytes: Vec<Vec<u8>> = match &step.work {
                StepWork::Items(values) => values.iter().map(host_bytes).collect(),
                StepWork::Compensate(_) => Vec::new(),
            };
            schema::ActivationOutcome::Work(
                bytes
                    .iter()
                    .map(|bytes| effect(verdict(bytes), bytes))
                    .collect(),
            )
        }
        EnginePayload::Work { items, .. } => schema::ActivationOutcome::Work(
            items
                .iter()
                .map(|item| match &item.item {
                    crate::cell::CellItem::Input(payload) => {
                        let bytes = host_bytes(&payload.value);
                        effect(verdict(&bytes), &bytes)
                    }
                    _ => effect(schema::Verdict::Ok, &[]),
                })
                .collect(),
        ),
        _ => schema::ActivationOutcome::Failed(schema::WorkError {
            message: "unexpected activation".to_string(),
            non_retryable: true,
        }),
    }
}

fn host_bytes(value: &Arc<dyn std::any::Any + Send + Sync>) -> Vec<u8> {
    value
        .downcast_ref::<HostPayload>()
        .map(|p| p.bytes.clone())
        .unwrap_or_default()
}

fn effect(verdict: schema::Verdict, bytes: &[u8]) -> schema::ItemEffect {
    let outputs = match verdict {
        schema::Verdict::Ok => vec![schema::Output {
            target: schema::OutputTarget::Plugin {
                channel: "out".to_string(),
            },
            key: None,
            event_time: None,
            metadata: Vec::new(),
            payload: bytes.to_vec(),
        }],
        _ => Vec::new(),
    };
    schema::ItemEffect {
        verdict,
        changes: Vec::new(),
        outputs,
        consumed_offset: None,
        event_time: 0,
        timers: Vec::new(),
        checkpoint: false,
        dlq: None,
        complete: None,
    }
}

/// One message into workflow `id` on its first entry channel.
fn feed(node: &RunnerNode, id: &str, message: &[u8]) {
    let instance = node.get_workflow(id).expect("the workflow is registered");
    crate::routing::feed_workflow_frame(&instance, message.to_vec(), &Default::default());
}

/// One message into workflow `id` naming `channel`, at input-log `offset`.
fn feed_at(node: &RunnerNode, id: &str, channel: &str, offset: u64, message: &[u8]) {
    let instance = node.get_workflow(id).expect("the workflow is registered");
    let metadata = std::collections::HashMap::from([
        ("channel".to_string(), channel.to_string()),
        ("flz.offset".to_string(), offset.to_string()),
    ]);
    crate::routing::feed_workflow_frame(&instance, message.to_vec(), &metadata);
}

/// The `IterPark` workflow `id`'s run shipped when it suspended.
fn await_park(rx: &Receiver<RunnerMessage>, id: &str) -> crate::pb::IterPark {
    loop {
        match rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|_| panic!("workflow {id} never parked"))
        {
            RunnerMessage::EmitControlRequest { command, .. } => {
                if let crate::pb::command_request::Command::IterPark(park) = *command {
                    if park.operative_id == id {
                        return park;
                    }
                }
            }
            RunnerMessage::EmitWorkflowCompleted {
                operative_id,
                status,
                ..
            } if operative_id == id => panic!("the run ended with `{status}` instead of parking"),
            _ => {}
        }
    }
}

/// The outputs and the completion `(status, error, result)` of workflow `id`.
fn drain(rx: &Receiver<RunnerMessage>, id: &str) -> (Vec<Vec<u8>>, (String, String, Vec<u8>)) {
    let mut outs = Vec::new();
    loop {
        match rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|_| panic!("timed out waiting for workflow {id}"))
        {
            RunnerMessage::EmitWorkflowOutput {
                operative_id,
                payload,
            } if operative_id == id => outs.push(payload),
            RunnerMessage::EmitWorkflowCompleted {
                operative_id,
                status,
                error,
                result,
            } if operative_id == id => return (outs, (status, error, result)),
            _ => {}
        }
    }
}

/// A completion for `id` within `wait` is a failure of the scenario.
fn assert_no_completion(rx: &Receiver<RunnerMessage>, id: &str, wait: Duration) {
    let deadline = std::time::Instant::now() + wait;
    while let Ok(message) =
        rx.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
    {
        if let RunnerMessage::EmitWorkflowCompleted {
            operative_id,
            status,
            ..
        } = message
        {
            assert!(
                operative_id != id,
                "the run ended with status `{status}` instead of waiting"
            );
        }
    }
}

#[test]
fn a_host_workflow_takes_init_values_as_state_before_its_first_message() {
    let _env = crate::runner::env_test_guard();
    let (tx, rx) = unbounded::<RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    let id = format!("host_wf_init_{}", std::process::id());
    crate::host::register_host_class_json(host_class::workflow_json(&id, &["in"], 1).as_bytes())
        .expect("class registers");
    let host = FakeHost::serve(|_| schema::Verdict::Ok);
    let node = RunnerNode::new("test-ns".to_string());

    assert!(node
        .start_workflow(id.clone(), "host-init-1".into(), r#"{"p0":{"budget":100}}"#)
        .expect("init_values is a JSON object"));
    // The state crossed before the bind was answered, and nothing else did.
    assert_eq!(host.applied(), vec![r#"{"budget":100}"#.to_string()]);

    feed(&node, "host-init-1", b"ticket");
    let (outs, done) = drain(&rx, "host-init-1");
    assert_eq!(
        outs,
        vec![b"ticket".to_vec()],
        "the pass ran over the message"
    );
    assert_eq!(done.0, "ok");

    let refused = node
        .start_workflow(id, "host-init-2".into(), "not json")
        .expect_err("a document that is not a JSON object is refused");
    assert!(refused.to_string().contains("init_values"), "{refused}");
    assert!(node.get_workflow("host-init-2").is_none());
}

#[test]
fn a_durable_start_refused_for_its_init_values_leaves_no_cell_registered() {
    let _env = crate::runner::env_test_guard();
    let id = format!("host_wf_refused_{}", std::process::id());
    let durable = host_class::workflow_json(&id, &["in"], 1)
        .replace(r#""persistent":false"#, r#""persistent":true"#);
    crate::host::register_host_class_json(durable.as_bytes()).expect("class registers");
    let node = RunnerNode::new("test-ns".to_string());

    assert!(node
        .start_workflow(id, "host-refused-1".into(), "not json")
        .is_err());
    assert!(!crate::flusher::CellFlusher::instance().holds_class("host-refused-1"));
}

#[test]
fn a_message_the_host_cannot_read_parks_under_the_entry_channel_and_the_run_waits() {
    let _env = crate::runner::env_test_guard();
    let (tx, rx) = unbounded::<RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    let parks = capture_parks();
    let id = format!("host_wf_unread_{}", std::process::id());
    crate::host::register_host_class_json(host_class::workflow_json(&id, &["in"], 2).as_bytes())
        .expect("class registers");
    let _host = FakeHost::serve(|bytes| {
        if bytes == b"bad" {
            schema::Verdict::Unreadable {
                message: "not json".to_string(),
            }
        } else {
            schema::Verdict::Ok
        }
    });
    let node = RunnerNode::new("test-ns".to_string());
    assert!(node
        .start_workflow(id, "host-unread-1".into(), "")
        .expect("init_values is a JSON object"));

    feed(&node, "host-unread-1", b"bad");
    spin_until("the unreadable message to be parked", || {
        !parks.lock().expect("park capture poisoned").is_empty()
    });
    let park = parks.lock().expect("park capture poisoned").remove(0);
    assert_eq!(
        park.body.entry.channel, "in",
        "parked under the entry channel"
    );
    assert_eq!(park.body.entry.attempts, 2, "after the stage's ladder");
    assert_eq!(park.body.entry.error, "not json");
    assert_eq!(park.body.entry.payload.as_deref(), Some(&b"bad"[..]));
    assert_eq!(park.identity.1, "in");
    assert_no_completion(&rx, "host-unread-1", Duration::from_millis(300));

    feed(&node, "host-unread-1", b"good");
    let (outs, done) = drain(&rx, "host-unread-1");
    assert_eq!(
        outs,
        vec![b"good".to_vec()],
        "the next readable message ran the pass"
    );
    assert_eq!(done.0, "ok");
    state_sink::reset_for_test();
}

#[test]
fn a_durable_run_releases_what_it_parked_unread_when_it_suspends() {
    let _env = crate::runner::env_test_guard();
    let (tx, rx) = unbounded::<RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    let _parks = capture_parks();
    std::env::set_var("FLEZHA_WORKFLOW_PARK_IDLE_MS", "200");
    let id = format!("host_wf_release_{}", std::process::id());
    let durable = host_class::workflow_json(&id, &["in"], 1)
        .replace(r#""persistent":false"#, r#""persistent":true"#);
    crate::host::register_host_class_json(durable.as_bytes()).expect("class registers");
    let _host = FakeHost::serve(|_| schema::Verdict::Unreadable {
        message: "not json".to_string(),
    });
    let node = RunnerNode::new("test-ns".to_string());

    // The host's decoder refuses the message, so the stage parks it.
    assert!(node
        .start_workflow(id.clone(), "host-release-1".into(), "")
        .expect("init_values is a JSON object"));
    feed_at(&node, "host-release-1", "in", 3, b"bad");
    let park = await_park(&rx, "host-release-1");
    assert_eq!((park.iter, park.consumed_offset), (0, Some(3)));

    // The entrance parks a name the class does not declare. The rebound run
    // owed that message, and still suspends on the idle window rather than
    // holding out for a message that will never reach it.
    assert!(node
        .start_workflow_resumable(id, "host-release-2".into(), "", 0, 1, None)
        .expect("init_values is a JSON object"));
    feed_at(&node, "host-release-2", "refunds", 4, b"bad");
    let park = await_park(&rx, "host-release-2");
    assert_eq!(park.consumed_offset, Some(4));

    std::env::remove_var("FLEZHA_WORKFLOW_PARK_IDLE_MS");
    state_sink::reset_for_test();
}

#[test]
fn a_stream_given_the_unreadable_verdict_parks_the_item() {
    let _env = crate::runner::env_test_guard();
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    let parks = capture_parks();
    // A tier-off stream, so the park rides the park frame rather than the
    // effect record.
    let id = format!("host_stream_unread_{}", std::process::id());
    let json = format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","persistent":false,
             "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
             "tasklets":[{{"name":"p0"}}]}}"#
    );
    crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    let instance = crate::operative::instance::StreamInstance::of_class(&id, &format!("{id}_inst"))
        .expect("class registered");
    let _host = FakeHost::serve(|_| schema::Verdict::Unreadable {
        message: "not json".to_string(),
    });
    let registry = Arc::new(crate::operative::registry::OperativeRegistry::new());
    registry.register_stream(Arc::clone(&instance));

    crate::routing::OperativeDispatchHandle { registry }.dispatch(
        &instance.instance_id,
        b"bad".to_vec(),
        &Default::default(),
        stream_pair().0,
    );

    spin_until("the unreadable item to be parked", || {
        !parks.lock().expect("park capture poisoned").is_empty()
    });
    let park = parks.lock().expect("park capture poisoned").remove(0);
    assert_eq!(park.body.entry.error, "not json");
    assert_eq!(park.body.entry.payload.as_deref(), Some(&b"bad"[..]));
    state_sink::reset_for_test();
}
