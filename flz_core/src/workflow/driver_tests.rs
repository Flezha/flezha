//! A workflow's host path, served the way a C-ABI host serves it: the
//! `init_values` a bind carries land as state through the apply-state
//! command before the first message, a message the host's entry decoder
//! rejects rides the ladder and parks under the entry channel while the run
//! waits for the next one instead of ending, and a durable run's park
//! releases it. A stream given the same verdict parks the item at once.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crossbeam::channel::{unbounded, Receiver};

use crate::host::{apply_result, HostPayload};
use crate::poll_core::{EnginePayload, Polled};
use crate::runner::{RunnerMessage, RunnerNode};
use crate::test_support::{capture_parks, host_class, spin_until, stream_pair};
use crate::{schema, state_sink};

/// Answers every activation the poll core hands out while it lives: an
/// apply-state command is recorded and answered applied; each work item is
/// answered with `verdict` over the host payload's bytes, an `Ok` item
/// echoing its bytes to the plugin channel `out`.
struct FakeHost {
    stop: Arc<AtomicBool>,
    applied: Arc<Mutex<Vec<String>>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl FakeHost {
    fn serve(verdict: impl Fn(&[u8]) -> schema::Verdict + Send + 'static) -> Self {
        Self::answering(move |payload, applied| answer(payload, applied, &verdict))
    }

    /// Answers every activation with `respond`.
    fn answering(
        respond: impl Fn(&EnginePayload, &Mutex<Vec<String>>) -> schema::ActivationOutcome
            + Send
            + 'static,
    ) -> Self {
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
                            let outcome = respond(&act.payload, &applied);
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

use crate::test_support::host_class::bind as start;

/// One message into workflow `id` on its first entry channel.
fn feed(node: &RunnerNode, id: &str, message: &[u8]) {
    node.operative_dispatch_handle().dispatch(
        id,
        message.to_vec(),
        &Default::default(),
        stream_pair().0,
    );
}

/// One message into workflow `id` naming `channel`, at input-log `offset`.
fn feed_at(node: &RunnerNode, id: &str, channel: &str, offset: u64, message: &[u8]) {
    let metadata = std::collections::HashMap::from([
        ("channel".to_string(), channel.to_string()),
        ("flz.offset".to_string(), offset.to_string()),
    ]);
    node.operative_dispatch_handle()
        .dispatch(id, message.to_vec(), &metadata, stream_pair().0);
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
    let mut node = RunnerNode::new("test-ns".to_string());

    assert!(start(&mut node, &id, "host-init-1", r#"{"p0":{"budget":100}}"#, 0).is_empty());
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

    let refused = start(&mut node, &id, "host-init-2", "not json", 0);
    assert!(refused[0].contains("init_values"), "{refused:?}");
    assert!(!node.has_operative("host-init-2"));
}

#[test]
fn a_durable_start_refused_for_its_init_values_leaves_no_cell_registered() {
    let _env = crate::runner::env_test_guard();
    let id = format!("host_wf_refused_{}", std::process::id());
    let durable = host_class::workflow_json(&id, &["in"], 1)
        .replace(r#""persistent":false"#, r#""persistent":true"#);
    crate::host::register_host_class_json(durable.as_bytes()).expect("class registers");
    let mut node = RunnerNode::new("test-ns".to_string());

    assert!(!start(&mut node, &id, "host-refused-1", "not json", 0).is_empty());
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
    let mut node = RunnerNode::new("test-ns".to_string());
    assert!(start(&mut node, &id, "host-unread-1", "", 0).is_empty());

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
    std::env::set_var("FLEZHA_PARK_IDLE_MS", "200");
    let id = format!("host_wf_release_{}", std::process::id());
    let durable = host_class::workflow_json(&id, &["in"], 1).replace(
        r#""persistent":false"#,
        r#""persistent":true,"sleep_when_idle":true"#,
    );
    crate::host::register_host_class_json(durable.as_bytes()).expect("class registers");
    let _host = FakeHost::serve(|_| schema::Verdict::Unreadable {
        message: "not json".to_string(),
    });
    let mut node = RunnerNode::new("test-ns".to_string());

    // The host's decoder refuses the message, so the stage parks it.
    assert!(start(&mut node, &id, "host-release-1", "", 0).is_empty());
    feed_at(&node, "host-release-1", "in", 3, b"bad");
    let park = await_park(&rx, "host-release-1");
    assert_eq!((park.iter, park.consumed_offset), (0, Some(3)));

    // The entrance parks a name the class does not declare. The rebound run
    // owed that message, and still suspends on the idle window rather than
    // holding out for a message that will never reach it.
    assert!(start(&mut node, &id, "host-release-2", "", 1).is_empty());
    feed_at(&node, "host-release-2", "refunds", 4, b"bad");
    let park = await_park(&rx, "host-release-2");
    assert_eq!(park.consumed_offset, Some(4));

    std::env::remove_var("FLEZHA_PARK_IDLE_MS");
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
    let instance =
        crate::operative::instance::OperativeInstance::of_class(&id, &format!("{id}_inst"))
            .expect("class registered");
    let _host = FakeHost::serve(|_| schema::Verdict::Unreadable {
        message: "not json".to_string(),
    });
    let registry = Arc::new(crate::operative::registry::OperativeRegistry::new());
    registry.register(Arc::clone(&instance));

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

// ---- a keyed tasklet on the host path -------------------------------------

/// `p0` splits a message on `,` into one released value per part; `k1` is
/// reached by a keyed edge and echoes `k:<cell key>:<item>` to `out`.
fn keyed_json(id: &str, mid_retry_max: u32) -> String {
    format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","persistent":false,
             "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}},
                         {{"name":"mid","kind":"inter_task","producer":0,
                           "retry_max":{mid_retry_max},"retry_backoff_ms":1,
                           "chains":[[{{"link":"key"}},{{"link":"into","tasklet":1}}]]}},
                         {{"name":"out","kind":"plugin","producer":1,
                           "chains":[[{{"link":"sink","name":"out"}}]]}}],
             "tasklets":[{{"name":"p0"}},{{"name":"k1","cell_mode":"dynamic"}}],
             "workflow":{{}}}}"#
    )
}

/// What the keyed host did: the crossings it answered, by entry count.
#[derive(Default)]
struct Crossings(Mutex<Vec<usize>>);

/// A host for [`keyed_json`]: every prepare answers each payload's own bytes
/// as its key, or `refuse` for every one.
fn keyed_host(mid: u32, crossings: Arc<Crossings>, refuse: bool) -> FakeHost {
    FakeHost::answering(move |payload, _| match payload {
        EnginePayload::Prepare(crossing) => {
            crossings.0.lock().unwrap().push(crossing.entries.len());
            schema::ActivationOutcome::Prepared(
                crossing
                    .entries
                    .iter()
                    .map(|entry| match refuse {
                        true => Err(schema::WorkError {
                            message: "no key for that".to_string(),
                            non_retryable: true,
                        }),
                        false => Ok(schema::PrepareAnswer {
                            keys: vec![String::from_utf8_lossy(&entry.bytes).to_string()],
                            event_time: None,
                        }),
                    })
                    .collect(),
            )
        }
        EnginePayload::Work { slot, items } => schema::ActivationOutcome::Work(
            items
                .iter()
                .map(|item| {
                    let bytes = match &item.item {
                        crate::cell::CellItem::Input(payload) => host_bytes(&payload.value),
                        _ => Vec::new(),
                    };
                    keyed_effect(slot, mid, &bytes)
                })
                .collect(),
        ),
        _ => schema::ActivationOutcome::Failed(schema::WorkError {
            message: "unexpected activation".to_string(),
            non_retryable: true,
        }),
    })
}

fn keyed_effect(slot: &crate::cell::CellSlot, mid: u32, bytes: &[u8]) -> schema::ItemEffect {
    let output = |target, payload: Vec<u8>| schema::Output {
        target,
        key: None,
        event_time: None,
        metadata: Vec::new(),
        payload,
    };
    let outputs = match slot.cell_key.as_deref() {
        None => bytes
            .split(|b| *b == b',')
            .map(|part| {
                output(
                    schema::OutputTarget::Edge {
                        channel: mid,
                        consumer: 1,
                    },
                    part.to_vec(),
                )
            })
            .collect(),
        Some(key) => vec![output(
            schema::OutputTarget::Plugin {
                channel: "out".to_string(),
            },
            format!("k:{key}:{}", String::from_utf8_lossy(bytes)).into_bytes(),
        )],
    };
    schema::ItemEffect {
        outputs,
        ..effect(schema::Verdict::Ok, &[])
    }
}

fn keyed_class(id: &str, mid_retry_max: u32) -> u32 {
    crate::host::register_host_class_json(keyed_json(id, mid_retry_max).as_bytes())
        .expect("class registers");
    crate::operative::registry::get_class(id)
        .and_then(|topology| topology.lookup_channel_id("mid"))
        .expect("the keyed class declares `mid`")
}

fn live_keys(node: &RunnerNode, id: &str) -> Vec<String> {
    let instance = node.get_operative(id).expect("a live instance");
    let mut keys: Vec<String> = instance.dynamic_registries[1]
        .as_ref()
        .expect("k1 is keyed")
        .cells
        .iter()
        .map(|e| e.key().clone())
        .collect();
    keys.sort();
    keys
}

#[test]
fn a_host_workflows_released_values_cross_once_for_their_keys() {
    let _env = crate::runner::env_test_guard();
    let (tx, rx) = unbounded::<RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    let id = format!("host_wf_keyed_{}", std::process::id());
    let mid = keyed_class(&id, 1);
    let crossings = Arc::new(Crossings::default());
    let _host = keyed_host(mid, Arc::clone(&crossings), false);
    let mut node = RunnerNode::new("test-ns".to_string());

    assert!(start(&mut node, &id, "host-keyed-1", "", 0).is_empty());
    feed(&node, "host-keyed-1", b"a,b,c");
    let (mut outs, done) = drain(&rx, "host-keyed-1");
    outs.sort();
    assert_eq!(done.0, "ok");
    assert_eq!(
        outs,
        [b"k:a:a".to_vec(), b"k:b:b".to_vec(), b"k:c:c".to_vec()]
    );
    assert_eq!(live_keys(&node, "host-keyed-1"), ["a", "b", "c"]);
    assert_eq!(
        *crossings.0.lock().unwrap(),
        [3],
        "the three released values crossed together"
    );
}

#[test]
fn a_resumed_host_workflow_crosses_its_journaled_handoff() {
    let _env = crate::runner::env_test_guard();
    let (tx, rx) = unbounded::<RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    let id = format!("host_wf_keyed_resume_{}", std::process::id());
    let mid = keyed_class(&id, 1);
    let crossings = Arc::new(Crossings::default());
    let _host = keyed_host(mid, Arc::clone(&crossings), false);
    let codec = crate::operative::registry::get_class(&id)
        .and_then(|topology| topology.channel(mid).and_then(|c| c.codec.clone()))
        .expect("a host inter-tasklet channel journals through its codec");
    let released = |bytes: &[u8]| crate::pb::TaskHandoffItem {
        channel_id: mid,
        payload: (codec.encode)(&HostPayload {
            bytes: bytes.to_vec(),
            channel: mid,
            keys: Vec::new(),
            event_time: None,
            metadata: Vec::new(),
        })
        .expect("encodes"),
    };
    let mut node = RunnerNode::new("test-ns".to_string());
    let mut errors = Vec::new();
    crate::command::CommandAction::handle(
        &crate::pb::AddOperative {
            operative_id: "host-keyed-resume".to_string(),
            factory_id: id.clone(),
            task_journal: Some(crate::pb::TaskJournalState {
                committed_task_idxs: vec![0],
                task_handoffs: vec![crate::pb::TaskEdgeHandoff {
                    task_idx: 0,
                    handoff: vec![released(b"x"), released(b"y")],
                }],
                ..Default::default()
            }),
            ..Default::default()
        },
        &mut node,
        "",
        &mut errors,
    );
    assert!(errors.is_empty(), "{errors:?}");

    // The committed root is skipped; the re-forwarded message resumes the pass.
    feed(&node, "host-keyed-resume", b"ignored");
    let (mut outs, done) = drain(&rx, "host-keyed-resume");
    outs.sort();
    assert_eq!(done.0, "ok");
    assert_eq!(outs, [b"k:x:x".to_vec(), b"k:y:y".to_vec()]);
    assert_eq!(live_keys(&node, "host-keyed-resume"), ["x", "y"]);
    assert_eq!(*crossings.0.lock().unwrap(), [2]);
}

#[test]
fn a_crossing_the_host_refuses_fails_the_stage_and_parks_nothing() {
    let _env = crate::runner::env_test_guard();
    let (tx, rx) = unbounded::<RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    let parks = capture_parks();
    let id = format!("host_wf_keyed_refused_{}", std::process::id());
    let mid = keyed_class(&id, 2);
    let crossings = Arc::new(Crossings::default());
    let _host = keyed_host(mid, Arc::clone(&crossings), true);
    let mut node = RunnerNode::new("test-ns".to_string());

    assert!(start(&mut node, &id, "host-keyed-refused", "", 0).is_empty());
    feed(&node, "host-keyed-refused", b"a");
    let (outs, done) = drain(&rx, "host-keyed-refused");
    assert!(outs.is_empty());
    assert_eq!(done.0, "error");
    assert!(done.1.contains("no key for that"), "{}", done.1);
    assert_eq!(
        *crossings.0.lock().unwrap(),
        [1, 1],
        "the value climbed the channel's ladder: two attempts"
    );
    assert!(parks.lock().expect("park capture poisoned").is_empty());
    state_sink::reset_for_test();
}

#[test]
fn a_host_streams_entry_crossing_mints_its_keyed_cells() {
    let _env = crate::runner::env_test_guard();
    let id = format!("host_stream_keyed_{}", std::process::id());
    let crossings = Arc::new(Crossings::default());
    let json = format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","persistent":false,
             "channels":[{{"name":"in","kind":"entry",
                           "chains":[[{{"link":"key"}},{{"link":"into","tasklet":0}}]]}},
                         {{"name":"out","kind":"plugin","producer":0,
                           "chains":[[{{"link":"sink","name":"out"}}]]}}],
             "tasklets":[{{"name":"k0","cell_mode":"dynamic"}}]}}"#
    );
    crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    let instance =
        crate::operative::instance::OperativeInstance::of_class(&id, &format!("{id}_inst"))
            .expect("class registered");
    let _host = keyed_host(u32::MAX, Arc::clone(&crossings), false);
    let registry = Arc::new(crate::operative::registry::OperativeRegistry::new());
    registry.register(Arc::clone(&instance));

    crate::routing::OperativeDispatchHandle { registry }.dispatch(
        &instance.instance_id,
        b"q".to_vec(),
        &Default::default(),
        stream_pair().0,
    );
    let keyed = instance.dynamic_registries[0]
        .as_ref()
        .expect("k0 is keyed");
    spin_until("the entry crossing to mint the key's Cell", || {
        keyed.cells.contains_key("q")
    });
    assert_eq!(*crossings.0.lock().unwrap(), [1]);
}

// ---- time on the host path ------------------------------------------------

/// `in -> e0 -> out`: `e0` runs on `clock`, and on its first message arms a
/// processing-time timer 30 ms out, whose firing releases `fired`.
fn timed_json(id: &str, clock: &str) -> String {
    format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","persistent":false,
             "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}},
                         {{"name":"out","kind":"plugin","producer":0,
                           "chains":[[{{"link":"sink","name":"out"}}]]}}],
             "tasklets":[{{"name":"e0","clock":"{clock}"}}],
             "workflow":{{"loop_mode":"per_input"}}}}"#
    )
}

/// Answers a crossing with each payload's bytes as its event time, and each
/// item with its bytes on `out` (`fired` for a firing), arming a timer on
/// `arm`; every item's `now` is recorded.
fn timed_host(nows: Arc<Mutex<Vec<Option<u64>>>>) -> FakeHost {
    FakeHost::answering(move |payload, _| match payload {
        EnginePayload::Prepare(crossing) => schema::ActivationOutcome::Prepared(
            crossing
                .entries
                .iter()
                .map(|entry| {
                    Ok(schema::PrepareAnswer {
                        keys: Vec::new(),
                        event_time: String::from_utf8_lossy(&entry.bytes).parse().ok(),
                    })
                })
                .collect(),
        ),
        EnginePayload::Work { items, .. } => schema::ActivationOutcome::Work(
            items
                .iter()
                .map(|item| {
                    nows.lock().unwrap().push(item.now);
                    match &item.item {
                        crate::cell::CellItem::Input(payload) => {
                            let bytes = host_bytes(&payload.value);
                            let mut armed = effect(schema::Verdict::Ok, &bytes);
                            if bytes == b"arm" {
                                let at = crate::util::wall_now_ms() + 30;
                                armed.timers = vec![crate::timer::TimerOp::register(
                                    crate::timer::TimerKind::ProcessingTime,
                                    at,
                                )];
                            }
                            armed
                        }
                        _ => effect(schema::Verdict::Ok, b"fired"),
                    }
                })
                .collect(),
        ),
        _ => schema::ActivationOutcome::Failed(schema::WorkError {
            message: "unexpected activation".to_string(),
            non_retryable: true,
        }),
    })
}

/// Outputs of workflow `id` until `count` arrived.
fn outputs(rx: &Receiver<RunnerMessage>, id: &str, count: usize) -> Vec<Vec<u8>> {
    let mut outs = Vec::new();
    while outs.len() < count {
        match rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|_| panic!("workflow {id} released {outs:?} and then nothing"))
        {
            RunnerMessage::EmitWorkflowOutput {
                operative_id,
                payload,
            } if operative_id == id => outs.push(payload),
            _ => {}
        }
    }
    outs
}

#[test]
fn a_host_workflows_entry_crosses_for_its_event_time_and_is_dispatched_at_it() {
    let _env = crate::runner::env_test_guard();
    let (tx, rx) = unbounded::<RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    let id = format!("host_wf_event_time_{}", std::process::id());
    crate::host::register_host_class_json(timed_json(&id, "event_time").as_bytes())
        .expect("class registers");
    let nows = Arc::new(Mutex::new(Vec::new()));
    let _host = timed_host(Arc::clone(&nows));
    let mut node = RunnerNode::new("test-ns".to_string());

    assert!(start(&mut node, &id, "host-et-1", "", 0).is_empty());
    feed(&node, "host-et-1", b"1234");
    assert_eq!(outputs(&rx, "host-et-1", 1), [b"1234".to_vec()]);
    assert_eq!(
        *nows.lock().unwrap(),
        [Some(1234)],
        "the item ran at the event time the host answered on the way in"
    );
}

#[test]
fn a_host_workflow_tasklets_timer_fires_into_a_pass_of_its_own() {
    let _env = crate::runner::env_test_guard();
    let (tx, rx) = unbounded::<RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    let id = format!("host_wf_timer_{}", std::process::id());
    crate::host::register_host_class_json(timed_json(&id, "wall").as_bytes())
        .expect("class registers");
    let _host = timed_host(Arc::new(Mutex::new(Vec::new())));
    let mut node = RunnerNode::new("test-ns".to_string());

    assert!(start(&mut node, &id, "host-timer-1", "", 0).is_empty());
    feed(&node, "host-timer-1", b"arm");
    assert_eq!(
        outputs(&rx, "host-timer-1", 2),
        [b"arm".to_vec(), b"fired".to_vec()],
        "the firing ran with no second message"
    );
}
