//! A steps relay's host path, served the way a C-ABI host serves it: the
//! `init_values` a bind carries land as state through the apply-state
//! command before the first message, a message the host's entry decoder
//! rejects rides the ladder and parks under the entry channel while the run
//! waits for the next one instead of ending, and a durable run's park
//! releases it. A flow relay given the same verdict parks the item at once.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crossbeam::channel::{unbounded, Receiver};
use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;

use crate::cell::{CellItem, CellSlot};
use crate::host::HostPayload;
use crate::poll_core::{EnginePayload, Polled};
use crate::runner::{RunnerMessage, RunnerNode};
use crate::steps::instance::{PassOrigin, Phase};
use crate::test_support::steps::{
    ack_control_request, assert_no_completion, await_completion, await_park, feed_at, feed_with,
};
use crate::test_support::{capture_parks, host_class, reply, spin_until, stream_pair, FakeHost};
use crate::timer::{TimerKind, TimerOp};
use crate::{schema, state_sink, ErrorModel};

mod call;
mod undo;

impl FakeHost {
    /// Answers each work item with `verdict` over the host payload's bytes,
    /// an `Ok` item echoing its bytes to the plugin channel `out`, and records
    /// every apply-state command, answered applied.
    fn serve(verdict: impl Fn(&[u8]) -> schema::Verdict + Send + 'static) -> Self {
        Self::answering(move |payload, applied| answer(payload, applied, &verdict))
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

/// What a host reads of an input: a keyed Cell's prepared values (one per
/// Cell in these tests), any other input's own bytes.
fn host_bytes(value: &Arc<dyn std::any::Any + Send + Sync>) -> Vec<u8> {
    value
        .downcast_ref::<HostPayload>()
        .map(|p| match &p.form {
            crate::schema::InputForm::Prepared(values) => values.concat(),
            _ => p.bytes.clone(),
        })
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

/// The connection a test's messages ride and the frames the run writes back
/// on it: an opted-out steps relay's plugin outputs leave on the connection their
/// item came in on, as a flow relay's do.
struct Wire {
    stream: Arc<FlezhaStream>,
    frames: Receiver<(String, Vec<u8>, HashMap<String, String>)>,
}

impl Wire {
    fn new() -> Self {
        let (stream, controller_end) = stream_pair();
        let (tx, frames) = unbounded();
        thread::spawn(move || {
            let controller_end = FlezhaStream::unix(controller_end);
            while let Ok(frame) = payloader::extract_payload(&controller_end) {
                if tx.send(frame).is_err() {
                    break;
                }
            }
        });
        Self { stream, frames }
    }

    /// The payloads written for steps relay `id` until nothing more arrives.
    fn outputs_of(&self, id: &str) -> Vec<Vec<u8>> {
        self.frames
            .try_iter()
            .chain(std::iter::from_fn(|| {
                self.frames.recv_timeout(Duration::from_millis(200)).ok()
            }))
            .filter(|(payload_id, _, _)| payload_id == id)
            .map(|(_, payload, _)| payload)
            .collect()
    }
}

#[test]
fn a_host_steps_takes_init_values_as_state_before_its_first_message() {
    let _env = crate::runner::env_test_guard();
    let (tx, rx) = unbounded::<RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    let id = format!("host_steps_init_{}", std::process::id());
    crate::host::register_host_class_json(host_class::steps_json(&id, &["in"], 1).as_bytes())
        .expect("class registers");
    let host = FakeHost::serve(|_| schema::Verdict::Ok);
    let mut node = RunnerNode::new("test-ns".to_string());
    let wire = Wire::new();

    assert!(start(&mut node, &id, "host-init-1", r#"{"p0":{"budget":100}}"#, 0).is_empty());
    // The state crossed before the bind was answered, and nothing else did.
    assert_eq!(host.applied(), vec![r#"{"budget":100}"#.to_string()]);

    feed_with(&node, "host-init-1", b"ticket", &[], &wire.stream);
    let done = await_completion(&rx, "host-init-1");
    let outs = wire.outputs_of("host-init-1");
    assert_eq!(
        outs,
        vec![b"ticket".to_vec()],
        "the pass ran over the message"
    );
    assert_eq!(done.0, "ok");

    let refused = start(&mut node, &id, "host-init-2", "not json", 0);
    assert_eq!(
        refused[0].code(),
        "state.init_values_invalid_json",
        "{refused:?}"
    );
    assert!(!node.has_relay("host-init-2"));
}

#[test]
fn a_durable_start_refused_for_its_init_values_leaves_no_cell_registered() {
    let _env = crate::runner::env_test_guard();
    let id = format!("host_steps_refused_{}", std::process::id());
    let durable = host_class::steps_json(&id, &["in"], 1).replace(
        r#""durable":false"#,
        r#""durable":{"local":{"sleep_after":false}}"#,
    );
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
    let id = format!("host_steps_unread_{}", std::process::id());
    crate::host::register_host_class_json(host_class::steps_json(&id, &["in"], 2).as_bytes())
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
    let wire = Wire::new();
    assert!(start(&mut node, &id, "host-unread-1", "", 0).is_empty());

    feed_with(
        &node,
        "host-unread-1",
        b"bad",
        &[("flz.reply_to", "w:unread")],
        &wire.stream,
    );
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
    assert_eq!(
        park.body.reply_to.as_deref(),
        Some("w:unread"),
        "its own call"
    );
    assert_eq!(park.identity.1, "in");
    assert_no_completion(&rx, "host-unread-1", Duration::from_millis(300));

    feed_with(&node, "host-unread-1", b"good", &[], &wire.stream);
    let done = await_completion(&rx, "host-unread-1");
    let outs = wire.outputs_of("host-unread-1");
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
    let id = format!("host_steps_release_{}", std::process::id());
    let durable = host_class::steps_json(&id, &["in"], 1).replace(
        r#""durable":false"#,
        r#""durable":{"local":{"sleep_after":200}}"#,
    );
    crate::host::register_host_class_json(durable.as_bytes()).expect("class registers");
    let _host = FakeHost::serve(|_| schema::Verdict::Unreadable {
        message: "not json".to_string(),
    });
    let mut node = RunnerNode::new("test-ns".to_string());

    // The host's decoder refuses the message, so the stage parks it.
    assert!(start(&mut node, &id, "host-release-1", "", 0).is_empty());
    feed_at(&node.get_relay("host-release-1").expect("bound"), b"bad", 3);
    let park = await_park(&rx, "host-release-1");
    assert_eq!((park.iter, park.consumed_offset), (0, Some(3)));

    // The entrance parks a name the class does not declare. The rebound run
    // owed that message, and still suspends on the idle window rather than
    // holding out for a message that will never reach it.
    assert!(start(&mut node, &id, "host-release-2", "", 1).is_empty());
    feed_with(
        &node,
        "host-release-2",
        b"bad",
        &[("channel", "refunds"), ("flz.offset", "4")],
        &stream_pair().0,
    );
    let park = await_park(&rx, "host-release-2");
    assert_eq!(park.consumed_offset, Some(4));

    state_sink::reset_for_test();
}

/// A re-forward is safe to repeat: the driver drops a message its journal
/// already released and runs the first one it has not.
#[test]
fn a_message_the_instance_already_took_starts_no_pass() {
    use crate::command::CommandAction;
    let _env = crate::runner::env_test_guard();
    let (tx, rx) = unbounded::<RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    let id = format!("host_steps_taken_{}", std::process::id());
    crate::host::register_host_class_json(host_class::steps_json(&id, &["in"], 1).as_bytes())
        .expect("class registers");
    let _host = FakeHost::serve(|_| schema::Verdict::Ok);
    let mut node = RunnerNode::new("test-ns".to_string());

    let mut errors = Vec::new();
    crate::pb::AddRelay {
        relay_id: "host-taken-1".to_string(),
        factory_id: id.clone(),
        task_journal: Some(crate::pb::TaskJournalState {
            released_offset: Some(5),
            ..Default::default()
        }),
        ..Default::default()
    }
    .handle(&mut node, "", &mut errors);
    assert!(errors.is_empty(), "{errors:?}");

    let taken = node.get_relay("host-taken-1").expect("bound");
    feed_at(&taken, b"released", 5);
    assert_no_completion(&rx, "host-taken-1", Duration::from_millis(300));

    feed_at(&taken, b"fresh", 6);
    let done = await_completion(&rx, "host-taken-1");
    assert_eq!(done.0, "ok", "offset 6 ran its pass: {done:?}");
    state_sink::reset_for_test();
}

#[test]
fn a_flow_given_the_unreadable_verdict_parks_the_item() {
    let _env = crate::runner::env_test_guard();
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    let parks = capture_parks();
    // A tier-off flow relay, so the park rides the park frame rather than the
    // effect record.
    let id = format!("host_flow_unread_{}", std::process::id());
    let json = format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","durable":false,
             "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
             "tasklets":[{{"name":"p0"}}]}}"#
    );
    crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    let instance = crate::relay::instance::RelayInstance::of_class(&id, &format!("{id}_inst"))
        .expect("class registered");
    let _host = FakeHost::serve(|_| schema::Verdict::Unreadable {
        message: "not json".to_string(),
    });
    let registry = Arc::new(crate::relay::registry::RelayRegistry::new());
    registry.register(Arc::clone(&instance));

    let handle = crate::routing::RelayDispatchHandle { registry };
    let called = HashMap::from([("flz.reply_to".to_string(), "w:bad".to_string())]);
    for metadata in [called, HashMap::new()] {
        handle.dispatch(
            &instance.instance_id,
            b"bad".to_vec(),
            &metadata,
            stream_pair().0,
        );
    }

    spin_until("both unreadable items to be parked", || {
        parks.lock().expect("park capture poisoned").len() == 2
    });
    let parked = std::mem::take(&mut *parks.lock().expect("park capture poisoned"));
    assert!(parked.iter().all(|park| park.body.entry.error == "not json"
        && park.body.entry.payload.as_deref() == Some(&b"bad"[..])));
    let mut calls: Vec<Option<String>> = parked.into_iter().map(|p| p.body.reply_to).collect();
    calls.sort();
    assert_eq!(
        calls,
        vec![None, Some("w:bad".to_string())],
        "a park frame carries the call only when its item had one"
    );
    state_sink::reset_for_test();
}

// ---- a keyed tasklet on the host path -------------------------------------

/// `p0` splits a message on `,` into one released value per part; `k1` is
/// reached by a keyed edge and echoes `k:<cell key>:<item>` to `out`.
fn keyed_json(id: &str, mid_retry_max: u32) -> String {
    format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","durable":false,
             "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}},
                         {{"name":"mid","kind":"inter_task","producer":0,
                           "retry_max":{mid_retry_max},"retry_backoff_ms":1,
                           "chains":[[{{"link":"key"}},{{"link":"into","tasklet":1}}]]}},
                         {{"name":"out","kind":"plugin","producer":1,
                           "chains":[[{{"link":"sink","name":"out"}}]]}}],
             "tasklets":[{{"name":"p0"}},{{"name":"k1"}}],
             "run":"steps"}}"#
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
            keyed_by_bytes(crossing, refuse)
        }
        EnginePayload::Work { slot, items, .. } => schema::ActivationOutcome::Work(
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

/// Each payload's own bytes as its key and its one value for every consumer,
/// or `refuse` for every one.
fn keyed_by_bytes(
    crossing: &crate::host::PrepareCrossing,
    refuse: bool,
) -> schema::ActivationOutcome {
    let channel = crossing.instance.topology.channel(crossing.channel);
    let consumers: Vec<u32> = channel.map_or(Vec::new(), |c| {
        c.consumers.iter().map(|edge| edge.tasklet).collect()
    });
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
                    values: (consumers.iter())
                        .map(|&consumer| schema::PreparedValue {
                            consumer,
                            keys: vec![String::from_utf8_lossy(&entry.bytes).to_string()],
                            bytes: entry.bytes.clone(),
                        })
                        .collect(),
                    event_time: None,
                }),
            })
            .collect(),
    )
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
    crate::relay::registry::get_class(id)
        .and_then(|topology| topology.lookup_channel_id("mid"))
        .expect("the keyed class declares `mid`")
}

fn live_keys(node: &RunnerNode, id: &str) -> Vec<String> {
    let instance = node.get_relay(id).expect("a live instance");
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
fn a_host_steps_released_values_cross_once_for_their_keys() {
    let _env = crate::runner::env_test_guard();
    let (tx, rx) = unbounded::<RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    let id = format!("host_steps_keyed_{}", std::process::id());
    let mid = keyed_class(&id, 1);
    let crossings = Arc::new(Crossings::default());
    let _host = keyed_host(mid, Arc::clone(&crossings), false);
    let mut node = RunnerNode::new("test-ns".to_string());
    let wire = Wire::new();

    assert!(start(&mut node, &id, "host-keyed-1", "", 0).is_empty());
    feed_with(&node, "host-keyed-1", b"a,b,c", &[], &wire.stream);
    let done = await_completion(&rx, "host-keyed-1");
    let mut outs = wire.outputs_of("host-keyed-1");
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
fn a_resumed_host_steps_crosses_its_journaled_handoff() {
    let _env = crate::runner::env_test_guard();
    let (tx, rx) = unbounded::<RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    let id = format!("host_steps_keyed_resume_{}", std::process::id());
    let mid = keyed_class(&id, 1);
    let crossings = Arc::new(Crossings::default());
    let _host = keyed_host(mid, Arc::clone(&crossings), false);
    let codec = crate::relay::registry::get_class(&id)
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
            form: crate::schema::InputForm::Untransformed,
            entry: None,
        })
        .expect("encodes"),
    };
    let reseeded = (codec.decode)(released(b"x").payload).expect("decodes");
    assert_eq!(
        reseeded.downcast_ref::<HostPayload>().map(|p| &p.form),
        Some(&crate::schema::InputForm::Untransformed),
        "a steps release re-seeds untransformed"
    );
    let mut node = RunnerNode::new("test-ns".to_string());
    let wire = Wire::new();
    // The journal's handoff re-seeds on the secondary stream, which is where
    // the stages it feeds write their outputs.
    let _sink = state_sink::test_guard();
    state_sink::set_secondary_stream(Some(Arc::clone(&wire.stream)));
    let mut errors = Vec::new();
    crate::command::CommandAction::handle(
        &crate::pb::AddRelay {
            relay_id: "host-keyed-resume".to_string(),
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
    feed_with(&node, "host-keyed-resume", b"ignored", &[], &wire.stream);
    let done = await_completion(&rx, "host-keyed-resume");
    let mut outs = wire.outputs_of("host-keyed-resume");
    outs.sort();
    assert_eq!(done.0, "ok");
    assert_eq!(outs, [b"k:x:x".to_vec(), b"k:y:y".to_vec()]);
    assert_eq!(live_keys(&node, "host-keyed-resume"), ["x", "y"]);
    assert_eq!(*crossings.0.lock().unwrap(), [2]);
    state_sink::set_secondary_stream(None);
}

#[test]
fn a_crossing_the_host_refuses_fails_the_stage_and_parks_nothing() {
    let _env = crate::runner::env_test_guard();
    let (tx, rx) = unbounded::<RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    let parks = capture_parks();
    let id = format!("host_steps_keyed_refused_{}", std::process::id());
    let mid = keyed_class(&id, 2);
    let crossings = Arc::new(Crossings::default());
    let _host = keyed_host(mid, Arc::clone(&crossings), true);
    let mut node = RunnerNode::new("test-ns".to_string());
    let wire = Wire::new();

    assert!(start(&mut node, &id, "host-keyed-refused", "", 0).is_empty());
    feed_with(&node, "host-keyed-refused", b"a", &[], &wire.stream);
    let done = await_completion(&rx, "host-keyed-refused");
    let outs = wire.outputs_of("host-keyed-refused");
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
fn a_host_flow_entry_crossing_mints_its_keyed_cells() {
    let _env = crate::runner::env_test_guard();
    let id = format!("host_flow_keyed_{}", std::process::id());
    let crossings = Arc::new(Crossings::default());
    let json = format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","durable":false,
             "channels":[{{"name":"in","kind":"entry",
                           "chains":[[{{"link":"key"}},{{"link":"into","tasklet":0}}]]}},
                         {{"name":"out","kind":"plugin","producer":0,
                           "chains":[[{{"link":"sink","name":"out"}}]]}}],
             "tasklets":[{{"name":"k0"}}]}}"#
    );
    crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    let instance = crate::relay::instance::RelayInstance::of_class(&id, &format!("{id}_inst"))
        .expect("class registered");
    let _host = keyed_host(u32::MAX, Arc::clone(&crossings), false);
    let registry = Arc::new(crate::relay::registry::RelayRegistry::new());
    registry.register(Arc::clone(&instance));

    crate::routing::RelayDispatchHandle { registry }.dispatch(
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
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","durable":false,
             "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}},
                         {{"name":"out","kind":"plugin","producer":0,
                           "chains":[[{{"link":"sink","name":"out"}}]]}}],
             "tasklets":[{{"name":"e0","clock":"{clock}"}}],
             "run":{{"steps":{{"each":"per_input"}}}}}}"#
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
                        values: Vec::new(),
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

/// Outputs of steps relay `id` until `count` arrived.
fn outputs(wire: &Wire, id: &str, count: usize) -> Vec<Vec<u8>> {
    let mut outs = Vec::new();
    while outs.len() < count {
        let (payload_id, payload, _) = wire
            .frames
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|_| panic!("steps {id} released {outs:?} and then nothing"));
        if payload_id == id {
            outs.push(payload);
        }
    }
    outs
}

#[test]
fn a_host_steps_entry_crosses_for_its_event_time_and_is_dispatched_at_it() {
    let _env = crate::runner::env_test_guard();
    let (tx, _rx) = unbounded::<RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    let id = format!("host_steps_event_time_{}", std::process::id());
    crate::host::register_host_class_json(timed_json(&id, "event_time").as_bytes())
        .expect("class registers");
    let nows = Arc::new(Mutex::new(Vec::new()));
    let _host = timed_host(Arc::clone(&nows));
    let mut node = RunnerNode::new("test-ns".to_string());
    let wire = Wire::new();

    assert!(start(&mut node, &id, "host-et-1", "", 0).is_empty());
    feed_with(&node, "host-et-1", b"1234", &[], &wire.stream);
    assert_eq!(outputs(&wire, "host-et-1", 1), [b"1234".to_vec()]);
    assert_eq!(
        *nows.lock().unwrap(),
        [Some(1234)],
        "the item ran at the event time the host answered on the way in"
    );
}

#[test]
fn a_host_steps_tasklets_timer_fires_into_a_pass_of_its_own() {
    let _env = crate::runner::env_test_guard();
    let (tx, _rx) = unbounded::<RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    let id = format!("host_steps_timer_{}", std::process::id());
    crate::host::register_host_class_json(timed_json(&id, "wall").as_bytes())
        .expect("class registers");
    let _host = timed_host(Arc::new(Mutex::new(Vec::new())));
    let mut node = RunnerNode::new("test-ns".to_string());
    let wire = Wire::new();

    assert!(start(&mut node, &id, "host-timer-1", "", 0).is_empty());
    feed_with(&node, "host-timer-1", b"arm", &[], &wire.stream);
    assert_eq!(
        outputs(&wire, "host-timer-1", 2),
        [b"arm".to_vec(), b"fired".to_vec()],
        "the firing ran with no second message"
    );
}

/// Serves [`keyed_json`] as a `per_input` loop on the test's own thread, so
/// a due timer is promoted only where the test polls: `k1`'s `a` registers
/// a timer already due, and cancels it too when `cancel` is set, `b` is held
/// unanswered, and a firing records the origin of the pass it runs in.
#[derive(Default)]
struct OwnTimerHost {
    mid: u32,
    cancel: bool,
    armed: bool,
    held: Option<crate::poll_core::EngineActivation>,
    origins: Vec<PassOrigin>,
}

impl OwnTimerHost {
    fn serve_until(&mut self, done: impl Fn(&Self) -> bool) {
        let core = crate::poll_core::global();
        while !done(self) {
            let Polled::Activation(act) = core.poll(Duration::from_secs(5)) else {
                panic!("the run handed out nothing more");
            };
            match self.answer(&act.payload) {
                Some(outcome) => reply(act, outcome),
                None => self.held = Some(act),
            }
        }
    }

    fn answer(&mut self, payload: &EnginePayload) -> Option<schema::ActivationOutcome> {
        match payload {
            EnginePayload::Prepare(crossing) => Some(keyed_by_bytes(crossing, false)),
            EnginePayload::Work { slot, items, .. } => self
                .settle(slot, &items[0].item)
                .map(|settled| schema::ActivationOutcome::Work(vec![settled])),
            _ => panic!("an activation the run never asks for"),
        }
    }

    fn settle(&mut self, slot: &CellSlot, item: &CellItem) -> Option<schema::ItemEffect> {
        let CellItem::Input(input) = item else {
            let inst = slot.instance.upgrade().expect("a live instance");
            let sheet = inst.steps.as_ref().expect("a steps run").sheet();
            self.origins
                .extend(sheet.pass.as_ref().map(|pass| pass.origin.clone()));
            return Some(effect(schema::Verdict::Ok, b"fired"));
        };
        let mut settled = keyed_effect(slot, self.mid, &host_bytes(&input.value));
        match slot.cell_key.as_deref() {
            Some("b") => return None,
            Some(_) => {
                self.armed = true;
                let (kind, due) = (TimerKind::ProcessingTime, crate::util::wall_now_ms() - 1);
                settled.timers = vec![TimerOp::register(kind, due)];
                if self.cancel {
                    settled.timers.push(TimerOp::cancel(kind, due));
                }
            }
            None => {}
        }
        Some(settled)
    }
}

/// [`keyed_json`] as a `per_input` loop, registered under `name`, and its host.
fn own_timer_class(name: &str) -> (String, OwnTimerHost) {
    let id = format!("{name}_{}", std::process::id());
    let json = keyed_json(&id, 1).replace(
        r#""run":"steps""#,
        r#""run":{"steps":{"each":"per_input"}}"#,
    );
    crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    let mid = crate::relay::registry::get_class(&id)
        .and_then(|topology| topology.lookup_channel_id("mid"))
        .expect("the class declares `mid`");
    let host = OwnTimerHost {
        mid,
        ..Default::default()
    };
    (id, host)
}

#[test]
fn a_timer_its_own_stage_set_fires_in_a_pass_of_its_own_after_that_stage_commits() {
    let _env = crate::runner::env_test_guard();
    let (tx, _rx) = unbounded::<RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    let (id, mut host) = own_timer_class("host_steps_own_timer");
    let mid = host.mid;
    let mut node = RunnerNode::new("test-ns".to_string());
    let wire = Wire::new();
    assert!(start(&mut node, &id, "host-own-timer-1", "", 0).is_empty());

    feed_with(&node, "host-own-timer-1", b"a,b", &[], &wire.stream);
    host.serve_until(|host| host.armed && host.held.is_some());
    crate::poll_core::global().promote_due_wakes();
    let b = host.held.take().expect("`b` is held");
    let EnginePayload::Work { slot, .. } = &b.payload else {
        unreachable!("`b` is a work activation")
    };
    let settled = keyed_effect(slot, mid, b"b");
    reply(b, schema::ActivationOutcome::Work(vec![settled]));
    host.serve_until(|host| !host.origins.is_empty());

    let mut outs = outputs(&wire, "host-own-timer-1", 3);
    outs.sort();
    assert_eq!(
        outs,
        [b"fired".to_vec(), b"k:a:a".to_vec(), b"k:b:b".to_vec()]
    );
    assert!(
        matches!(host.origins.as_slice(), [PassOrigin::Timer { node: 1, .. }]),
        "the firing joined the stage that set it: {:?}",
        host.origins
    );
}

#[test]
fn a_timer_its_own_stage_set_and_cancelled_never_fires() {
    let _env = crate::runner::env_test_guard();
    let (tx, _rx) = unbounded::<RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    let (id, host) = own_timer_class("host_steps_own_timer_cancel");
    let mut host = OwnTimerHost {
        cancel: true,
        ..host
    };
    let mut node = RunnerNode::new("test-ns".to_string());
    let wire = Wire::new();
    assert!(start(&mut node, &id, "host-own-cancel-1", "", 0).is_empty());

    feed_with(&node, "host-own-cancel-1", b"a", &[], &wire.stream);
    host.serve_until(|host| host.armed);
    assert_eq!(outputs(&wire, "host-own-cancel-1", 1), [b"k:a:a".to_vec()]);
    crate::poll_core::global().promote_due_wakes();

    let inst = node
        .get_relay("host-own-cancel-1")
        .expect("a live instance");
    let sheet = inst.steps.as_ref().expect("a steps run").sheet();
    assert_eq!(
        (
            sheet.phase,
            sheet.iter,
            sheet.pass.as_ref().map(|p| p.origin.clone())
        ),
        (Phase::Waiting, 1, None),
        "the cancelled timer opened a pass"
    );
}

// ---- a failed turn of a per-input keep-progress loop ------------------------

/// `p0` forwards each message to `p1` on `mid`, arms a timer on `arm` whose
/// firing fails, and declares an undo; `p1` answers `re:<message>` on `out`
/// and fails every attempt of its three on `boom`.
fn turn_json(id: &str, persistent: bool, each: &str, rollback: &str) -> String {
    let durable = if persistent {
        r#"{"local":{"sleep_after":false}}"#
    } else {
        "false"
    };
    format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","durable":{durable},
             "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}},
                         {{"name":"mid","kind":"inter_task","producer":0,
                           "retry_max":3,"retry_backoff_ms":1,
                           "chains":[[{{"link":"into","tasklet":1}}]]}},
                         {{"name":"out","kind":"plugin","producer":1,
                           "chains":[[{{"link":"sink","name":"out"}}]]}}],
             "tasklets":[{{"name":"p0","compensate_overridden":true}},{{"name":"p1"}}],
             "run":{{"steps":{{"each":"{each}","on_failure":"{rollback}"}}}}}}"#
    )
}

/// A host for [`turn_json`], counting the undos `p0` ran.
fn turn_host(mid: u32, undos: Arc<std::sync::atomic::AtomicUsize>) -> FakeHost {
    FakeHost::answering(move |payload, applied| match payload {
        EnginePayload::Command {
            kind: schema::ActivationKind::CompensateRun { .. },
            ..
        } => {
            undos.fetch_add(1, Ordering::SeqCst);
            schema::ActivationOutcome::Work(vec![schema::ItemEffect {
                outputs: Vec::new(),
                ..effect(schema::Verdict::Ok, &[])
            }])
        }
        EnginePayload::Work { slot, items, .. } => schema::ActivationOutcome::Work(
            items
                .iter()
                .map(|item| turn_effect(slot, mid, item))
                .collect(),
        ),
        other => answer(other, applied, &|_: &[u8]| schema::Verdict::Ok),
    })
}

fn turn_effect(
    slot: &crate::cell::CellSlot,
    mid: u32,
    item: &crate::poll_core::ItemAttempt,
) -> schema::ItemEffect {
    let failed = |message: &str| {
        effect(
            schema::Verdict::RetryableError {
                message: message.to_string(),
            },
            &[],
        )
    };
    let bytes = match &item.item {
        crate::cell::CellItem::Input(payload) => host_bytes(&payload.value),
        _ => return failed("the firing failed"),
    };
    if slot.tasklet_id != 0 {
        return match bytes.as_slice() {
            b"boom" => failed("model unavailable"),
            _ => effect(
                schema::Verdict::Ok,
                format!("re:{}", String::from_utf8_lossy(&bytes)).as_bytes(),
            ),
        };
    }
    let mut forwarded = forwarded(mid, 1, &bytes);
    if bytes == b"arm" {
        forwarded.timers = vec![crate::timer::TimerOp::register(
            crate::timer::TimerKind::ProcessingTime,
            crate::util::wall_now_ms() + 30,
        )];
    }
    forwarded
}

/// An `Ok` effect sending `bytes` on to `consumer` over edge `channel`.
fn forwarded(channel: u32, consumer: u32, bytes: &[u8]) -> schema::ItemEffect {
    schema::ItemEffect {
        outputs: vec![schema::Output {
            target: schema::OutputTarget::Edge { channel, consumer },
            key: None,
            event_time: None,
            metadata: Vec::new(),
            payload: bytes.to_vec(),
        }],
        ..effect(schema::Verdict::Ok, &[])
    }
}

/// Register a [`turn_json`] class and serve it; the host's undo count.
fn turn_class(
    id: &str,
    persistent: bool,
    each: &str,
    rollback: &str,
) -> (FakeHost, Arc<std::sync::atomic::AtomicUsize>) {
    crate::host::register_host_class_json(turn_json(id, persistent, each, rollback).as_bytes())
        .expect("class registers");
    let mid = crate::relay::registry::get_class(id)
        .and_then(|topology| topology.lookup_channel_id("mid"))
        .expect("the class declares `mid`");
    let undos = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    (turn_host(mid, Arc::clone(&undos)), undos)
}

/// Bind `id` as the controller does.
fn bind_turns(
    node: &mut RunnerNode,
    factory: &str,
    id: &str,
    task_journal: Option<crate::pb::TaskJournalState>,
) {
    use crate::command::CommandAction;
    let mut errors = Vec::new();
    crate::pb::AddRelay {
        relay_id: id.to_string(),
        factory_id: factory.to_string(),
        task_journal,
        ..Default::default()
    }
    .handle(node, "", &mut errors);
    assert!(errors.is_empty(), "{errors:?}");
}

/// What the controller saw of one steps instance: its iteration commits,
/// the tasklets whose undo it was sent (re-sends included) under which
/// reference id, and its completion.
#[derive(Default)]
struct Seen {
    iters: Vec<crate::pb::IterCommit>,
    undone: Vec<(u32, u32)>,
    undo_refs: Vec<(u32, String)>,
    done: Option<(String, String)>,
}

/// How the fake controller answers one barrier send; a `bool` refuses on `true`.
enum Answer {
    Ack,
    Refuse,
    /// Never answered, as if the ack were lost.
    Ignore,
}

impl From<bool> for Answer {
    fn from(refuse: bool) -> Self {
        if refuse {
            Answer::Refuse
        } else {
            Answer::Ack
        }
    }
}

/// Answer `id`'s barriers as the controller does, as `answer` says, until
/// `until` holds or the run completes.
fn pump_turns<A: Into<Answer>>(
    rx: &Receiver<RunnerMessage>,
    id: &str,
    seen: &mut Seen,
    answer: impl Fn(&crate::pb::command_request::Command) -> A,
    until: impl Fn(&Seen) -> bool,
) {
    use crate::pb::command_request::Command;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !until(seen) && seen.done.is_none() {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        match rx.recv_timeout(remaining) {
            Ok(RunnerMessage::EmitControlRequest {
                reference_id,
                command,
            }) => {
                match &*command {
                    Command::IterCommit(commit) if commit.relay_id == id => {
                        seen.iters.push(commit.clone())
                    }
                    Command::TaskCommit(commit) if commit.relay_id == id && commit.compensating => {
                        seen.undone.push((commit.task_idx, commit.iter));
                        seen.undo_refs.push((commit.task_idx, reference_id.clone()));
                    }
                    _ => {}
                }
                match answer(&command).into() {
                    Answer::Ack => ack_control_request(reference_id, *command),
                    Answer::Refuse => {
                        let refusal =
                            crate::FlezhaError::with("commit.not_owner", [("relay", id.into())]);
                        crate::shutdown::signal_result(&reference_id, Err(refusal));
                    }
                    Answer::Ignore => {}
                }
            }
            Ok(RunnerMessage::EmitRelayCompleted {
                relay_id,
                status,
                error,
                ..
            }) if relay_id == id => seen.done = Some((status, error)),
            Ok(_) => {}
            Err(_) => panic!("steps {id} stalled; the controller saw {:?}", seen.iters),
        }
    }
}

fn never(_: &crate::pb::command_request::Command) -> bool {
    false
}

/// Every test here binds and feeds under the same guards.
fn turn_env() -> (
    std::sync::MutexGuard<'static, ()>,
    std::sync::MutexGuard<'static, ()>,
    Receiver<RunnerMessage>,
) {
    let env = crate::runner::env_test_guard();
    let sink = state_sink::test_guard();
    state_sink::reset_for_test();
    let (tx, rx) = unbounded::<RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    (env, sink, rx)
}

#[test]
fn a_failed_turn_is_dead_lettered_undone_once_and_the_next_message_is_the_next_iteration() {
    let (_env, _sink, rx) = turn_env();
    let _parks = capture_parks();
    let id = format!("turn_dead_letter_{}", std::process::id());
    let (_host, undos) = turn_class(&id, true, "per_input", "keep_progress");
    let mut node = RunnerNode::new("test-ns".to_string());
    bind_turns(&mut node, &id, "turn-1", None);
    let mut seen = Seen::default();

    let turn = node.get_relay("turn-1").expect("bound");
    for (offset, message) in [(0, &b"hello"[..]), (1, b"boom"), (2, b"again")] {
        feed_at(&turn, message, offset);
    }
    pump_turns(&rx, "turn-1", &mut seen, never, |s| s.iters.len() == 3);

    let commits: Vec<_> = seen
        .iters
        .iter()
        .map(|c| (c.iter, c.consumed_offset, c.dead_letter.is_some()))
        .collect();
    assert_eq!(
        commits,
        [(0, Some(0), false), (1, Some(1), true), (2, Some(2), false)]
    );
    let dead_letter = seen.iters[1].dead_letter.as_ref().expect("dead-lettered");
    assert_eq!(
        (dead_letter.tasklet.as_str(), dead_letter.attempts),
        ("p1", 3)
    );
    assert!(
        dead_letter.error.contains("model unavailable"),
        "{dead_letter:?}"
    );
    assert_eq!(seen.iters[1].entry_channel, "in");
    assert_eq!(undos.load(Ordering::SeqCst), 1, "p0's hook ran once");
    assert_eq!(
        seen.undone,
        [(0, 1)],
        "one undo entry, for p0 of iteration 1"
    );
    assert!(seen.done.is_none(), "{:?}", seen.done);
    node.stop_relay("turn-1");
    state_sink::reset_for_test();
}

#[test]
fn a_resume_owing_a_failed_turns_undo_runs_it_once_and_waits() {
    let (_env, _sink, rx) = turn_env();
    let id = format!("turn_resume_{}", std::process::id());
    let (_host, undos) = turn_class(&id, true, "per_input", "keep_progress");
    let mut node = RunnerNode::new("test-ns".to_string());
    let p0 = crate::flusher::encode_cell_identity("turn-resume-1", "p0", "");
    let owed = crate::pb::TaskCommittedTask {
        task_idx: 0,
        iter: 1,
        cells_ran: vec![p0],
        ..Default::default()
    };
    let journal = crate::pb::TaskJournalState {
        iter: 2,
        entry_channel: "in".to_string(),
        consumed_offset: Some(1),
        released_offset: Some(1),
        committed_tasks: vec![owed.clone()],
        rewind_undo: vec![owed],
        ..Default::default()
    };
    bind_turns(&mut node, &id, "turn-resume-1", Some(journal));
    let mut seen = Seen::default();

    pump_turns(&rx, "turn-resume-1", &mut seen, never, |s| {
        !s.undone.is_empty()
    });
    assert_eq!(seen.undone, [(0, 1)]);
    assert_eq!(undos.load(Ordering::SeqCst), 1);

    feed_at(
        &node.get_relay("turn-resume-1").expect("bound"),
        b"again",
        2,
    );
    pump_turns(&rx, "turn-resume-1", &mut seen, never, |s| {
        !s.iters.is_empty()
    });
    assert_eq!(seen.iters[0].iter, 2, "the turn did not run again");
    assert_eq!(undos.load(Ordering::SeqCst), 1);
    assert!(seen.done.is_none(), "{:?}", seen.done);
    node.stop_relay("turn-resume-1");
    state_sink::reset_for_test();
}

/// Every run here fails and ends as it did before a failed turn could be
/// dead-lettered.
fn ends_on_failure(
    id: &str,
    persistent: bool,
    each: &str,
    rollback: &str,
    refuse: impl Fn(&crate::pb::command_request::Command) -> bool,
    messages: &[&[u8]],
) -> Seen {
    let (_env, _sink, rx) = turn_env();
    let _parks = capture_parks();
    let factory = format!("{id}_{}", std::process::id());
    let (_host, _undos) = turn_class(&factory, persistent, each, rollback);
    let mut node = RunnerNode::new("test-ns".to_string());
    bind_turns(&mut node, &factory, id, None);
    let mut seen = Seen::default();
    let turn = node.get_relay(id).expect("bound");
    for (offset, message) in messages.iter().enumerate() {
        feed_at(&turn, message, offset as u64);
    }
    pump_turns(&rx, id, &mut seen, refuse, |_| false);
    state_sink::reset_for_test();
    seen
}

/// An instance id, whether its class persists, its loop and rollback modes,
/// and the messages it is sent.
type EndingCase = (
    &'static str,
    bool,
    &'static str,
    &'static str,
    &'static [&'static [u8]],
);

#[test]
fn a_failure_the_dead_letter_does_not_cover_still_ends_the_run() {
    use crate::pb::command_request::Command;
    let cases: [EndingCase; 3] = [
        ("turn_all", true, "per_input", "undo_all", &[b"boom"]),
        (
            "turn_chained",
            false,
            "chained",
            "keep_progress",
            &[b"boom"],
        ),
        ("turn_timer", false, "per_input", "keep_progress", &[b"arm"]),
    ];
    for (id, persistent, each, rollback, messages) in cases {
        let seen = ends_on_failure(id, persistent, each, rollback, never, messages);
        assert_eq!(
            seen.done.as_ref().map(|d| d.0.as_str()),
            Some("error"),
            "{id}"
        );
        assert!(
            seen.iters.iter().all(|c| c.dead_letter.is_none()),
            "{id} dead-lettered its turn"
        );
    }

    // A stage commit the controller refuses is not the turn's failure.
    let refuse_stage = |c: &Command| matches!(c, Command::TaskCommit(t) if !t.compensating);
    let seen = ends_on_failure(
        "turn_refused",
        true,
        "per_input",
        "keep_progress",
        refuse_stage,
        &[b"hello"],
    );
    assert_eq!(seen.done.as_ref().map(|d| d.0.as_str()), Some("error"));
    assert!(seen.iters.is_empty());

    // Nor is a refused dead-letter commit: the run ends on it.
    let refuse_dead_letter =
        |c: &Command| matches!(c, Command::IterCommit(i) if i.dead_letter.is_some());
    let seen = ends_on_failure(
        "turn_park_refused",
        true,
        "per_input",
        "keep_progress",
        refuse_dead_letter,
        &[b"boom"],
    );
    let (status, error) = seen.done.expect("the run ended");
    assert_eq!(status, "error");
    assert!(error.contains("refused by controller"), "{error}");
}

#[test]
fn a_stop_during_a_turn_ends_it_cancelled() {
    let (_env, _sink, rx) = turn_env();
    let id = format!("turn_stop_{}", std::process::id());
    let (_host, _undos) = turn_class(&id, true, "per_input", "keep_progress");
    let mut node = RunnerNode::new("test-ns".to_string());
    bind_turns(&mut node, &id, "turn-stop-1", None);
    let mut seen = Seen::default();

    feed_at(&node.get_relay("turn-stop-1").expect("bound"), b"hello", 0);
    pump_turns(&rx, "turn-stop-1", &mut seen, never, |s| {
        !s.iters.is_empty()
    });
    let instance = node.get_relay("turn-stop-1").expect("running");
    crate::steps::stop(&instance);
    pump_turns(&rx, "turn-stop-1", &mut seen, never, |_| false);
    assert_eq!(seen.done.map(|d| d.0), Some("cancelled".to_string()));
    assert!(seen.iters.iter().all(|c| c.dead_letter.is_none()));
    state_sink::reset_for_test();
}

#[test]
fn a_run_that_journals_nothing_parks_its_failed_turn_and_answers_the_next() {
    let (_env, _sink, rx) = turn_env();
    let parks = capture_parks();
    let id = format!("turn_optout_{}", std::process::id());
    let (_host, undos) = turn_class(&id, false, "per_input", "keep_progress");
    let mut node = RunnerNode::new("test-ns".to_string());
    let wire = Wire::new();
    bind_turns(&mut node, &id, "turn-optout-1", None);

    feed_with(
        &node,
        "turn-optout-1",
        b"boom",
        &[("flz.reply_to", "w:boom")],
        &wire.stream,
    );
    spin_until("the failed turn to be parked", || {
        !parks.lock().expect("park capture poisoned").is_empty()
    });
    let park = parks.lock().expect("park capture poisoned").remove(0);
    assert_eq!(
        park.identity,
        (
            "turn-optout-1".to_string(),
            "_turn".to_string(),
            "_turn".to_string()
        )
    );
    assert_eq!(park.body.entry.channel, "in");
    assert_eq!(park.body.entry.payload, None);
    assert_eq!(park.body.origin_offset, None);
    assert_eq!(
        park.body.reply_to.as_deref(),
        Some("w:boom"),
        "the pass's call"
    );
    assert!(
        park.body
            .entry
            .error
            .starts_with("tasklet `p1` failed after 3 attempt(s): "),
        "{}",
        park.body.entry.error
    );
    spin_until("p0's undo", || undos.load(Ordering::SeqCst) == 1);

    feed_with(&node, "turn-optout-1", b"again", &[], &wire.stream);
    assert_eq!(outputs(&wire, "turn-optout-1", 1), [b"re:again".to_vec()]);
    assert_no_completion(&rx, "turn-optout-1", Duration::from_millis(200));
    state_sink::reset_for_test();
}

/// A two-stage `per_input` pass answers the call its message carried from the
/// second stage's sink, which never saw the message; a message nobody called
/// carries no token.
#[test]
fn a_per_input_pass_answers_its_call_from_its_last_stage() {
    let (_env, _sink, rx) = turn_env();
    let id = format!("turn_answer_{}", std::process::id());
    let json = turn_json(&id, false, "per_input", "keep_progress");
    crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    let mid = crate::relay::registry::get_class(&id)
        .and_then(|topology| topology.lookup_channel_id("mid"))
        .expect("the class declares `mid`");
    let _host = turn_host(mid, Arc::default());
    let mut node = RunnerNode::new("test-ns".to_string());
    let wire = Wire::new();
    bind_turns(&mut node, &id, "turn-answer-1", None);

    feed_with(
        &node,
        "turn-answer-1",
        b"hello",
        &[("flz.reply_to", "w:one")],
        &wire.stream,
    );
    feed_with(&node, "turn-answer-1", b"plain", &[], &wire.stream);
    let mut answers = Vec::new();
    while answers.len() < 2 {
        let (payload_id, payload, metadata) = wire
            .frames
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|_| panic!("the run answered {answers:?} and then nothing"));
        if payload_id == "turn-answer-1" {
            answers.push((payload, metadata.get("flz.reply_to").cloned()));
        }
    }
    assert_eq!(
        answers,
        vec![
            (b"re:hello".to_vec(), Some("w:one".to_string())),
            (b"re:plain".to_vec(), None),
        ]
    );
    assert_no_completion(&rx, "turn-answer-1", Duration::from_millis(200));
    state_sink::reset_for_test();
}
