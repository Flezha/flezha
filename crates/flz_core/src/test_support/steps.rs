//! The controller half a steps run talks to, for this crate's steps tests and
//! the Rust SDK's: what a barrier send, a journal entry and a lost ack look
//! like from the runner's side, plus the drains that turn `RunnerMessage`s
//! into a scenario's observations.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossbeam::channel::Receiver;
use flz_transport::transport::FlezhaStream;

use crate::runner::{RunnerMessage, RunnerNode};

/// Task-commits observed by the fake controller (`steps-task-journal`
/// barrier): every `TaskCommit` the drain helpers ack is recorded here
/// so the gating/handoff assertions can inspect what would be journaled.
pub static TASK_COMMITS: Mutex<Vec<crate::pb::TaskCommit>> = Mutex::new(Vec::new());

/// Iteration commits observed by the fake controller (`steps-durable-loop`
/// boundary) — what the journal would record as `iter_committed`.
pub static ITER_COMMITS: Mutex<Vec<crate::pb::IterCommit>> = Mutex::new(Vec::new());

/// Park notifications observed by the fake controller
/// (`durable-suspension`) — the instance evicted itself at a
/// committed boundary.
pub static ITER_PARKS: Mutex<Vec<crate::pb::IterPark>> = Mutex::new(Vec::new());

/// Every barrier send the fake controller saw, `(relay_id,
/// reference_id)`, re-sends included.
pub static BARRIER_SENDS: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

/// Reference ids the fake controller has journaled: a re-send under one of
/// them is acked from the existing entry, never recorded twice.
pub static JOURNALED_REFS: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Per relay, how many barriers lose their first ack on the way back
/// (`usize::MAX` = every send of every barrier). A lost ack is the real
/// controller's: the entry is journaled and the records it names released,
/// only the reply never reaches the runner.
pub static LOST_ACKS: Mutex<Vec<(String, usize)>> = Mutex::new(Vec::new());

/// Every plugin frame written STRAIGHT to the wire, bypassing any task
/// boundary. A durable structured relay writes none (its output rides its
/// Cells' effect records); an opted-out class does.
pub static WIRE_EMITS: Mutex<Vec<(String, Vec<u8>)>> = Mutex::new(Vec::new());

/// What a plugin receives, in order, per relay: an opted-out class's wire
/// output, and a durable one's record outputs once the commit naming the
/// records is acked.
pub static DELIVERED: Mutex<Vec<(String, Vec<u8>)>> = Mutex::new(Vec::new());

/// The `(payload_id, frame)` pairs a durable steps relay ships to the state
/// output, once a test installs a sink that records them here.
pub static WAL_FRAMES: Mutex<Vec<(String, Vec<u8>)>> = Mutex::new(Vec::new());

pub fn count_wal_frames(payload_id: &str) -> usize {
    WAL_FRAMES
        .lock()
        .unwrap()
        .iter()
        .filter(|(pid, _)| pid == payload_id)
        .count()
}

/// The wire a run's plugin frames are written to: each parsed back and
/// recorded as delivered.
pub fn capturing_wire() -> Arc<FlezhaStream> {
    Arc::new(FlezhaStream::Local(Arc::new(|frame: &[u8]| {
        let (relay_id, payload, _metadata) =
            flz_transport::service::payloader::extract_payload(frame)?;
        WIRE_EMITS
            .lock()
            .unwrap()
            .push((relay_id.clone(), payload.clone()));
        DELIVERED.lock().unwrap().push((relay_id, payload));
        Ok(())
    })))
}

/// `id`'s deliveries since the last take.
pub fn take_delivered(id: &str) -> Vec<Vec<u8>> {
    let mut delivered = DELIVERED.lock().unwrap();
    let (mine, rest): (Vec<_>, Vec<_>) = delivered.drain(..).partition(|(oid, _)| oid == id);
    *delivered = rest;
    mine.into_iter().map(|(_, payload)| payload).collect()
}

/// The outputs of the effect records `cells` name, as the controller releases
/// them once the entry naming them is durable, read back from the frames the
/// run shipped to the state output.
pub fn released(cells: &[crate::pb::CommittedCell]) -> Vec<Vec<u8>> {
    let frames = WAL_FRAMES.lock().unwrap();
    let records: Vec<(Vec<u8>, u64, Vec<Vec<u8>>)> = frames
        .iter()
        .filter(|(pid, _)| pid == crate::state_sink::CELL_EFFECT_RECORD_PAYLOAD_ID)
        .filter_map(|(_, frame)| effect_record(frame))
        .collect();
    cells
        .iter()
        .filter_map(|cell| Some((cell, cell.first_seq?, cell.seq?)))
        .flat_map(|(cell, first, last)| {
            records
                .iter()
                .filter(|(id, seq, _)| *id == cell.cell_id && (first..=last).contains(seq))
                .flat_map(|(_, _, outputs)| outputs.clone())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// A `cell_effect_record` frame's Cell, seq and output payloads.
fn effect_record(frame: &[u8]) -> Option<(Vec<u8>, u64, Vec<Vec<u8>>)> {
    let split = super::split_cell_frame(frame)?;
    let record: crate::flusher::EffectRecordBody = postcard::from_bytes(split.body).ok()?;
    Some((
        split.identity.to_vec(),
        split.seq,
        record.outputs.into_iter().map(|o| o.payload).collect(),
    ))
}

pub fn drain_task_commits() -> Vec<crate::pb::TaskCommit> {
    std::mem::take(&mut *TASK_COMMITS.lock().unwrap())
}

/// Wait, three seconds at most, until the journal holds a commit naming
/// `tasklet`'s Cell of instance `relay_id`; whether it does. Leaves the
/// commits in place, so a tasklet can watch its sibling's.
pub fn await_commit(relay_id: &str, tasklet: &str) -> bool {
    let cell = crate::flusher::encode_cell_identity(relay_id, tasklet, "");
    let committed = || {
        TASK_COMMITS
            .lock()
            .unwrap()
            .iter()
            .any(|commit| commit.cells_ran.iter().any(|ran| ran.cell_id == cell))
    };
    super::wait_until(Duration::from_secs(3), Duration::from_millis(2), committed)
}

pub fn drain_iter_commits() -> Vec<crate::pb::IterCommit> {
    std::mem::take(&mut *ITER_COMMITS.lock().unwrap())
}

pub fn drain_iter_parks() -> Vec<crate::pb::IterPark> {
    std::mem::take(&mut *ITER_PARKS.lock().unwrap())
}

pub fn drain_barrier_sends() -> Vec<(String, String)> {
    std::mem::take(&mut *BARRIER_SENDS.lock().unwrap())
}

/// The reference ids of `relay_id`'s barrier sends, in order.
pub fn barrier_sends_of(relay_id: &str) -> Vec<String> {
    BARRIER_SENDS
        .lock()
        .unwrap()
        .iter()
        .filter(|(id, _)| id == relay_id)
        .map(|(_, reference_id)| reference_id.clone())
        .collect()
}

pub fn lose_acks(relay_id: &str, count: usize) {
    LOST_ACKS
        .lock()
        .unwrap()
        .push((relay_id.to_string(), count));
}

/// Whether this send's ack is lost: a barrier's first send consumes one of
/// the relay's losses, a re-send is acked — unless every ack is lost.
pub fn ack_lost(relay_id: &str, first_send: bool) -> bool {
    let mut lost = LOST_ACKS.lock().unwrap();
    match lost.iter_mut().find(|(id, _)| id == relay_id) {
        Some((_, n)) if *n == usize::MAX => true,
        Some((_, n)) if *n > 0 && first_send => {
            *n -= 1;
            true
        }
        _ => false,
    }
}

/// The controller's barrier handling: a first send is journaled (`record`)
/// and the records it names released, a re-send is acked from the existing
/// entry, unless this relay's ack is lost on the way back.
pub fn journal_barrier(
    relay_id: &str,
    reference_id: &str,
    released: Vec<Vec<u8>>,
    record: impl FnOnce(),
) {
    BARRIER_SENDS
        .lock()
        .unwrap()
        .push((relay_id.to_string(), reference_id.to_string()));
    let fresh = {
        let mut refs = JOURNALED_REFS.lock().unwrap();
        let fresh = !refs.iter().any(|r| r == reference_id);
        if fresh {
            refs.push(reference_id.to_string());
        }
        fresh
    };
    if fresh {
        record();
    }
    if !ack_lost(relay_id, fresh) {
        crate::shutdown::signal_result(reference_id, Ok(String::new()));
    }
    if fresh {
        let mut delivered = DELIVERED.lock().unwrap();
        delivered.extend(released.into_iter().map(|p| (relay_id.to_string(), p)));
    }
}

pub fn wire_emits_for(id: &str) -> Vec<Vec<u8>> {
    WIRE_EMITS
        .lock()
        .unwrap()
        .iter()
        .filter(|(oid, _)| oid == id)
        .map(|(_, p)| p.clone())
        .collect()
}

/// Fake-controller half of every SDK-initiated control request: record the
/// frame and, for the two durability handshakes, answer the barrier with an
/// ack success, releasing what a task commit names to `DELIVERED` as the real
/// controller releases it AFTER that durable point. A park is one-way:
/// nothing to ack.
pub fn ack_control_request(reference_id: String, command: crate::pb::command_request::Command) {
    use crate::pb::command_request::Command;
    match command {
        Command::TaskCommit(commit) => {
            let relay_id = commit.relay_id.clone();
            journal_barrier(
                &relay_id,
                &reference_id,
                released(&commit.cells_ran),
                || TASK_COMMITS.lock().unwrap().push(commit),
            )
        }
        Command::IterCommit(commit) => {
            let relay_id = commit.relay_id.clone();
            journal_barrier(&relay_id, &reference_id, Vec::new(), || {
                ITER_COMMITS.lock().unwrap().push(commit)
            })
        }
        Command::IterPark(park) => ITER_PARKS.lock().unwrap().push(park),
        _ => {}
    }
}

/// The relay id an SDK-initiated control request belongs to.
pub fn control_request_relay(command: &crate::pb::command_request::Command) -> String {
    use crate::pb::command_request::Command;
    match command {
        Command::TaskCommit(c) => c.relay_id.clone(),
        Command::IterCommit(c) => c.relay_id.clone(),
        Command::IterPark(p) => p.relay_id.clone(),
        _ => String::new(),
    }
}

/// Bind steps relay `id` of `factory` as `AddRelay` does, resuming with
/// `expected` Cell restores, `owed_inbox` re-forwarded messages and the
/// journal; `Ok(false)` names an unregistered class, `Err` a refused
/// `init_values`.
pub fn start_resumable(
    node: &RunnerNode,
    factory: String,
    id: String,
    init_values: &str,
    expected: u32,
    owed_inbox: u32,
    journal: Option<crate::steps::instance::JournalResume>,
) -> Result<bool, crate::Error> {
    let resume = crate::steps::Resume {
        expected: expected as usize,
        owed_inbox,
        journal,
    };
    bind(
        node,
        factory,
        id,
        init_values,
        crate::relay::instance::KeyGroupRange::UNPARTITIONED,
        resume,
    )
}

/// A fresh bind of steps relay `id` as the partition member owning `range`.
pub fn start_partitioned(
    node: &RunnerNode,
    factory: &str,
    id: &str,
    range: crate::relay::instance::KeyGroupRange,
) -> bool {
    bind(
        node,
        factory.into(),
        id.into(),
        "",
        range,
        crate::steps::Resume::default(),
    )
    .expect("init_values is a JSON object")
}

fn bind(
    node: &RunnerNode,
    factory: String,
    id: String,
    init_values: &str,
    range: crate::relay::instance::KeyGroupRange,
    resume: crate::steps::Resume,
) -> Result<bool, crate::Error> {
    // A duplicate never initializes twice, as `AddRelay` guards it.
    if node.has_relay(&id) {
        return Ok(true);
    }
    let Some(instance) = node.start_relay(factory, id.clone(), range, resume) else {
        return Ok(false);
    };
    if let Err(e) = crate::command::cell_state::apply_init_values(&instance, init_values) {
        node.stop_relay(&id);
        return Err(e);
    }
    crate::steps::open(&instance);
    Ok(true)
}

/// A fresh bind of steps relay `id`.
pub fn start(
    node: &RunnerNode,
    factory: String,
    id: String,
    init_values: &str,
) -> Result<bool, crate::Error> {
    start_resumable(node, factory, id, init_values, 0, 0, None)
}

/// One Cell restore landed, carrying no saga marker.
pub fn restored(instance: &crate::relay::instance::RelayInstance) {
    crate::steps::restore_landed(instance, &[], false, false);
}

/// One message into steps relay `id` through the entrance, on its first entry
/// channel; `false` when no such instance is registered.
pub fn feed(node: &RunnerNode, id: &str, message: &[u8]) -> bool {
    feed_on(node, id, None, message)
}

/// One message into steps relay `id` naming `channel`, as `flz relay send
/// --channel` does.
pub fn feed_on(node: &RunnerNode, id: &str, channel: Option<&str>, message: &[u8]) -> bool {
    let channel = channel.map(|name| ("channel", name));
    node.has_relay(id) && {
        feed_with(
            node,
            id,
            message,
            channel.as_slice(),
            &crate::steps::unwired_stream(),
        );
        true
    }
}

/// One message into relay `id` riding `stream`, `metadata` stamped on it: the
/// dispatch a stream reader makes.
pub fn feed_with(
    node: &RunnerNode,
    id: &str,
    message: &[u8],
    metadata: &[(&str, &str)],
    stream: &Arc<FlezhaStream>,
) {
    let metadata: HashMap<String, String> = metadata
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    node.relay_dispatch_handle()
        .dispatch(id, message.to_vec(), &metadata, Arc::clone(stream));
}

/// One message into `instance` stamped with the input-log offset the
/// controller would have given it.
pub fn feed_at(instance: &Arc<crate::relay::instance::RelayInstance>, message: &[u8], offset: u64) {
    let registry = Arc::new(crate::relay::registry::RelayRegistry::new());
    registry.register(Arc::clone(instance));
    let metadata = HashMap::from([("flz.offset".to_string(), offset.to_string())]);
    crate::runner::RelayDispatchHandle { registry }.dispatch(
        &instance.instance_id,
        message.to_vec(),
        &metadata,
        crate::steps::unwired_stream(),
    );
}

/// Start `factory` under `id` and hand it `message` as its first input: what
/// a spawn, a `send` or `flz relay start --text` does.
pub fn start_fed(node: &RunnerNode, factory: &str, id: &str, message: &[u8]) -> bool {
    start(node, factory.into(), id.into(), "").expect("init_values is a JSON object")
        && feed(node, id, message)
}

/// Answer control requests until steps relay `id` completes, returning
/// what reached its plugins and the `(status, error, result)` completion
/// tuple: an opted-out emit straight off the wire, a durable relay's
/// record outputs once the commit naming them is acked.
pub fn drain(rx: &Receiver<RunnerMessage>, id: &str) -> (Vec<Vec<u8>>, (String, String, Vec<u8>)) {
    let done = await_completion(rx, id);
    (take_delivered(id), done)
}

/// How long a scenario waits for a run to progress before failing. Only a
/// bound on a lost run: a machine running several suites at once makes a
/// healthy run several times slower than alone.
const SCENARIO_WAIT: Duration = Duration::from_secs(30);

/// Answer control requests until steps relay `id` completes; its
/// `(status, error, result)`. A park tears the run down without a
/// completion, so it is reported as the run's `parked` outcome.
pub fn await_completion(rx: &Receiver<RunnerMessage>, id: &str) -> (String, String, Vec<u8>) {
    let deadline = Instant::now() + SCENARIO_WAIT;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let msg = rx
            .recv_timeout(remaining)
            .unwrap_or_else(|_| panic!("timed out waiting for steps {id}"));
        match msg {
            RunnerMessage::EmitRelayCompleted {
                relay_id,
                status,
                error,
                result,
                ..
            } if relay_id == id => return (status, error, result),
            RunnerMessage::EmitControlRequest {
                reference_id,
                command,
            } => {
                let for_id = control_request_relay(&command) == id;
                let parked = matches!(*command, crate::pb::command_request::Command::IterPark(_));
                ack_control_request(reference_id, *command);
                if for_id && parked {
                    return ("parked".to_string(), String::new(), Vec::new());
                }
            }
            _ => {}
        }
    }
}

/// The `IterPark` steps relay `id`'s run shipped when it suspended.
pub fn await_park(rx: &Receiver<RunnerMessage>, id: &str) -> crate::pb::IterPark {
    let (status, _, _) = await_completion(rx, id);
    assert_eq!(status, "parked", "the run ended instead of parking");
    let mut parks = ITER_PARKS.lock().unwrap();
    let at = parks.iter().rposition(|park| park.relay_id == id);
    parks.remove(at.expect("the park was recorded"))
}

/// What reached `id`'s plugins until `done` holds, every control request
/// answered on the way.
pub fn pump(
    rx: &Receiver<RunnerMessage>,
    id: &str,
    done: impl Fn(&[Vec<u8>]) -> bool,
) -> Vec<Vec<u8>> {
    let deadline = Instant::now() + SCENARIO_WAIT;
    let mut outs = Vec::new();
    loop {
        outs.extend(take_delivered(id));
        if done(&outs) {
            return outs;
        }
        assert!(
            Instant::now() < deadline,
            "{id} delivered {outs:?} and then nothing"
        );
        if let Ok(RunnerMessage::EmitControlRequest {
            reference_id,
            command,
        }) = rx.recv_timeout(Duration::from_millis(20))
        {
            ack_control_request(reference_id, *command);
        }
    }
}

/// A completion for `id` within `wait` fails the scenario; control requests
/// seen meanwhile are acked as the controller would.
pub fn assert_no_completion(rx: &Receiver<RunnerMessage>, id: &str, wait: Duration) {
    let deadline = Instant::now() + wait;
    while let Ok(message) = rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
        match message {
            RunnerMessage::EmitRelayCompleted {
                relay_id, status, ..
            } => assert!(
                relay_id != id,
                "steps {id} ended with status `{status}` instead of staying quiet"
            ),
            RunnerMessage::EmitControlRequest {
                reference_id,
                command,
            } => ack_control_request(reference_id, *command),
            _ => {}
        }
    }
}

/// Block until a specific output of `id` reaches its plugins, confirming an
/// interactive instance is live before stopping it.
pub fn wait_for_output(rx: &Receiver<RunnerMessage>, id: &str, payload: &[u8]) {
    pump(rx, id, |outs| outs.iter().any(|p| p == payload));
}
