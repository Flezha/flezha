//! A flow relay's deadlines. One whose class sets `sleep_after` parks at
//! iteration `0` once every Cell has been idle for that long, and not while a
//! Cell still holds work; one whose `sleep_after` is `false` never parks. One
//! that ends from inside (a tasklet's `complete`, or `max_duration_ms`) reports
//! its end once, after its Cells have run what they held; a bind arms the stop
//! as far out as the window it carries. The deadline is promoted as a waiting poller
//! promotes it, by a thread that takes no activation, so the Cells other
//! tests hold are never drained here.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam::channel::{unbounded, Receiver};

use crate::relay::instance::RelayInstance;
use crate::runner::{RunnerMessage, RunnerNode};
use crate::steps::Deadline;
use crate::ErrorModel;

const WINDOW_MS: u64 = 150;

/// A host flow class kept locally with one tasklet, sleeping after
/// `sleep_after_ms` or never.
fn flow_class(id: &str, sleep_after_ms: Option<u64>) {
    let sleep_after = sleep_after_ms.map_or("false".to_string(), |ms| ms.to_string());
    let json = format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0",
             "durable":{{"local":{{"sleep_after":{sleep_after}}}}},
             "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
             "tasklets":[{{"name":"p0"}}]}}"#
    );
    crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
}

/// Promotes due deadlines while it lives.
struct Promoter {
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Promoter {
    fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    crate::poll_core::global().promote_due_wakes();
                    thread::sleep(Duration::from_millis(5));
                }
            })
        };
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Promoter {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Bind `instance` of `factory` and answer it with the control channel.
fn bound(factory: &str, instance: &str) -> (RunnerNode, Receiver<RunnerMessage>) {
    bound_with(factory, instance, None)
}

/// [`bound`], the bind carrying `window_ms` of the run's `max_duration`.
fn bound_with(
    factory: &str,
    instance: &str,
    window_ms: Option<u64>,
) -> (RunnerNode, Receiver<RunnerMessage>) {
    use crate::command::CommandAction;
    let (tx, rx) = unbounded::<RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    let mut node = RunnerNode::new("test-ns".to_string());
    let mut errors = Vec::new();
    crate::pb::AddRelay {
        relay_id: instance.to_string(),
        factory_id: factory.to_string(),
        max_duration_remaining_ms: window_ms,
        ..Default::default()
    }
    .handle(&mut node, "", &mut errors);
    assert!(errors.is_empty(), "{errors:?}");
    (node, rx)
}

/// The `IterPark` `id` shipped within `within`, if any.
fn park_of(
    rx: &Receiver<RunnerMessage>,
    id: &str,
    within: Duration,
) -> Option<crate::pb::IterPark> {
    let deadline = Instant::now() + within;
    while let Some(left) = deadline.checked_duration_since(Instant::now()) {
        match rx.recv_timeout(left) {
            Ok(RunnerMessage::EmitControlRequest { command, .. }) => {
                if let crate::pb::command_request::Command::IterPark(park) = *command {
                    if park.relay_id == id {
                        return Some(park);
                    }
                }
            }
            Ok(_) => {}
            Err(_) => return None,
        }
    }
    None
}

/// The `RelayCompleted` `(status, result)` of `id` shipped within `within`.
fn completion_of(
    rx: &Receiver<RunnerMessage>,
    id: &str,
    within: Duration,
) -> Option<(String, Vec<u8>)> {
    let deadline = Instant::now() + within;
    while let Some(left) = deadline.checked_duration_since(Instant::now()) {
        match rx.recv_timeout(left) {
            Ok(RunnerMessage::EmitRelayCompleted {
                relay_id,
                status,
                result,
                ..
            }) if relay_id == id => return Some((status, result)),
            Ok(_) => {}
            Err(_) => return None,
        }
    }
    None
}

fn only_cell(node: &RunnerNode, id: &str) -> (Arc<RelayInstance>, Arc<crate::cell::CellSlot>) {
    let instance = node.get_relay(id).expect("bound");
    let slot = Arc::clone(&instance.all_slots()[0]);
    (instance, slot)
}

#[test]
fn an_idle_flow_that_sleeps_parks_once_at_iteration_zero() {
    let _env = crate::runner::env_test_guard();
    let factory = format!("sleep_idle_{}", std::process::id());
    flow_class(&factory, Some(WINDOW_MS));
    let _promoter = Promoter::start();
    let (_node, rx) = bound(&factory, "sleep-idle-1");

    let park = park_of(&rx, "sleep-idle-1", Duration::from_secs(5)).expect("the idle stream slept");
    assert_eq!(
        (park.iter, park.wake_deadline_ms, park.consumed_offset),
        (0, 0, None)
    );
    assert!(
        park_of(&rx, "sleep-idle-1", Duration::from_millis(4 * WINDOW_MS)).is_none(),
        "it asked to sleep once"
    );
}

#[test]
fn a_flow_that_does_not_declare_sleeping_never_parks() {
    let _env = crate::runner::env_test_guard();
    let factory = format!("sleep_never_{}", std::process::id());
    flow_class(&factory, None);
    let _promoter = Promoter::start();
    let (node, rx) = bound(&factory, "sleep-never-1");

    assert!(park_of(&rx, "sleep-never-1", Duration::from_millis(4 * WINDOW_MS)).is_none());
    let (instance, _) = only_cell(&node, "sleep-never-1");
    assert!(crate::poll_core::global()
        .instance_deadlines(instance.seq)
        .is_empty());
}

#[test]
fn a_cell_holding_work_keeps_the_relay_awake_until_it_is_done() {
    let _env = crate::runner::env_test_guard();
    let factory = format!("sleep_busy_{}", std::process::id());
    flow_class(&factory, Some(WINDOW_MS));
    let _promoter = Promoter::start();
    let hold = Duration::from_millis(3 * WINDOW_MS);

    // A retry backoff keeps the drain gate held.
    let (node, rx) = bound(&factory, "sleep-busy-retry");
    let (_, slot) = only_cell(&node, "sleep-busy-retry");
    assert!(slot.mailbox.claim_inflight());
    assert!(park_of(&rx, "sleep-busy-retry", hold).is_none());
    slot.mailbox.release_inflight();
    assert!(park_of(&rx, "sleep-busy-retry", Duration::from_secs(5)).is_some());

    // A pending timer.
    let (node, rx) = bound(&factory, "sleep-busy-timer");
    let (_, slot) = only_cell(&node, "sleep-busy-timer");
    let timer = |register| crate::schema::TimerOp {
        kind: crate::timer::TimerKind::ProcessingTime,
        scheduled_at: u64::MAX,
        register,
    };
    slot.timers.lock().unwrap().apply(&timer(true));
    assert!(park_of(&rx, "sleep-busy-timer", hold).is_none());
    slot.timers.lock().unwrap().apply(&timer(false));
    assert!(park_of(&rx, "sleep-busy-timer", Duration::from_secs(5)).is_some());

    // A buffer the flusher holds.
    let (node, rx) = bound(&factory, "sleep-busy-flush");
    let (_, slot) = only_cell(&node, "sleep-busy-flush");
    let flushing = slot.flush_buffer.lock().unwrap();
    assert!(park_of(&rx, "sleep-busy-flush", hold).is_none());
    drop(flushing);
    assert!(park_of(&rx, "sleep-busy-flush", Duration::from_secs(5)).is_some());
}

#[test]
fn recent_activity_re_arms_the_deadline_from_the_last_input() {
    let _env = crate::runner::env_test_guard();
    let window = Duration::from_millis(400);
    let factory = format!("sleep_rearm_{}", std::process::id());
    flow_class(&factory, Some(window.as_millis() as u64));
    let _promoter = Promoter::start();
    let started = Instant::now();
    let (node, rx) = bound(&factory, "sleep-rearm-1");
    let (instance, slot) = only_cell(&node, "sleep-rearm-1");
    let armed = crate::poll_core::global().instance_deadlines(instance.seq);

    thread::sleep(window / 2);
    slot.last_activity_ms
        .store(crate::util::monotonic_ms(), Ordering::Relaxed);
    assert_eq!(
        crate::poll_core::global().instance_deadlines(instance.seq),
        armed,
        "an input does not touch the wake list"
    );

    assert!(park_of(&rx, "sleep-rearm-1", Duration::from_secs(5)).is_some());
    // The activity stamp is whole milliseconds, so the deadline may land up
    // to one before the exact bound.
    assert!(
        started.elapsed() + Duration::from_millis(1) >= window + window / 2,
        "slept {:?} after the bind, inside the window of the last activity",
        started.elapsed()
    );
}

#[test]
fn a_class_kept_in_memory_has_no_sleep_interval_to_declare() {
    let json = r#"{"factory_id":"sleep_refused","sdk_lang":1,"sdk_version":"0.1.0",
                   "durable":{"false":{"sleep_after":1000}},
                   "channels":[{"name":"in","kind":"entry","chains":[[{"link":"into","tasklet":0}]]}],
                   "tasklets":[{"name":"p0"}]}"#;
    let refusal = crate::host::register_host_class_json(json.as_bytes()).expect_err("refused");
    assert_eq!(refusal.code(), "topology.unknown_field");
    assert_eq!(refusal.to_error().details["fields"], "sleep_after");
    assert!(crate::relay::registry::get_class("sleep_refused").is_none());
}

#[test]
fn a_flow_that_completes_from_inside_ends_once_its_cells_are_done() {
    let _env = crate::runner::env_test_guard();
    let factory = format!("complete_flow_{}", std::process::id());
    flow_class(&factory, None);
    let _promoter = Promoter::start();
    let (node, rx) = bound(&factory, "complete-flow-1");
    let (_, slot) = only_cell(&node, "complete-flow-1");

    assert!(slot.mailbox.claim_inflight(), "the Cell holds work");
    crate::relay::completion::complete(&slot, b"done".to_vec());
    assert!(
        completion_of(&rx, "complete-flow-1", Duration::from_millis(3 * WINDOW_MS)).is_none(),
        "the end waits for the Cell"
    );
    slot.mailbox.release_inflight();

    let (status, result) =
        completion_of(&rx, "complete-flow-1", Duration::from_secs(5)).expect("the instance ended");
    assert_eq!(
        (status.as_str(), result.as_slice()),
        ("completed", &b"done"[..])
    );
    assert!(
        completion_of(&rx, "complete-flow-1", Duration::from_millis(3 * WINDOW_MS)).is_none(),
        "it ended once"
    );
}

/// A flow class kept locally whose run stops after `window_ms`.
fn stop_class(factory: &str, window_ms: u64) {
    let json = format!(
        r#"{{"factory_id":"{factory}","sdk_lang":1,"sdk_version":"0.1.0","max_duration_ms":{window_ms},
             "durable":{{"local":{{"sleep_after":false}}}},
             "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
             "tasklets":[{{"name":"p0"}}]}}"#
    );
    crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
}

#[test]
fn a_flow_with_a_wall_clock_stop_ends_cancelled() {
    let _env = crate::runner::env_test_guard();
    let factory = format!("stop_flow_{}", std::process::id());
    stop_class(&factory, WINDOW_MS);
    let _promoter = Promoter::start();
    let started = Instant::now();
    let (_node, rx) = bound(&factory, "stop-flow-1");

    let (status, result) =
        completion_of(&rx, "stop-flow-1", Duration::from_secs(5)).expect("the stop ended it");
    assert_eq!((status.as_str(), result.len()), ("cancelled", 0));
    assert!(started.elapsed() >= Duration::from_millis(WINDOW_MS));
}

#[test]
fn a_bind_arms_what_is_left_of_the_window_it_carries() {
    let _env = crate::runner::env_test_guard();
    let factory = format!("window_flow_{}", std::process::id());
    stop_class(&factory, 60_000);
    let due = |id: &str, window_ms: Option<u64>| {
        let (node, _rx) = bound_with(&factory, id, window_ms);
        let (instance, _) = only_cell(&node, id);
        let core = crate::poll_core::global();
        let (_, due) = core
            .instance_deadlines(instance.seq)
            .into_iter()
            .find(|(deadline, _)| *deadline == Deadline::MaxDuration)
            .expect("max_duration armed");
        core.disarm_instance(instance.seq, Deadline::MaxDuration, due);
        due
    };

    let whole = due("window-whole", None);
    let left = due("window-left", Some(500));

    assert!(
        whole.abs_diff(left + 59_500) < 1_000,
        "500 left arms about 0.5 s out, the whole window 60 s: {whole} vs {left}"
    );
}

#[test]
fn a_spent_window_ends_the_flow_at_once() {
    let _env = crate::runner::env_test_guard();
    let factory = format!("spent_flow_{}", std::process::id());
    stop_class(&factory, 60_000);
    let _promoter = Promoter::start();
    let (_node, rx) = bound_with(&factory, "spent-flow-1", Some(0));

    // Far inside the class's whole window of 60 s.
    let (status, _) = completion_of(&rx, "spent-flow-1", Duration::from_secs(5))
        .expect("the spent window ended it");
    assert_eq!(status, "cancelled");
}
