//! An unbounded operator whose class sleeps parks at iteration `0` once every Cell has
//! been idle for the window, and not while a Cell still holds work; one that
//! does not sleep never parks. The deadline is promoted as a waiting poller
//! promotes it, by a thread that takes no activation, so the Cells other
//! tests hold are never drained here.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam::channel::{unbounded, Receiver};

use crate::operator::instance::OperatorInstance;
use crate::runner::{RunnerMessage, RunnerNode};
use crate::test_support::host_class;

const WINDOW_MS: u64 = 150;

/// A persistent host unbounded class with one tasklet.
fn unbounded_class(id: &str, sleeps: bool) {
    let json = format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","persistent":true,
             "sleep_when_idle":{sleeps},
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
    let (tx, rx) = unbounded::<RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    let mut node = RunnerNode::new("test-ns".to_string());
    let errors = host_class::bind(&mut node, factory, instance, "", 0);
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
                    if park.operator_id == id {
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

fn only_cell(node: &RunnerNode, id: &str) -> (Arc<OperatorInstance>, Arc<crate::cell::CellSlot>) {
    let instance = node.get_operator(id).expect("bound");
    let slot = Arc::clone(&instance.all_slots()[0]);
    (instance, slot)
}

#[test]
fn an_idle_unbounded_that_sleeps_parks_once_at_iteration_zero() {
    let _env = crate::runner::env_test_guard();
    std::env::set_var("FLEZHA_PARK_IDLE_MS", WINDOW_MS.to_string());
    let factory = format!("sleep_idle_{}", std::process::id());
    unbounded_class(&factory, true);
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
    std::env::remove_var("FLEZHA_PARK_IDLE_MS");
}

#[test]
fn an_unbounded_that_does_not_declare_sleeping_never_parks() {
    let _env = crate::runner::env_test_guard();
    std::env::set_var("FLEZHA_PARK_IDLE_MS", WINDOW_MS.to_string());
    let factory = format!("sleep_never_{}", std::process::id());
    unbounded_class(&factory, false);
    let _promoter = Promoter::start();
    let (node, rx) = bound(&factory, "sleep-never-1");

    assert!(park_of(&rx, "sleep-never-1", Duration::from_millis(4 * WINDOW_MS)).is_none());
    let (instance, _) = only_cell(&node, "sleep-never-1");
    assert!(crate::poll_core::global()
        .instance_deadlines(instance.seq)
        .is_empty());
    std::env::remove_var("FLEZHA_PARK_IDLE_MS");
}

#[test]
fn a_cell_holding_work_keeps_the_operator_awake_until_it_is_done() {
    let _env = crate::runner::env_test_guard();
    std::env::set_var("FLEZHA_PARK_IDLE_MS", WINDOW_MS.to_string());
    let factory = format!("sleep_busy_{}", std::process::id());
    unbounded_class(&factory, true);
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
    std::env::remove_var("FLEZHA_PARK_IDLE_MS");
}

#[test]
fn recent_activity_re_arms_the_deadline_from_the_last_input() {
    let _env = crate::runner::env_test_guard();
    let window = Duration::from_millis(400);
    std::env::set_var("FLEZHA_PARK_IDLE_MS", window.as_millis().to_string());
    let factory = format!("sleep_rearm_{}", std::process::id());
    unbounded_class(&factory, true);
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
    assert!(
        started.elapsed() >= window + window / 2,
        "slept {:?} after the bind, inside the window of the last activity",
        started.elapsed()
    );
    std::env::remove_var("FLEZHA_PARK_IDLE_MS");
}

#[test]
fn sleeping_without_persistence_is_refused_by_name() {
    let json = r#"{"factory_id":"sleep_refused","sdk_lang":1,"sdk_version":"0.1.0",
                   "persistent":false,"sleep_when_idle":true,
                   "channels":[{"name":"in","kind":"entry","chains":[[{"link":"into","tasklet":0}]]}],
                   "tasklets":[{"name":"p0"}]}"#;
    let refusal = crate::host::register_host_class_json(json.as_bytes())
        .expect_err("refused")
        .to_string();
    assert!(refusal.contains("SleepWithoutPersistence"), "{refusal}");
    assert!(crate::operator::registry::get_class("sleep_refused").is_none());
}
