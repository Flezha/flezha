//! The coordinator's tests: the heartbeat's self-report and the ladder a
//! heartbeat that reaches nothing arms, and the workflow output a runner
//! holds while it has no secondary.

use super::*;

/// The whole key set, pinned. The controller's gate refuses a key it
/// holds no slot for, which catches an addition; nothing there catches a
/// removal — the mirror simply stops being fed — so the set is written
/// out here, in the crate that builds it.
#[test]
fn the_heartbeat_builds_exactly_the_contracted_keys() {
    let mut keys: Vec<String> = super::heartbeat_metadata().into_keys().collect();
    keys.sort();

    let mut expected = vec![
        "blocked_parks",
        "credit_parked",
        "dispatches",
        "drops_newest",
        "drops_oldest",
        "inflight",
        "inflight_oldest_ms",
        "pending_push_oldest_ms",
        "pending_pushes",
        "restore_discarded_inputs",
        "timers_armed",
        "timers_fired",
    ];
    expected.sort_unstable();

    assert_eq!(
        keys, expected,
        "the heartbeat's key set is a cross-SDK contract"
    );
}

/// A rename silently blinds the controller, and a counter that reports a
/// stale value blinds it just as thoroughly.
#[test]
fn the_heartbeat_carries_every_overflow_counter() {
    let before = super::heartbeat_metadata();
    for key in [
        "blocked_parks",
        "drops_newest",
        "drops_oldest",
        "pending_pushes",
        "pending_push_oldest_ms",
    ] {
        assert!(before.contains_key(key), "the heartbeat must carry {key}");
    }

    crate::cell::PUSHES_BLOCKED_TOTAL.fetch_add(3, std::sync::atomic::Ordering::Relaxed);
    crate::cell::DROPS_NEWEST_TOTAL.fetch_add(5, std::sync::atomic::Ordering::Relaxed);
    crate::cell::DROPS_OLDEST_TOTAL.fetch_add(7, std::sync::atomic::Ordering::Relaxed);
    let after = super::heartbeat_metadata();

    for (key, bumped) in [
        ("blocked_parks", 3u64),
        ("drops_newest", 5),
        ("drops_oldest", 7),
    ] {
        let read = |m: &HashMap<String, String>| m[key].parse::<u64>().expect("an integer");
        assert_eq!(
            read(&after) - read(&before),
            bumped,
            "{key} must report what the core counted"
        );
    }
}

/// An idle Operative's heartbeat is its only outbound signal; one that
/// reaches no stream must ask for a reconnect, or the stall is silent.
#[test]
fn heartbeat_reaching_no_stream_arms_the_reconnect_ladder() {
    let _g = crate::state_sink::test_guard();
    std::env::set_var("FLEZHA_BROKEN_PIPE_RECONNECT_THRESHOLD", "3");
    std::env::set_var("FLEZHA_BROKEN_PIPE_WINDOW_MS", "60000");
    crate::state_sink::reset_for_test();

    // The signature of a dead secondary: the stream map is empty, so every
    // heartbeat write is a no-op over zero streams.
    let empty: RwLock<HashMap<u32, FlezhaStream>> = RwLock::new(HashMap::new());
    heartbeat_tick(&empty);
    heartbeat_tick(&empty);
    heartbeat_tick(&empty);

    assert!(
        crate::state_sink::reconnect_requested_for_test(),
        "heartbeats that reach no stream must arm the reconnect ladder"
    );

    std::env::remove_var("FLEZHA_BROKEN_PIPE_RECONNECT_THRESHOLD");
    std::env::remove_var("FLEZHA_BROKEN_PIPE_WINDOW_MS");
    crate::state_sink::reset_for_test();
}

/// A removed workflow's Cells leave the flusher with it: an entry holds its
/// Cell's buffer, so one left behind is drained for the life of the process.
#[test]
fn a_stopped_workflow_leaves_no_cell_with_the_flusher() {
    let _env = crate::runner::env_test_guard();
    let factory = format!("flusher_stop_{}", std::process::id());
    let class = crate::test_support::host_class::workflow_json(&factory, &["in"], 1)
        .replace(r#""persistent":false"#, r#""persistent":true"#);
    crate::host::register_host_class_json(class.as_bytes()).expect("class registers");
    let mut node = RunnerNode::new("test-ns".to_string());
    let id = format!("{factory}_inst");
    let errors = crate::test_support::host_class::bind(&mut node, &factory, &id, "", 0);
    assert!(errors.is_empty(), "{errors:?}");
    let flusher = crate::flusher::CellFlusher::instance();
    assert!(flusher.holds_class(&id), "its Cell was registered");

    assert!(node.stop_operative(&id));

    assert!(!flusher.holds_class(&id), "its Cell left with it");
}
