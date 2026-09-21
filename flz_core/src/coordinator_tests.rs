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

#[cfg(unix)]
mod workflow_output_tests {
    use std::io::Read;
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    use super::*;
    use crate::flusher::SendFrameFn;
    use crate::state_sink;

    const OPERATIVE: &str = "op-workflow-emit";
    const BODY: &[u8] = b"the workflow's only output";

    /// The stream map a runner holds between a closed secondary and the
    /// rebind that replaces it.
    fn no_streams() -> RwLock<HashMap<u32, FlezhaStream>> {
        RwLock::new(HashMap::new())
    }

    /// One stream whose peer is gone: every write on it fails.
    fn dead_stream_map() -> RwLock<HashMap<u32, FlezhaStream>> {
        let (near, far) = UnixStream::pair().expect("a socket pair");
        drop(far);
        RwLock::new(HashMap::from([(0u32, FlezhaStream::Unix(near))]))
    }

    #[test]
    fn a_workflow_output_with_no_secondary_is_held_not_dropped() {
        let _g = state_sink::test_guard();
        state_sink::reset_for_test();

        write_workflow_output(&no_streams(), OPERATIVE, BODY);

        assert_eq!(
            state_sink::ring_len_for_test(),
            1,
            "a workflow output with no stream to take it must be held, not printed and dropped"
        );
        state_sink::reset_for_test();
    }

    #[test]
    fn a_workflow_output_whose_write_fails_is_held_not_dropped() {
        let _g = state_sink::test_guard();
        state_sink::reset_for_test();

        write_workflow_output(&dead_stream_map(), OPERATIVE, BODY);

        assert_eq!(
            state_sink::ring_len_for_test(),
            1,
            "a workflow output the wire refused must be held for the reconnect"
        );
        state_sink::reset_for_test();
    }

    /// The ring replays a record by re-framing it with empty metadata, so a
    /// held output must be the framed bytes — otherwise a stamped output comes
    /// back stripped of the identity it was framed with.
    #[test]
    fn a_held_workflow_output_replays_exactly_as_it_was_framed() {
        let _g = state_sink::test_guard();
        state_sink::reset_for_test();

        let expected = payloader::build_without_metadata(OPERATIVE, BODY);
        write_workflow_output(&no_streams(), OPERATIVE, BODY);

        let (near, mut far) = UnixStream::pair().expect("a socket pair");
        far.set_read_timeout(Some(Duration::from_secs(2)))
            .expect("a read deadline, so a lost frame fails instead of hanging");
        state_sink::set_secondary_stream(Some(Arc::new(FlezhaStream::Unix(near))));
        let sink: SendFrameFn = Arc::new(|_payload_id: &str, _frame: &[u8]| Ok(()));
        state_sink::install_global_sink(sink);

        let mut seen = vec![0u8; expected.len()];
        far.read_exact(&mut seen)
            .expect("the held output must reach the stream the reconnect wires");
        assert_eq!(seen, expected, "replayed exactly as it was framed");
        assert_eq!(state_sink::ring_len_for_test(), 0, "the ring is drained");

        state_sink::set_secondary_stream(None);
        state_sink::reset_for_test();
    }
}
