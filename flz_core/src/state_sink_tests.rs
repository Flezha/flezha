use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use super::{reset_for_test as reset_state, *};
use crate::flusher::SendError;

/// A sink that records every frame; `fail` flips it to `TransportDown`.
fn recording_sink(fail: Arc<AtomicBool>) -> (SendFrameFn, Arc<StdMutex<Vec<Vec<u8>>>>) {
    let seen = Arc::new(StdMutex::new(Vec::new()));
    let seen_c = Arc::clone(&seen);
    let f: SendFrameFn = Arc::new(move |_payload_id: &str, frame: &[u8]| {
        if fail.load(Ordering::SeqCst) {
            Err(SendError::TransportDown("test".into()))
        } else {
            seen_c.lock().unwrap().push(frame.to_vec());
            Ok(())
        }
    });
    (f, seen)
}

const PID: &str = super::CELL_STATE_CHANGE_PAYLOAD_ID;

/// A stream whose peer is gone, so every write to it refuses. A pipe takes
/// EPIPE on the write and not on the close, so one write lands in the void.
fn dead_stream() -> Arc<FlezhaStream> {
    let (stream, ctrl_end) = crate::test_support::stream_pair();
    drop(ctrl_end);
    let mut w: &FlezhaStream = &stream;
    let _ = w.write_all(b"gone");
    stream
}

/// Held, and the install replays it onto the live secondary.
#[test]
fn a_durable_frame_the_wire_refuses_is_held_and_replayed() {
    let _g = super::test_guard();
    reset_state();

    let stream = dead_stream();
    let held = write(Class::Durable, Some(&stream), b"rec".to_vec());
    assert!(matches!(held, Verdict::Held));
    assert_eq!(ring_len_for_test(), 1);

    // The framed entry replays onto the live secondary, not through the
    // sink that would re-frame it.
    let (live, mut peer) = crate::test_support::stream_pair();
    set_secondary_stream(Some(Arc::clone(&live)));
    let (sink, _seen) = recording_sink(Arc::new(AtomicBool::new(false)));
    install_global_sink(sink);

    let mut got = [0u8; 3];
    peer.read_exact(&mut got).expect("the held frame replays");
    assert_eq!(&got, b"rec");
    assert_eq!(ring_len_for_test(), 0);
    set_secondary_stream(None);
    reset_state();
}

/// Nothing bound is answered exactly as a refusal is: the class answers the
/// fact, not the shape of the failure.
#[test]
fn a_durable_frame_with_no_stream_bound_is_held() {
    let _g = super::test_guard();
    reset_state();

    assert!(matches!(
        write(Class::Durable, None, b"rec".to_vec()),
        Verdict::Held
    ));
    assert_eq!(ring_len_for_test(), 1);
    reset_state();
}

/// The bytes that come back are the bytes that went in: a re-framed record
/// would carry empty metadata and strip a keyed output of its stamps.
#[test]
fn an_output_the_wire_refuses_is_held_already_framed() {
    let _g = super::test_guard();
    reset_state();

    let framed = flz_transport::service::payloader::build_from_binary(
        "op-1",
        b"body",
        std::collections::HashMap::from([("flz.key".to_string(), "k".to_string())]),
    )
    .unwrap();
    assert!(matches!(
        write(Class::Output, Some(&dead_stream()), framed.clone()),
        Verdict::Held
    ));

    let (live, mut peer) = crate::test_support::stream_pair();
    set_secondary_stream(Some(Arc::clone(&live)));
    let (sink, _seen) = recording_sink(Arc::new(AtomicBool::new(false)));
    install_global_sink(sink);

    let mut got = vec![0u8; framed.len()];
    peer.read_exact(&mut got).expect("the held output replays");
    assert_eq!(got, framed, "the replay ships the bytes the emit framed");
    set_secondary_stream(None);
    reset_state();
}

/// Neither holds anything: a re-driven reply is a stale answer, and a
/// replayed heartbeat reports a moment that has passed.
#[test]
fn a_control_frame_and_a_probe_are_refused_rather_than_held() {
    let _g = super::test_guard();
    reset_state();

    for class in [Class::Control, Class::Probe, Class::Preview] {
        let stream = dead_stream();
        let verdict = write(class, Some(&stream), b"c".to_vec());
        assert!(
            matches!(verdict, Verdict::Refused(_)),
            "{class:?} must refuse to its caller, answered {verdict:?}"
        );
        assert!(verdict.refused().is_err());
        assert_eq!(ring_len_for_test(), 0, "{class:?} must hold nothing");
    }
    reset_state();
}

/// Fails as: the controller reads a torn header off a data stream several
/// threads write, closes it as `frame_too_large`, and the runner reconnects.
#[test]
fn concurrent_writers_never_tear_a_frame() {
    let _g = super::test_guard();
    reset_state();

    const FRAMES: usize = 16;
    const BODY: usize = 512 * 1024;
    let (stream, ctrl_end) = crate::test_support::stream_pair();
    ctrl_end
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    let reader = std::thread::spawn(move || {
        for _ in 0..FRAMES * 2 {
            let (_, body, _) = flz_transport::service::payloader::extract_payload(&ctrl_end)
                .expect("a whole frame");
            assert!(
                body.len() == BODY && body.iter().all(|b| *b == body[0]),
                "a frame arrived with another writer's bytes inside it"
            );
        }
    });
    // One writer on a cloned handle: a clone is the same socket, so the same lock.
    let handles = [
        Arc::clone(&stream),
        Arc::new(stream.clone_handle().unwrap()),
    ];
    let writers: Vec<_> = [0xAA_u8, 0xBB]
        .into_iter()
        .zip(handles)
        .map(|(fill, stream)| {
            std::thread::spawn(move || {
                for _ in 0..FRAMES {
                    let frame = flz_transport::service::payloader::build_without_metadata(
                        "output",
                        &vec![fill; BODY],
                    );
                    write(Class::Output, Some(&stream), frame);
                }
            })
        })
        .collect();

    let read = reader.join();
    writers.into_iter().for_each(|w| w.join().unwrap());
    reset_state();
    assert!(
        read.is_ok(),
        "the controller's side could not parse the stream"
    );
}

#[test]
fn frames_with_no_sink_buffer_and_replay_on_install() {
    let _g = super::test_guard();
    reset_state();

    send(PID, b"a".to_vec());
    send(PID, b"b".to_vec());

    let (sink, seen) = recording_sink(Arc::new(AtomicBool::new(false)));
    install_global_sink(sink);

    assert_eq!(
        *seen.lock().unwrap(),
        vec![b"a".to_vec(), b"b".to_vec()],
        "ring replays in order"
    );
    reset_state();
}

#[test]
fn live_sink_sends_directly_without_buffering() {
    let _g = super::test_guard();
    reset_state();

    let (sink, seen) = recording_sink(Arc::new(AtomicBool::new(false)));
    install_global_sink(sink);
    send(PID, b"x".to_vec());

    assert_eq!(*seen.lock().unwrap(), vec![b"x".to_vec()]);
    assert!(
        STATE.lock().unwrap().ring.is_empty(),
        "no buffering on success"
    );
    reset_state();
}

#[test]
fn failed_writes_buffer_then_replay_when_stream_recovers() {
    let _g = super::test_guard();
    reset_state();

    let fail = Arc::new(AtomicBool::new(true));
    let (sink, _seen) = recording_sink(Arc::clone(&fail));
    install_global_sink(sink);

    send(PID, b"1".to_vec());
    send(PID, b"2".to_vec());
    assert_eq!(STATE.lock().unwrap().ring.len(), 2);

    // Stream recovers; a fresh install replays the ring in order.
    let (sink2, seen2) = recording_sink(Arc::new(AtomicBool::new(false)));
    install_global_sink(sink2);

    assert_eq!(*seen2.lock().unwrap(), vec![b"1".to_vec(), b"2".to_vec()]);
    assert!(STATE.lock().unwrap().ring.is_empty());
    reset_state();
}

#[test]
fn ring_drops_oldest_beyond_capacity() {
    let _g = super::test_guard();
    std::env::set_var(ENV_RING_CAPACITY, "3");
    reset_state();

    for i in 0..5u8 {
        send(PID, vec![i]); // no sink → ring
    }
    let ring: Vec<Vec<u8>> = STATE
        .lock()
        .unwrap()
        .ring
        .iter()
        .map(|entry| match entry {
            Pending::Record { frame, .. } => frame.clone(),
            Pending::Framed(frame) => frame.clone(),
        })
        .collect();
    assert_eq!(
        ring,
        vec![vec![2u8], vec![3u8], vec![4u8]],
        "oldest dropped"
    );

    std::env::remove_var(ENV_RING_CAPACITY);
    reset_state();
}

/// The ring holds the frame's last copy, so an eviction is a real loss —
/// and the heartbeat's `drops_oldest` is where this wire's shedding is
/// already reported.
#[test]
fn a_ring_eviction_counts_as_a_drop_oldest_shed() {
    let _g = super::test_guard();
    std::env::set_var(ENV_RING_CAPACITY, "1");
    reset_state();

    let before = crate::cell::drops_oldest_total();
    send(PID, b"first".to_vec());
    send(PID, b"second".to_vec());

    assert!(
        crate::cell::drops_oldest_total() > before,
        "an evicted frame must reach the counter the heartbeat reports"
    );
    assert_eq!(
        STATE.lock().unwrap().evicted_since_install,
        1,
        "one eviction, reported once the stream comes back"
    );

    std::env::remove_var(ENV_RING_CAPACITY);
    reset_state();
}

#[test]
fn broken_pipe_flood_crosses_threshold_once_per_window() {
    let _g = super::test_guard();
    std::env::set_var(ENV_FLOOD_THRESHOLD, "3");
    std::env::set_var(ENV_FLOOD_WINDOW_MS, "60000");
    reset_state();

    let mut st = STATE.lock().unwrap();
    let now = Instant::now();
    assert!(!st.note_broken_pipe(now));
    assert!(!st.note_broken_pipe(now));
    assert!(st.note_broken_pipe(now), "3rd failure crosses threshold");
    // Already requested: no second trigger while that request is still
    // plausibly in flight (see the re-arm test for what happens after).
    assert!(!st.note_broken_pipe(now));
    drop(st);

    std::env::remove_var(ENV_FLOOD_THRESHOLD);
    std::env::remove_var(ENV_FLOOD_WINDOW_MS);
    reset_state();
}

#[test]
fn send_through_failing_sink_drives_flood_branch_without_trigger() {
    // Exercises send()'s flood path end-to-end: a failing sink past the
    // threshold returns `should_reconnect`, and the request_reconnect
    // call is a safe no-op when no coordinator trigger is installed.
    let _g = super::test_guard();
    std::env::set_var(ENV_FLOOD_THRESHOLD, "3");
    std::env::set_var(ENV_FLOOD_WINDOW_MS, "60000");
    reset_state();

    let (sink, _seen) = recording_sink(Arc::new(AtomicBool::new(true)));
    install_global_sink(sink);
    for i in 0..5u8 {
        send(PID, vec![i]); // every write fails → buffered + counted
    }
    // All five frames buffered (drop-oldest not reached at default cap).
    assert_eq!(STATE.lock().unwrap().ring.len(), 5);

    std::env::remove_var(ENV_FLOOD_THRESHOLD);
    std::env::remove_var(ENV_FLOOD_WINDOW_MS);
    reset_state();
}

/// A requested reconnect that never lands must not disarm the detector
/// forever: the ring would absorb every later record silently.
#[test]
fn unanswered_reconnect_request_rearms_so_the_stall_cannot_be_permanent() {
    let _g = super::test_guard();
    std::env::set_var(ENV_FLOOD_THRESHOLD, "3");
    std::env::set_var(ENV_FLOOD_WINDOW_MS, "60000");
    reset_state();

    let mut st = STATE.lock().unwrap();
    let t0 = Instant::now();
    assert!(!st.note_broken_pipe(t0));
    assert!(!st.note_broken_pipe(t0));
    assert!(st.note_broken_pipe(t0), "3rd failure requests a reconnect");

    // No `install_global_sink` follows — the reconnect never landed. Ten
    // minutes later the sink is still dead and writes are still failing.
    let much_later = t0 + Duration::from_secs(600);
    st.note_broken_pipe(much_later);
    st.note_broken_pipe(much_later);
    assert!(
        st.note_broken_pipe(much_later),
        "a still-dead sink long past the retry window must request again"
    );
    drop(st);

    std::env::remove_var(ENV_FLOOD_THRESHOLD);
    std::env::remove_var(ENV_FLOOD_WINDOW_MS);
    reset_state();
}

#[test]
fn stale_broken_pipes_age_out_of_window() {
    let _g = super::test_guard();
    std::env::set_var(ENV_FLOOD_THRESHOLD, "2");
    std::env::set_var(ENV_FLOOD_WINDOW_MS, "100");
    reset_state();

    let mut st = STATE.lock().unwrap();
    let t0 = Instant::now();
    assert!(!st.note_broken_pipe(t0));
    // A failure past the window evicts t0, so this counts as the 1st.
    let later = t0 + Duration::from_millis(200);
    assert!(
        !st.note_broken_pipe(later),
        "stale failure aged out, no flood"
    );
    drop(st);

    std::env::remove_var(ENV_FLOOD_THRESHOLD);
    std::env::remove_var(ENV_FLOOD_WINDOW_MS);
    reset_state();
}
