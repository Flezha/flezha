//! Parks of a frame naming a channel the relay does not declare, at rate and
//! under a process that cannot start threads: held by the one park owner,
//! never by a thread per frame, and never lost.

use super::*;
use crate::test_support::{capture_unacked_parks, passes_alone, CapturedPark};

/// Live threads of this process whose kernel name starts with `prefix`
/// (the kernel keeps 15 bytes: `flezha-unresolv`).
fn live_threads_named(prefix: &str) -> usize {
    std::fs::read_dir("/proc/self/task")
        .expect("/proc/self/task")
        .filter_map(|task| std::fs::read_to_string(task.ok()?.path().join("comm")).ok())
        .filter(|comm| comm.trim_end().starts_with(prefix))
        .count()
}

type Parks = std::sync::Mutex<Vec<CapturedPark>>;

/// The distinct parks shipped so far; a re-send repeats one.
fn shipped_ids(parks: &Parks) -> Vec<String> {
    let parks = parks.lock().expect("park capture poisoned");
    let mut ids: Vec<String> = parks.iter().map(|p| p.body.park_id.clone()).collect();
    ids.sort();
    ids.dedup();
    ids
}

/// Ack every park shipped so far and wait for `instance` to let go of them.
fn ack_all(parks: &Parks, instance: &RelayInstance) {
    for id in shipped_ids(parks) {
        crate::park::complete_ack(&id);
    }
    crate::test_support::spin_until("the parks to finish", || !instance.ladders_running());
}

/// [`passes_alone`] for the test `name` of this module: `None` inside the
/// child, its stderr in the parent once it passed.
fn in_own_process(name: &str) -> Option<String> {
    passes_alone(&format!("routing::tests::park_bound::{name}"))
}

/// Fails when: a source with one misspelled channel name, at rate, holds one
/// blocking OS thread per frame while the controller has not acked.
#[test]
fn unacked_parks_of_a_misspelled_channel_hold_a_bounded_number_of_threads() {
    const FRAMES: usize = 2_000;
    const BOUND: usize = 64;
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    let parks = capture_unacked_parks();
    let instance = host_class::instance("kb278_bound", &["prices"], &[], &[1]);

    for i in 0..FRAMES {
        feed(&instance, Some("pricse"), format!("frame-{i}").as_bytes());
    }
    crate::test_support::spin_until("every park to ship once", || {
        shipped_ids(&parks).len() == FRAMES
    });
    let live = live_threads_named("flezha-unresolv");
    ack_all(&parks, &instance);
    state_sink::reset_for_test();

    assert!(
        live <= BOUND,
        "{live} park threads alive for {FRAMES} unacked frames; parks must be bounded"
    );
}

/// Lower this process's thread limit to none: clone(2) fails with EAGAIN.
/// The limit in force before comes back.
fn no_more_threads() -> libc::rlimit {
    let mut saved = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    unsafe { libc::getrlimit(libc::RLIMIT_NPROC, &mut saved) };
    let none = libc::rlimit {
        rlim_cur: 0,
        rlim_max: saved.rlim_max,
    };
    unsafe { libc::setrlimit(libc::RLIMIT_NPROC, &none) };
    saved
}

/// Fails when: a park handed off while the process cannot start threads is
/// printed and lost, or passes without waiting for the owner to start.
#[test]
fn a_park_whose_thread_cannot_spawn_is_not_lost() {
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("skipped: RLIMIT_NPROC does not bind root");
        return;
    }
    let Some(stderr) = in_own_process("a_park_whose_thread_cannot_spawn_is_not_lost") else {
        let _guard = state_sink::test_guard();
        state_sink::reset_for_test();
        let parks = capture_parks();
        let (cue, cued) = std::sync::mpsc::channel::<Arc<RelayInstance>>();
        let feeder = std::thread::spawn(move || {
            let instance = cued.recv().expect("an instance to feed");
            feed(&instance, Some("pricse"), b"the only copy");
        });
        // The flusher is every runner's thread; the park owner is not yet.
        crate::flusher::CellFlusher::instance();
        let saved = no_more_threads();
        // The first instance of this process cannot start the park owner.
        let instance = host_class::instance_of("kb278_spawn", &["prices"], &[], &[], false);
        cue.send(Arc::clone(&instance)).expect("the feeder waits");
        crate::test_support::spin_until("the hand-off to wait for the owner", || {
            crate::park::PARK_STALLS_TOTAL.load(std::sync::atomic::Ordering::Relaxed) == 1
        });
        unsafe { libc::setrlimit(libc::RLIMIT_NPROC, &saved) };

        feeder
            .join()
            .expect("the hand-off goes on once the owner starts");
        crate::test_support::spin_until("the park to ship", || {
            parks.lock().expect("park capture poisoned").len() == 1
        });
        crate::test_support::spin_until("the park to finish", || !instance.ladders_running());
        state_sink::reset_for_test();
        return;
    };
    assert_eq!(
        stderr.matches("the park owner cannot start yet").count(),
        1,
        "one line for the wait:\n{stderr}"
    );
    assert!(
        !stderr.contains("failed to spawn"),
        "no park needs a thread of its own:\n{stderr}"
    );
}

/// Fails when: a ladder whose re-attempt finds its consumer's mailbox full
/// holds up another relay's park until that consumer drains.
#[test]
fn a_ladder_into_a_full_mailbox_does_not_hold_up_another_relays_park() {
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    let parks = capture_parks();
    let (blocked, _producer, consumer) = tier_edge("kb278_full_consumer");
    let stream = dead_stream();
    let first = make_cell_payload(&host_value(b"first"), &stream, None, false);
    crate::test_support::saturate(&consumer.mailbox, CellItem::Input(first));
    let channel = (blocked.topology.channel(1)).expect("the inter-tasklet channel");
    crate::retry::hand_edge_ladder(
        &blocked,
        channel,
        &channel.consumers[0],
        &host_value(b"retried"),
        &crate::routing::Delivery {
            stream: &stream,
            consumed_offset: None,
            call: None,
            replay: false,
        },
        "the first attempt's failure".to_string(),
    );
    let blocked_pushes =
        || (consumer.counters().pushes_blocked_total).load(std::sync::atomic::Ordering::Relaxed);
    crate::test_support::spin_until("the re-attempt to find no room", || blocked_pushes() > 0);

    let other = host_class::instance("kb278_other_relay", &["prices"], &[], &[1]);
    feed(&other, Some("pricse"), b"another relay's frame");
    crate::test_support::spin_until("the other relay's park to ship", || {
        parks.lock().expect("park capture poisoned").len() == 1
    });

    consumer.mailbox.pop().expect("the first item");
    crate::test_support::spin_until("the retried item to land", || {
        !blocked.ladders_running() && !other.ladders_running()
    });
    assert_eq!(
        consumer.mailbox.len(),
        1,
        "the retried item is delivered, not lost"
    );
    state_sink::reset_for_test();
}

/// Fails when: a hand-off past the owner's bound is dropped, or passes
/// without waiting, or waits without a count and a line saying so.
#[test]
fn a_hand_off_past_the_bound_waits_for_a_free_slot_and_says_so() {
    let Some(stderr) =
        in_own_process("a_hand_off_past_the_bound_waits_for_a_free_slot_and_says_so")
    else {
        let _guard = state_sink::test_guard();
        state_sink::reset_for_test();
        crate::park::set_pending_bound(2);
        let parks = capture_unacked_parks();
        let instance = host_class::instance("kb278_full", &["prices"], &[], &[1]);
        let fed = Arc::clone(&instance);
        let feeder = std::thread::spawn(move || {
            for i in 0..3 {
                feed(&fed, Some("pricse"), format!("frame-{i}").as_bytes());
            }
        });

        crate::test_support::spin_until("the third hand-off to stall", || {
            crate::park::PARK_STALLS_TOTAL.load(std::sync::atomic::Ordering::Relaxed) == 1
        });
        crate::test_support::spin_until("the first two parks to ship", || {
            shipped_ids(&parks).len() == 2
        });
        assert!(
            !feeder.is_finished(),
            "the third frame's reader waits while nothing is acked"
        );
        crate::park::complete_ack(&shipped_ids(&parks)[0]);
        feeder
            .join()
            .expect("the reader goes on once a slot is free");
        crate::test_support::spin_until("the third park to ship", || {
            shipped_ids(&parks).len() == 3
        });
        ack_all(&parks, &instance);
        state_sink::reset_for_test();
        return;
    };
    assert_eq!(
        stderr.matches("the park owner is full").count(),
        1,
        "one line per stall:\n{stderr}"
    );
}

/// Fails when: a ladder that once waited on a host's answer keeps its park
/// outside the bound, so unacked parks grow past it.
#[test]
fn a_park_whose_ladder_once_waited_counts_against_the_bound() {
    let Some(stderr) = in_own_process("a_park_whose_ladder_once_waited_counts_against_the_bound")
    else {
        let _guard = state_sink::test_guard();
        state_sink::reset_for_test();
        crate::park::set_pending_bound(2);
        let parks = capture_unacked_parks();
        let instance = host_class::instance("kb278_waited", &["prices"], &[], &[1]);
        let channel = (instance.topology.channel(0)).expect("the entry channel");
        let mut looks = 0;
        // A host's answer that is not there on the first look, then a failure.
        let step = move || {
            looks += 1;
            match looks {
                1 => crate::retry::Attempt::Pending,
                _ => crate::retry::Attempt::Failed("the host refused".to_string()),
            }
        };
        crate::retry::hand_ladder(
            &instance,
            "prepare for channel `prices`".to_string(),
            crate::retry::ParkTarget::entry(channel),
            (None, None, None),
            "the first crossing's failure".to_string(),
            step,
            None,
        );
        crate::test_support::spin_until("the waited ladder's park to ship", || {
            shipped_ids(&parks).len() == 1
        });
        let fed = Arc::clone(&instance);
        let feeder = std::thread::spawn(move || {
            for i in 0..2 {
                feed(&fed, Some("pricse"), format!("frame-{i}").as_bytes());
            }
        });
        crate::test_support::spin_until("the second frame to stall", || {
            crate::park::PARK_STALLS_TOTAL.load(std::sync::atomic::Ordering::Relaxed) == 1
        });
        assert!(!feeder.is_finished(), "the bound counts the waited park");
        crate::park::complete_ack(&shipped_ids(&parks)[0]);
        feeder
            .join()
            .expect("the reader goes on once a slot is free");
        crate::test_support::spin_until("both frames to ship", || shipped_ids(&parks).len() == 3);
        ack_all(&parks, &instance);
        state_sink::reset_for_test();
        return;
    };
    assert_eq!(
        stderr.matches("the park owner is full").count(),
        1,
        "{stderr}"
    );
}
