//! The governor's tests: its answers as a host's own supervisor reads them,
//! and the whole pool over real threads.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::*;
use crate::cell::CellItem;
use crate::poll_core::tests::fixtures::{build_slot, wait_until};
use crate::test_support::push_input;

/// What a test pool's workers share: a gate that holds input `0` in "user
/// code" while it is up, and the inputs served — the two halves of "one cell
/// stuck in user code must not stop another cell's work".
#[derive(Default)]
struct Work {
    gate: AtomicBool,
    gated: AtomicBool,
    served: AtomicUsize,
}

impl Work {
    fn run(&self, core: &PollCore, act: EngineActivation) {
        let gated = act.items().iter().any(|item| {
            matches!(&item.item, CellItem::Input(p) if p.value.downcast_ref::<usize>() == Some(&0))
        });
        while gated && self.gate.load(Ordering::Acquire) {
            self.gated.store(true, Ordering::Release);
            std::thread::yield_now();
        }
        self.served.fetch_add(act.items().len(), Ordering::Release);
        core.complete(act.token);
    }
}

struct Pool {
    governor: Arc<Governor>,
    core: &'static PollCore,
    work: Arc<Work>,
}

impl Pool {
    fn live(&self) -> usize {
        self.governor.live.load(Ordering::Acquire)
    }
}

/// A private core with its own pool, so a case never observes (or disturbs)
/// another's. Leaked because a pool's threads outlive the case that started
/// them; `retire_ms` stands in for the production streak, since what is under
/// test is the retirement, not its duration.
fn private_pool(base: usize, ceiling: usize, retire_ms: u64) -> Pool {
    let core: &'static PollCore = Box::leak(Box::new(PollCore::new()));
    let governor = Governor::new(base, ceiling, Duration::from_millis(retire_ms));
    let work = Arc::new(Work::default());
    let (pool, shared) = (Arc::clone(&governor), Arc::clone(&work));
    governor.start(core, move |grown| {
        let (pool, work) = (Arc::clone(&pool), Arc::clone(&shared));
        std::thread::Builder::new()
            .spawn(move || pool.work(core, grown, |act| work.run(core, act)))
            .map(|_| ())
    });
    Pool {
        governor,
        core,
        work,
    }
}

/// Growth is measured against the LIVE count: the ceiling holds exactly, and
/// a departed worker frees the headroom it held instead of pushing the
/// ceiling permanently out of reach.
#[test]
fn supervise_grows_to_the_ceiling_and_a_departure_frees_headroom() {
    let core = PollCore::new();
    let governor = Governor::new(1, 2, RETIRE_AFTER);
    // Queued work and nobody inside `poll`: every wait observes starvation.
    let (_i, slot) = build_slot("poll-pool-ceiling");
    push_input(&core, &slot, 1);

    assert_eq!(governor.supervise(&core), Supervise::Spawn { grown: true });
    assert_eq!(
        governor.supervise(&core),
        Supervise::Idle,
        "the ceiling must hold exactly"
    );
    governor.left();
    assert_eq!(
        governor.supervise(&core),
        Supervise::Spawn { grown: true },
        "a departed worker's seat is headroom again"
    );
    core.shutdown();
    assert_eq!(governor.supervise(&core), Supervise::Stop);
}

/// A shrunken pool that still keeps up starves nobody, so no starvation
/// signal would ever replace a dead worker: the supervisor restores the base
/// on its own, every missing seat back to back rather than one per wait.
#[test]
fn supervise_restores_a_short_pool_without_waiting_between_seats() {
    let core = PollCore::new();
    let governor = Governor::new(2, 8, RETIRE_AFTER);
    governor.left();
    governor.left();

    assert_eq!(governor.supervise(&core), Supervise::Spawn { grown: false });
    let started = Instant::now();
    assert_eq!(governor.supervise(&core), Supervise::Spawn { grown: false });
    assert!(
        started.elapsed() < SUPERVISOR_WAIT / 2,
        "the second missing seat waited for a supervisor turn"
    );
    assert_eq!(governor.live.load(Ordering::Acquire), 2);
    assert_eq!(
        governor.supervise(&core),
        Supervise::Idle,
        "the base is whole"
    );
}

/// The whole point of the adaptive pool: with every base worker blocked in
/// user code, queued work for another cell is still served — the core reports
/// starvation, the supervisor answers with a thread — and the grown thread
/// retires once the burst is over.
#[test]
fn a_starved_pool_grows_to_serve_the_queued_work_and_shrinks_back() {
    let pool = private_pool(1, 4, 1_500);
    pool.work.gate.store(true, Ordering::Release);
    let (_ia, blocked) = build_slot("poll-pool-blocked");
    let (_ib, healthy) = build_slot("poll-pool-healthy");

    push_input(pool.core, &blocked, 0);
    wait_until("the base worker to enter user code", || {
        pool.work.gated.load(Ordering::Acquire)
    });
    push_input(pool.core, &healthy, 1);

    wait_until("the queued work to be served while the gate holds", || {
        pool.work.served.load(Ordering::Acquire) >= 1
    });
    assert!(
        pool.work.gate.load(Ordering::Acquire),
        "the healthy cell was served only after the blocked one finished"
    );
    assert!(pool.live() > 1, "starvation did not grow the pool");

    pool.work.gate.store(false, Ordering::Release);
    wait_until("the grown worker to retire", || pool.live() == 1);
    pool.core.shutdown();
}

/// The accounting has to survive an unwind, because that is exactly the exit
/// a trailing release would skip: a worker gone but still counted keeps a seat
/// in the ceiling and hides its own death from the supervisor.
#[test]
fn a_worker_that_unwinds_frees_its_seat() {
    let governor = Governor::new(1, 4, RETIRE_AFTER);
    assert!(governor.reserve(governor.ceiling));

    let claim = Arc::clone(&governor);
    let died = std::thread::spawn(move || {
        let _seat = Seat(&claim);
        panic!("a poll worker died");
    })
    .join();

    assert!(died.is_err(), "the worker really did unwind");
    assert_eq!(
        governor.live.load(Ordering::Acquire),
        1,
        "an unwinding worker must release its seat"
    );
}

/// The replacement a top-up spawns has to actually fetch.
#[test]
fn a_short_pool_is_topped_back_up_to_base_and_serves_again() {
    let pool = private_pool(2, 8, 300);
    wait_until("the base pool to come up", || pool.live() == 2);

    // A worker died: its seat is free and the count is short.
    pool.governor.left();
    wait_until("the supervisor to replace the dead worker", || {
        pool.live() == 2
    });

    let (_i, slot) = build_slot("poll-pool-replaced");
    let before = pool.work.served.load(Ordering::Acquire);
    push_input(pool.core, &slot, 1);
    wait_until("the replacement worker to serve work", || {
        pool.work.served.load(Ordering::Acquire) > before
    });
    pool.core.shutdown();
}

/// A workload that never starves observes exactly the base pool: work is fed
/// one round at a time, so a worker is always inside `poll` and the
/// supervisor has nothing to answer.
#[test]
fn a_never_starved_run_keeps_exactly_the_base_pool() {
    let pool = private_pool(2, 8, 300);
    let (_i, slot) = build_slot("poll-pool-steady");

    for i in 1..=20 {
        push_input(pool.core, &slot, i);
        wait_until("the round to complete", || {
            pool.work.served.load(Ordering::Acquire) >= i
        });
    }
    assert_eq!(pool.live(), 2, "a healthy run grew threads it did not need");
    pool.core.shutdown();
}
