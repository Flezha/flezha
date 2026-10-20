//! The host's growable pool of native threads, and nothing else.
//!
//! Every ABI call that can park runs here, above all `complete`: the pool must
//! be able to grow or the event-loop rule only moves the deadlock one level
//! out — with a fixed pool of N threads all parked pushing to consumer C, the
//! completion that would drain C has no thread to run on.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use flz_core::poll_pool::Governor;

use super::DEFAULT_FETCHERS;

/// `(live, busy, peak, queued)` — the pool's own bookkeeping, which is what
/// both `settle` and `hostStats` report on.
pub(crate) type Snapshot = (u32, u32, u32, u32);

type Job = Box<dyn FnOnce() + Send + 'static>;

struct PoolState {
    queue: VecDeque<Job>,
    /// Threads currently waiting on the condvar for a job.
    idle: usize,
    live: usize,
    peak: usize,
    closing: bool,
}

/// A growable pool of native threads for every ABI call that can park.
///
/// Growth is driven by this host's own observation — "a job is queued and no
/// thread is waiting to take it" — and not by the core's starvation wait,
/// because the core cannot see this queue.
///
/// The pool must be able to grow or the event-loop rule only moves the
/// deadlock one level out: with a fixed pool of N threads all parked pushing
/// to consumer C, the completion that would drain C has no thread to run on,
/// and C is never drained.
pub(crate) struct BlockingPool {
    state: Mutex<PoolState>,
    ready: Condvar,
    /// Notified on every change to the bookkeeping above, so a caller waiting
    /// for the pool to reach a state waits rather than polls.
    changed: Condvar,
    base: usize,
    ceiling: usize,
    idle_retire: Duration,
}

impl BlockingPool {
    pub(crate) fn with_retire(base: usize, ceiling: usize, idle_retire: Duration) -> Arc<Self> {
        let base = base.max(1);
        let pool = Arc::new(Self {
            state: Mutex::new(PoolState {
                queue: VecDeque::new(),
                idle: 0,
                live: 0,
                peak: 0,
                closing: false,
            }),
            ready: Condvar::new(),
            changed: Condvar::new(),
            base,
            ceiling: ceiling.max(base),
            idle_retire,
        });
        for _ in 0..base {
            pool.spawn_worker();
        }
        pool
    }

    /// Start one thread and count it. `live` is incremented before the spawn
    /// and rolled back on failure, so a thread that never ran cannot leave the
    /// pool believing it has capacity it does not.
    fn spawn_worker(self: &Arc<Self>) {
        {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state.live += 1;
            state.peak = state.peak.max(state.live);
            self.changed.notify_all();
        }
        let me = Arc::clone(self);
        let started = std::thread::Builder::new()
            .name("flz-blocking".into())
            .spawn(move || me.worker());
        if started.is_err() {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state.live -= 1;
            self.changed.notify_all();
        }
    }

    pub(crate) fn submit(self: &Arc<Self>, job: impl FnOnce() + Send + 'static) {
        let grow = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state.queue.push_back(Box::new(job));
            self.changed.notify_all();
            state.idle < state.queue.len() && state.live < self.ceiling
        };
        if grow {
            self.spawn_worker();
        }
        self.ready.notify_one();
    }

    fn worker(self: Arc<Self>) {
        loop {
            let job = {
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                loop {
                    if let Some(job) = state.queue.pop_front() {
                        self.changed.notify_all();
                        break job;
                    }
                    if state.closing {
                        state.live -= 1;
                        self.changed.notify_all();
                        return;
                    }
                    state.idle += 1;
                    self.changed.notify_all();
                    let (next, wait) = self
                        .ready
                        .wait_timeout(state, self.idle_retire)
                        .unwrap_or_else(|p| p.into_inner());
                    state = next;
                    state.idle -= 1;
                    if wait.timed_out() && state.queue.is_empty() && state.live > self.base {
                        state.live -= 1;
                        self.changed.notify_all();
                        return;
                    }
                    self.changed.notify_all();
                }
            };
            job();
        }
    }

    pub(crate) fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.closing = true;
        self.ready.notify_all();
    }

    /// Wait until `ready` holds of the pool's own bookkeeping, or `within`
    /// passes; answers whether it held. The one place anything waits on this
    /// pool: `drain` asks for "nothing queued and nothing running", a test
    /// asks for whatever it is about to assert.
    pub(crate) fn settle(&self, within: Duration, ready: impl Fn(Snapshot) -> bool) -> bool {
        let deadline = Instant::now() + within;
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            if ready(Self::seen(&state)) {
                return true;
            }
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                return false;
            };
            state = self
                .changed
                .wait_timeout(state, left)
                .unwrap_or_else(|p| p.into_inner())
                .0;
        }
    }

    pub(crate) fn drain(&self, within: Duration) {
        self.settle(within, |(_, busy, _, queued)| busy == 0 && queued == 0);
    }

    fn seen(state: &PoolState) -> Snapshot {
        (
            state.live as u32,
            (state.live - state.idle.min(state.live)) as u32,
            state.peak as u32,
            state.queue.len() as u32,
        )
    }

    pub(crate) fn snapshot(&self) -> Snapshot {
        Self::seen(&self.state.lock().unwrap_or_else(|p| p.into_inner()))
    }
}

static POOL: OnceLock<Arc<BlockingPool>> = OnceLock::new();

/// The process's blocking pool, sized by the governor's policy rather than a
/// knob minted for Node: its base, ceiling and retire time. Its growth stays
/// its own, on a queued job, because the core cannot see this queue.
pub(crate) fn blocking_pool() -> &'static Arc<BlockingPool> {
    POOL.get_or_init(|| {
        let policy = Governor::from_env(DEFAULT_FETCHERS as usize);
        BlockingPool::with_retire(policy.base, policy.ceiling, policy.retire_after)
    })
}
#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use super::*;

    /// Wait for a condition on the pool's own bookkeeping, or fail. The pool
    /// notifies every change, so this is a wait and not a poll.
    fn until(pool: &Arc<BlockingPool>, what: &str, ready: impl Fn(Snapshot) -> bool) {
        assert!(
            pool.settle(Duration::from_secs(5), ready),
            "timed out waiting for {what}: {:?}",
            pool.snapshot()
        );
    }

    /// The deadlock this closes: with a fixed pool of N threads all parked
    /// pushing to one consumer, the completion that would drain that consumer
    /// has no thread to run on. Growth is what makes the queued one runnable.
    #[test]
    fn a_blocked_pool_grows_so_a_queued_completion_still_runs() {
        let pool = BlockingPool::with_retire(2, 8, Duration::from_millis(50));
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let release_rx = Arc::new(Mutex::new(release_rx));
        let (ran_tx, ran_rx) = mpsc::channel::<u32>();

        // Occupy every base thread with a job that parks, exactly as a push
        // into a full mailbox parks.
        for _ in 0..2 {
            let rx = Arc::clone(&release_rx);
            pool.submit(move || {
                let _ = rx.lock().unwrap().recv();
            });
        }
        until(&pool, "the base threads to be busy", |(_, busy, _, _)| {
            busy == 2
        });

        // A further completion is queued with no free thread. A fixed pool
        // would leave it queued until one of the parked pushes returned —
        // which is what the parked pushes are waiting for.
        let ran = ran_tx.clone();
        pool.submit(move || {
            let _ = ran.send(7);
        });
        assert_eq!(
            ran_rx.recv_timeout(Duration::from_secs(5)),
            Ok(7),
            "the queued completion never ran, so the pool did not grow"
        );
        assert!(pool.snapshot().0 > 2, "the pool grew past its base");

        let _ = release_tx.send(());
        let _ = release_tx.send(());
        pool.drain(Duration::from_secs(5));
        pool.close();
    }

    /// And it comes back: a grown thread is not a permanent cost.
    #[test]
    fn threads_above_the_base_retire_after_an_idle_streak() {
        let pool = BlockingPool::with_retire(1, 8, Duration::from_millis(50));
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let release_rx = Arc::new(Mutex::new(release_rx));
        for _ in 0..4 {
            let rx = Arc::clone(&release_rx);
            pool.submit(move || {
                let _ = rx.lock().unwrap().recv();
            });
        }
        until(&pool, "the pool to grow", |(size, _, _, _)| size >= 4);

        for _ in 0..4 {
            let _ = release_tx.send(());
        }
        until(&pool, "the pool to return to base", |(size, _, _, _)| {
            size == 1
        });
        pool.close();
    }

    /// The ceiling is a ceiling. A sustained wedge must not spawn threads
    /// without bound; the supervisor's latched warning is the report.
    #[test]
    fn growth_stops_at_the_ceiling() {
        let pool = BlockingPool::with_retire(1, 3, Duration::from_millis(50));
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let release_rx = Arc::new(Mutex::new(release_rx));
        for _ in 0..10 {
            let rx = Arc::clone(&release_rx);
            pool.submit(move || {
                let _ = rx.lock().unwrap().recv();
            });
        }
        until(&pool, "the pool to reach its ceiling", |(size, _, _, _)| {
            size == 3
        });
        // Watched, not waited out: the ceiling holds for as long as it is observed.
        assert!(
            !flz_core::test_support::wait_until(Duration::from_millis(100), Duration::ZERO, || {
                pool.snapshot().0 != 3
            }),
            "the pool grew past its ceiling"
        );
        for _ in 0..10 {
            let _ = release_tx.send(());
        }
        pool.drain(Duration::from_secs(5));
        pool.close();
    }
}
