//! The pool governor: one policy for every host's poll threads.
//!
//! The core decides what runs and caps how much may be underway; a host only
//! brings the threads (`docs/internal/sdk/poll_core.md`), and the governor says
//! how many: a base set that never retires, plus one supervisor parked in the
//! core's starvation wait — work queued with no thread inside `poll` at all.
//! Each observation adds one thread, up to a ceiling, and the threads above the
//! base retire after an idle streak, so a workload that never starves observes
//! exactly the base pool. The supervisor is also the pool's only self-heal: a
//! shrunken-but-sufficient pool starves nobody, so it restores a pool it finds
//! short instead of waiting for a starvation signal.
//!
//! A host brings threads through a [`Spawner`] ([`Governor::start`]), or asks
//! [`Governor::supervise`] from a supervisor of its own.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::poll_core::{EngineActivation, PollCore, Polled, Starvation};

const ENV_WORKER_POOL_SIZE: &str = "FLEZHA_WORKER_POOL_SIZE";
const ENV_WORKER_POOL_MAX: &str = "FLEZHA_WORKER_POOL_MAX";
/// One poll turn: also the step of a grown worker's idle streak and how fast
/// a worker notices shutdown.
pub const POLL_TIMEOUT: Duration = Duration::from_millis(250);
/// Only bounds how often the supervisor re-arms; the core decides when its
/// wait returns.
pub const SUPERVISOR_WAIT: Duration = Duration::from_secs(1);
pub const RETIRE_AFTER: Duration = Duration::from_secs(30);
/// How long a host's shutdown waits for its workers to leave.
pub const DRAIN_BOUND: Duration = Duration::from_secs(5);

/// One supervisor decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Supervise {
    /// Spawn one worker into the seat already reserved for it; a `grown` one
    /// retires when idle. A spawn that fails gives the seat back through
    /// [`Governor::left`].
    Spawn {
        grown: bool,
    },
    Idle,
    Stop,
}

/// A host's way to start one more worker thread.
pub trait Spawner: Send + 'static {
    fn spawn(&self, grown: bool) -> std::io::Result<()>;
}

impl<F: Fn(bool) -> std::io::Result<()> + Send + 'static> Spawner for F {
    fn spawn(&self, grown: bool) -> std::io::Result<()> {
        self(grown)
    }
}

/// One poll-worker pool: born with its `base` seats reserved, which the host
/// fills with workers that never retire.
pub struct Governor {
    pub base: usize,
    pub ceiling: usize,
    /// How long a grown worker polls without work before retiring; a host's
    /// tests shorten it through [`Governor::new`].
    pub retire_after: Duration,
    live: AtomicUsize,
    stopped: AtomicBool,
    at_ceiling: AtomicBool,
    /// The last answer was a top-up: answer the next one without a wait, so a
    /// pool short by several is restored at once. Any departure clears it, so
    /// a spawn that keeps failing is retried once per wait, not in a hot loop.
    topping_up: AtomicBool,
}

impl Governor {
    /// The pool the environment asks for over `default_base`, the host's own
    /// meaning of a worker; the ceiling defaults to ten times the base.
    pub fn from_env(default_base: usize) -> Arc<Self> {
        let base = flz_transport::env::env_parse_positive(ENV_WORKER_POOL_SIZE, default_base);
        let ceiling = flz_transport::env::env_parse_positive(ENV_WORKER_POOL_MAX, base * 10);
        Self::new(base, ceiling, RETIRE_AFTER)
    }

    /// The ceiling is never below the base.
    pub fn new(base: usize, ceiling: usize, retire_after: Duration) -> Arc<Self> {
        Arc::new(Self {
            base,
            ceiling: ceiling.max(base),
            retire_after,
            live: AtomicUsize::new(base),
            stopped: AtomicBool::new(false),
            at_ceiling: AtomicBool::new(false),
            topping_up: AtomicBool::new(false),
        })
    }

    /// Spawn the base workers, then supervise, on a thread of its own: the
    /// whole pool for a host whose workers are threads it can spawn, started
    /// the same way from a thread that may not spawn them itself.
    pub fn start(self: &Arc<Self>, core: &'static PollCore, spawner: impl Spawner) {
        let governor = Arc::clone(self);
        std::thread::Builder::new()
            .name("flz-poll-supervisor".to_string())
            .spawn(move || {
                for _ in 0..governor.base {
                    governor.spawn_or_leave(&spawner, false);
                }
                loop {
                    match governor.supervise(core) {
                        Supervise::Spawn { grown } => governor.spawn_or_leave(&spawner, grown),
                        Supervise::Idle => {}
                        Supervise::Stop => return,
                    }
                }
            })
            .expect("spawn poll supervisor");
    }

    // A host that stopped the pool before refusing the spawn ended the pool:
    // that refusal is its stop, not a failure to report.
    fn spawn_or_leave(&self, spawner: &impl Spawner, grown: bool) {
        if let Err(e) = spawner.spawn(grown) {
            self.left();
            if !self.stopped() {
                flz_transport::notice!(Warn, "could not spawn a poll worker: {e}");
            }
        }
    }

    /// The pool's one control decision, at most [`SUPERVISOR_WAIT`] away.
    ///
    /// A pool gone SHORT is restored first: a worker that left released its
    /// seat, and nothing else would bring it back. Then every starvation
    /// observation means work is queued with nobody inside `poll` to take it,
    /// so the answer is one more thread. Growth is self-limiting without a
    /// settle here: the spawned worker's own `poll` entry restarts the core's
    /// dwell, so the next thread is at least a dwell away and only if the
    /// runner is still stuck.
    pub fn supervise(&self, core: &PollCore) -> Supervise {
        if self.stopped() || core.is_shut_down() {
            return Supervise::Stop;
        }
        if self.topping_up.swap(false, Ordering::AcqRel) {
            if let Some(answer) = self.top_up() {
                return answer;
            }
        }
        let starvation = core.wait_starvation(SUPERVISOR_WAIT);
        if matches!(starvation, Starvation::ShuttingDown) || self.stopped() {
            return Supervise::Stop;
        }
        if let Some(answer) = self.top_up() {
            return answer;
        }
        if matches!(starvation, Starvation::Observed) {
            if self.reserve(self.ceiling) {
                self.at_ceiling.store(false, Ordering::Release);
                return Supervise::Spawn { grown: true };
            }
            // Latched: a sustained wedge would say this once a second forever.
            if !self.at_ceiling.swap(true, Ordering::AcqRel) {
                flz_transport::notice!(
                    Warn,
                    "the core is starving and the worker pool is at its ceiling of {}; raise \
                     FLEZHA_WORKER_POOL_MAX, or find what is holding the handlers",
                    self.ceiling
                );
            }
        }
        Supervise::Idle
    }

    /// One replacement if the pool is below `base`. Each is logged: a pool
    /// respawning into a repeating crash is a hot loop, and the log is what
    /// makes it visible.
    fn top_up(&self) -> Option<Supervise> {
        if !self.reserve(self.base) {
            return None;
        }
        self.topping_up.store(true, Ordering::Release);
        flz_transport::notice!(
            Warn,
            "replaced a poll worker that left a pool of base {}",
            self.base
        );
        Some(Supervise::Spawn { grown: false })
    }

    /// Reserve a seat below `limit`. The seat is taken BEFORE the spawn
    /// because a retiring worker may be leaving concurrently and the ceiling
    /// has to hold exactly.
    fn reserve(&self, limit: usize) -> bool {
        self.live
            .try_update(Ordering::AcqRel, Ordering::Acquire, |live| {
                (live < limit).then_some(live + 1)
            })
            .is_ok()
    }

    /// A worker's turn after a poll that timed out: `true` when a grown
    /// worker has idled long enough to retire. Never called on the busy path.
    pub fn idle_turn(&self, grown: bool, idle: &mut Duration) -> bool {
        *idle += POLL_TIMEOUT;
        grown && *idle >= self.retire_after
    }

    /// End this pool while the core runs on, as a local run's end does: the
    /// supervisor's next answer, at most [`SUPERVISOR_WAIT`] away, is `Stop`.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }

    fn stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    /// The seats taken now: the workers alive and any spawn under way.
    pub fn live(&self) -> usize {
        self.live.load(Ordering::Acquire)
    }

    /// A seat released: a worker that left, or a spawn that failed. Answers
    /// the seats still taken; a release with none taken changes nothing.
    pub fn left(&self) -> usize {
        let taken = self
            .live
            .try_update(Ordering::AcqRel, Ordering::Acquire, |live| {
                live.checked_sub(1)
            });
        self.topping_up.store(false, Ordering::Release);
        taken.map_or(0, |live| live - 1)
    }

    /// The claim on a seat already reserved, for a worker whose loop is the
    /// host's own.
    pub fn seat(&self) -> Seat<'_> {
        Seat(self)
    }

    /// One worker's loop on the calling thread, `run` answering each
    /// activation. The seat is released on any exit.
    pub fn work(&self, core: &PollCore, grown: bool, mut run: impl FnMut(EngineActivation)) {
        let _seat = self.seat();
        let mut idle = Duration::ZERO;
        loop {
            match core.poll(POLL_TIMEOUT) {
                Polled::Activation(act) => {
                    idle = Duration::ZERO;
                    run(act);
                }
                Polled::Timeout => {
                    if self.idle_turn(grown, &mut idle) {
                        break;
                    }
                }
                Polled::ShuttingDown => break,
            }
        }
    }
}

/// A worker's claim on one seat, released on ANY exit — a clean retirement,
/// or an unwind straight past the loop. The live count is the ceiling's
/// denominator and the supervisor's top-up trigger, so a departed worker still
/// counted would both shrink the pool's headroom and hide the death: an
/// ordinary trailing release is skipped by exactly the exit that most needs it.
pub struct Seat<'a>(&'a Governor);

impl Drop for Seat<'_> {
    fn drop(&mut self) {
        self.0.left();
        if std::thread::panicking() {
            flz_transport::notice!(
                Error,
                "a poll worker panicked out of its loop; the supervisor will replace it"
            );
        }
    }
}

#[cfg(test)]
#[path = "poll_pool_tests.rs"]
mod tests;
