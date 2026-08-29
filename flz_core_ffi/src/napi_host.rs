//! The native half of the Node host: the threads the SDK brings. The core
//! decides what runs; JavaScript only ever runs handlers, so the parts that
//! block live here.
//!
//! * **Fetchers** loop `poll → decode → hand off → poll` on this module's own
//!   threads, never libuv's shared pool, so one is always free to enter `poll`.
//! * **The blocking pool** runs every ABI call that can park, above all
//!   `complete`; it grows, because a fixed pool whose every thread parks
//!   pushing to one consumer has no thread left to run the completion that
//!   would drain it.
//! * **The supervisor** parks in the core's starvation wait and answers an
//!   observation with one more fetcher.
//!
//! Dispatch targets are threadsafe-function handles, one per registered event
//! loop; `pick_loop` keeps an entity on the loop that holds its state.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;

use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;

use crate::napi_glue::{activation_to_js, context, JsActivation};
use crate::veneer::{self, CtxPtr, Polled, Refusal};
use crate::{FLZ_POLL_SHUTTING_DOWN, FLZ_STARVATION_OBSERVED};

/// Poll timeout per turn. Bounds how fast a shutdown request is observed —
/// documented as that bound, because it is what an operator waits out.
const POLL_TIMEOUT_MS: u64 = 250;

/// The supervisor's park length. Only bounds how often it re-arms; the core
/// decides when the wait returns.
const SUPERVISOR_WAIT_MS: u64 = 1_000;

/// How often the standard-input watcher retries a non-blocking read. It has
/// nothing else to do, and shutdown latency is already bounded by the poll
/// timeout, so this only has to be small next to that.
const STDIN_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// How long a pool thread above the base stays idle before retiring.
const POOL_IDLE_RETIRE: Duration = Duration::from_secs(30);

/// How long `stopHost` waits for the fetchers and the pool to drain.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Fetchers started at boot when the caller names no count — Java's
/// `DEFAULT_FETCHER_COUNT`, for the same reason: a fetcher never runs user
/// code, so two cover the hand-off latency of one being between polls.
const DEFAULT_FETCHERS: u32 = 2;

// ---------------------------------------------------------------------------
// The blocking pool
// ---------------------------------------------------------------------------

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
    base: usize,
    ceiling: usize,
    idle_retire: Duration,
}

impl BlockingPool {
    fn new(base: usize, ceiling: usize) -> Arc<Self> {
        Self::with_retire(base, ceiling, POOL_IDLE_RETIRE)
    }

    /// The retirement streak is a parameter only so a test can watch a grown
    /// thread retire without waiting out the production streak.
    fn with_retire(base: usize, ceiling: usize, idle_retire: Duration) -> Arc<Self> {
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
        }
        let me = Arc::clone(self);
        let started = std::thread::Builder::new()
            .name("flz-blocking".into())
            .spawn(move || me.worker());
        if started.is_err() {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state.live -= 1;
        }
    }

    pub(crate) fn submit(self: &Arc<Self>, job: impl FnOnce() + Send + 'static) {
        let grow = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state.queue.push_back(Box::new(job));
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
                        break job;
                    }
                    if state.closing {
                        state.live -= 1;
                        return;
                    }
                    state.idle += 1;
                    let (next, wait) = self
                        .ready
                        .wait_timeout(state, self.idle_retire)
                        .unwrap_or_else(|p| p.into_inner());
                    state = next;
                    state.idle -= 1;
                    if wait.timed_out() && state.queue.is_empty() && state.live > self.base {
                        state.live -= 1;
                        return;
                    }
                }
            };
            job();
        }
    }

    fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.closing = true;
        self.ready.notify_all();
    }

    fn drain(&self, deadline: Duration) {
        let step = Duration::from_millis(10);
        let mut waited = Duration::ZERO;
        while waited < deadline {
            {
                let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                if state.queue.is_empty() && state.idle == state.live {
                    return;
                }
            }
            std::thread::sleep(step);
            waited += step;
        }
    }

    fn snapshot(&self) -> (u32, u32, u32, u32) {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        (
            state.live as u32,
            (state.live - state.idle.min(state.live)) as u32,
            state.peak as u32,
            state.queue.len() as u32,
        )
    }
}

static POOL: OnceLock<Arc<BlockingPool>> = OnceLock::new();

fn env_usize(key: &str) -> Option<usize> {
    std::env::var(key).ok()?.trim().parse().ok()
}

/// The process's blocking pool, sized from the existing worker-pool
/// configuration rather than a knob minted for Node:
/// `FLEZHA_WORKER_POOL_SIZE` is the base and `FLEZHA_WORKER_POOL_MAX` the
/// ceiling, defaulting to ten times the base and never below it.
pub(crate) fn blocking_pool() -> &'static Arc<BlockingPool> {
    POOL.get_or_init(|| {
        let base = env_usize("FLEZHA_WORKER_POOL_SIZE")
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_FETCHERS as usize);
        let ceiling = env_usize("FLEZHA_WORKER_POOL_MAX")
            .filter(|n| *n > 0)
            .unwrap_or(base * 10);
        BlockingPool::new(base, ceiling)
    })
}

// ---------------------------------------------------------------------------
// Dispatch targets
// ---------------------------------------------------------------------------

type DispatchFn = ThreadsafeFunction<JsActivation, (), JsActivation, Status, false>;

struct DispatchTarget {
    id: u32,
    call: DispatchFn,
}

static TARGETS: Mutex<Vec<DispatchTarget>> = Mutex::new(Vec::new());
static NEXT_TARGET_ID: AtomicU32 = AtomicU32::new(1);
static ROUND_ROBIN: AtomicUsize = AtomicUsize::new(0);

/// Which dispatch loop holds each outstanding activation.
///
/// This is what makes "a dispatch loop that ends abandons what it held"
/// structural rather than a message protocol between isolates: the hand-off
/// records the token here, the answer removes it, and releasing a target
/// abandons whatever is left. A worker that dies mid-activation cannot report
/// anything, and this is the only place that still knows what it had.
static HELD: OnceLock<dashmap::DashMap<u64, u32>> = OnceLock::new();

fn held() -> &'static dashmap::DashMap<u64, u32> {
    HELD.get_or_init(dashmap::DashMap::new)
}

/// Forget a token that has been answered. Called by the veneer's `complete`
/// and `abandon`, which are the only two ways an activation is answered.
pub(crate) fn release_token(token: u64) {
    held().remove(&token);
}

/// Abandon every activation a departing target still held.
fn abandon_held_by(target: u32) {
    let orphans: Vec<u64> = held()
        .iter()
        .filter(|entry| *entry.value() == target)
        .map(|entry| *entry.key())
        .collect();
    if orphans.is_empty() {
        return;
    }
    log_line(
        2,
        &format!(
            "dispatch loop {target} ended holding {} activation(s); abandoning them so their \
             entities are not retired",
            orphans.len()
        ),
    );
    for token in orphans {
        held().remove(&token);
        abandon_quietly(token);
    }
}

fn targets() -> std::sync::MutexGuard<'static, Vec<DispatchTarget>> {
    TARGETS.lock().unwrap_or_else(|p| p.into_inner())
}

/// Register this event loop as a dispatch target and answer its handle.
///
/// Every configuration registers the same way: the single-loop default is one
/// registration from the main thread, and a worker tier is one registration
/// per worker. That is the whole difference between the two, which is why
/// enabling workers is a flag rather than a different dispatch path.
#[napi]
pub fn register_dispatch(callback: Function<JsActivation, ()>) -> Result<u32> {
    let call: DispatchFn = callback
        .build_threadsafe_function()
        // No cap: a bounded queue would make the hand-off a place a fetcher
        // can block, and a fetcher that blocks is a fetcher not in `poll`.
        .max_queue_size::<0>()
        .build()?;
    let id = NEXT_TARGET_ID.fetch_add(1, Ordering::Relaxed);
    targets().push(DispatchTarget { id, call });
    Ok(id)
}

/// Release a dispatch target. Its loop stops receiving activations at once;
/// whatever it still held is the caller's to abandon.
#[napi]
pub fn unregister_dispatch(id: u32) -> Result<()> {
    let mut live = targets();
    if let Some(index) = live.iter().position(|t| t.id == id) {
        let target = live.remove(index);
        drop(live);
        // Dropping the last reference releases the threadsafe function, which
        // also un-refs the loop it was keeping alive.
        drop(target);
        abandon_held_by(id);
    }
    Ok(())
}

/// How many event loops are registered to take activations.
#[napi]
pub fn dispatch_loop_count() -> u32 {
    targets().len() as u32
}

// ---------------------------------------------------------------------------
// Fetchers and the supervisor
// ---------------------------------------------------------------------------

static CLOSING: AtomicBool = AtomicBool::new(true);
static LIVE_FETCHERS: AtomicUsize = AtomicUsize::new(0);
static FETCHER_CEILING: AtomicUsize = AtomicUsize::new(0);
static FETCHERS_STARTED: AtomicU32 = AtomicU32::new(0);
static HANDED_OFF: AtomicUsize = AtomicUsize::new(0);
static ABANDONED: AtomicUsize = AtomicUsize::new(0);

fn abandon_quietly(token: u64) {
    let Ok(ctx) = context() else { return };
    if veneer::abandon(ctx, token).is_ok() {
        ABANDONED.fetch_add(1, Ordering::Relaxed);
    } else {
        log_line(2, &format!("abandoning activation {token} failed"));
    }
}

/// Which dispatch loop an activation belongs to.
///
/// **Affinity by entity, not round-robin.** The core's exclusivity guarantee
/// means no two activations for one entity run at once, so round-robin would
/// be safe for concurrency — but a dispatch loop is a separate JavaScript
/// isolate, and after a restore fold it is the loop that holds that entity's
/// materialized state. Sending the next activation elsewhere would find no
/// state to fold into. Work that names no entity — the prepare crossing, which
/// runs before any key exists — round-robins instead.
///
/// This is the whole difference between the single-loop and multi-loop
/// configurations: with one target both arms answer 0.
///
/// The prepare crossing is the ONLY activation that names no entity — it runs
/// before the key exists, which is what it is computing — so it is the only
/// one that round-robins. A missing `entity_key` does not mean "no entity": a
/// single-entity procedure's entity is the one with no key of its own, and it
/// holds state exactly like a keyed one.
fn pick_loop(activation: &JsActivation, loops: usize) -> usize {
    if activation.kind == "key_request" {
        return ROUND_ROBIN.fetch_add(1, Ordering::Relaxed) % loops;
    }
    let mut hash: u32 = 0x811c_9dc5;
    for byte in activation
        .operative
        .as_bytes()
        .iter()
        .chain(activation.procedure.to_be_bytes().iter())
        .chain(activation.entity_key.as_deref().unwrap_or("").as_bytes())
    {
        hash = (hash ^ u32::from(*byte)).wrapping_mul(0x0100_0193);
    }
    hash as usize % loops
}

/// Hand one decoded activation to the loop `pick_loop` selects.
///
/// Returns whether a loop took it. Anything else abandons: an activation the
/// host cannot deliver would otherwise retire its entity for the life of the
/// process, which is the one failure per-entity exclusivity makes permanent.
fn hand_off(token: u64, activation: JsActivation) -> bool {
    let live = targets();
    if live.is_empty() {
        drop(live);
        abandon_quietly(token);
        return false;
    }
    let index = pick_loop(&activation, live.len());
    let target_id = live[index].id;
    // Recorded BEFORE the call: the target may run the activation and answer
    // it on another thread before `call` returns, and a token recorded after
    // that would never be removed.
    held().insert(token, target_id);
    let status = live[index]
        .call
        .call(activation, ThreadsafeFunctionCallMode::NonBlocking);
    drop(live);
    if status == Status::Ok {
        HANDED_OFF.fetch_add(1, Ordering::Relaxed);
        return true;
    }
    held().remove(&token);
    log_line(
        2,
        &format!("dispatch hand-off refused ({status:?}); abandoning activation {token}"),
    );
    abandon_quietly(token);
    false
}

/// One fetcher turn. `false` ends the thread.
fn fetch_turn(ctx: CtxPtr) -> bool {
    match veneer::poll(ctx, POLL_TIMEOUT_MS) {
        Ok(Polled::Timeout) => true,
        Ok(Polled::ShuttingDown) => false,
        Ok(Polled::Activation(act)) => {
            let token = act.token;
            hand_off(token, activation_to_js(act));
            true
        }
        Err(Refusal::Shape(message)) => {
            log_line(3, &message);
            true
        }
        Err(refusal) => {
            log_line(3, &format!("core poll failed: {refusal}"));
            true
        }
    }
}

fn fetch_loop() {
    let Ok(ctx) = context() else {
        LIVE_FETCHERS.fetch_sub(1, Ordering::AcqRel);
        return;
    };
    let ended_cleanly = loop {
        if CLOSING.load(Ordering::Acquire) {
            break true;
        }
        // A panic crossing a fetcher would unwind out of a thread the runner
        // needs; catching it here is what makes "a fetch thread that ends on
        // an error is replaced" reachable rather than a process abort.
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fetch_turn(ctx))) {
            Ok(true) => continue,
            Ok(false) => break true,
            Err(_) => {
                log_line(3, "fetcher panicked; starting a replacement");
                break false;
            }
        }
    };
    let remaining = LIVE_FETCHERS.fetch_sub(1, Ordering::AcqRel) - 1;
    if !ended_cleanly && !CLOSING.load(Ordering::Acquire) {
        start_fetcher();
        return;
    }
    if remaining == 0 && !CLOSING.load(Ordering::Acquire) {
        // Worth a line: this is the runner learning its work is over from the
        // core rather than from an operator, and it is the only path by which
        // a healthy-looking process stops fetching.
        log_line(1, "the last fetcher left without a stop being requested");
        report_drained();
    }
}

/// Start one fetcher, counted before the spawn so a failed spawn cannot leave
/// the host believing it has a poller it does not.
fn start_fetcher() -> bool {
    LIVE_FETCHERS.fetch_add(1, Ordering::AcqRel);
    match std::thread::Builder::new()
        .name(format!(
            "flz-fetcher-{}",
            FETCHERS_STARTED.fetch_add(1, Ordering::Relaxed) + 1
        ))
        .spawn(fetch_loop)
    {
        Ok(_) => true,
        Err(e) => {
            LIVE_FETCHERS.fetch_sub(1, Ordering::AcqRel);
            log_line(3, &format!("could not start a fetcher: {e}"));
            false
        }
    }
}

/// The starvation supervisor: park in the core's wait, and answer every
/// observation with one more fetcher up to the ceiling.
///
/// This covers what the core can see — work queued with no thread inside
/// `poll`, which for this host means a direct activation (a restore fold, a
/// snapshot request, a workflow step) arriving while every fetcher is between
/// polls. It is not a substitute for the pool's growth rule: the core cannot
/// see the host's completion queue, and this thread cannot see the core's.
fn supervise_loop() {
    let Ok(ctx) = context() else { return };
    let mut at_ceiling_logged = false;
    while !CLOSING.load(Ordering::Acquire) {
        let Ok(outcome) = veneer::wait_starvation(ctx, SUPERVISOR_WAIT_MS) else {
            continue;
        };
        if outcome == FLZ_POLL_SHUTTING_DOWN {
            return;
        }
        if outcome != FLZ_STARVATION_OBSERVED || CLOSING.load(Ordering::Acquire) {
            continue;
        }
        let ceiling = FETCHER_CEILING.load(Ordering::Acquire);
        if LIVE_FETCHERS.load(Ordering::Acquire) >= ceiling {
            if !at_ceiling_logged {
                log_line(
                    2,
                    &format!(
                        "starvation observed with the fetcher set at its ceiling ({ceiling}); \
                         raise FLEZHA_WORKER_POOL_MAX if the work is blocked rather than wedged"
                    ),
                );
                at_ceiling_logged = true;
            }
            continue;
        }
        if start_fetcher() {
            at_ceiling_logged = false;
        }
    }
}

/// Start the native half of the host: `fetchers` poll threads and one
/// starvation supervisor. At least one dispatch target must be registered
/// first, or the first activation would have nowhere to go.
#[napi]
pub fn start_host(fetchers: Option<u32>) -> Result<()> {
    context()?;
    if targets().is_empty() {
        return Err(Error::new(
            Status::InvalidArg,
            "no dispatch loop registered: call registerDispatch before startHost",
        ));
    }
    if !CLOSING.swap(false, Ordering::AcqRel) {
        return Err(Error::new(
            Status::InvalidArg,
            "the native host is already running",
        ));
    }
    let count = fetchers.filter(|n| *n > 0).unwrap_or(DEFAULT_FETCHERS) as usize;
    let ceiling = env_usize("FLEZHA_WORKER_POOL_MAX")
        .filter(|n| *n > 0)
        .unwrap_or(count * 10)
        .max(count);
    FETCHER_CEILING.store(ceiling, Ordering::Release);
    // The pool exists before the first activation can be handed off, so a
    // completion never waits on a cold start.
    let _ = blocking_pool();
    for _ in 0..count {
        start_fetcher();
    }
    std::thread::Builder::new()
        .name("flz-starvation-supervisor".into())
        .spawn(supervise_loop)
        .map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("could not start the starvation supervisor: {e}"),
            )
        })?;
    Ok(())
}

/// Stop fetching, release every dispatch target and drain the blocking pool.
///
/// Asynchronous, because it waits: a fetcher leaves at the end of its current
/// poll turn, so this takes up to the poll timeout, and the pool still holds
/// completions the core is waiting on.
#[napi(ts_return_type = "Promise<void>")]
pub fn stop_host<'a>(env: &'a Env) -> Result<Object<'a>> {
    CLOSING.store(true, Ordering::Release);
    let (deferred, promise) = env.create_deferred::<(), _>()?;
    // Not the blocking pool: this waits for that pool to drain, and a thread
    // waiting for its own pool is one the pool cannot free.
    std::thread::Builder::new()
        .name("flz-host-stop".into())
        .spawn(move || {
            let deadline = std::time::Instant::now() + DRAIN_TIMEOUT;
            while LIVE_FETCHERS.load(Ordering::Acquire) > 0 && std::time::Instant::now() < deadline
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            // Releasing every target un-refs the loops the host was keeping
            // alive, so a drained runner's process can exit on its own. What
            // a target still held is abandoned rather than left to retire its
            // entity.
            let departing: Vec<u32> = targets().iter().map(|t| t.id).collect();
            targets().clear();
            for id in departing {
                abandon_held_by(id);
            }
            let pool = blocking_pool();
            pool.drain(DRAIN_TIMEOUT);
            pool.close();
            deferred.resolve(|_| Ok(()));
        })
        .map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("could not start the host stop thread: {e}"),
            )
        })?;
    Ok(promise)
}

/// Stop the process when standard input reaches end of file.
///
/// Native, and that is the point. A conformance harness stops a runner by
/// closing its pipes rather than by signalling it, and the obvious place to
/// observe that is the event loop — which is exactly the thread a compute-bound
/// handler holds. A runner whose only loop is wedged would then be
/// unstoppable without a signal, and in the single-loop configuration there is
/// no second loop to notice.
///
/// So the watcher is a thread this binding owns. On end of file it wakes every
/// blocked poller, giving a healthy runner `grace_ms` to drain and exit on its
/// own; a runner that has not exited by then is terminated, because there is
/// nothing left that could.
#[napi]
pub fn watch_stdin_eof(grace_ms: u32) -> Result<()> {
    let ctx = context()?;
    std::thread::Builder::new()
        .name("flz-stdin-eof".into())
        .spawn(move || {
            use std::io::{ErrorKind, Read};
            let mut byte = [0u8; 1];
            loop {
                match std::io::stdin().read(&mut byte) {
                    // End of file: the far end closed. This is the only read
                    // outcome that means shutdown.
                    Ok(0) => break,
                    Ok(_) => continue,
                    // NOT end of file. Standard input is non-blocking whenever
                    // the runtime has taken an interest in it — which it has
                    // the moment a worker thread exists — so a read with
                    // nothing buffered answers `WouldBlock`. Treating that as
                    // end of file shuts the runner down a few hundred
                    // milliseconds after it connects, every time, and only in
                    // the multi-loop configuration.
                    Err(e) if e.kind() == ErrorKind::WouldBlock => {
                        std::thread::sleep(STDIN_POLL_INTERVAL);
                    }
                    Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                    // Anything else means the descriptor is gone, which is
                    // end of file by another name.
                    Err(_) => break,
                }
            }
            log_line(1, "standard input reached end of file; draining");
            veneer::shutdown(ctx);
            std::thread::sleep(Duration::from_millis(u64::from(grace_ms)));
            std::process::exit(0);
        })
        .map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("could not start the standard-input watcher: {e}"),
            )
        })?;
    Ok(())
}

#[napi(object)]
pub struct JsHostStats {
    pub fetchers: u32,
    pub fetcher_ceiling: u32,
    pub dispatch_loops: u32,
    pub pool_size: u32,
    pub pool_busy: u32,
    pub pool_peak: u32,
    pub pool_queued: u32,
    pub handed_off: u32,
    pub abandoned: u32,
}

/// The host's own counters. The concurrency tests read these rather than
/// inferring the pool's behaviour from timing.
#[napi]
pub fn host_stats() -> JsHostStats {
    let (pool_size, pool_busy, pool_peak, pool_queued) = blocking_pool().snapshot();
    JsHostStats {
        fetchers: LIVE_FETCHERS.load(Ordering::Acquire) as u32,
        fetcher_ceiling: FETCHER_CEILING.load(Ordering::Acquire) as u32,
        dispatch_loops: targets().len() as u32,
        pool_size,
        pool_busy,
        pool_peak,
        pool_queued,
        handed_off: HANDED_OFF.load(Ordering::Relaxed) as u32,
        abandoned: ABANDONED.load(Ordering::Relaxed) as u32,
    }
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

/// One diagnostic from a native thread.
///
/// An object rather than a `(level, message)` tuple: a threadsafe function
/// built over a tuple delivers no arguments at all, so the host logger saw
/// `undefined` for both and every native diagnostic was lost.
#[napi(object)]
pub struct JsLogLine {
    pub level: i32,
    pub message: String,
}

type LogFn = ThreadsafeFunction<JsLogLine, (), JsLogLine, Status, false>;
static LOGGER: Mutex<Option<LogFn>> = Mutex::new(None);

type DrainedFn = ThreadsafeFunction<(), (), (), Status, false>;
static DRAINED: Mutex<Option<DrainedFn>> = Mutex::new(None);

/// Report that the last fetcher left without the host being told to stop.
///
/// That is how a controller-requested stop reaches JavaScript: the core
/// answers every blocked poller with its shutting-down status, the fetchers
/// end, and nothing else would tell the runner its work is over. A runner that
/// stayed up would keep its process alive holding a core that has stopped.
fn report_drained() {
    if let Some(callback) = DRAINED.lock().unwrap_or_else(|p| p.into_inner()).as_ref() {
        callback.call((), ThreadsafeFunctionCallMode::NonBlocking);
    }
}

/// Register the drained callback. One per process; a second replaces it.
#[napi]
pub fn set_host_drained_callback(callback: Option<Function<(), ()>>) -> Result<()> {
    let next = match callback {
        Some(f) => Some(
            f.build_threadsafe_function()
                .max_queue_size::<0>()
                .build()?,
        ),
        None => None,
    };
    let previous = std::mem::replace(
        &mut *DRAINED.lock().unwrap_or_else(|p| p.into_inner()),
        next,
    );
    drop(previous);
    Ok(())
}

/// Route one native diagnostic to the registered logger. Native threads have
/// no other way to reach the host's logger, and dropping the line when none is
/// registered is deliberate: the SDK's own default logger installs one at
/// runner construction.
fn log_line(level: i32, message: &str) {
    if let Some(logger) = LOGGER.lock().unwrap_or_else(|p| p.into_inner()).as_ref() {
        logger.call(
            JsLogLine {
                level,
                message: message.to_string(),
            },
            ThreadsafeFunctionCallMode::NonBlocking,
        );
    }
}

pub(crate) fn set_log_callback(callback: Option<Function<JsLogLine, ()>>) -> Result<()> {
    let next = match callback {
        Some(f) => Some(
            f.build_threadsafe_function()
                .max_queue_size::<0>()
                .build()?,
        ),
        None => None,
    };
    let previous = std::mem::replace(&mut *LOGGER.lock().unwrap_or_else(|p| p.into_inner()), next);
    drop(previous);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use super::*;

    /// Wait for a condition on the pool, or fail. Polling rather than a
    /// rendezvous because what is being observed is the pool's own
    /// bookkeeping, which no test-side channel can be handed.
    fn until(pool: &Arc<BlockingPool>, what: &str, ready: impl Fn((u32, u32, u32, u32)) -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if ready(pool.snapshot()) {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("timed out waiting for {what}: {:?}", pool.snapshot());
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
        let watched = std::time::Instant::now();
        while watched.elapsed() < Duration::from_millis(100) {
            assert_eq!(pool.snapshot().0, 3, "the pool grew past its ceiling");
            std::thread::yield_now();
        }
        for _ in 0..10 {
            let _ = release_tx.send(());
        }
        pool.drain(Duration::from_secs(5));
        pool.close();
    }

    fn activation(operative: &str, procedure: u32, key: Option<&str>) -> JsActivation {
        activation_of("work", operative, procedure, key)
    }

    fn activation_of(
        kind: &str,
        operative: &str,
        procedure: u32,
        key: Option<&str>,
    ) -> JsActivation {
        JsActivation {
            token: napi::bindgen_prelude::BigInt::from(1u64),
            operative: operative.to_string(),
            procedure,
            entity_key: key.map(str::to_string),
            change_seq: napi::bindgen_prelude::BigInt::from(0u64),
            factory: "f".to_string(),
            kind: kind.to_string(),
            items: None,
            snapshot: None,
            changes: None,
            state_json: None,
            key_request: None,
            args: None,
            outputs: None,
        }
    }

    /// One entity, one loop. A dispatch loop is a separate isolate holding
    /// that entity's materialized state after its restore fold, so an
    /// activation sent elsewhere would find nothing to fold into.
    #[test]
    fn an_entity_always_picks_the_same_loop() {
        let first = pick_loop(&activation("op", 3, Some("AAPL")), 4);
        for _ in 0..50 {
            assert_eq!(pick_loop(&activation("op", 3, Some("AAPL")), 4), first);
        }
        assert!(first < 4);
        // And the single-loop configuration is the same function answering 0.
        assert_eq!(pick_loop(&activation("op", 3, Some("AAPL")), 1), 0);
    }

    /// Distinct entities are spread rather than piled onto one loop.
    #[test]
    fn distinct_entities_reach_more_than_one_loop() {
        let mut seen = std::collections::HashSet::new();
        for n in 0..64 {
            seen.insert(pick_loop(&activation("op", 0, Some(&format!("k{n}"))), 4));
        }
        assert!(seen.len() > 1, "every entity landed on one loop: {seen:?}");
    }

    /// The prepare crossing runs before any key exists, so it names no entity
    /// and round-robins: there is no state anywhere to be near.
    #[test]
    fn the_prepare_crossing_round_robins() {
        let crossing = || activation_of("key_request", "op", 0, None);
        let start = pick_loop(&crossing(), 3);
        assert_eq!(pick_loop(&crossing(), 3), (start + 1) % 3);
        assert_eq!(pick_loop(&crossing(), 3), (start + 2) % 3);
    }

    /// A single-entity procedure carries no `entity_key`, and that is NOT the
    /// same as naming no entity: its one entity holds state exactly like a
    /// keyed one, so its activations must reach the loop that folded it.
    #[test]
    fn a_single_entity_procedure_is_not_round_robined() {
        let single = || activation("op", 5, None);
        let home = pick_loop(&single(), 4);
        for _ in 0..50 {
            assert_eq!(pick_loop(&single(), 4), home);
        }
        // And it is a different home from another procedure's single entity,
        // so one loop does not collect every unkeyed procedure.
        let others: std::collections::HashSet<usize> = (0..32)
            .map(|p| pick_loop(&activation("op", p, None), 4))
            .collect();
        assert!(
            others.len() > 1,
            "every unkeyed procedure landed on one loop"
        );
    }
}
