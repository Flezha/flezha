//! The native half of the Node host: the threads the SDK brings. The core
//! decides what runs; JavaScript only ever runs handlers, so the parts that
//! block live here.
//!
//! * **Fetchers** loop `poll → decode → hand off → poll` on this module's own
//!   threads, never libuv's shared pool, so one is always free to enter `poll`.
//! * **The blocking pool** ([`pool`]) runs every ABI call that can park, above
//!   all `complete`; it grows, because a fixed pool whose every thread parks
//!   pushing to one consumer has no thread left to run the completion that
//!   would drain it.
//! * **The governor** (`flz_core::poll_pool`) sizes the fetchers: its
//!   supervisor answers a starvation observation with one more fetcher and a
//!   short pool with a replacement, and a grown fetcher retires when idle.
//!
//! Dispatch targets are threadsafe-function handles, one per registered event
//! loop; `pick_loop` keeps a cell on the loop that holds its state.

mod pool;

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use flz_core::poll_core::{CommandReply, EngineActivation, EnginePayload};
use flz_core::poll_pool::{self, Governor};
use flz_core::schema;
use flz_core::util::LockRecover;
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{
    ThreadsafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode,
};
use napi_derive::napi;
pub(crate) use pool::blocking_pool;

use crate::napi_glue::{context, Values};
use crate::veneer::{self, notice, shape, CtxPtr, Polled, Refusal};

/// Fetchers started at boot when the caller names no count — Java's
/// `DEFAULT_FETCHER_COUNT`, for the same reason: a fetcher never runs user
/// code, so two cover the hand-off latency of one being between polls.
const DEFAULT_FETCHERS: u32 = 2;

// ---------------------------------------------------------------------------
// Dispatch targets
// ---------------------------------------------------------------------------

/// The activation crosses to the loop thread as the decoded `schema` value
/// and becomes JavaScript objects inside the callback, which runs there: a
/// fetcher thread has no `Env`, and the objects had to be built on the loop
/// in any case.
type DispatchFn = ThreadsafeFunction<schema::Activation, (), Unknown<'static>, Status, false>;

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
             cells are not retired",
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
pub fn register_dispatch(callback: Function<Unknown<'static>, ()>) -> Result<u32> {
    let call: DispatchFn = callback
        .build_threadsafe_function()
        // No cap: a bounded queue would make the hand-off a place a fetcher
        // can block, and a fetcher that blocks is a fetcher not in `poll`.
        .max_queue_size::<0>()
        .build_callback(|ctx: ThreadsafeCallContext<schema::Activation>| {
            shape::activation(&Values::of(ctx.env), ctx.value)
        })?;
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
/// The fetchers' pool while the host runs; a later start replaces it.
static GOVERNOR: Mutex<Option<Arc<Governor>>> = Mutex::new(None);
static FETCHERS_STARTED: AtomicU32 = AtomicU32::new(0);
static HANDED_OFF: AtomicUsize = AtomicUsize::new(0);
static ABANDONED: AtomicUsize = AtomicUsize::new(0);

fn governor() -> Option<Arc<Governor>> {
    GOVERNOR.lock_recover().clone()
}

fn fetchers() -> usize {
    governor().map_or(0, |g| g.live())
}

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
/// **Affinity by cell, not round-robin.** The core's exclusivity guarantee
/// means no two activations for one cell run at once, so round-robin would
/// be safe for concurrency — but a dispatch loop is a separate JavaScript
/// isolate, and after a restore fold it is the loop that holds that cell's
/// materialized state. Sending the next activation elsewhere would find no
/// state to fold into. Work that names no cell — the prepare crossing, which
/// runs before any key exists — round-robins instead.
///
/// This is the whole difference between the single-loop and multi-loop
/// configurations: with one target both arms answer 0.
///
/// The prepare crossing is the ONLY activation that names no cell — it runs
/// before the key exists, which is what it is computing — so it is the only
/// one that round-robins. A missing `cell_key` does not mean "no cell": a
/// single-cell tasklet's cell is the one with no key of its own, and it
/// holds state exactly like a keyed one.
fn pick_loop(activation: &schema::Activation, loops: usize) -> usize {
    if matches!(activation.kind, schema::ActivationKind::KeyRequest(_)) {
        return ROUND_ROBIN.fetch_add(1, Ordering::Relaxed) % loops;
    }
    let target = &activation.target;
    let mut hash: u32 = 0x811c_9dc5;
    for byte in target
        .relay
        .as_bytes()
        .iter()
        .chain(target.tasklet.to_be_bytes().iter())
        .chain(target.cell_key.as_deref().unwrap_or("").as_bytes())
    {
        hash = (hash ^ u32::from(*byte)).wrapping_mul(0x0100_0193);
    }
    hash as usize % loops
}

/// Hand one decoded activation to the loop `pick_loop` selects.
///
/// Returns whether a loop took it. Anything else abandons: an activation the
/// host cannot deliver would otherwise retire its cell for the life of the
/// process, which is the one failure per-cell exclusivity makes permanent.
fn hand_off(token: u64, activation: schema::Activation) -> bool {
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

/// Marks the token of a whole-relay leave's copy. The core numbers its
/// activations from 1 upward and never reaches this bit.
const LEAVE_COPY: u64 = 1 << 63;
static NEXT_COPY: AtomicU64 = AtomicU64::new(1);

/// The loops a whole-relay leave's copies go to: every loop but `home`, the
/// one the leave itself reaches.
fn copy_loops(home: usize, loops: usize) -> impl Iterator<Item = usize> {
    (0..loops).filter(move |index| *index != home)
}

/// Hand a copy of a whole-relay leave to every loop the leave itself does
/// not reach: each holds the cells of the relay its keys picked. Run as the
/// core submits the leave, so every copy is queued ahead of anything handed
/// out once the leave is answered.
fn hand_leave_copies(target: &schema::CellRef, factory: &'static str) {
    let Ok(ctx) = context() else { return };
    // SAFETY: a context the glue answers is live until the host stops.
    let ctx = unsafe { &*ctx.get() };
    let live = targets();
    let ids: Vec<u32> = live.iter().map(|t| t.id).collect();
    leave_copies(ctx, target, factory, &ids, |index, copy| {
        live[index]
            .call
            .call(copy, ThreadsafeFunctionCallMode::NonBlocking)
            == Status::Ok
    });
}

/// [`hand_leave_copies`] over the loops `ids`, `deliver` handing loop
/// `index` its copy. A copy is held as an outstanding leave whose answer goes
/// nowhere, so the core is answered once, by the original. No loop, as once
/// the host stopped, holds nothing to drop.
fn leave_copies(
    ctx: &crate::FlzContext,
    target: &schema::CellRef,
    factory: &'static str,
    ids: &[u32],
    mut deliver: impl FnMut(usize, schema::Activation) -> bool,
) {
    if ids.is_empty() {
        return;
    }
    let leave = |token| EngineActivation {
        token,
        payload: EnginePayload::Leave {
            target: target.clone(),
            factory,
            reply: CommandReply::new(|_| ()),
        },
    };
    let shape = flz_core::host::to_schema(&leave(0));
    for index in copy_loops(pick_loop(&shape, ids.len()), ids.len()) {
        let act = leave(LEAVE_COPY | NEXT_COPY.fetch_add(1, Ordering::Relaxed));
        let token = act.token;
        let copy = schema::Activation {
            token,
            ..shape.clone()
        };
        ctx.hold(act);
        held().insert(token, ids[index]);
        if !deliver(index, copy) {
            held().remove(&token);
            ctx.outstanding.remove(&token);
        }
    }
}

/// One fetcher turn: `None` ends the thread, else whether the poll timed out.
fn fetch_turn(ctx: CtxPtr) -> Option<bool> {
    match veneer::poll(ctx, poll_pool::POLL_TIMEOUT.as_millis() as u64) {
        Ok(Polled::Timeout) => Some(true),
        Ok(Polled::ShuttingDown) => None,
        Ok(Polled::Activation(act)) => {
            let token = act.token;
            hand_off(token, act);
            Some(false)
        }
        Err(Refusal::Shape(message)) => {
            log_line(3, &message);
            Some(false)
        }
        Err(refusal) => {
            log_line(3, &format!("core poll failed: {refusal}"));
            Some(false)
        }
    }
}

fn fetch_loop(governor: &Governor, grown: bool) {
    if let Ok(ctx) = context() {
        let mut idle = Duration::ZERO;
        while !CLOSING.load(Ordering::Acquire) {
            // A panic crossing a fetcher would unwind out of a thread the
            // runner needs; caught, the fetcher leaves and the governor
            // replaces it.
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fetch_turn(ctx))) {
                Ok(Some(false)) => idle = Duration::ZERO,
                Ok(Some(true)) if !governor.idle_turn(grown, &mut idle) => {}
                Ok(_) => break,
                Err(_) => {
                    log_line(3, "fetcher panicked; the pool replaces it");
                    break;
                }
            }
        }
    }
    if governor.left() == 0 && !CLOSING.load(Ordering::Acquire) {
        // Worth a line: the core has stopped handing out work. On a
        // controller-requested stop this is the only way the runner learns
        // it; on the host's own stop it is what the fetchers were kept
        // polling for.
        log_line(1, "the last fetcher left on the core's word");
        report_drained();
    }
}

/// Start one fetcher into the seat the governor reserved for it.
fn start_fetcher(governor: Arc<Governor>, grown: bool) -> std::io::Result<()> {
    let number = FETCHERS_STARTED.fetch_add(1, Ordering::Relaxed) + 1;
    std::thread::Builder::new()
        .name(format!("flz-fetcher-{number}"))
        .spawn(move || fetch_loop(&governor, grown))
        .map(drop)
}

/// Start the native half of the host: the governor's base of fetchers over
/// `fetchers` (the caller's default) and its supervisor. At least one
/// dispatch target must be registered first, or the first activation would
/// have nowhere to go.
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
    flz_core::host::install_relay_leave_hook(hand_leave_copies);
    let base = fetchers.filter(|n| *n > 0).unwrap_or(DEFAULT_FETCHERS);
    let governor = Governor::from_env(base as usize);
    if let Some(previous) = GOVERNOR.lock_recover().replace(Arc::clone(&governor)) {
        previous.stop();
    }
    // The pool exists before the first activation can be handed off, so a
    // completion never waits on a cold start.
    let _ = blocking_pool();
    let seats = Arc::clone(&governor);
    governor.start(flz_core::poll_core::global(), move |grown| {
        start_fetcher(Arc::clone(&seats), grown)
    });
    Ok(())
}

/// Stop fetching without the core's word.
///
/// A local run ends while the process-wide poll core keeps running — there is
/// no controller session to shut down, and shutting the core would stop every
/// later run in the process — so the flag is raised first and the fetchers
/// leave on their next poll timeout instead of on `stop_host`'s deadline.
#[napi]
pub fn stop_fetching() {
    CLOSING.store(true, Ordering::Release);
    if let Some(governor) = governor() {
        governor.stop();
    }
}

/// Stop fetching, release every dispatch target and drain the blocking pool.
///
/// Asynchronous, because it waits: a fetcher leaves when its poll reports the
/// core shutting down, which the core says only once the shutdown `shutdown()`
/// began has shipped every Cell's state through these same fetchers. So this
/// takes that sequence plus one poll turn, and the pool still holds completions
/// the core is waiting on.
#[napi(ts_return_type = "Promise<void>")]
pub fn stop_host<'a>(env: &'a Env) -> Result<Object<'a>> {
    let (deferred, promise) = env.create_deferred::<(), _>()?;
    // Not the blocking pool: this waits for that pool to drain, and a thread
    // waiting for its own pool is one the pool cannot free.
    std::thread::Builder::new()
        .name("flz-host-stop".into())
        .spawn(move || {
            let deadline = std::time::Instant::now() + poll_pool::DRAIN_BOUND;
            while fetchers() > 0 && std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
            // Only now: a fetcher leaving early on this flag would take the
            // last poller the shutdown sequence needs with it.
            stop_fetching();
            // Releasing every target un-refs the loops the host was keeping
            // alive, so a drained runner's process can exit on its own. What
            // a target still held is abandoned rather than left to retire its
            // cell.
            let departing: Vec<u32> = targets().iter().map(|t| t.id).collect();
            targets().clear();
            for id in departing {
                abandon_held_by(id);
            }
            let pool = blocking_pool();
            pool.drain(poll_pool::DRAIN_BOUND);
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

/// Ask the core to watch standard input and end the process on end of file, on
/// a thread of its own rather than the event loop a compute-bound handler may
/// hold (`flz_core::shutdown::watch_stdin_eof` says why).
#[napi]
pub fn watch_stdin_eof() -> Result<()> {
    context()?;
    crate::veneer::watch_stdin_eof();
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
        fetchers: fetchers() as u32,
        fetcher_ceiling: governor().map_or(0, |g| g.ceiling) as u32,
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

type LogFn =
    ThreadsafeFunction<JsLogLine, (), JsLogLine, Status, false, false, { notice::QUEUE_CAPACITY }>;
static LOGGER: Mutex<Option<LogFn>> = Mutex::new(None);

type DrainedFn = ThreadsafeFunction<(), (), (), Status, false>;
static DRAINED: Mutex<Option<DrainedFn>> = Mutex::new(None);

/// Report that the last fetcher left on the core's shutting-down status.
///
/// That is how a controller-requested stop reaches JavaScript: the core
/// answers every blocked poller with its shutting-down status, the fetchers
/// end, and nothing else would tell the runner its work is over. A runner that
/// stayed up would keep its process alive holding a core that has stopped. On
/// the host's own stop the same report reaches a stop already running.
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
/// runner construction. Never waits: a full queue drops the line and counts it.
fn log_line(level: i32, message: &str) {
    if let Some(logger) = LOGGER.lock().unwrap_or_else(|p| p.into_inner()).as_ref() {
        let line = JsLogLine {
            level,
            message: message.to_string(),
        };
        if logger.call(line, ThreadsafeFunctionCallMode::NonBlocking) == Status::QueueFull {
            notice::count_dropped();
        }
    }
}

/// Register the host's logger, and the core's notices with it; `None` sends
/// the notices to standard error again.
pub(crate) fn set_log_callback(callback: Option<Function<JsLogLine, ()>>) -> Result<()> {
    let next = match callback {
        Some(f) => Some(
            f.build_threadsafe_function()
                .max_queue_size::<{ notice::QUEUE_CAPACITY }>()
                .build()?,
        ),
        None => None,
    };
    let hooked = next.is_some();
    let previous = std::mem::replace(&mut *LOGGER.lock().unwrap_or_else(|p| p.into_inner()), next);
    drop(previous);
    if hooked {
        notice::set_hook(Box::new(|level, _target, text| {
            log_line(level as i32, text)
        }));
    } else {
        notice::clear_hook();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn activation(relay: &str, tasklet: u32, key: Option<&str>) -> schema::Activation {
        activation_of(schema::ActivationKind::Work(vec![]), relay, tasklet, key)
    }

    fn activation_of(
        kind: schema::ActivationKind,
        relay: &str,
        tasklet: u32,
        key: Option<&str>,
    ) -> schema::Activation {
        schema::Activation {
            token: 1,
            target: schema::CellRef {
                relay: relay.to_string(),
                tasklet,
                cell_key: key.map(str::to_string),
            },
            kind,
            change_seq: 0,
            factory: "f".to_string(),
            cell_identity: None,
        }
    }

    /// One cell, one loop. A dispatch loop is a separate isolate holding
    /// that cell's materialized state after its restore fold, so an
    /// activation sent elsewhere would find nothing to fold into.
    #[test]
    fn a_cell_always_picks_the_same_loop() {
        let first = pick_loop(&activation("op", 3, Some("AAPL")), 4);
        for _ in 0..50 {
            assert_eq!(pick_loop(&activation("op", 3, Some("AAPL")), 4), first);
        }
        assert!(first < 4);
        // And the single-loop configuration is the same function answering 0.
        assert_eq!(pick_loop(&activation("op", 3, Some("AAPL")), 1), 0);
    }

    /// Distinct cells are spread rather than piled onto one loop.
    #[test]
    fn distinct_cells_reach_more_than_one_loop() {
        let mut seen = std::collections::HashSet::new();
        for n in 0..64 {
            seen.insert(pick_loop(&activation("op", 0, Some(&format!("k{n}"))), 4));
        }
        assert!(seen.len() > 1, "every cell landed on one loop: {seen:?}");
    }

    /// The prepare crossing runs before any key exists, so it names no cell
    /// and round-robins: there is no state anywhere to be near.
    #[test]
    fn the_prepare_crossing_round_robins() {
        let crossing = || {
            activation_of(
                schema::ActivationKind::KeyRequest(schema::KeyRequest {
                    channel: 0,
                    payloads: vec![],
                    want_keys: true,
                    want_event_time: false,
                }),
                "op",
                0,
                None,
            )
        };
        let start = pick_loop(&crossing(), 3);
        assert_eq!(pick_loop(&crossing(), 3), (start + 1) % 3);
        assert_eq!(pick_loop(&crossing(), 3), (start + 2) % 3);
    }

    /// A single-cell tasklet carries no `cell_key`, and that is NOT the
    /// same as naming no cell: its one cell holds state exactly like a
    /// keyed one, so its activations must reach the loop that folded it.
    #[test]
    fn a_single_cell_tasklet_is_not_round_robined() {
        let single = || activation("op", 5, None);
        let home = pick_loop(&single(), 4);
        for _ in 0..50 {
            assert_eq!(pick_loop(&single(), 4), home);
        }
        // And it is a different home from another tasklet's single cell,
        // so one loop does not collect every unkeyed tasklet.
        let others: std::collections::HashSet<usize> = (0..32)
            .map(|p| pick_loop(&activation("op", p, None), 4))
            .collect();
        assert!(others.len() > 1, "every unkeyed tasklet landed on one loop");
    }

    /// Copies [`four_loops`] handed out, by loop.
    static DELIVERED: Mutex<Vec<(usize, schema::Activation)>> = Mutex::new(Vec::new());

    /// The relay leave hook over four loops, as the host installs it.
    fn four_loops(target: &schema::CellRef, factory: &'static str) {
        let ctx = context().expect("a context");
        // SAFETY: the test's context stays live until it ends.
        let ctx = unsafe { &*ctx.get() };
        leave_copies(ctx, target, factory, &[1, 2, 3, 4], |index, copy| {
            DELIVERED.lock_recover().push((index, copy));
            true
        });
    }

    /// A whole-relay leave names no cell, yet each cell of the relay sits on
    /// the loop its key picked. Stopping the relay reaches every loop, the
    /// original's and a copy on each other, and the core is answered once,
    /// by the original: a copy's answer, or its abandon, goes nowhere.
    #[test]
    fn a_stopped_relay_leaves_every_loop_and_is_answered_once() {
        crate::napi_glue::context_new(flz_core::SCHEMA_VERSION).expect("a context");
        let ctx = context().expect("a context");
        flz_core::host::install_relay_leave_hook(four_loops);
        let (id, relay) = (format!("napi_leave_{}", std::process::id()), "napi-leave-1");
        let json = format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","durable":false,"run":"flow",
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"key"}},{{"link":"into","tasklet":0}}]]}}],
                 "tasklets":[{{"name":"p0","cells_for_overridden":true}}]}}"#
        );
        flz_core::host::register_host_class_json(json.as_bytes()).expect("class registers");
        let node = flz_core::runner::RunnerNode::new("test-ns".to_string());
        let partition = flz_core::relay::instance::KeyGroupRange::UNPARTITIONED;
        node.start_relay(id, relay.to_string(), partition, Default::default())
            .expect("the relay starts");

        assert!(node.stop_relay(relay));
        let copies = std::mem::take(&mut *DELIVERED.lock_recover());
        let Ok(Polled::Activation(original)) = veneer::poll(ctx, 10_000) else {
            panic!("the relay's leave is handed out");
        };
        assert_eq!(original.target.relay, relay);
        let mut reached: Vec<usize> = copies.iter().map(|(index, _)| *index).collect();
        reached.push(pick_loop(&original, 4));
        reached.sort_unstable();
        assert_eq!(
            reached,
            [0, 1, 2, 3],
            "a loop keeps the stopped relay's cells"
        );

        let answer = |token| schema::ActivationResult {
            token,
            outcome: schema::ActivationOutcome::StateApplied,
        };
        let (abandoned, completed) = copies.split_first().expect("copies");
        veneer::abandon(ctx, abandoned.1.token).expect("a copy abandons");
        for (_, copy) in completed {
            veneer::complete(ctx, &answer(copy.token)).expect("a copy completes");
        }
        assert!(
            flz_core::host::pending_leave(relay).is_some(),
            "a copy answered the core"
        );
        veneer::complete(ctx, &answer(original.token)).expect("the leave completes");
        assert!(flz_core::host::pending_leave(relay).is_none());
        crate::napi_glue::context_free();
    }

    /// Once the host stopped no loop is left, and a leave submitted then
    /// has nothing to reach.
    #[test]
    fn a_leave_with_no_loop_registered_hands_out_no_copy() {
        let ctx = crate::veneer::context_new(flz_core::SCHEMA_VERSION).expect("a context");
        let target = schema::CellRef {
            relay: "op".to_string(),
            tasklet: 0,
            cell_key: None,
        };
        // SAFETY: freed below, after its last use.
        leave_copies(unsafe { &*ctx }, &target, "f", &[], |_, _| {
            panic!("no loop to deliver to")
        });
        crate::veneer::context_free(CtxPtr::of(ctx));
    }
}
