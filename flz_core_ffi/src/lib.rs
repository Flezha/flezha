//! The C ABI over `flz_core`'s poll/complete engine.
//!
//! One opaque context per host process. Any host thread may call `poll`; any
//! host thread — including a different one — may call `complete`. Ownership
//! follows the workspace FFI convention: whoever allocates frees; buffers the
//! core hands out are returned through [`flz_core_buf_free`].
//!
//! Schema negotiation happens at context creation: a shim built against a
//! different `SCHEMA_VERSION` is refused with an error naming both versions,
//! before any controller connection is attempted.
//!
//! # Diagnostics: this crate installs no tracing subscriber
//!
//! Deliberate, and the reason is the shape of a veneer host. This crate is
//! loaded into a Node, Python, Ruby or PHP process the application owns; a
//! subscriber claims a process-global slot at load time, writes to that
//! process's stderr and cannot be handed back. Two veneers in one process, or
//! a veneer beside another Rust extension, would then contend for the same
//! slot. So the kernel's `tracing` events are dropped in a veneer host unless
//! the host installs a subscriber itself, and `RUST_LOG` alone changes
//! nothing there.
//!
//! The Rust SDK's `flezha_runner::diag::init_default_tracing` is the opt-in a
//! standalone application calls; this crate does not depend on the SDK, so a
//! veneer cannot reach it. Giving the veneers an opt-in of their own means a
//! new exported entry point and a call in each of the four hosts — a
//! cross-SDK change, owed by every veneer at once, not a side effect of this
//! one.

mod error;
#[cfg(feature = "fake-controller")]
pub mod fake_controller;
#[cfg(feature = "jni")]
pub mod jni_glue;
mod local;
// `napi-derive` emits the module-registration constructor only outside
// `cfg(test)`, so under the test harness every exported entry point looks
// unreferenced. The allow is scoped to that build, and to these two modules,
// so genuinely dead code elsewhere still reports.
#[cfg_attr(test, allow(dead_code))]
#[cfg(feature = "napi")]
pub mod napi_glue;
#[cfg_attr(test, allow(dead_code))]
#[cfg(feature = "napi")]
mod napi_host;
#[cfg(feature = "python")]
pub mod python_glue;
#[cfg(feature = "ruby")]
pub mod ruby_glue;
pub mod veneer;

use std::ffi::{c_char, CStr, CString};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, TryLockError};
use std::time::Duration;

use dashmap::DashMap;
pub use error::Error;
use flz_core::poll_core::{EngineActivation, Polled, Starvation};
use flz_core::runner::{ClientResponseMessage, RunnerMessage};
use flz_core::schema;
use flz_core::spawn::{IfRunning, SpawnHandle, SpawnOptions};
/// The local-mode entry points, exported from the crate root like every other
/// C symbol: the module is a source-file split, not an ABI boundary.
pub use local::*;

/// Poll produced an activation; the out buffer holds its encoded bytes.
pub const FLZ_POLL_ACTIVATION: i32 = 1;
/// The starvation wait observed starvation: the host owes a thread.
pub const FLZ_STARVATION_OBSERVED: i32 = 1;
/// Poll timed out with no work.
pub const FLZ_POLL_TIMEOUT: i32 = 0;
/// The context is shutting down; every blocked poller is released.
pub const FLZ_POLL_SHUTTING_DOWN: i32 = 2;
/// Generic failure; read [`flz_core_last_error`].
pub const FLZ_ERR: i32 = -1;
/// Success for non-poll calls.
pub const FLZ_OK: i32 = 0;
/// A spawn result read found no answer yet; the out buffer is untouched.
pub const FLZ_SPAWN_PENDING: i32 = 1;
/// A spawn resolved to a failure; the out buffer holds it as JSON.
pub const FLZ_SPAWN_FAILED: i32 = 2;

/// A polled activation the host has not answered, with the spawn handles its
/// tasklets made: they live exactly as long as it does.
struct Outstanding {
    activation: EngineActivation,
    spawns: Vec<u64>,
}

/// Opaque context handle.
pub struct FlzContext {
    outstanding: DashMap<u64, Outstanding>,
    last_error: Mutex<Option<CString>>,
    started: AtomicBool,
    declared: Arc<RwLock<Vec<String>>>,
    /// The local run this context drives and the channel its sinks release
    /// onto, when it has one. A started context dials a controller and never
    /// holds one; the two are alternatives.
    local: Mutex<Option<LocalSession>>,
    /// The host's end of that channel, held apart from the run so a thread
    /// parked in [`flz_core_local_next_output`] holds nothing `feed` needs.
    local_outputs: Mutex<Option<std::sync::mpsc::Receiver<Vec<u8>>>>,
    /// The spawn handles of every outstanding activation, by number.
    spawns: DashMap<u64, Arc<Mutex<SpawnHandle>>>,
    last_spawn: AtomicU64,
}

/// Where a registered sink's releases go: the host takes them back through
/// [`flz_core_local_next_output`].
type LocalReleases = std::sync::mpsc::Sender<Vec<u8>>;

/// What a local run is, from the ABI's side: the run itself, and the sender
/// its registered sinks release onto.
struct LocalSession {
    run: Arc<flz_core::local::LocalRun>,
    releases: std::sync::mpsc::Sender<Vec<u8>>,
}

thread_local! {
    static INIT_ERROR: std::cell::RefCell<Option<CString>> = const { std::cell::RefCell::new(None) };
}

fn set_init_error(error: Error) {
    INIT_ERROR.with(|slot| {
        *slot.borrow_mut() = Some(CString::new(error.to_string()).unwrap_or_default());
    });
}

/// Hand `bytes` to the caller through the out pointers; the caller frees
/// them with [`flz_core_buf_free`].
///
/// # Safety
/// `buf_out`/`len_out` must be valid writable pointers.
unsafe fn hand_out(bytes: Vec<u8>, buf_out: *mut *mut u8, len_out: *mut usize) {
    let mut bytes = bytes.into_boxed_slice();
    *buf_out = bytes.as_mut_ptr();
    *len_out = bytes.len();
    std::mem::forget(bytes);
}

impl FlzContext {
    /// Record `error` as the context's last error; the status every failing
    /// entry point returns. The one place an [`Error`] becomes a C string.
    fn fail(&self, error: Error) -> i32 {
        *self.last_error.lock().unwrap_or_else(|p| p.into_inner()) =
            Some(CString::new(error.to_string()).unwrap_or_default());
        FLZ_ERR
    }

    /// Take a polled activation by token, releasing the spawn handles it
    /// made. `Err` carries the status the entry point returns, with the reason
    /// already recorded for `last_error` — an unknown token means the veneer
    /// answered twice, or answered one it never held, and either is a defect
    /// to surface rather than a no-op.
    fn take_outstanding(&self, token: u64) -> Result<EngineActivation, i32> {
        let (_, outstanding) = self
            .outstanding
            .remove(&token)
            .ok_or_else(|| self.fail(Error::UnknownToken(token)))?;
        // Dropping a handle reports a failure nobody read, or hands a pending
        // one to the reaper; a later read of its number answers released.
        for handle in outstanding.spawns {
            self.spawns.remove(&handle);
        }
        Ok(outstanding.activation)
    }

    /// This context's local run and its release channel, cloned out of the
    /// lock so a call that blocks holds nothing. `Err` is the refusal a
    /// local-mode call gets on a context that has none.
    fn local_session(&self) -> Result<(Arc<flz_core::local::LocalRun>, LocalReleases), Error> {
        self.local
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|session| (Arc::clone(&session.run), session.releases.clone()))
            .ok_or(Error::NoLocalRun)
    }
}

/// The schema version this core speaks; a shim compares before connecting.
#[no_mangle]
pub extern "C" fn flz_core_schema_version() -> u32 {
    flz_core::SCHEMA_VERSION
}

/// Create a context. `shim_schema_version` is the version the calling shim
/// was built against; a mismatch returns NULL and
/// [`flz_core_init_error`] names both versions. No controller connection is
/// attempted here.
#[no_mangle]
pub extern "C" fn flz_core_context_new(shim_schema_version: u32) -> *mut FlzContext {
    if shim_schema_version != flz_core::SCHEMA_VERSION {
        set_init_error(Error::SchemaMismatch {
            shim: shim_schema_version,
            core: flz_core::SCHEMA_VERSION,
        });
        return std::ptr::null_mut();
    }
    Box::into_raw(Box::new(FlzContext {
        outstanding: DashMap::new(),
        last_error: Mutex::new(None),
        started: AtomicBool::new(false),
        declared: Arc::new(RwLock::new(Vec::new())),
        local: Mutex::new(None),
        local_outputs: Mutex::new(None),
        spawns: DashMap::new(),
        last_spawn: AtomicU64::new(0),
    }))
}

/// The error that made the last [`flz_core_context_new`] on THIS thread
/// return NULL, or NULL. Borrowed until the next failing creation.
#[no_mangle]
pub extern "C" fn flz_core_init_error() -> *const c_char {
    INIT_ERROR.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|s| s.as_ptr())
            .unwrap_or(std::ptr::null())
    })
}

/// The last error recorded on `ctx`, or NULL. Borrowed until the next
/// failing call on the same context.
///
/// # Safety
/// `ctx` must be a live pointer from [`flz_core_context_new`].
#[no_mangle]
pub unsafe extern "C" fn flz_core_last_error(ctx: *const FlzContext) -> *const c_char {
    let Some(ctx) = ctx.as_ref() else {
        return std::ptr::null();
    };
    let guard = ctx.last_error.lock().unwrap_or_else(|p| p.into_inner());
    guard
        .as_ref()
        .map(|s| s.as_ptr())
        .unwrap_or(std::ptr::null())
}

/// Declare a host class from its JSON form — the complete host-class
/// declaration (`flz_core::host::HostClassDecl`): channels graph, cell
/// modes, clock kinds, delivery tier, correction window, header lang/format,
/// mailbox/retry/overflow policies. A declaration the core cannot honor is
/// refused with an error naming the field.
///
/// # Safety
/// `ctx` must be live; `json`/`json_len` must describe a valid UTF-8 buffer.
#[no_mangle]
pub unsafe extern "C" fn flz_core_declare_class(
    ctx: *mut FlzContext,
    json: *const u8,
    json_len: usize,
) -> i32 {
    let Some(ctx) = ctx.as_mut() else {
        return FLZ_ERR;
    };
    let bytes = std::slice::from_raw_parts(json, json_len);
    let factory_id = match serde_json::from_slice::<serde_json::Value>(bytes) {
        Ok(v) => v
            .get("factory_id")
            .and_then(|f| f.as_str())
            .map(str::to_string),
        Err(_) => None,
    };
    if let Err(e) = flz_core::host::register_host_class_json(bytes) {
        return ctx.fail(e.into());
    }
    if let Some(factory_id) = factory_id {
        ctx.declared
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .push(factory_id);
    }
    FLZ_OK
}

/// Judge a host class declaration without registering it: the read-only half
/// of [`flz_core_declare_class`]. On [`FLZ_OK`], `*buf_out`/`*len_out` hold
/// the DERIVED declaration as JSON — the consumers, channel kinds, sink names
/// and acknowledgement granularity the core read off the record. On
/// [`FLZ_ERR`] they hold the verdict as DATA — `{"issues":[{"name","text"}, …]}`
/// — so no caller parses prose to learn which refusals fired.
/// [`flz_core_last_error`] carries the same verdict rendered for a reader, and
/// the status is still [`FLZ_ERR`] so a caller that ignores the buffer fails
/// rather than registers. Free the buffer with [`flz_core_buf_free`].
///
/// This is what a conformance run submits: every SDK's builder can ask for
/// the verdict its author would get, with nothing registered and no runner.
///
/// # Safety
/// `ctx` must be live; `json`/`json_len` must describe a valid UTF-8 buffer;
/// `buf_out`/`len_out` must be valid writable pointers.
#[no_mangle]
pub unsafe extern "C" fn flz_core_check_class(
    ctx: *mut FlzContext,
    json: *const u8,
    json_len: usize,
    buf_out: *mut *mut u8,
    len_out: *mut usize,
) -> i32 {
    let Some(ctx) = ctx.as_mut() else {
        return FLZ_ERR;
    };
    let bytes = std::slice::from_raw_parts(json, json_len);
    match flz_core::host::check_class_json(bytes) {
        Ok(derived) => {
            hand_out(
                serde_json::to_vec(&derived).unwrap_or_default(),
                buf_out,
                len_out,
            );
            FLZ_OK
        }
        Err(issues) => {
            let verdict = serde_json::json!({
                "issues": issues
                    .iter()
                    .map(|issue| {
                        serde_json::json!({"name": issue.name(), "text": issue.to_string()})
                    })
                    .collect::<Vec<_>>(),
            });
            hand_out(
                serde_json::to_vec(&verdict).unwrap_or_default(),
                buf_out,
                len_out,
            );
            ctx.fail(flz_core::Error::Declaration(flz_core::host::render_issues(&issues)).into())
        }
    }
}

/// Connect to the controller and start the runner loop. Transport
/// configuration (socket path, `FLEZHA_TRANSPORT`) rides the same
/// environment the Rust SDK reads.
///
/// # Safety
/// `ctx` must be live; `namespace` must be a NUL-terminated string.
#[no_mangle]
pub unsafe extern "C" fn flz_core_start(ctx: *mut FlzContext, namespace: *const c_char) -> i32 {
    let Some(ctx) = ctx.as_mut() else {
        return FLZ_ERR;
    };
    let Ok(namespace) = CStr::from_ptr(namespace).to_str() else {
        return ctx.fail(Error::Utf8("namespace"));
    };
    if ctx.started.swap(true, Ordering::AcqRel) {
        return ctx.fail(Error::AlreadyStarted);
    }
    let (otx, orx) = crossbeam::channel::unbounded::<RunnerMessage>();
    let (confirm_tx, confirm_rx) = crossbeam::channel::unbounded::<ClientResponseMessage>();
    let bridge = flz_core::shutdown::ShutdownBridge::new();
    flz_core::shutdown::register_bridge(Arc::clone(&bridge));
    let _runner = flz_core::runner::start(
        namespace.to_string(),
        Arc::clone(&ctx.declared),
        (otx, orx),
        confirm_tx,
        bridge,
    );
    match confirm_rx.recv_timeout(Duration::from_secs(10)) {
        Ok(_) => FLZ_OK,
        Err(_) => ctx.fail(Error::ConnectUnconfirmed),
    }
}

/// Block for up to `timeout_ms` for one activation. On
/// [`FLZ_POLL_ACTIVATION`], `*buf_out`/`*len_out` hold the encoded
/// [`schema::Activation`]; free with [`flz_core_buf_free`].
///
/// # Safety
/// `ctx` must be live; `buf_out`/`len_out` must be valid writable pointers.
#[no_mangle]
pub unsafe extern "C" fn flz_core_poll(
    ctx: *mut FlzContext,
    timeout_ms: u64,
    buf_out: *mut *mut u8,
    len_out: *mut usize,
) -> i32 {
    let Some(ctx) = ctx.as_mut() else {
        return FLZ_ERR;
    };
    match flz_core::poll_core::global().poll(Duration::from_millis(timeout_ms)) {
        Polled::Timeout => FLZ_POLL_TIMEOUT,
        Polled::ShuttingDown => FLZ_POLL_SHUTTING_DOWN,
        Polled::Activation(act) => {
            let encoded = schema::encode_activation(&flz_core::host::to_schema(&act));
            let outstanding = Outstanding {
                activation: act,
                spawns: Vec::new(),
            };
            ctx.outstanding
                .insert(outstanding.activation.token, outstanding);
            hand_out(encoded, buf_out, len_out);
            FLZ_POLL_ACTIVATION
        }
    }
}

/// Block for up to `timeout_ms` until the core observes starvation: work has
/// been queued (or a deferred wake is due) with no thread inside `poll` and
/// the jobs-underway cap not reached. Answers in the same three-way shape as
/// [`flz_core_poll`] — [`FLZ_STARVATION_OBSERVED`], [`FLZ_POLL_TIMEOUT`], or
/// [`FLZ_POLL_SHUTTING_DOWN`] — so a supervisor loop ends with the runner.
///
/// For hosts whose fetch threads can be occupied by user code: they grow
/// their pool on an observation. A host that hands every activation to a
/// cheap concurrency primitive (virtual threads, goroutines, an event loop)
/// returns to `poll` immediately and never needs to call this.
///
/// # Safety
/// `ctx` must be a live pointer from [`flz_core_context_new`].
#[no_mangle]
pub unsafe extern "C" fn flz_core_wait_starvation(ctx: *mut FlzContext, timeout_ms: u64) -> i32 {
    if ctx.is_null() {
        return FLZ_ERR;
    }
    match flz_core::poll_core::global().wait_starvation(Duration::from_millis(timeout_ms)) {
        Starvation::Observed => FLZ_STARVATION_OBSERVED,
        Starvation::Timeout => FLZ_POLL_TIMEOUT,
        Starvation::ShuttingDown => FLZ_POLL_SHUTTING_DOWN,
    }
}

/// Complete an activation with an encoded [`schema::ActivationResult`].
/// Accepted from any thread, including one that never polled.
///
/// # Safety
/// `ctx` must be live; `buf`/`len` must describe a readable buffer.
#[no_mangle]
pub unsafe extern "C" fn flz_core_complete(
    ctx: *mut FlzContext,
    buf: *const u8,
    len: usize,
) -> i32 {
    let Some(ctx) = ctx.as_mut() else {
        return FLZ_ERR;
    };
    let bytes = std::slice::from_raw_parts(buf, len);
    let result = match schema::decode_result(bytes) {
        Ok(r) => r,
        Err(e) => return ctx.fail(Error::ResultDecode(e)),
    };
    let act = match ctx.take_outstanding(result.token) {
        Ok(act) => act,
        Err(status) => return status,
    };
    match flz_core::host::apply_result(flz_core::poll_core::global(), act, result) {
        Ok(()) => FLZ_OK,
        Err(e) => ctx.fail(e.into()),
    }
}

/// Abandon a polled activation: re-hand its untouched items and release its
/// cell, exactly as if it had never been polled. Accepted from any thread.
///
/// The third and last way to answer a handout, and the one the ABI must name
/// explicitly because **no unwind crosses this boundary**: a veneer whose host
/// language throws, or whose thread dies, cannot reach [`flz_core_complete`],
/// and per-cell exclusivity would then retire that cell for the life of
/// the process. A veneer SHALL answer every polled activation exactly once and
/// SHALL make that structural in its own language — a `finally`, a
/// destructor, a scope guard — not a discipline at each return site.
///
/// No retry-ladder attempt is consumed: an abandon is not a failure, it is
/// work that never ran. Only pass a token whose items the veneer did NOT
/// process; abandoning a partly-processed batch would execute those items
/// twice. An unknown or already-answered token is refused with
/// [`FLZ_ERR`] and the reason in [`flz_core_last_error`].
///
/// # Safety
/// `ctx` must be a live pointer from [`flz_core_context_new`].
#[no_mangle]
pub unsafe extern "C" fn flz_core_abandon(ctx: *mut FlzContext, token: u64) -> i32 {
    let Some(ctx) = ctx.as_mut() else {
        return FLZ_ERR;
    };
    let act = match ctx.take_outstanding(token) {
        Ok(act) => act,
        Err(status) => return status,
    };
    match flz_core::host::abandon_activation(flz_core::poll_core::global(), act) {
        Ok(()) => FLZ_OK,
        Err(e) => ctx.fail(e.into()),
    }
}

/// Ship every live operative's state as a `RunnerShutdownNotification`, then
/// wake every blocked poller with [`FLZ_POLL_SHUTTING_DOWN`] and stop the
/// runner loop. Unpolled work follows the existing durability rules (it is
/// journaled controller-side and re-forwarded on the next bind).
///
/// Returns at once. The notification serializes each Cell through the host,
/// which needs the caller's loops still polling, so the sequence runs on its
/// own thread and the core reports shutting down only once it is done. That
/// thread reads nothing of `ctx`, so freeing the context cannot race it. The
/// runner loop is not told anything: a stopping runner that reconnected would
/// be handed work it will never do, and the loop ends with the process.
///
/// # Safety
/// `ctx` must be live.
#[no_mangle]
pub unsafe extern "C" fn flz_core_shutdown(ctx: *mut FlzContext) {
    if ctx.is_null() {
        return;
    }
    let sequence = || {
        flz_core::shutdown::run_active_graceful_shutdown();
        flz_core::poll_core::global().shutdown();
    };
    if std::thread::Builder::new()
        .name("flz-shutdown".into())
        .spawn(sequence)
        .is_err()
    {
        eprintln!("[flezha] could not start the shutdown thread; stopping without shipping state");
        flz_core::poll_core::global().shutdown();
    }
}

/// Watch standard input and end the process when it reaches end of file, on a
/// thread the core owns.
///
/// A conformance harness stops a runner by closing its pipes rather than by
/// signalling it, and the thread that would notice is the host's own loop —
/// exactly the thread a compute-bound handler holds. One call installs the
/// core's watch, so no host reads descriptor 0 itself and the rule "a read
/// error is not end of file" is written once rather than once per host. The
/// watch reaches the exit through the same registered bridge the SIGTERM
/// handler does, and a healthy runner has `grace_ms` to drain first.
///
/// Idempotent: the second call installs nothing.
#[no_mangle]
pub extern "C" fn flz_core_watch_stdin_eof(grace_ms: u32) {
    flz_core::shutdown::watch_stdin_eof(grace_ms);
}

/// Ask for a workflow instance from the live activation `token`: start
/// `factory_id` under `id`, or under an id the controller generates when `id`
/// is empty. `if_running` (`"deliver"`, the default when empty, or `"error"`)
/// applies only to an id that is already running. A nonzero `replay` sends
/// nothing and resolves the handle to `skipped_on_replay` at once.
///
/// Never waits: `*handle_out` names the answer for [`flz_core_spawn_result`].
/// A refusal reaches only that read. [`FLZ_ERR`] means an unreadable argument
/// or a token that is not outstanding.
///
/// # Safety
/// `ctx` must be live; strings NUL-terminated; `payload`/`payload_len`
/// readable; `handle_out` writable.
#[no_mangle]
pub unsafe extern "C" fn flz_core_spawn_request(
    ctx: *mut FlzContext,
    token: u64,
    factory_id: *const c_char,
    id: *const c_char,
    payload: *const u8,
    payload_len: usize,
    if_running: *const c_char,
    replay: i32,
    handle_out: *mut u64,
) -> i32 {
    let Some(ctx) = ctx.as_ref() else {
        return FLZ_ERR;
    };
    let (Ok(factory_id), Ok(id), Ok(if_running)) = (
        CStr::from_ptr(factory_id).to_str(),
        CStr::from_ptr(id).to_str(),
        CStr::from_ptr(if_running).to_str(),
    ) else {
        return ctx.fail(Error::SpawnArgsUtf8);
    };
    let Some(if_running) = IfRunning::from_wire(if_running) else {
        return ctx.fail(Error::SpawnIfRunning(if_running.to_string()));
    };
    // Held until the handle is recorded, so the activation cannot complete in
    // between and leave the handle unreleased.
    let Some(mut outstanding) = ctx.outstanding.get_mut(&token) else {
        return ctx.fail(Error::UnknownToken(token));
    };
    let payload = if payload_len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(payload, payload_len).to_vec()
    };
    let options = (!id.is_empty()).then(|| SpawnOptions::id(id).if_running(if_running));
    let spawn = flz_core::spawn::request(factory_id.to_string(), payload, options, replay != 0);
    let handle = ctx.last_spawn.fetch_add(1, Ordering::Relaxed) + 1;
    ctx.spawns.insert(handle, Arc::new(Mutex::new(spawn)));
    outstanding.spawns.push(handle);
    *handle_out = handle;
    FLZ_OK
}

/// Read a spawn handle, waiting up to `timeout_ms` for the controller's answer;
/// a timeout of 0 never waits. [`FLZ_OK`]: `out` holds
/// `{"id","outcome"}`, the id the request resolved to and one of `spawned`,
/// `delivered`, `skipped_on_replay`. [`FLZ_SPAWN_FAILED`]: `out` holds
/// `{"id","error","message"}`, `error` one of `already_running`,
/// `factory_unknown`, `factory_not_workflow`, `refused`, `timeout`,
/// `unavailable`. A resolved handle
/// answers every later read the same. [`FLZ_SPAWN_PENDING`]: no answer yet,
/// `out` untouched. [`FLZ_ERR`]: the handle was released, because the
/// activation that made it was completed or abandoned. Free `out` with
/// [`flz_core_buf_free`].
///
/// A failure rides the buffer rather than [`flz_core_last_error`] because a
/// host raises one error per kind, and the last-error slot is shared by every
/// thread on the context.
///
/// # Safety
/// `ctx` must be live; `out`/`out_len` must be valid writable pointers.
#[no_mangle]
pub unsafe extern "C" fn flz_core_spawn_result(
    ctx: *mut FlzContext,
    handle: u64,
    timeout_ms: u64,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    let Some(ctx) = ctx.as_ref() else {
        return FLZ_ERR;
    };
    let Some(spawn) = ctx.spawns.get(&handle).map(|entry| Arc::clone(&entry)) else {
        return ctx.fail(Error::SpawnReleased(handle));
    };
    let mut held = match (timeout_ms, spawn.try_lock()) {
        (_, Ok(held)) => held,
        (_, Err(TryLockError::Poisoned(p))) => p.into_inner(),
        // Another thread's waiting read holds the handle, so no answer is in.
        (0, Err(TryLockError::WouldBlock)) => return FLZ_SPAWN_PENDING,
        (_, Err(TryLockError::WouldBlock)) => spawn.lock().unwrap_or_else(|p| p.into_inner()),
    };
    // A reader that waited for the lock may find the handle already released.
    if !ctx.spawns.contains_key(&handle) {
        return ctx.fail(Error::SpawnReleased(handle));
    }
    let Some(answer) = held.wait_timeout(Duration::from_millis(timeout_ms)) else {
        return FLZ_SPAWN_PENDING;
    };
    let (status, json) = match answer {
        Ok(result) => (
            FLZ_OK,
            serde_json::json!({"id": result.id, "outcome": result.outcome.as_wire()}),
        ),
        Err(error) => (
            FLZ_SPAWN_FAILED,
            serde_json::json!({
                "id": held.id(),
                "error": error.as_wire(),
                "message": error.to_string(),
            }),
        ),
    };
    hand_out(serde_json::to_vec(&json).unwrap_or_default(), out, out_len);
    status
}

/// Administer the namespace this runner's connection proved: one entry for the
/// whole surface, because the verb is data. `verb` names a function of the
/// controller's published consumer surface (`"Operative.start"`) and `args` is
/// a JSON array of that call's arguments other than the namespace, which the
/// controller fills from the connection. The published function's own answer
/// comes back as JSON in `result_out`/`result_len_out` (free with
/// [`flz_core_buf_free`]).
///
/// Adding a verb to the surface therefore costs nothing here and nothing in
/// any host's glue: it is one typed wrapper in the host's own language.
///
/// Blocks for the controller's answer.
///
/// # Safety
/// `ctx` must be live; strings NUL-terminated; buffers readable/writable.
#[no_mangle]
pub unsafe extern "C" fn flz_core_admin_call(
    ctx: *mut FlzContext,
    verb: *const c_char,
    args: *const c_char,
    result_out: *mut *mut u8,
    result_len_out: *mut usize,
) -> i32 {
    let Some(ctx) = ctx.as_mut() else {
        return FLZ_ERR;
    };
    let (Ok(verb), Ok(args)) = (CStr::from_ptr(verb).to_str(), CStr::from_ptr(args).to_str())
    else {
        return ctx.fail(Error::AdminArgsUtf8);
    };
    match flz_core::admin::call(verb, args) {
        Ok(result) => {
            hand_out(result.into_bytes(), result_out, result_len_out);
            FLZ_OK
        }
        Err(e) => ctx.fail(e.into()),
    }
}

/// The core's monotonic clock, in nanoseconds since an arbitrary process
/// origin: the one clock every host's `ctx` reads, so two hosts in one
/// process date an event the same way.
#[no_mangle]
pub extern "C" fn flz_core_monotonic_nanos() -> u64 {
    flz_core::util::monotonic_nanos()
}

/// Free a buffer returned by [`flz_core_poll`].
///
/// # Safety
/// `buf`/`len` must be exactly what a poll call handed out, freed once.
#[no_mangle]
pub unsafe extern "C" fn flz_core_buf_free(buf: *mut u8, len: usize) {
    if !buf.is_null() {
        drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(buf, len)));
    }
}

/// Destroy the context. Outstanding activations are dropped; their cells
/// release on the engine's drain-tail protocol at the next round.
///
/// # Safety
/// `ctx` must be a live pointer from [`flz_core_context_new`], freed once.
#[no_mangle]
pub unsafe extern "C" fn flz_core_context_free(ctx: *mut FlzContext) {
    if !ctx.is_null() {
        drop(Box::from_raw(ctx));
    }
}

/// Log-callback registration. The one host-function exception the contract
/// allows; wiring the tracing bridge onto it lands with the first shim that
/// consumes it — the entry point exists so the ABI surface is complete.
#[no_mangle]
pub extern "C" fn flz_core_set_log_callback(
    _callback: Option<extern "C" fn(level: i32, message: *const c_char)>,
) {
}
