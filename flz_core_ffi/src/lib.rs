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

#[cfg(feature = "jni")]
mod jni_glue;
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

use std::ffi::{c_char, CStr, CString};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use crossbeam::channel::Sender;
use dashmap::DashMap;
use flz_core::poll_core::{EngineActivation, Polled, Starvation};
use flz_core::runner::{ClientResponseMessage, RunnerMessage};
use flz_core::schema;

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

/// Opaque context handle.
pub struct FlzContext {
    outstanding: DashMap<u64, EngineActivation>,
    last_error: Mutex<Option<CString>>,
    started: AtomicBool,
    runner_tx: Mutex<Option<Sender<RunnerMessage>>>,
    declared: Arc<RwLock<Vec<String>>>,
}

thread_local! {
    static INIT_ERROR: std::cell::RefCell<Option<CString>> = const { std::cell::RefCell::new(None) };
}

fn set_init_error(msg: String) {
    INIT_ERROR.with(|slot| {
        *slot.borrow_mut() = Some(CString::new(msg).unwrap_or_default());
    });
}

impl FlzContext {
    fn fail(&self, msg: impl Into<String>) -> i32 {
        *self.last_error.lock().unwrap_or_else(|p| p.into_inner()) =
            Some(CString::new(msg.into()).unwrap_or_default());
        FLZ_ERR
    }

    /// Take a polled activation by token. `Err` carries the status the entry
    /// point returns, with the reason already recorded for `last_error` — an
    /// unknown token means the veneer answered twice, or answered one it never
    /// held, and either is a defect to surface rather than a no-op.
    fn take_outstanding(&self, token: u64) -> Result<EngineActivation, i32> {
        self.outstanding
            .remove(&token)
            .map(|(_, act)| act)
            .ok_or_else(|| self.fail(format!("unknown activation token {token}")))
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
        set_init_error(format!(
            "activation schema version mismatch: shim speaks {}, core speaks {}",
            shim_schema_version,
            flz_core::SCHEMA_VERSION
        ));
        return std::ptr::null_mut();
    }
    Box::into_raw(Box::new(FlzContext {
        outstanding: DashMap::new(),
        last_error: Mutex::new(None),
        started: AtomicBool::new(false),
        runner_tx: Mutex::new(None),
        declared: Arc::new(RwLock::new(Vec::new())),
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
/// declaration (`flz_core::host::HostClassDecl`): channels graph, entity
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
        return ctx.fail(e);
    }
    if let Some(factory_id) = factory_id {
        ctx.declared
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .push(factory_id);
    }
    FLZ_OK
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
        return ctx.fail("namespace is not valid UTF-8");
    };
    if ctx.started.swap(true, Ordering::AcqRel) {
        return ctx.fail("context already started");
    }
    let (otx, orx) = crossbeam::channel::unbounded::<RunnerMessage>();
    let (confirm_tx, confirm_rx) = crossbeam::channel::unbounded::<ClientResponseMessage>();
    let bridge = flz_core::shutdown::ShutdownBridge::new();
    flz_core::shutdown::register_bridge(Arc::clone(&bridge));
    let _runner = flz_core::runner::start(
        namespace.to_string(),
        Arc::clone(&ctx.declared),
        (otx.clone(), orx),
        confirm_tx,
        bridge,
    );
    *ctx.runner_tx.lock().unwrap_or_else(|p| p.into_inner()) = Some(otx);
    match confirm_rx.recv_timeout(Duration::from_secs(10)) {
        Ok(_) => FLZ_OK,
        Err(_) => ctx.fail("controller connect not confirmed within 10s"),
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
            ctx.outstanding.insert(act.token, act);
            let mut bytes = encoded.into_boxed_slice();
            *buf_out = bytes.as_mut_ptr();
            *len_out = bytes.len();
            std::mem::forget(bytes);
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
        Err(e) => return ctx.fail(format!("activation result failed to decode: {e}")),
    };
    let act = match ctx.take_outstanding(result.token) {
        Ok(act) => act,
        Err(status) => return status,
    };
    match flz_core::host::apply_result(flz_core::poll_core::global(), act, result) {
        Ok(()) => FLZ_OK,
        Err(e) => ctx.fail(e),
    }
}

/// Abandon a polled activation: re-hand its untouched items and release its
/// entity, exactly as if it had never been polled. Accepted from any thread.
///
/// The third and last way to answer a handout, and the one the ABI must name
/// explicitly because **no unwind crosses this boundary**: a veneer whose host
/// language throws, or whose thread dies, cannot reach [`flz_core_complete`],
/// and per-entity exclusivity would then retire that entity for the life of
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
        Err(e) => ctx.fail(e),
    }
}

/// Wake every blocked poller with [`FLZ_POLL_SHUTTING_DOWN`] and stop the
/// runner loop. Unpolled work follows the existing durability rules (it is
/// journaled controller-side and re-forwarded on the next bind).
///
/// # Safety
/// `ctx` must be live.
#[no_mangle]
pub unsafe extern "C" fn flz_core_shutdown(ctx: *mut FlzContext) {
    let Some(ctx) = ctx.as_mut() else { return };
    flz_core::poll_core::global().shutdown();
    if let Some(tx) = ctx
        .runner_tx
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take()
    {
        let _ = tx.send(RunnerMessage::Reconnect("controller_requested"));
        drop(tx);
    }
}

/// Emit one output payload from a live workflow run-sugar body — the ABI form
/// of `WorkflowCtx::emit`. Best-effort and at-least-once, like operative
/// output.
///
/// # Safety
/// `ctx` must be live; `instance_id` NUL-terminated; `buf`/`len` readable.
#[no_mangle]
pub unsafe extern "C" fn flz_core_workflow_emit(
    ctx: *mut FlzContext,
    instance_id: *const c_char,
    buf: *const u8,
    len: usize,
) -> i32 {
    let Some(ctx) = ctx.as_mut() else {
        return FLZ_ERR;
    };
    let Ok(instance_id) = CStr::from_ptr(instance_id).to_str() else {
        return ctx.fail("instance id is not valid UTF-8");
    };
    let bytes = std::slice::from_raw_parts(buf, len).to_vec();
    flz_core::runner::emit_workflow_output(instance_id.to_string(), bytes);
    FLZ_OK
}

/// Block up to `timeout_ms` for the next inbox item of a live workflow
/// run-sugar body — the ABI form of `WorkflowCtx::recv`. On
/// [`FLZ_POLL_ACTIVATION`] the payload is in `buf_out`/`len_out` (free with
/// [`flz_core_buf_free`]); [`FLZ_POLL_TIMEOUT`] = no item yet; `FLZ_ERR` =
/// no live body / inbox closed.
///
/// # Safety
/// `ctx` must be live; `instance_id` NUL-terminated; out pointers writable.
#[no_mangle]
pub unsafe extern "C" fn flz_core_workflow_recv(
    ctx: *mut FlzContext,
    instance_id: *const c_char,
    timeout_ms: u64,
    buf_out: *mut *mut u8,
    len_out: *mut usize,
) -> i32 {
    let Some(ctx) = ctx.as_mut() else {
        return FLZ_ERR;
    };
    let Ok(instance_id) = CStr::from_ptr(instance_id).to_str() else {
        return ctx.fail("instance id is not valid UTF-8");
    };
    match flz_core::host::workflow_session_recv(instance_id, Duration::from_millis(timeout_ms)) {
        Ok(Some(payload)) => {
            let mut bytes = payload.into_boxed_slice();
            *buf_out = bytes.as_mut_ptr();
            *len_out = bytes.len();
            std::mem::forget(bytes);
            FLZ_POLL_ACTIVATION
        }
        Ok(None) => FLZ_POLL_TIMEOUT,
        Err(e) => ctx.fail(e),
    }
}

/// 1 once the named workflow body's cancellation token tripped (external stop
/// / `max_duration`) — the ABI form of `WorkflowCtx::is_cancelled`. Also 1
/// for an unknown instance (a finished body is as cancelled as it gets).
///
/// # Safety
/// `instance_id` must be NUL-terminated.
#[no_mangle]
pub unsafe extern "C" fn flz_core_workflow_cancelled(
    _ctx: *const FlzContext,
    instance_id: *const c_char,
) -> i32 {
    let Ok(instance_id) = CStr::from_ptr(instance_id).to_str() else {
        return 1;
    };
    i32::from(flz_core::host::workflow_session_cancelled(instance_id))
}

/// Spawn-or-deliver a workflow instance from a live host procedure — the ABI
/// form of `ctx.spawn_workflow`. `if_running` is the wire string
/// (`"deliver"` | `"error"`). Blocks for the controller's outcome (one of
/// `spawned | delivered | already_running | ...`), returned in
/// `outcome_out`/`outcome_len_out` (free with [`flz_core_buf_free`]).
///
/// # Safety
/// `ctx` must be live; strings NUL-terminated; buffers readable/writable.
#[no_mangle]
pub unsafe extern "C" fn flz_core_spawn_workflow(
    ctx: *mut FlzContext,
    instance_id: *const c_char,
    factory_id: *const c_char,
    payload: *const u8,
    payload_len: usize,
    if_running: *const c_char,
    outcome_out: *mut *mut u8,
    outcome_len_out: *mut usize,
) -> i32 {
    let Some(ctx) = ctx.as_mut() else {
        return FLZ_ERR;
    };
    let (Ok(instance_id), Ok(factory_id), Ok(if_running)) = (
        CStr::from_ptr(instance_id).to_str(),
        CStr::from_ptr(factory_id).to_str(),
        CStr::from_ptr(if_running).to_str(),
    ) else {
        return ctx.fail("spawn arguments are not valid UTF-8");
    };
    let payload = std::slice::from_raw_parts(payload, payload_len).to_vec();
    let reference_id = format!(
        "spawn-workflow-{}-{}",
        instance_id,
        flz_core::util::nonce_suffix()
    );
    // Register before sending: the reply can then never arrive before its
    // waiter exists.
    let rx = flz_core::shutdown::register_pending_ack(reference_id.clone());
    let sent = flz_core::runner::coordinator_try_send(
        flz_core::runner::RunnerMessage::EmitControlRequest {
            reference_id: reference_id.clone(),
            command: flz_core::pb::command_request::Command::SpawnWorkflowRequest(
                flz_core::pb::SpawnWorkflowRequest {
                    instance_id: instance_id.to_string(),
                    factory_id: factory_id.to_string(),
                    payload,
                    if_running: if_running.to_string(),
                },
            ),
        },
    );
    if !sent {
        flz_core::shutdown::drop_pending_ack(&reference_id);
        return ctx.fail("no control plane: spawn request could not be routed");
    }
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(Ok(outcome)) => {
            let mut bytes = outcome.into_bytes().into_boxed_slice();
            *outcome_out = bytes.as_mut_ptr();
            *outcome_len_out = bytes.len();
            std::mem::forget(bytes);
            FLZ_OK
        }
        Ok(Err(refusal)) => ctx.fail(refusal),
        Err(_) => {
            flz_core::shutdown::drop_pending_ack(&reference_id);
            ctx.fail("spawn request not answered within 30s")
        }
    }
}

/// The core's monotonic clock, in nanoseconds since an arbitrary process
/// origin. The reading [`flz_core_entry_last_delivery_nanos`] is measured
/// against — a host derives an idle gap by subtracting one from the other.
#[no_mangle]
pub extern "C" fn flz_core_monotonic_nanos() -> u64 {
    flz_core::util::monotonic_nanos()
}

/// When entry channel `channel` last received a frame, on the clock
/// [`flz_core_monotonic_nanos`] reads. `0` means no frame has ever been
/// delivered to it — which is also the answer for a channel no declared
/// operative has.
///
/// # Safety
/// `channel` must be a NUL-terminated string.
#[no_mangle]
pub unsafe extern "C" fn flz_core_entry_last_delivery_nanos(channel: *const c_char) -> u64 {
    let Ok(channel) = CStr::from_ptr(channel).to_str() else {
        return 0;
    };
    flz_core::entry_input::observation_of(channel)
        .and_then(|cell| cell.last_delivery_nanos())
        .unwrap_or(0)
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

/// Destroy the context. Outstanding activations are dropped; their entities
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
