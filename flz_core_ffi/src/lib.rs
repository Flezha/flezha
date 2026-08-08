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

use std::ffi::{c_char, CStr, CString};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use crossbeam::channel::Sender;
use dashmap::DashMap;
use flz_core::poll_core::{EngineActivation, Polled};
use flz_core::runner::{ClientResponseMessage, RunnerMessage};
use flz_core::schema;

/// Poll produced an activation; the out buffer holds its encoded bytes.
pub const FLZ_POLL_ACTIVATION: i32 = 1;
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

/// Declare a host class from its JSON form:
/// `{"factory_id": "...", "schema_version": 1, "persistent": false,
///   "procedures": [{"name": "...", "entry_channel": "...",
///                   "mailbox_capacity": 1024, "retry_max": 3,
///                   "retry_backoff_ms": 50, "output_plugin": null}]}`
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

    #[derive(serde::Deserialize)]
    struct ProcJson {
        name: String,
        entry_channel: String,
        #[serde(default = "default_mailbox_capacity")]
        mailbox_capacity: usize,
        #[serde(default = "default_retry_max")]
        retry_max: u32,
        #[serde(default = "default_retry_backoff_ms")]
        retry_backoff_ms: u64,
        #[serde(default)]
        output_plugin: Option<String>,
    }
    fn default_mailbox_capacity() -> usize {
        1024
    }
    fn default_retry_max() -> u32 {
        flz_core::channel::RetryPolicy::default().max_attempts
    }
    fn default_retry_backoff_ms() -> u64 {
        flz_core::channel::RetryPolicy::default().backoff_base_ms
    }
    #[derive(serde::Deserialize)]
    struct ClassJson {
        factory_id: String,
        #[serde(default = "default_schema_version")]
        schema_version: u32,
        #[serde(default)]
        persistent: bool,
        procedures: Vec<ProcJson>,
    }
    fn default_schema_version() -> u32 {
        1
    }

    let parsed: ClassJson = match serde_json::from_slice(bytes) {
        Ok(p) => p,
        Err(e) => return ctx.fail(format!("class declaration is not valid JSON: {e}")),
    };
    let factory_id = parsed.factory_id.clone();
    let decl = flz_core::host::HostClassDecl {
        factory_id: parsed.factory_id,
        schema_version: parsed.schema_version,
        persistent: parsed.persistent,
        procedures: parsed
            .procedures
            .into_iter()
            .map(|p| flz_core::host::HostProcedureDecl {
                name: p.name,
                entry_channel: p.entry_channel,
                retry: flz_core::channel::RetryPolicy {
                    max_attempts: p.retry_max,
                    backoff_base_ms: p.retry_backoff_ms,
                },
                mailbox_capacity: p.mailbox_capacity,
                output_plugin: p.output_plugin,
            })
            .collect(),
    };
    if let Err(e) = flz_core::host::register_host_class(decl) {
        return ctx.fail(e);
    }
    ctx.declared
        .write()
        .unwrap_or_else(|p| p.into_inner())
        .push(factory_id);
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
    let _runner = flz_core::fast::start(
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
    let Some((_, act)) = ctx.outstanding.remove(&result.token) else {
        return ctx.fail(format!("unknown activation token {}", result.token));
    };
    match flz_core::host::apply_result(flz_core::poll_core::global(), act, result) {
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
