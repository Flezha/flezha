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
//! # Failures: one error document
//!
//! Every failing entry answers its failing status ([`FLZ_ERR`], or NULL from
//! [`flz_core_context_new`]) and leaves the JSON error document
//! `{"error":{"code","class","message","details","hint"}}` in
//! [`flz_core_last_error`] (or [`flz_core_init_error`]). No entry hands a
//! failure out through a buffer of its own; a caller branches on `code` and
//! `class`, never on `message`.
//!
//! # Notices: the host's own log
//!
//! The core's notices reach the host through [`flz_core_set_log_callback`] or
//! [`flz_core_take_notices`]; with neither they go to standard error. This
//! crate installs no `tracing` subscriber: a veneer is loaded into a process
//! the application owns.

mod error;
#[cfg(feature = "fake-controller")]
pub mod fake_controller;
#[cfg(feature = "jni")]
pub mod jni_glue;
mod knob;
mod local;
mod notice;
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

use std::collections::HashMap;
use std::ffi::{c_char, CStr, CString};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, TryLockError};
use std::time::Duration;

use dashmap::DashMap;
pub use error::Error;
use flz_core::poll_core::{EngineActivation, Polled, Starvation};
use flz_core::spawn::{SpawnHandle, SpawnOptions};
use flz_core::{schema, ErrorModel};
/// The entry points the source-file splits hold, exported from the crate root
/// like every other C symbol: a module is not an ABI boundary.
pub use {knob::*, local::*, notice::*};

/// Poll produced an activation; the out buffer holds its encoded bytes.
pub const FLZ_POLL_ACTIVATION: i32 = 1;
/// The starvation wait observed starvation: the host owes a thread.
pub const FLZ_STARVATION_OBSERVED: i32 = 1;
/// Poll timed out with no work.
pub const FLZ_POLL_TIMEOUT: i32 = 0;
/// The context is shutting down; every blocked poller is released.
pub const FLZ_POLL_SHUTTING_DOWN: i32 = 2;
/// Failure; [`flz_core_last_error`] holds the JSON error document.
pub const FLZ_ERR: i32 = -1;
/// Success for non-poll calls.
pub const FLZ_OK: i32 = 0;
/// A spawn result read found no answer yet; the out buffer is untouched.
pub const FLZ_SPAWN_PENDING: i32 = 1;
/// A spawn read's wait that ends at the core's own bound for the request.
pub const FLZ_WAIT_CORE_BOUND: u64 = u64::MAX;
/// [`flz_core_effect_begin`]: run the function, then answer
/// [`flz_core_effect_end`] or [`flz_core_effect_release`].
pub const FLZ_EFFECT_RUN: i32 = 0;
/// [`flz_core_effect_begin`]: the out buffer holds the kept result; the
/// function does not run. [`flz_core_effect_end`]: the out buffer holds the
/// result kept first, which the call returns instead of its own.
pub const FLZ_EFFECT_KEPT: i32 = 1;
/// [`flz_core_effect_begin`]: nothing re-runs this invocation, so nothing is
/// kept; run the function, return its result and answer
/// [`flz_core_effect_release`].
pub const FLZ_EFFECT_UNRECORDED: i32 = 2;

/// What a call's place in its invocation is counted by: spawns by offset,
/// effects by work item and name.
#[derive(Hash, PartialEq, Eq)]
enum Counted {
    Spawn(i64),
    Effect(u32, String),
}

/// An effect whose function is running, by work item.
struct Running {
    name: String,
    identity: String,
    offset: Option<u64>,
}

/// A polled activation the host has not answered, with the spawn handles its
/// tasklets made: they live exactly as long as it does.
struct Outstanding {
    activation: EngineActivation,
    spawns: Vec<u64>,
    /// Calls made so far per [`Counted`] key: a spawn's position and an
    /// effect's ordinal in the invocation.
    positions: HashMap<Counted, u32>,
    /// The effects whose functions are running, by work item. A list rather
    /// than a map: built for every activation, it must cost nothing until an
    /// effect runs.
    effects: Vec<(u32, Running)>,
    /// The next part number per work item: counted per invocation attempt,
    /// and a retry is handed out in an activation of its own.
    part_sequences: HashMap<u32, u32>,
}

/// Opaque context handle.
pub struct FlzContext {
    outstanding: DashMap<u64, Outstanding>,
    last_error: Mutex<Option<CString>>,
    session: flz_core::session::Session,
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
    static RENDERED: std::cell::RefCell<Option<CString>> = const { std::cell::RefCell::new(None) };
}

/// The JSON error document a failing entry leaves behind. JSON escapes every
/// control character, so the document never holds the NUL a C string ends at.
fn error_document(error: &Error) -> CString {
    CString::new(error.to_error().json()).unwrap_or_default()
}

fn set_init_error(error: Error) {
    INIT_ERROR.with(|slot| {
        *slot.borrow_mut() = Some(error_document(&error));
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

impl Outstanding {
    /// The effect running in `item`, which no longer is.
    fn take_effect(&mut self, item: u32) -> Option<Running> {
        let at = self
            .effects
            .iter()
            .position(|(running, _)| *running == item)?;
        Some(self.effects.swap_remove(at).1)
    }

    /// The next call's place among those counted under `counted`.
    fn next_position(&mut self, counted: Counted) -> u32 {
        let position = self.positions.entry(counted).or_insert(0);
        let next = *position;
        *position += 1;
        next
    }
}

impl FlzContext {
    /// Record `error` as the context's last error; the status every failing
    /// entry point returns. The one place an [`Error`] becomes a C string.
    fn fail(&self, error: Error) -> i32 {
        *self.last_error.lock().unwrap_or_else(|p| p.into_inner()) = Some(error_document(&error));
        FLZ_ERR
    }

    /// [`FLZ_OK`], or the refusal recorded as by [`Self::fail`].
    fn status<E: Into<Error>>(&self, result: Result<(), E>) -> i32 {
        result.map_or_else(|error| self.fail(error.into()), |()| FLZ_OK)
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
/// was built against; a mismatch returns NULL and [`flz_core_init_error`]
/// holds `runtime.schema_mismatch`, naming both versions. No controller
/// connection is attempted here.
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
        session: Default::default(),
        local: Mutex::new(None),
        local_outputs: Mutex::new(None),
        spawns: DashMap::new(),
        last_spawn: AtomicU64::new(0),
    }))
}

/// The JSON error document of the failure that made the last
/// [`flz_core_context_new`] on THIS thread return NULL, or NULL. Borrowed
/// until the next failing creation.
#[no_mangle]
pub extern "C" fn flz_core_init_error() -> *const c_char {
    INIT_ERROR.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|s| s.as_ptr())
            .unwrap_or(std::ptr::null())
    })
}

/// The JSON error document of the last failure recorded on `ctx`, or NULL.
/// Borrowed until the next failing call on the same context.
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

/// The JSON error document the catalogue renders for `code` with its details
/// filled from `details_json`: the refusal a host makes itself, reading as the
/// core's own. `Err` names the argument that is not a JSON object.
pub fn render_error_document(code: &str, details_json: &str) -> Result<String, String> {
    match serde_json::from_str::<serde_json::Value>(details_json) {
        Ok(serde_json::Value::Object(details)) => {
            Ok(flz_core::FlezhaError::new(code, details).json())
        }
        _ => Err("error details must be a JSON object".to_string()),
    }
}

/// [`render_error_document`] for a host with no glue of its own, or NULL when
/// an argument is not UTF-8 or the details are not a JSON object. Borrowed
/// until the next call on THIS thread.
///
/// # Safety
/// `code` and `details_json` must be valid NUL-terminated strings.
#[no_mangle]
pub unsafe extern "C" fn flz_core_error_document(
    code: *const c_char,
    details_json: *const c_char,
) -> *const c_char {
    if code.is_null() || details_json.is_null() {
        return std::ptr::null();
    }
    let (Ok(code), Ok(details)) = (
        CStr::from_ptr(code).to_str(),
        CStr::from_ptr(details_json).to_str(),
    ) else {
        return std::ptr::null();
    };
    let Ok(document) = render_error_document(code, details) else {
        return std::ptr::null();
    };
    RENDERED.with(|slot| {
        let mut slot = slot.borrow_mut();
        *slot = CString::new(document).ok();
        slot.as_ref().map_or(std::ptr::null(), |s| s.as_ptr())
    })
}

/// Declare a host class from its JSON form — the complete host-class
/// declaration (`flz_core::host::HostClassDecl`): channels graph, cell
/// modes, clock kinds, delivery tier, correction window, header lang/format,
/// mailbox/retry/overflow policies. A declaration the core cannot honor is
/// refused with the same error document [`flz_core_check_class`] leaves.
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
    let factory_id = serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|v| v.get("factory_id")?.as_str().map(str::to_string))
        .unwrap_or_default();
    let register = || flz_core::host::register_host_class_json(bytes).map_err(Error::from);
    ctx.status(ctx.session.declare(&factory_id, register))
}

/// Undeclare the class registered under `factory_id`, the inverse of
/// [`flz_core_declare_class`]. After start the runner's coordinator checks for
/// live instances, serialized with the controller's binds, and refuses with
/// `runtime.relay_type_in_use` while any exist; otherwise the controller is
/// sent the remaining factory set. Blocks for that round trip.
///
/// # Safety
/// `ctx` must be live; `factory_id` must be a NUL-terminated string.
#[no_mangle]
pub unsafe extern "C" fn flz_core_undeclare_class(
    ctx: *mut FlzContext,
    factory_id: *const c_char,
) -> i32 {
    let Some(ctx) = ctx.as_mut() else {
        return FLZ_ERR;
    };
    let Ok(factory_id) = CStr::from_ptr(factory_id).to_str() else {
        return ctx.fail(Error::Utf8("factory_id"));
    };
    ctx.status(ctx.session.undeclare(factory_id))
}

/// Judge a host class declaration without registering it: the read-only half
/// of [`flz_core_declare_class`]. On [`FLZ_OK`], `*buf_out`/`*len_out` hold
/// the DERIVED declaration as JSON — the consumers, channel kinds, sink names
/// and acknowledgement granularity the core read off the record; free it with
/// [`flz_core_buf_free`]. On [`FLZ_ERR`] the out pointers are untouched and
/// [`flz_core_last_error`] holds the refusal: its code is the first issue's
/// `topology.*` code, and `details.issues` lists every issue that fired as
/// `{code, message, details}`, so no caller parses prose to learn which
/// refusals fired.
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
            ctx.fail(flz_core::Error::Declaration(flz_core::host::render_issues(&issues)).into())
        }
    }
}

/// Connect to the controller and start the runner loop. Transport
/// configuration (socket path, `FLEZHA_TRANSPORT`) rides the same
/// environment the Rust SDK reads. A connect the controller refuses fails
/// with the controller's own error (for example `runner.namespace_not_proven`).
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
    start_runner(ctx, namespace, Vec::new())
}

/// [`flz_core_start`], with the connection settings handed over as a JSON
/// object instead of read from the environment: `socket_files_path`,
/// `transport`, `quic_addresses`, `ca_cert_path`, `quic_client_cert`,
/// `quic_client_key` and `namespace_secret`, each a string standing in for
/// its `FLEZHA_*` variable. A key left out resolves as `flz_core_start`
/// resolves it. An unknown key or a value that is not a string is refused
/// before anything connects. The process environment is never written.
///
/// # Safety
/// `ctx` must be live; `namespace` must be a NUL-terminated string;
/// `settings`/`settings_len` must describe a readable buffer.
#[no_mangle]
pub unsafe extern "C" fn flz_core_start_with(
    ctx: *mut FlzContext,
    namespace: *const c_char,
    settings: *const u8,
    settings_len: usize,
) -> i32 {
    let Some(ctx) = ctx.as_mut() else {
        return FLZ_ERR;
    };
    let Ok(namespace) = CStr::from_ptr(namespace).to_str() else {
        return ctx.fail(Error::Utf8("namespace"));
    };
    let settings = if settings.is_null() {
        &[][..]
    } else {
        std::slice::from_raw_parts(settings, settings_len)
    };
    match connect_settings(settings) {
        Ok(handed_over) => start_runner(ctx, namespace, handed_over),
        Err(refusal) => ctx.fail(refusal),
    }
}

/// The settings document as `(variable, value)` pairs.
fn connect_settings(bytes: &[u8]) -> Result<Vec<(&'static str, String)>, Error> {
    let refused = |setting: &str, reason| Error::ConnectSetting {
        setting: setting.to_string(),
        reason,
    };
    let document: serde_json::Map<String, serde_json::Value> = serde_json::from_slice(bytes)
        .map_err(|_| refused("", "the settings are not a JSON object"))?;
    document
        .into_iter()
        .map(|(key, value)| {
            let var = flz_transport::env::CONNECT_SETTINGS
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, var)| *var)
                .ok_or_else(|| refused(&key, "not a connection setting"))?;
            match value {
                serde_json::Value::String(value) => Ok((var, value)),
                _ => Err(refused(&key, "its value is not a string")),
            }
        })
        .collect()
}

fn start_runner(
    ctx: &mut FlzContext,
    namespace: &str,
    handed_over: Vec<(&'static str, String)>,
) -> i32 {
    if flz_core::poll_core::global().is_shut_down() {
        return ctx.fail(Error::ShutDown);
    }
    ctx.status(ctx.session.start(namespace, handed_over))
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
                positions: HashMap::new(),
                effects: Vec::new(),
                part_sequences: HashMap::new(),
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

/// Ship every live relay's state as a `RunnerShutdownNotification`, then
/// wake every blocked poller with [`FLZ_POLL_SHUTTING_DOWN`] and stop the
/// runner loop. Unpolled work follows the existing durability rules (it is
/// journaled controller-side and re-forwarded on the next bind).
///
/// Returns at once. The notification serializes each Cell through the host,
/// which needs the caller's loops still polling, so the sequence runs on its
/// own thread and the core reports shutting down only once it is done. That
/// thread reads nothing of `ctx`, so freeing the context cannot race it. The
/// session ends last: a stopping runner that reconnected would be handed work
/// it will never do.
///
/// # Safety
/// `ctx` must be live.
#[no_mangle]
pub unsafe extern "C" fn flz_core_shutdown(ctx: *mut FlzContext) {
    if ctx.is_null() {
        return;
    }
    let sequence = || {
        flz_core::shutdown::exit(flz_core::shutdown::Exit {
            wait: Some(Duration::ZERO),
            abandon: true,
            ..Default::default()
        })
    };
    if std::thread::Builder::new()
        .name("flz-shutdown".into())
        .spawn(sequence)
        .is_err()
    {
        flz_transport::notice!(Error, "no shutdown thread: stopping without shipping state");
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
/// watch ends the runner as the SIGTERM handler does, with the same grace.
///
/// Idempotent: the second call installs nothing.
#[no_mangle]
pub extern "C" fn flz_core_watch_stdin_eof() {
    flz_core::shutdown::watch_stdin_eof();
}

/// Ask for a steps instance from the live activation `token`: start
/// `factory_id` under `id`, or under an id the controller generates when `id`
/// is empty. `if_running` (`"deliver"`, the default when empty, or `"error"`)
/// applies only to an id that is already running. A nonzero `replay` sends
/// nothing and resolves the handle to `skipped_on_replay` at once. `offset`
/// is the input-log offset of the item the calling tasklet is taking (`-1`
/// for none): with the Cell and the call's position it names the request, so
/// a re-run of that item resolves to what the first run's request did.
///
/// Never waits: `*handle_out`'s answer, a controller's refusal too, is [`flz_core_spawn_result`]'s.
/// [`FLZ_ERR`], sending nothing: bad UTF-8, `if_running` without `id`, or an unknown token.
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
    offset: i64,
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
        return ctx.fail(Error::Utf8("spawn arguments"));
    };
    let options = match SpawnOptions::from_wire(id, if_running) {
        Ok(options) => options,
        Err(refusal) => return ctx.fail(Error::Refused(refusal)),
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
    let origin_position = outstanding.next_position(Counted::Spawn(offset));
    let origin = flz_core::origin::InvocationOrigin {
        cell_identity: outstanding.activation.cell_identity(),
        offset: u64::try_from(offset).ok(),
    };
    let spawn = flz_core::spawn::request(
        factory_id.to_string(),
        payload,
        options,
        replay != 0,
        origin,
        origin_position,
    );
    let handle = ctx.last_spawn.fetch_add(1, Ordering::Relaxed) + 1;
    ctx.spawns.insert(handle, Arc::new(Mutex::new(spawn)));
    outstanding.spawns.push(handle);
    *handle_out = handle;
    FLZ_OK
}

/// Begin `ctx.effect(name)` in `item`, the index of a work item of the live
/// activation `token` (any index for a compensation). [`FLZ_EFFECT_KEPT`]: the
/// out buffer holds the result kept for this call, which the host decodes and
/// returns without running the function; free it with [`flz_core_buf_free`].
/// [`FLZ_EFFECT_RUN`]: run the function, then answer [`flz_core_effect_end`]
/// with its encoded result, or [`flz_core_effect_release`] when it raised.
/// [`FLZ_EFFECT_UNRECORDED`]: the invocation took no input-log offset, so the
/// host runs the function, returns its result and answers
/// [`flz_core_effect_release`]. [`FLZ_ERR`]: `effect.bad_name`,
/// `effect.nested` while the item's function is still running, or
/// `effect.outside_invocation` for a token that is not outstanding.
///
/// # Safety
/// `ctx` must be live; `name` NUL-terminated; `out`/`out_len` writable.
#[no_mangle]
pub unsafe extern "C" fn flz_core_effect_begin(
    ctx: *mut FlzContext,
    token: u64,
    item: u32,
    name: *const c_char,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    use flz_core::effect_result;
    let Some(ctx) = ctx.as_ref() else {
        return FLZ_ERR;
    };
    let Ok(name) = CStr::from_ptr(name).to_str() else {
        return ctx.fail(Error::Utf8("effect name"));
    };
    if let Err(refusal) = effect_result::check_name(name) {
        return ctx.fail(Error::Refused(refusal));
    }
    let Some(mut outstanding) = ctx.outstanding.get_mut(&token) else {
        return ctx.fail(Error::Refused(effect_result::outside_invocation(name)));
    };
    if outstanding
        .effects
        .iter()
        .any(|(running, _)| *running == item)
    {
        return ctx.fail(Error::Refused(effect_result::nested(name)));
    }
    let Some((offset, phase, attempt)) = outstanding.activation.invocation(item as usize) else {
        return ctx.fail(Error::Refused(effect_result::outside_invocation(name)));
    };
    let counted = Counted::Effect(item, name.to_string());
    let ordinal = outstanding.positions.get(&counted).copied().unwrap_or(0);
    let origin = flz_core::origin::InvocationOrigin {
        cell_identity: outstanding.activation.cell_identity(),
        offset,
    };
    let resolved = effect_result::resolve(origin, phase, name, ordinal, attempt);
    outstanding.positions.insert(counted, resolved.ordinal + 1);
    let identity = resolved.identity;
    if let Some(result) = resolved.kept {
        hand_out(result, out, out_len);
        return FLZ_EFFECT_KEPT;
    }
    let status = if identity.is_empty() {
        FLZ_EFFECT_UNRECORDED
    } else {
        FLZ_EFFECT_RUN
    };
    let running = Running {
        name: name.to_string(),
        identity,
        offset,
    };
    outstanding.effects.push((item, running));
    status
}

/// Keep the encoded result of the function an [`FLZ_EFFECT_RUN`] begin ran,
/// waiting for the controller to keep it. [`FLZ_OK`]: the host returns its own
/// result. [`FLZ_EFFECT_KEPT`]: the out buffer holds the result the controller
/// kept first for this call, which the host decodes and returns instead; free
/// it with [`flz_core_buf_free`]. [`FLZ_ERR`]: `effect.too_large`,
/// `effect.not_recorded`, or `effect.outside_invocation` for an item with no
/// effect begun.
///
/// # Safety
/// `ctx` must be live; `payload`/`payload_len` readable (`payload` may be null
/// when `payload_len` is 0); `out`/`out_len` writable.
#[no_mangle]
pub unsafe extern "C" fn flz_core_effect_end(
    ctx: *mut FlzContext,
    token: u64,
    item: u32,
    payload: *const u8,
    payload_len: usize,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    let Some(ctx) = ctx.as_ref() else {
        return FLZ_ERR;
    };
    let taken = ctx.outstanding.get_mut(&token).and_then(|mut outstanding| {
        let running = outstanding.take_effect(item)?;
        Some((running, outstanding.activation.cell_identity().to_vec()))
    });
    let Some((running, cell)) = taken else {
        return ctx.fail(Error::Refused(flz_core::effect_result::outside_invocation(
            "",
        )));
    };
    let payload = if payload_len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(payload, payload_len).to_vec()
    };
    let origin = flz_core::origin::InvocationOrigin {
        cell_identity: &cell,
        offset: running.offset,
    };
    match flz_core::effect_result::keep(origin, &running.name, running.identity, payload) {
        Ok(None) => FLZ_OK,
        Ok(Some(first)) => {
            hand_out(first, out, out_len);
            FLZ_EFFECT_KEPT
        }
        Err(refusal) => ctx.fail(Error::Refused(refusal)),
    }
}

/// End an effect begun in `item` without keeping anything: its function raised,
/// or the begin answered [`FLZ_EFFECT_UNRECORDED`]. A token or item with no
/// effect begun is a no-op, so a host may answer it from a `finally`.
///
/// # Safety
/// `ctx` must be live.
#[no_mangle]
pub unsafe extern "C" fn flz_core_effect_release(
    ctx: *mut FlzContext,
    token: u64,
    item: u32,
) -> i32 {
    let Some(ctx) = ctx.as_ref() else {
        return FLZ_ERR;
    };
    if let Some(mut outstanding) = ctx.outstanding.get_mut(&token) {
        outstanding.take_effect(item);
    }
    FLZ_OK
}

/// Send a part of the invocation of `item`, the index of a work item of the
/// live activation `token`, with the payload the host encoded as that
/// tasklet's emit. Never waits. [`FLZ_OK`] for a part that left and for one
/// the drop rules dropped; [`FLZ_ERR`] with `part.no_output`, or with
/// `part.outside_invocation` for a token that is not outstanding or an item
/// its activation does not hold.
///
/// # Safety
/// `ctx` must be live; `payload`/`payload_len` readable.
#[no_mangle]
pub unsafe extern "C" fn flz_core_part(
    ctx: *mut FlzContext,
    token: u64,
    item: u32,
    payload: *const u8,
    payload_len: usize,
) -> i32 {
    let Some(ctx) = ctx.as_ref() else {
        return FLZ_ERR;
    };
    let Some(mut outstanding) = ctx.outstanding.get_mut(&token) else {
        return ctx.fail(flz_core::part::outside_invocation(None).into());
    };
    let Outstanding {
        activation,
        part_sequences,
        ..
    } = &mut *outstanding;
    let Some((slot, work)) = activation.host_item(item as usize) else {
        return ctx.fail(flz_core::part::outside_invocation(None).into());
    };
    let payload = if payload_len == 0 {
        &[][..]
    } else {
        std::slice::from_raw_parts(payload, payload_len)
    };
    let sequence = part_sequences.entry(item).or_insert(0);
    match flz_core::part::send(slot, work, sequence, payload) {
        Ok(_) => FLZ_OK,
        Err(refusal) => ctx.fail(refusal.into()),
    }
}

/// The pending timers of the Cell an outstanding work activation runs, as the
/// core keeps them. A restore hands a host its state and not its timers, so a
/// host that keeps its own copy reads them here once after one. On [`FLZ_OK`],
/// `*buf_out`/`*len_out` hold the postcard `Vec<TimerOp>`, each a
/// registration; free with [`flz_core_buf_free`].
///
/// # Safety
/// `ctx` must be live; `buf_out`/`len_out` must be valid writable pointers.
#[no_mangle]
pub unsafe extern "C" fn flz_core_pending_timers(
    ctx: *mut FlzContext,
    token: u64,
    buf_out: *mut *mut u8,
    len_out: *mut usize,
) -> i32 {
    let Some(ctx) = ctx.as_ref() else {
        return FLZ_ERR;
    };
    let ops = match ctx.outstanding.get(&token).as_deref() {
        Some(Outstanding {
            activation:
                EngineActivation {
                    payload: flz_core::poll_core::EnginePayload::Work { slot, .. },
                    ..
                },
            ..
        }) => {
            use flz_core::util::LockRecover;
            slot.timers.lock_recover().as_register_ops()
        }
        _ => return ctx.fail(Error::UnknownToken(token)),
    };
    hand_out(
        postcard::to_stdvec(&ops).expect("timer ops encode"),
        buf_out,
        len_out,
    );
    FLZ_OK
}

/// Read a spawn handle, waiting up to `timeout_ms` (0 never waits,
/// [`FLZ_WAIT_CORE_BOUND`] to the request's deadline) for the controller's
/// answer. [`FLZ_OK`]: `out` holds `{"id","outcome"}`, the id the request
/// resolved to and `spawned`, `delivered` or `skipped_on_replay`; free it with
/// [`flz_core_buf_free`]. [`FLZ_SPAWN_PENDING`]: no answer yet, `out` untouched.
/// [`FLZ_ERR`]: `out` untouched and [`flz_core_last_error`] holds the
/// controller's code (`relay.already_running` or any other refusal's own), the
/// core's (`spawn.timeout`, `spawn.unavailable`) or `runtime.spawn_handle_released`
/// once the handle's activation ended. A resolved handle answers every later
/// read the same.
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
    match answer {
        Ok(result) => {
            let json = serde_json::json!({"id": result.id, "outcome": result.outcome.as_wire()});
            hand_out(serde_json::to_vec(&json).unwrap_or_default(), out, out_len);
            FLZ_OK
        }
        Err(error) => ctx.fail(Error::Spawn(error)),
    }
}

/// Administer the namespace this runner's connection proved: one entry for the
/// whole surface, because the verb is data. `verb` names a function of the
/// controller's published consumer surface (`"Relay.start"`) and `args` is
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
        return ctx.fail(Error::Utf8("administration arguments"));
    };
    match flz_core::admin::call(verb, args) {
        Ok(result) => {
            hand_out(result.into_bytes(), result_out, result_len_out);
            FLZ_OK
        }
        Err(e) => ctx.fail(e.into()),
    }
}

/// Send `message` to the entry channel `channel` of `relay`, as
/// [`flz_core::admin::send`] does: through the controller, answering its
/// verdict as JSON in `result_out`/`result_len_out` (free with
/// [`flz_core_buf_free`]), or with `local` nonzero in process to a relay
/// bound on this runner, answering `null`. An empty `msg_id` or `key` is none.
/// Blocks for the controller's answer, or while a local consumer's mailbox is
/// full.
///
/// # Safety
/// `ctx` must be live; strings NUL-terminated; `message`/`len` a readable
/// slice (`message` may be null when `len` is 0); buffers writable.
#[no_mangle]
pub unsafe extern "C" fn flz_core_send_message(
    ctx: *mut FlzContext,
    relay: *const c_char,
    channel: *const c_char,
    message: *const u8,
    len: usize,
    msg_id: *const c_char,
    key: *const c_char,
    local: i32,
    result_out: *mut *mut u8,
    result_len_out: *mut usize,
) -> i32 {
    let Some(ctx) = ctx.as_ref() else {
        return FLZ_ERR;
    };
    let (Ok(relay), Ok(channel), Ok(msg_id), Ok(key)) = (
        CStr::from_ptr(relay).to_str(),
        CStr::from_ptr(channel).to_str(),
        CStr::from_ptr(msg_id).to_str(),
        CStr::from_ptr(key).to_str(),
    ) else {
        return ctx.fail(Error::Utf8("send arguments"));
    };
    let message = if len == 0 {
        &[][..]
    } else {
        std::slice::from_raw_parts(message, len)
    };
    let (msg_id, key) = (
        (!msg_id.is_empty()).then_some(msg_id),
        (!key.is_empty()).then_some(key),
    );
    match flz_core::admin::send(relay, message, channel, msg_id, key, local != 0) {
        Ok(answer) => {
            hand_out(answer.into_bytes(), result_out, result_len_out);
            FLZ_OK
        }
        Err(e) => ctx.fail(e.into()),
    }
}

/// The ids of the relay instances bound on this process's runner, as a
/// JSON array of strings in `buf_out`/`len_out` (free with
/// [`flz_core_buf_free`]). Never contacts the controller.
///
/// # Safety
/// `ctx` must be live; `buf_out`/`len_out` must be writable.
#[no_mangle]
pub unsafe extern "C" fn flz_core_relays(
    ctx: *mut FlzContext,
    buf_out: *mut *mut u8,
    len_out: *mut usize,
) -> i32 {
    if ctx.is_null() {
        return FLZ_ERR;
    }
    let ids = flz_core::runner::bound().ids();
    hand_out(
        serde_json::to_vec(&ids).unwrap_or_default(),
        buf_out,
        len_out,
    );
    FLZ_OK
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

#[cfg(all(test, unix))]
mod tests {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixListener;

    use flz_core::pb;
    use flz_transport::errors::FlezhaError;

    use super::*;

    /// A host's own refusal renders as the catalogue says; details that are
    /// not a JSON object render nothing.
    #[test]
    fn a_host_refusal_renders_through_the_catalogue() {
        let code = CString::new("runtime.identity_too_long").unwrap();
        let details = CString::new(r#"{"part":"tasklet name","value":"t","limit":255}"#).unwrap();
        let document = unsafe { flz_core_error_document(code.as_ptr(), details.as_ptr()) };
        let document = unsafe { CStr::from_ptr(document) }
            .to_string_lossy()
            .into_owned();
        let error = FlezhaError::from_document(&document).expect("an error document");
        assert_eq!(error.code, "runtime.identity_too_long");
        assert_eq!(
            error.message,
            "the tasklet name 't' is longer than 255 bytes"
        );

        let list = CString::new("[]").unwrap();
        assert!(unsafe { flz_core_error_document(code.as_ptr(), list.as_ptr()) }.is_null());
    }

    /// A refused check leaves one error document naming every issue, and
    /// hands nothing out through the buffer.
    #[test]
    fn a_refused_check_names_every_issue_in_the_error_document() {
        let decl = br#"{"factory_id":"ffi_check","sdk_lang":1,"sdk_version":"0.1.0",
            "durable":{"false":{"sleep_after":1000}},"tasklets":[],"channels":[]}"#;
        let ctx = flz_core_context_new(flz_core::SCHEMA_VERSION);
        let mut buf: *mut u8 = std::ptr::null_mut();
        let mut len = 0;
        let status =
            unsafe { flz_core_check_class(ctx, decl.as_ptr(), decl.len(), &mut buf, &mut len) };

        assert_eq!(status, FLZ_ERR);
        assert!(buf.is_null(), "a refusal rides last_error alone");
        let document = unsafe { CStr::from_ptr(flz_core_last_error(ctx)) }
            .to_string_lossy()
            .into_owned();
        let error = FlezhaError::from_document(&document).expect("an error document");
        let codes: Vec<&str> = error.details["issues"]
            .as_array()
            .expect("details.issues")
            .iter()
            .filter_map(|issue| issue["code"].as_str())
            .collect();
        assert_eq!(codes, ["topology.no_tasklets", "topology.unknown_field"]);
        assert_eq!(error.code, codes[0], "the first issue names the refusal");
        unsafe { flz_core_context_free(ctx) };
    }

    static NOTICES: Mutex<Vec<String>> = Mutex::new(Vec::new());

    extern "C" fn noted(_level: i32, text: *const c_char) {
        // SAFETY: the core hands a NUL-terminated text borrowed for the call.
        let text = unsafe { CStr::from_ptr(text) }
            .to_string_lossy()
            .into_owned();
        NOTICES.lock().unwrap().push(text);
    }

    /// A connect the controller refuses fails the start with the
    /// controller's own error. The runner is process-global, so this is the
    /// only start this test binary makes.
    #[test]
    fn a_refused_connect_fails_the_start_with_the_controllers_error() {
        let dir = std::env::temp_dir().join(format!("flz-ffi-refused-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("socket dir");
        std::env::set_var("FLEZHA_TRANSPORT", "uds");
        std::env::set_var("FLEZHA_SOCKET_FILES_PATH", &dir);
        std::env::set_var("FLEZHA_RUNNER_ID_PATH", dir.join("runner.id"));
        let path = flz_transport::utility::socket_dir::control_socket_path_in(&dir)
            .expect("control socket path");
        let listener = UnixListener::bind(path).expect("bind the control socket");

        let refusal = FlezhaError::new("runner.namespace_not_proven", Default::default());
        let frame = flz_core::command::framed_request(
            String::new(),
            pb::command_request::Command::ConnectRefused(pb::ConnectRefused {
                error: Some(pb::Error::from(&refusal)),
            }),
        );
        let controller = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("the runner connects");
            let _ = stream.read(&mut [0u8; 4096]);
            stream.write_all(&frame).expect("write the refusal");
        });

        flz_core_set_log_callback(Some(noted));
        let ctx = flz_core_context_new(flz_core::SCHEMA_VERSION);
        let namespace = CString::new("unproven").expect("a namespace");
        let status = unsafe { flz_core_start(ctx, namespace.as_ptr()) };
        controller.join().expect("the controller answered");
        let said = "the controller refused the connect: error: ";
        assert!(NOTICES.lock().unwrap().iter().any(|n| n.starts_with(said)));

        assert_eq!(status, FLZ_ERR, "a refused connect is not a started runner");
        let document = unsafe { CStr::from_ptr(flz_core_last_error(ctx)) }
            .to_string_lossy()
            .into_owned();
        assert_eq!(FlezhaError::from_document(&document), Some(refusal));
    }
}
