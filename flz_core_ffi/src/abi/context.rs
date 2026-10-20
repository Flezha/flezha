//! The context's life, its error documents, and the classes it declares.

use std::ffi::{c_char, CStr, CString};
use std::sync::atomic::AtomicU64;
use std::sync::Mutex;

use dashmap::DashMap;

use crate::{hand_out, set_init_error, Error, FlzContext, FLZ_OK, INIT_ERROR, RENDERED};

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
        pool: Mutex::new(None),
    }))
}

/// The JSON error document of the last context-free failure on THIS thread,
/// a [`flz_core_context_new`] that returned NULL or a refused
/// [`flz_core_response_decode`], or NULL. Borrowed until the next one.
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
    live!(mut ctx);
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
    live!(mut ctx);
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
    live!(mut ctx);
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
