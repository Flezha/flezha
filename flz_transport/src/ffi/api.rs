//! C FFI entry points for libflezha.
//!
//! All functions follow these conventions:
//! - Return `FlezhaStatus` (except `flezha_last_error`, `flezha_version`, `flezha_config_default`)
//! - On failure, set thread-local error string retrievable via `flezha_last_error()`
//! - Opaque `FlezhaContext*` pointer — caller must not inspect internals
//! - Whoever allocates, frees: library buffers freed via `flezha_frame_free`

use std::ffi::CStr;
use std::os::raw::c_char;

use super::config::{FlezhaConfig, FlezhaConfigOwned};
use super::context::FlezhaContext;
use super::error::set_last_error;
use super::types::*;

// ---------------------------------------------------------------------------
// Version
// ---------------------------------------------------------------------------

static VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "\0");

/// Return the library version as a null-terminated string.
/// The returned pointer is valid for the lifetime of the process.
#[no_mangle]
pub extern "C" fn flezha_version() -> *const c_char {
    VERSION.as_ptr() as *const c_char
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// Create a new context. Returns null on failure (check `flezha_last_error`).
///
/// # Safety
/// - `config` string pointers must be valid C strings or null.
/// - The returned pointer must eventually be passed to `flezha_context_destroy`.
#[no_mangle]
pub unsafe extern "C" fn flezha_context_new(config: FlezhaConfig) -> *mut FlezhaContext {
    let owned = FlezhaConfigOwned::from_c(&config);
    let ctx = Box::new(FlezhaContext::new(owned));
    Box::into_raw(ctx)
}

/// Destroy a context and free all associated resources.
///
/// # Safety
/// - `ctx` must be a pointer returned by `flezha_context_new`.
/// - Must not be called concurrently with other procedures on the same context.
/// - After this call, `ctx` is invalid and must not be used.
#[no_mangle]
pub unsafe extern "C" fn flezha_context_destroy(ctx: *mut FlezhaContext) -> FlezhaStatus {
    if ctx.is_null() {
        set_last_error("null context pointer");
        return FlezhaStatus::InvalidArgument;
    }
    drop(Box::from_raw(ctx));
    FlezhaStatus::Ok
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

/// Register a logging callback on the context.
///
/// # Safety
/// - `ctx` must be a valid context pointer.
/// - `callback` may be called from any library-managed thread.
/// - `user_data` is passed through to the callback unchanged.
#[no_mangle]
pub unsafe extern "C" fn flezha_set_log_callback(
    ctx: *mut FlezhaContext,
    callback: FlezhaLogFn,
    user_data: *mut std::ffi::c_void,
) -> FlezhaStatus {
    if ctx.is_null() {
        set_last_error("null context pointer");
        return FlezhaStatus::InvalidArgument;
    }
    let ctx = &*ctx;
    match ctx.log_state.lock() {
        Ok(mut state) => {
            state.callback = callback;
            state.user_data = user_data;
            FlezhaStatus::Ok
        }
        Err(_) => {
            set_last_error("log state lock poisoned");
            FlezhaStatus::InternalError
        }
    }
}

// ---------------------------------------------------------------------------
// Payloader (standalone, no context needed)
// ---------------------------------------------------------------------------

/// Build a binary frame from payload ID, message bytes, and optional JSON metadata.
///
/// On success, writes the frame to `*out` with length `*out_len`.
/// The caller must free the returned buffer via `flezha_frame_free`.
///
/// # Safety
/// - `payload_id` must be a valid C string.
/// - `message` must point to `msg_len` valid bytes (or be null if `msg_len` is 0).
/// - `metadata_json` may be null (treated as empty metadata).
/// - `out` and `out_len` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn flezha_frame_build(
    payload_id: *const c_char,
    message: *const u8,
    msg_len: usize,
    metadata_json: *const c_char,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> FlezhaStatus {
    if payload_id.is_null() || out.is_null() || out_len.is_null() {
        set_last_error("null pointer argument");
        return FlezhaStatus::InvalidArgument;
    }

    let pid = match CStr::from_ptr(payload_id).to_str() {
        Ok(s) => s,
        Err(_) => {
            set_last_error("payload_id is not valid UTF-8");
            return FlezhaStatus::InvalidArgument;
        }
    };

    let msg_slice = if message.is_null() {
        &[]
    } else {
        std::slice::from_raw_parts(message, msg_len)
    };

    // Parse optional JSON metadata
    let metadata = if metadata_json.is_null() {
        std::collections::HashMap::new()
    } else {
        let json_str = match CStr::from_ptr(metadata_json).to_str() {
            Ok(s) => s,
            Err(_) => {
                set_last_error("metadata_json is not valid UTF-8");
                return FlezhaStatus::InvalidArgument;
            }
        };
        if json_str.is_empty() {
            std::collections::HashMap::new()
        } else {
            match serde_json::from_str(json_str) {
                Ok(m) => m,
                Err(e) => {
                    set_last_error(format!("metadata_json parse error: {}", e));
                    return FlezhaStatus::InvalidArgument;
                }
            }
        }
    };

    let frame = crate::service::payloader::build_from_binary(pid, msg_slice, metadata);

    let mut boxed = frame.into_boxed_slice();
    *out_len = boxed.len();
    *out = boxed.as_mut_ptr();
    std::mem::forget(boxed);

    FlezhaStatus::Ok
}

/// Free a buffer previously returned by `flezha_frame_build`.
///
/// # Safety
/// - `data` must have been returned by `flezha_frame_build`.
/// - `len` must match the `out_len` from that call.
/// - Must only be called once per buffer.
#[no_mangle]
pub unsafe extern "C" fn flezha_frame_free(data: *mut u8, len: usize) -> FlezhaStatus {
    if data.is_null() {
        return FlezhaStatus::Ok; // freeing null is a no-op
    }
    let _ = Vec::from_raw_parts(data, len, len);
    FlezhaStatus::Ok
}
