//! The language-neutral half of a binding: context creation, the last-error
//! read, the out-buffer hand-off, the typed calls and the refusal shape. A
//! binding adds only how its language builds and reads a value, and how a
//! [`Refusal`] becomes its exception.
//!
//! Every typed call goes through the `extern "C"` entry point — never the
//! Rust behind it — so the last-error slot is written exactly as PHP, which
//! takes the C ABI as a C ABI, sees it. A buffer the core hands out is read
//! over the borrowed slice and freed before the call returns; no native
//! buffer reaches a host heap.

pub mod shape;

use std::ffi::{c_char, CStr, CString};
use std::fmt;

use flz_core::schema;

use crate::{
    FlzContext, FLZ_OK, FLZ_POLL_ACTIVATION, FLZ_POLL_SHUTTING_DOWN, FLZ_POLL_TIMEOUT,
    FLZ_STARVATION_OBSERVED,
};

/// What a binding can raise: the context's last error, a creation-time
/// schema mismatch, or a value the binding could not map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    Core(String),
    Schema(String),
    Shape(String),
}

impl Refusal {
    pub fn message(&self) -> &str {
        match self {
            Refusal::Core(m) | Refusal::Schema(m) | Refusal::Shape(m) => m,
        }
    }

    pub fn into_message(self) -> String {
        match self {
            Refusal::Core(m) | Refusal::Schema(m) | Refusal::Shape(m) => m,
        }
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for Refusal {}

/// The context pointer, carried to whichever thread makes the call. The ABI's
/// own contract is that any host thread may call any entry point.
#[derive(Clone, Copy, Debug)]
pub struct CtxPtr(*mut FlzContext);

unsafe impl Send for CtxPtr {}

impl CtxPtr {
    pub fn of(ctx: *mut FlzContext) -> Self {
        Self(ctx)
    }

    /// A method rather than a public field, so a closure capturing it takes
    /// the whole `Send` wrapper — edition 2021 captures `ctx.0` by field,
    /// which is the bare pointer and is not.
    pub fn get(self) -> *mut FlzContext {
        self.0
    }
}

fn read_c_str(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    // SAFETY: a non-null pointer from the ABI is a NUL-terminated string that
    // lives until the next failing call on the same context or thread.
    Some(
        unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned(),
    )
}

/// Create a context, or the refusal `flz_core_init_error` recorded.
pub fn context_new(shim_schema_version: u32) -> Result<*mut FlzContext, Refusal> {
    let ctx = crate::flz_core_context_new(shim_schema_version);
    if ctx.is_null() {
        let message = read_c_str(crate::flz_core_init_error())
            .unwrap_or_else(|| "flz_core context creation failed".to_string());
        return Err(Refusal::Schema(message));
    }
    Ok(ctx)
}

pub fn context_free(ctx: CtxPtr) {
    unsafe { crate::flz_core_context_free(ctx.get()) }
}

/// The context's last recorded error, if any.
pub fn last_error(ctx: CtxPtr) -> Option<String> {
    read_c_str(unsafe { crate::flz_core_last_error(ctx.get()) })
}

/// The context's last recorded error, as the message of the exception a
/// failing call raises.
pub fn last_error_of(ctx: CtxPtr) -> String {
    last_error(ctx).unwrap_or_else(|| "unknown native failure".to_string())
}

/// A non-poll status as a result: `FLZ_OK`, or the context's last error.
pub fn verdict(ctx: CtxPtr, status: i32) -> Result<(), Refusal> {
    if status == FLZ_OK {
        Ok(())
    } else {
        Err(Refusal::Core(last_error_of(ctx)))
    }
}

pub fn c_string(what: &str, value: &str) -> Result<CString, Refusal> {
    CString::new(value).map_err(|_| Refusal::Shape(format!("{what} contains a NUL byte")))
}

/// Run one entry point that hands out a buffer, and read what it handed out.
///
/// `handed_out` is the status that means a buffer was produced —
/// [`FLZ_POLL_ACTIVATION`] for poll and the workflow receive, [`FLZ_OK`] for
/// spawn; every other status leaves the out pointers untouched. `read` runs
/// over the borrowed slice BEFORE `flz_core_buf_free`, so no caller copies
/// first.
pub fn with_buffer<T>(
    handed_out: i32,
    call: impl FnOnce(*mut *mut u8, *mut usize) -> i32,
    read: impl FnOnce(&[u8]) -> T,
) -> (i32, Option<T>) {
    let mut buf: *mut u8 = std::ptr::null_mut();
    let mut len: usize = 0;
    let status = call(&mut buf, &mut len);
    if status != handed_out {
        return (status, None);
    }
    // SAFETY: the entry point handed out `len` bytes at `buf`, owned by the
    // core until freed below, and nothing else reads them meanwhile.
    let value = read(unsafe { std::slice::from_raw_parts(buf, len) });
    unsafe { crate::flz_core_buf_free(buf, len) };
    (status, Some(value))
}

/// The three answers of a poll-shaped entry point.
#[derive(Debug)]
pub enum Polled<T> {
    Activation(T),
    Timeout,
    ShuttingDown,
}

fn core(ctx: CtxPtr) -> Refusal {
    Refusal::Core(last_error_of(ctx))
}

/// Block up to `timeout_ms` for one activation, decoded in place.
pub fn poll(ctx: CtxPtr, timeout_ms: u64) -> Result<Polled<schema::Activation>, Refusal> {
    let (status, decoded) = with_buffer(
        FLZ_POLL_ACTIVATION,
        |buf, len| unsafe { crate::flz_core_poll(ctx.get(), timeout_ms, buf, len) },
        schema::decode_activation,
    );
    let decoded = decoded
        .transpose()
        .map_err(|e| Refusal::Shape(format!("activation failed to decode: {e}")))?;
    match (status, decoded) {
        (FLZ_POLL_ACTIVATION, Some(act)) => Ok(Polled::Activation(act)),
        (FLZ_POLL_TIMEOUT, _) => Ok(Polled::Timeout),
        (FLZ_POLL_SHUTTING_DOWN, _) => Ok(Polled::ShuttingDown),
        _ => Err(core(ctx)),
    }
}

/// Park up to `timeout_ms` for a starvation observation; answers the ABI's
/// three-way status (`FLZ_STARVATION_OBSERVED`, `FLZ_POLL_TIMEOUT`,
/// `FLZ_POLL_SHUTTING_DOWN`) so a supervisor loop allocates nothing per park.
pub fn wait_starvation(ctx: CtxPtr, timeout_ms: u64) -> Result<i32, Refusal> {
    match unsafe { crate::flz_core_wait_starvation(ctx.get(), timeout_ms) } {
        status @ (FLZ_STARVATION_OBSERVED | FLZ_POLL_TIMEOUT | FLZ_POLL_SHUTTING_DOWN) => {
            Ok(status)
        }
        _ => Err(core(ctx)),
    }
}

pub fn complete(ctx: CtxPtr, result: &schema::ActivationResult) -> Result<(), Refusal> {
    let encoded = schema::encode_result(result);
    let status = unsafe { crate::flz_core_complete(ctx.get(), encoded.as_ptr(), encoded.len()) };
    verdict(ctx, status)
}

pub fn abandon(ctx: CtxPtr, token: u64) -> Result<(), Refusal> {
    verdict(ctx, unsafe { crate::flz_core_abandon(ctx.get(), token) })
}

pub fn shutdown(ctx: CtxPtr) {
    unsafe { crate::flz_core_shutdown(ctx.get()) }
}

pub fn declare_class(ctx: CtxPtr, json: &[u8]) -> Result<(), Refusal> {
    let status = unsafe { crate::flz_core_declare_class(ctx.get(), json.as_ptr(), json.len()) };
    verdict(ctx, status)
}

pub fn start(ctx: CtxPtr, namespace: &CStr) -> Result<(), Refusal> {
    verdict(ctx, unsafe {
        crate::flz_core_start(ctx.get(), namespace.as_ptr())
    })
}

pub fn workflow_emit(ctx: CtxPtr, instance: &CStr, payload: &[u8]) -> Result<(), Refusal> {
    let status = unsafe {
        crate::flz_core_workflow_emit(
            ctx.get(),
            instance.as_ptr(),
            payload.as_ptr(),
            payload.len(),
        )
    };
    verdict(ctx, status)
}

/// The next inbox item of a live workflow body; `None` on timeout.
pub fn workflow_recv(
    ctx: CtxPtr,
    instance: &CStr,
    timeout_ms: u64,
) -> Result<Option<Vec<u8>>, Refusal> {
    let (status, payload) = with_buffer(
        FLZ_POLL_ACTIVATION,
        |buf, len| unsafe {
            crate::flz_core_workflow_recv(ctx.get(), instance.as_ptr(), timeout_ms, buf, len)
        },
        <[u8]>::to_vec,
    );
    match (status, payload) {
        (FLZ_POLL_ACTIVATION, Some(bytes)) => Ok(Some(bytes)),
        (FLZ_POLL_TIMEOUT, _) => Ok(None),
        _ => Err(core(ctx)),
    }
}

pub fn workflow_cancelled(ctx: CtxPtr, instance: &CStr) -> bool {
    unsafe { crate::flz_core_workflow_cancelled(ctx.get(), instance.as_ptr()) != 0 }
}

/// Spawn-or-deliver a workflow instance; the controller's outcome string.
pub fn spawn_workflow(
    ctx: CtxPtr,
    instance: &CStr,
    factory: &CStr,
    payload: &[u8],
    if_running: &CStr,
) -> Result<String, Refusal> {
    let (status, outcome) = with_buffer(
        FLZ_OK,
        |out, out_len| unsafe {
            crate::flz_core_spawn_workflow(
                ctx.get(),
                instance.as_ptr(),
                factory.as_ptr(),
                payload.as_ptr(),
                payload.len(),
                if_running.as_ptr(),
                out,
                out_len,
            )
        },
        |bytes| String::from_utf8_lossy(bytes).into_owned(),
    );
    verdict(ctx, status)?;
    Ok(outcome.unwrap_or_default())
}

pub fn entry_last_delivery_nanos(channel: &CStr) -> u64 {
    unsafe { crate::flz_core_entry_last_delivery_nanos(channel.as_ptr()) }
}

/// Register an episode latch over `channel`; `0` means the name was refused.
pub fn idle_watch_new(channel: &CStr, threshold_nanos: u64) -> u64 {
    unsafe { crate::flz_core_idle_watch_new(channel.as_ptr(), threshold_nanos) }
}

/// Advance a watch's latch: the ABI's status and the episode's gap in
/// nanoseconds, `0` when there is no episode.
pub fn idle_watch_poll(id: u64) -> (i32, u64) {
    let mut gap = 0u64;
    let status = unsafe { crate::flz_core_idle_watch_poll(id, &mut gap) };
    (status, gap)
}

pub fn idle_watch_free(id: u64) {
    crate::flz_core_idle_watch_free(id)
}
