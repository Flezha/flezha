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
    FLZ_SPAWN_FAILED, FLZ_SPAWN_PENDING, FLZ_STARVATION_OBSERVED,
};

/// The entry points that can park the calling thread. A host whose unit of
/// concurrency is bound to one thread keeps every one of these off it.
pub const BLOCKING_ENTRY_POINTS: &[&str] = &[
    "flz_core_start",
    "flz_core_poll",
    "flz_core_wait_starvation",
    "flz_core_complete",
    // With a nonzero timeout. A read with a timeout of 0 never waits, so a
    // host may make that one on any thread.
    "flz_core_spawn_result",
    "flz_core_admin_call",
    // A local run parks the caller in all three: `feed` while the consumer's
    // mailbox is full, and the other two by their own budget. The host's poll
    // loop is what drains them, so a host that parked its only dispatch thread
    // in one of these would wait on work it has stopped doing.
    "flz_core_local_feed",
    "flz_core_local_next_output",
    "flz_core_local_run_until_idle",
];

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
/// `hands_out` is every status that means a buffer was produced —
/// [`FLZ_POLL_ACTIVATION`] for poll and the bounded operator receive, [`FLZ_OK`] and
/// [`FLZ_SPAWN_FAILED`] for the spawn result, both of [`FLZ_OK`] and
/// [`FLZ_ERR`] for the topology check, whose answer is a verdict either way;
/// every other status leaves the out pointers untouched. `read` runs over the
/// borrowed slice BEFORE `flz_core_buf_free`, so no caller copies first.
pub fn with_buffer<T>(
    hands_out: &[i32],
    call: impl FnOnce(*mut *mut u8, *mut usize) -> i32,
    read: impl FnOnce(&[u8]) -> T,
) -> (i32, Option<T>) {
    let mut buf: *mut u8 = std::ptr::null_mut();
    let mut len: usize = 0;
    let status = call(&mut buf, &mut len);
    if !hands_out.contains(&status) {
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
        &[FLZ_POLL_ACTIVATION],
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

/// Ask the core to watch standard input and end the process on end of file.
/// The watch is the core's, on a thread no host's virtual machine owns, so a
/// host carries no reader of descriptor 0 of its own.
pub fn watch_stdin_eof(grace_ms: u32) {
    crate::flz_core_watch_stdin_eof(grace_ms)
}

pub fn declare_class(ctx: CtxPtr, json: &[u8]) -> Result<(), Refusal> {
    let status = unsafe { crate::flz_core_declare_class(ctx.get(), json.as_ptr(), json.len()) };
    verdict(ctx, status)
}

/// Ask for a declaration's verdict without registering it. `Ok` carries the
/// derived declaration as JSON, `Err` the refusal the core rendered — the
/// veneer's `validate()` is this call and nothing else.
pub fn check_class(ctx: CtxPtr, json: &[u8]) -> Result<Vec<u8>, (Refusal, Vec<u8>)> {
    let (status, answer) = with_buffer(
        &[crate::FLZ_OK, crate::FLZ_ERR],
        |buf, len| unsafe {
            crate::flz_core_check_class(ctx.get(), json.as_ptr(), json.len(), buf, len)
        },
        <[u8]>::to_vec,
    );
    let answer = answer.unwrap_or_default();
    // The refusal carries BOTH halves: the rendered message a reader gets, and
    // the verdict a caller reads names off. Handing back only one made every
    // binding fetch the other itself.
    verdict(ctx, status)
        .map_err(|refusal| (refusal, answer.clone()))
        .map(|()| answer)
}

pub fn start(ctx: CtxPtr, namespace: &CStr) -> Result<(), Refusal> {
    verdict(ctx, unsafe {
        crate::flz_core_start(ctx.get(), namespace.as_ptr())
    })
}

/// Start the declared class as a local run: no controller, no socket. The
/// host keeps its own poll loop, which is what runs the tasklets.
pub fn local_start(ctx: CtxPtr, factory_id: &CStr, operator_id: &CStr) -> Result<(), Refusal> {
    verdict(ctx, unsafe {
        crate::flz_core_local_start(ctx.get(), factory_id.as_ptr(), operator_id.as_ptr())
    })
}

/// Queue the sink named `channel` for [`local_next_output`].
pub fn local_on_sink(ctx: CtxPtr, channel: &CStr) -> Result<(), Refusal> {
    verdict(ctx, unsafe {
        crate::flz_core_local_on_sink(ctx.get(), channel.as_ptr())
    })
}

/// Feed one value's wire bytes to a declared entry channel.
pub fn local_feed(ctx: CtxPtr, channel: &CStr, payload: &[u8]) -> Result<(), Refusal> {
    verdict(ctx, unsafe {
        crate::flz_core_local_feed(ctx.get(), channel.as_ptr(), payload.as_ptr(), payload.len())
    })
}

/// The next released sink output as encoded bytes, or `None` when none
/// arrived within `timeout_ms`.
pub fn local_next_output(ctx: CtxPtr, timeout_ms: u64) -> Result<Option<Vec<u8>>, Refusal> {
    let (status, copied) = with_buffer(
        &[FLZ_POLL_ACTIVATION],
        |buf, len| unsafe { crate::flz_core_local_next_output(ctx.get(), timeout_ms, buf, len) },
        <[u8]>::to_vec,
    );
    match (status, copied) {
        (FLZ_POLL_ACTIVATION, Some(bytes)) => Ok(Some(bytes)),
        (FLZ_POLL_TIMEOUT, _) => Ok(None),
        _ => Err(core(ctx)),
    }
}

/// Block until the graph is idle; a timeout is a refusal.
pub fn local_run_until_idle(ctx: CtxPtr, timeout_ms: u64) -> Result<(), Refusal> {
    verdict(ctx, unsafe {
        crate::flz_core_local_run_until_idle(ctx.get(), timeout_ms)
    })
}

/// End the local run; a context with none is a no-op.
pub fn local_stop(ctx: CtxPtr) {
    unsafe { crate::flz_core_local_stop(ctx.get()) };
}

/// Request a spawn from the live activation `token`; the handle number, at
/// once. An empty `id` asks the controller to generate one.
pub fn spawn_request(
    ctx: CtxPtr,
    token: u64,
    factory: &CStr,
    id: &CStr,
    payload: &[u8],
    if_running: &CStr,
    replay: bool,
) -> Result<u64, Refusal> {
    let mut handle = 0;
    let status = unsafe {
        crate::flz_core_spawn_request(
            ctx.get(),
            token,
            factory.as_ptr(),
            id.as_ptr(),
            payload.as_ptr(),
            payload.len(),
            if_running.as_ptr(),
            i32::from(replay),
            &mut handle,
        )
    };
    verdict(ctx, status).map(|()| handle)
}

/// Read a spawn handle, waiting up to `timeout_ms`: `None` while pending,
/// otherwise the answer as JSON, carrying `outcome` when it resolved and
/// `error` when it failed. A released handle is a refusal.
pub fn spawn_result(ctx: CtxPtr, handle: u64, timeout_ms: u64) -> Result<Option<String>, Refusal> {
    let (status, answer) = with_buffer(
        &[FLZ_OK, FLZ_SPAWN_FAILED],
        |out, out_len| unsafe {
            crate::flz_core_spawn_result(ctx.get(), handle, timeout_ms, out, out_len)
        },
        |bytes| String::from_utf8_lossy(bytes).into_owned(),
    );
    match status {
        FLZ_OK | FLZ_SPAWN_FAILED => Ok(answer),
        FLZ_SPAWN_PENDING => Ok(None),
        _ => Err(core(ctx)),
    }
}

/// Administer the namespace this connection proved: the published function's
/// own answer, as JSON. One wrapper for every verb, because the verb is data.
pub fn admin_call(ctx: CtxPtr, verb: &CStr, args: &CStr) -> Result<String, Refusal> {
    let (status, result) = with_buffer(
        &[FLZ_OK],
        |out, out_len| unsafe {
            crate::flz_core_admin_call(ctx.get(), verb.as_ptr(), args.as_ptr(), out, out_len)
        },
        |bytes| String::from_utf8_lossy(bytes).into_owned(),
    );
    verdict(ctx, status)?;
    Ok(result.unwrap_or_default())
}
