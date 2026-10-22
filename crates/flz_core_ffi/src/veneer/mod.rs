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
/// The core's notice seam, for a host that forwards notices itself.
pub use flz_transport::notice;

use crate::{
    FlzContext, FLZ_EFFECT_KEPT, FLZ_EFFECT_RUN, FLZ_EFFECT_UNRECORDED, FLZ_ERR, FLZ_OK,
    FLZ_POLL_ACTIVATION, FLZ_POLL_SHUTTING_DOWN, FLZ_POLL_TIMEOUT, FLZ_SPAWN_PENDING,
};

/// Which bindings reach an entry beside a host that binds the C ABI itself,
/// which reaches every one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reach {
    /// The C ABI only: buffer, error and settings plumbing a veneer does itself.
    CAbi,
    /// The C ABI and the Python, Ruby and Node glue.
    Glue,
    /// Every binding.
    Every,
}

/// One entry of the native surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    /// The entry's name, which each binding spells in its own language.
    pub name: &'static str,
    /// The C ABI symbol.
    pub abi: &'static str,
    /// Whether the entry can park the calling thread. A host whose unit of
    /// concurrency is bound to one thread keeps every one of these off it.
    pub blocking: bool,
    /// Which bindings reach the entry.
    pub reach: Reach,
}

impl Entry {
    /// The name in lower camel case, as Java and JavaScript spell it.
    pub fn camel_name(&self) -> String {
        let mut words = self.name.split('_');
        let first = words.next().unwrap_or_default().to_string();
        words.fold(first, |mut out, word| {
            let mut chars = word.chars();
            out.extend(chars.next().map(|c| c.to_ascii_uppercase()));
            out.push_str(chars.as_str());
            out
        })
    }
}

macro_rules! surface {
    ($(($name:ident, $blocking:literal, $reach:ident)),* $(,)?) => {
        &[$(Entry {
            name: stringify!($name),
            abi: concat!("flz_core_", stringify!($name)),
            blocking: $blocking,
            reach: Reach::$reach,
        }),*]
    };
}

/// The native surface: one row per C ABI entry. The Python, Ruby and Node
/// glue render their `native_surface` from it, and a crate test holds it equal
/// to the exports, `test/fixtures/abi_surface.txt` and, for its JNI rows,
/// `test/fixtures/jni_surface.txt`.
pub const SURFACE: &[Entry] = surface![
    (schema_version, false, Every),
    (context_new, false, Every),
    (context_free, false, Every),
    (init_error, false, CAbi),
    (last_error, false, Glue),
    (error_document, false, Every),
    (buf_free, false, CAbi),
    (set_log_callback, false, CAbi),
    (take_notices, false, Every),
    (declare_class, false, Every),
    // After start, for the coordinator's live-instance check.
    (undeclare_class, true, Every),
    (check_class, false, Every),
    (start, true, Every),
    (start_with, true, CAbi),
    (local_start, false, Every),
    (local_on_sink, false, Every),
    // A local run parks the caller in all three: `feed` while the consumer's
    // mailbox is full, and the other two by their own budget. The host's poll
    // loop is what drains them, so a host that parked its only dispatch thread
    // in one of these would wait on work it has stopped doing.
    (local_feed, true, Every),
    (local_next_output, true, Every),
    (local_run_until_idle, true, Every),
    (local_stop, false, Every),
    (poll, true, Every),
    (pool_open, false, Every),
    (pool_supervise, true, Every),
    (pool_left, false, Every),
    (complete, true, Every),
    (abandon, false, Every),
    (shutdown, false, Every),
    (watch_stdin_eof, false, Every),
    // A host that installs its own termination-signal handlers, and hands the
    // signal over: `terminate` drains within the grace and never returns.
    (host_owns_signals, false, CAbi),
    (terminate, true, CAbi),
    (spawn_request, false, Every),
    // With a nonzero timeout. A read with a timeout of 0 never waits, so a
    // host may make that one on any thread.
    (spawn_result, true, Every),
    (part, false, Every),
    (effect_begin, false, Every),
    // For the controller to keep the result.
    (effect_end, true, Every),
    (effect_release, false, Every),
    (admin_call, true, Every),
    // For the controller's answer, or while a local consumer's mailbox is full.
    (send_message, true, Every),
    (relays, false, Every),
    (monotonic_nanos, false, Every),
    (knob, false, Every),
    (response_decode, false, Every),
];

/// The rows the Python, Ruby and Node glue export.
pub fn glue_entries() -> impl Iterator<Item = &'static Entry> {
    SURFACE
        .iter()
        .filter(|entry| matches!(entry.reach, Reach::Glue | Reach::Every))
}

/// What a binding can raise: the context's last error, a creation-time
/// schema mismatch, or a value the binding could not map. The first two carry
/// the core's JSON error document.
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

/// `message` as a JSON error document: itself when it already is one, and
/// otherwise `internal.uncatalogued` naming it, for a failure a binding words
/// itself (a value it could not map).
pub fn document(message: String) -> String {
    if flz_core::FlezhaError::from_document(&message).is_some() {
        return message;
    }
    flz_core::FlezhaError::with(
        flz_transport::errors::UNCATALOGUED,
        [("reason", message.into())],
    )
    .json()
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
/// [`FLZ_POLL_ACTIVATION`] for poll and the local output read, [`FLZ_OK`]
/// for the rest; every other status leaves the out pointers untouched. `read` runs over the
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

/// Open the context's poll-worker pool over the host's `default_base`: its
/// policy in [`crate::flz_core_pool_open`]'s order.
pub fn pool_open(ctx: CtxPtr, default_base: u32) -> Result<[u32; 5], Refusal> {
    let mut policy = [0; 5];
    let status = unsafe { crate::flz_core_pool_open(ctx.get(), default_base, &mut policy) };
    verdict(ctx, status).map(|()| policy)
}

/// The pool's next decision as the ABI's status (`FLZ_POOL_*`), so a
/// supervisor loop allocates nothing per answer.
pub fn pool_supervise(ctx: CtxPtr) -> Result<i32, Refusal> {
    match unsafe { crate::flz_core_pool_supervise(ctx.get()) } {
        FLZ_ERR => Err(core(ctx)),
        status => Ok(status),
    }
}

pub fn pool_left(ctx: CtxPtr) -> Result<(), Refusal> {
    verdict(ctx, unsafe { crate::flz_core_pool_left(ctx.get()) })
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
pub fn watch_stdin_eof() {
    crate::flz_core_watch_stdin_eof()
}

pub fn declare_class(ctx: CtxPtr, json: &[u8]) -> Result<(), Refusal> {
    let status = unsafe { crate::flz_core_declare_class(ctx.get(), json.as_ptr(), json.len()) };
    verdict(ctx, status)
}

pub fn undeclare_class(ctx: CtxPtr, factory_id: &CStr) -> Result<(), Refusal> {
    verdict(ctx, unsafe {
        crate::flz_core_undeclare_class(ctx.get(), factory_id.as_ptr())
    })
}

/// Ask for a declaration's verdict without registering it. `Ok` carries the
/// derived declaration as JSON, `Err` the refusal whose `details.issues` lists
/// every issue — the veneer's `validate()` is this call and nothing else.
pub fn check_class(ctx: CtxPtr, json: &[u8]) -> Result<Vec<u8>, Refusal> {
    let (status, answer) = with_buffer(
        &[FLZ_OK],
        |buf, len| unsafe {
            crate::flz_core_check_class(ctx.get(), json.as_ptr(), json.len(), buf, len)
        },
        <[u8]>::to_vec,
    );
    verdict(ctx, status).map(|()| answer.unwrap_or_default())
}

pub fn start(ctx: CtxPtr, namespace: &CStr) -> Result<(), Refusal> {
    verdict(ctx, unsafe {
        crate::flz_core_start(ctx.get(), namespace.as_ptr())
    })
}

/// [`start`] with the connection settings handed over as a JSON object
/// rather than read from the environment.
pub fn start_with(ctx: CtxPtr, namespace: &CStr, settings: &[u8]) -> Result<(), Refusal> {
    verdict(ctx, unsafe {
        crate::flz_core_start_with(
            ctx.get(),
            namespace.as_ptr(),
            settings.as_ptr(),
            settings.len(),
        )
    })
}

/// Start the declared class as a local run: no controller, no socket. The
/// host keeps its own poll loop, which is what runs the tasklets.
pub fn local_start(ctx: CtxPtr, factory_id: &CStr, relay_id: &CStr) -> Result<(), Refusal> {
    verdict(ctx, unsafe {
        crate::flz_core_local_start(ctx.get(), factory_id.as_ptr(), relay_id.as_ptr())
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
/// once. An empty `id` asks the controller to generate one. `offset` is the
/// input-log offset of the item the calling tasklet is taking.
#[allow(clippy::too_many_arguments)]
pub fn spawn_request(
    ctx: CtxPtr,
    token: u64,
    factory: &CStr,
    id: &CStr,
    payload: &[u8],
    if_running: &CStr,
    replay: bool,
    offset: Option<u64>,
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
            offset.map_or(-1, |o| o as i64),
            &mut handle,
        )
    };
    verdict(ctx, status).map(|()| handle)
}

/// Send a part of the invocation of the live activation `token`'s work item
/// `item`; never waits. A part the core dropped is `Ok` as well.
pub fn part(ctx: CtxPtr, token: u64, item: u32, payload: &[u8]) -> Result<(), Refusal> {
    verdict(ctx, unsafe {
        crate::flz_core_part(ctx.get(), token, item, payload.as_ptr(), payload.len())
    })
}

/// What a begun `ctx.effect` does next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectStart {
    /// The result kept for this call: return it, run nothing.
    Kept(Vec<u8>),
    /// Run the function, then [`effect_end`] or [`effect_release`].
    Run,
    /// Nothing re-runs the invocation: run the function, return its result,
    /// then [`effect_release`].
    Unrecorded,
}

/// Begin `ctx.effect(name)` in the work item `item` of the live activation
/// `token`.
pub fn effect_begin(
    ctx: CtxPtr,
    token: u64,
    item: u32,
    name: &CStr,
) -> Result<EffectStart, Refusal> {
    let (status, kept) = with_buffer(
        &[FLZ_EFFECT_KEPT],
        |out, out_len| unsafe {
            crate::flz_core_effect_begin(ctx.get(), token, item, name.as_ptr(), out, out_len)
        },
        <[u8]>::to_vec,
    );
    match status {
        FLZ_EFFECT_KEPT => Ok(EffectStart::Kept(kept.unwrap_or_default())),
        FLZ_EFFECT_RUN => Ok(EffectStart::Run),
        FLZ_EFFECT_UNRECORDED => Ok(EffectStart::Unrecorded),
        _ => Err(core(ctx)),
    }
}

/// Keep the encoded result of a function [`effect_begin`] ran; blocks for the
/// controller. `None`: the call returns its own result; `Some`: the result the
/// controller kept first, which it returns instead.
pub fn effect_end(
    ctx: CtxPtr,
    token: u64,
    item: u32,
    payload: &[u8],
) -> Result<Option<Vec<u8>>, Refusal> {
    let (status, first) = with_buffer(
        &[FLZ_EFFECT_KEPT],
        |out, out_len| unsafe {
            crate::flz_core_effect_end(
                ctx.get(),
                token,
                item,
                payload.as_ptr(),
                payload.len(),
                out,
                out_len,
            )
        },
        <[u8]>::to_vec,
    );
    match status {
        FLZ_OK => Ok(None),
        FLZ_EFFECT_KEPT => Ok(first),
        _ => Err(core(ctx)),
    }
}

/// End a begun effect without keeping anything.
pub fn effect_release(ctx: CtxPtr, token: u64, item: u32) {
    unsafe { crate::flz_core_effect_release(ctx.get(), token, item) };
}

/// Read a spawn handle, waiting up to `timeout_ms`: `None` while pending,
/// otherwise the resolved answer as JSON. A failed spawn, and a released
/// handle, is a refusal.
pub fn spawn_result(ctx: CtxPtr, handle: u64, timeout_ms: u64) -> Result<Option<String>, Refusal> {
    let (status, answer) = with_buffer(
        &[FLZ_OK],
        |out, out_len| unsafe {
            crate::flz_core_spawn_result(ctx.get(), handle, timeout_ms, out, out_len)
        },
        |bytes| String::from_utf8_lossy(bytes).into_owned(),
    );
    match status {
        FLZ_OK => Ok(answer),
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

/// Send `message` to the entry channel `channel` of `relay`, answering as
/// JSON: the controller's verdict, or `null` for a local send. An empty
/// `msg_id` or `key` is none.
pub fn send_message(
    ctx: CtxPtr,
    relay: &CStr,
    channel: &CStr,
    message: &[u8],
    msg_id: &CStr,
    key: &CStr,
    local: bool,
) -> Result<String, Refusal> {
    let (status, answer) = with_buffer(
        &[FLZ_OK],
        |out, out_len| unsafe {
            crate::flz_core_send_message(
                ctx.get(),
                relay.as_ptr(),
                channel.as_ptr(),
                message.as_ptr(),
                message.len(),
                msg_id.as_ptr(),
                key.as_ptr(),
                i32::from(local),
                out,
                out_len,
            )
        },
        |bytes| String::from_utf8_lossy(bytes).into_owned(),
    );
    verdict(ctx, status)?;
    Ok(answer.unwrap_or_default())
}

/// The ids of the relay instances bound on this process's runner.
pub fn relays(ctx: CtxPtr) -> Result<Vec<String>, Refusal> {
    let (status, ids) = with_buffer(
        &[FLZ_OK],
        |out, out_len| unsafe { crate::flz_core_relays(ctx.get(), out, out_len) },
        |bytes| serde_json::from_slice::<Vec<String>>(bytes).unwrap_or_default(),
    );
    verdict(ctx, status)?;
    Ok(ids.unwrap_or_default())
}

/// A knob's text parsed by the core's rule for `kind`: `None` when it is no
/// value of that kind, so the host keeps its default.
pub fn knob(kind: &str, raw: &str) -> Result<Option<i64>, Refusal> {
    let (kind_c, raw) = (c_string("knob kind", kind)?, c_string("knob value", raw)?);
    let mut out = 0;
    match unsafe { crate::flz_core_knob(kind_c.as_ptr(), raw.as_ptr(), &mut out) } {
        FLZ_OK => Ok(Some(out)),
        crate::FLZ_KNOB_UNSET => Ok(None),
        _ => Err(Refusal::Shape(format!("`{kind}` is not a knob kind"))),
    }
}

/// A decoded `Response`: the answer's bytes, or the JSON error document the
/// call ended in.
#[derive(Debug)]
pub struct ResponseParts<'a> {
    pub target: &'a str,
    pub created: bool,
    pub request: &'a [u8],
    pub result: Result<&'a [u8], &'a str>,
}

/// Decode a `Response` channel payload with the core's decoder into `out`,
/// as [`crate::flz_core_response_decode`] lays it out. Context-free.
pub fn response_into(
    payload: &[u8],
    out: &mut [u8],
    parts: &mut [usize; 5],
) -> Result<(), Refusal> {
    let Some(out) = out.get_mut(..payload.len()) else {
        return Err(Refusal::Shape(
            "a Response buffer shorter than its payload".into(),
        ));
    };
    let (bytes, len, out) = (payload.as_ptr(), payload.len(), out.as_mut_ptr());
    match unsafe { crate::flz_core_response_decode(bytes, len, out, parts.as_mut_ptr()) } {
        FLZ_OK => Ok(()),
        _ => Err(Refusal::Core(
            read_c_str(crate::flz_core_init_error()).unwrap_or_default(),
        )),
    }
}

/// [`response_into`] read into its [`ResponseParts`].
pub fn response_decode<T>(
    payload: &[u8],
    read: impl FnOnce(ResponseParts<'_>) -> T,
) -> Result<T, Refusal> {
    let (mut out, mut p) = (vec![0; payload.len()], [0; 5]);
    response_into(payload, &mut out, &mut p)?;
    let text = |range| std::str::from_utf8(&out[range]).unwrap_or_default();
    let result = p[0] + p[1]..p[0] + p[1] + p[2];
    Ok(read(ResponseParts {
        target: text(0..p[0]),
        created: p[3] == 1,
        request: &out[p[0]..p[0] + p[1]],
        result: (p[4] == 1)
            .then(|| &out[result.clone()])
            .ok_or_else(|| text(result)),
    }))
}

/// One core notice a host writes through its own log.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Notice {
    /// `info`, `warn`, `error` or `always`: the local-mode banner, written
    /// whatever the host's level filter says.
    pub level: String,
    pub target: String,
    pub text: String,
}

/// The notices held since the last take; the first take starts the holding.
pub fn take_notices() -> Vec<Notice> {
    let (_, notices) = with_buffer(
        &[FLZ_OK],
        |out, out_len| unsafe { crate::flz_core_take_notices(out, out_len) },
        |bytes| serde_json::from_slice::<Vec<Notice>>(bytes).unwrap_or_default(),
    );
    notices.unwrap_or_default()
}
