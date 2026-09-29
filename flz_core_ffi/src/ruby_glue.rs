//! Ruby bindings over this crate's C ABI for the `flezha` gem's native
//! extension, one module function per C entry point.
//!
//! Ruby has one VM per process, so the context lives in [`CONTEXT`] and no
//! handle crosses into Ruby. Every blocking entry point runs inside
//! [`without_gvl`], the one place the GVL rule is implemented; the poll
//! timeout the caller passes bounds how quickly `Thread#kill`, `Interrupt`
//! and a runner close are observed, because the ABI has no per-thread wake.
//! The log callback is not veneered: the core's diagnostics come from
//! threads Ruby did not create, which CRuby cannot accept a call from.
//! Payloads reach Ruby as binary `String`s, which own their bytes.

use std::ffi::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicPtr, Ordering};

use magnus::prelude::*;
use magnus::{
    function, Error, Exception, IntoValue, RArray, RClass, RHash, RString, Ruby, TryConvert, Value,
};

use crate::veneer::{self, shape, CtxPtr, Polled, Refusal};
use crate::FlzContext;

/// The one context this process holds. Null until `context_new`.
static CONTEXT: AtomicPtr<FlzContext> = AtomicPtr::new(std::ptr::null_mut());

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// The Ruby handle of the calling thread.
///
/// Every function below is reached only from Ruby, through `function!`, so the
/// handle is always there. magnus itself takes the same shortcut on the return
/// path of every bound function, which is the same thread and the same
/// precondition — stating it once here beats threading an impossible error
/// through twenty signatures.
fn ruby() -> Ruby {
    // SAFETY: reached only from a `function!`-bound entry point, which Ruby
    // calls with the GVL held on a thread it owns.
    unsafe { Ruby::get_unchecked() }
}

/// A native failure as the gem's one error type, `Flezha::Error`, built from
/// the JSON error document. A failure this module words itself (a value it
/// could not map) is `internal.uncatalogued`.
fn native_error(ruby: &Ruby, message: impl Into<String>) -> Error {
    let document = veneer::document(message.into());
    ruby.define_module("Flezha")
        .and_then(|flezha| flezha.const_get::<_, RClass>("Error"))
        .and_then(|class| class.funcall::<_, _, Exception>("from_document", (document,)))
        .map_or_else(|failure| failure, Error::from)
}

/// The one mapping of a refusal onto the gem's error type.
fn raise(ruby: &Ruby, refusal: Refusal) -> Error {
    native_error(ruby, refusal.into_message())
}

/// The live context, or an error naming what the caller skipped.
fn context(ruby: &Ruby) -> Result<CtxPtr, Error> {
    let ptr = CONTEXT.load(Ordering::Acquire);
    if ptr.is_null() {
        return Err(native_error(
            ruby,
            "flz_core context not created: call Flezha::Native.context_new first",
        ));
    }
    Ok(CtxPtr::of(ptr))
}

// ---------------------------------------------------------------------------
// The GVL seam
// ---------------------------------------------------------------------------

/// What crosses into the GVL-released call and back. The closure is moved in
/// and its answer moved out; nothing here touches a Ruby object, which is the
/// whole precondition for running without the lock.
struct GvlCall<F, T> {
    work: Option<F>,
    answer: Option<std::thread::Result<T>>,
}

unsafe extern "C" fn gvl_trampoline<F, T>(data: *mut c_void) -> *mut c_void
where
    F: FnOnce() -> T,
{
    let call = &mut *(data as *mut GvlCall<F, T>);
    // A panic must not unwind across the C frame that called us; it is
    // captured here and resumed once the GVL is back.
    let work = call.work.take();
    call.answer = Some(catch_unwind(AssertUnwindSafe(move || {
        (work.expect("GVL closure taken twice"))()
    })));
    std::ptr::null_mut()
}

/// Run one blocking ABI call with the global VM lock released.
///
/// The single place the GVL rule is implemented, so "no blocking core call
/// holds the GVL" is a property of one function rather than of six correct
/// copies. `work` runs on this same OS thread and MUST NOT touch a Ruby
/// object — every caller below converts to and from Ruby outside it.
///
/// No unblocking function is registered: the ABI has no per-thread wake, so
/// the call's own timeout is the interrupt granularity (see the module docs).
fn without_gvl<F, T>(work: F) -> T
where
    F: FnOnce() -> T,
{
    let mut call = GvlCall {
        work: Some(work),
        answer: None,
    };
    unsafe {
        rb_sys::rb_thread_call_without_gvl(
            Some(gvl_trampoline::<F, T>),
            &mut call as *mut GvlCall<F, T> as *mut c_void,
            None,
            std::ptr::null_mut(),
        );
    }
    match call.answer.expect("GVL closure did not run") {
        Ok(value) => value,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

// ---------------------------------------------------------------------------
// Ruby <-> schema conversion
//
// The veneer decodes postcard on the Rust side (`schema::decode_activation`)
// so the gem never carries a second implementation of the wire contract. The
// shape is a flat Hash with a `:kind` discriminator rather than a class per
// variant, because Ruby has no sum type and a discriminated Hash is what the
// dispatcher case/whens on anyway. Absent fields are absent, not nil. Which
// key carries which field is `veneer::shape`'s; this module says only how Ruby
// builds and reads a value.
// ---------------------------------------------------------------------------

/// The one mapping of a refusal onto the gem's exception classes.
impl From<Refusal> for Error {
    fn from(refusal: Refusal) -> Error {
        raise(&ruby(), refusal)
    }
}

/// The Ruby half of [`shape::Sink`] and [`shape::Source`]: a handle over the
/// VM, not state.
#[derive(Clone, Copy)]
pub struct Values<'a>(pub &'a Ruby);

impl shape::Sink for Values<'_> {
    type Value = Value;
    type Map = RHash;
    type List = RArray;
    type Error = Error;

    fn map(&self) -> Result<RHash, Error> {
        Ok(self.0.hash_new())
    }

    fn set(&self, map: &mut RHash, key: shape::Key, value: Value) -> Result<(), Error> {
        map.aset(
            self.0.sym_new(key.spell(<Self as shape::Sink>::CAMEL)),
            value,
        )
    }

    fn finish_map(&self, map: RHash) -> Result<Value, Error> {
        Ok(map.as_value())
    }

    fn list(&self, capacity: usize) -> Result<RArray, Error> {
        Ok(self.0.ary_new_capa(capacity))
    }

    fn push(&self, list: &mut RArray, value: Value) -> Result<(), Error> {
        list.push(value)
    }

    fn finish_list(&self, list: RArray) -> Result<Value, Error> {
        Ok(list.as_value())
    }

    /// A binary `String`, which owns its bytes: Ruby has no borrowed-buffer
    /// type a handler could keep past the invocation.
    fn bytes(&self, v: Vec<u8>) -> Result<Value, Error> {
        Ok(self.0.str_from_slice(&v).as_value())
    }

    fn text(&self, v: String) -> Result<Value, Error> {
        Ok(v.into_value_with(self.0))
    }

    fn word(&self, v: &'static str) -> Result<Value, Error> {
        Ok(self.0.sym_new(v).as_value())
    }

    fn pair(&self, key: String, value: String) -> Result<Value, Error> {
        let pair = self.0.ary_new_capa(2);
        pair.push(key)?;
        pair.push(value)?;
        Ok(pair.as_value())
    }

    fn u64(&self, v: u64) -> Result<Value, Error> {
        Ok(v.into_value_with(self.0))
    }

    fn u32(&self, v: u32) -> Result<Value, Error> {
        Ok(v.into_value_with(self.0))
    }

    fn bool(&self, v: bool) -> Result<Value, Error> {
        Ok(v.into_value_with(self.0))
    }
}

impl shape::Source for Values<'_> {
    type Value = Value;
    type Map = RHash;
    type Error = Error;

    fn get(&self, map: &RHash, key: shape::Key) -> Result<Option<Value>, Error> {
        Ok(
            match map.get(self.0.sym_new(key.spell(<Self as shape::Source>::CAMEL))) {
                Some(v) if !v.is_nil() => Some(v),
                _ => None,
            },
        )
    }

    fn map(&self, v: &Value, what: &str) -> Result<RHash, Error> {
        RHash::from_value(*v).ok_or_else(|| native_error(self.0, format!("{what} must be a Hash")))
    }

    fn text(&self, v: &Value) -> Result<String, Error> {
        String::try_convert(*v)
    }

    fn bytes(&self, v: &Value, what: &str) -> Result<Vec<u8>, Error> {
        read_bytes(self.0, what, *v)
    }

    fn u64(&self, v: &Value, _what: &str) -> Result<u64, Error> {
        u64::try_convert(*v)
    }

    fn u32(&self, v: &Value) -> Result<u32, Error> {
        u32::try_convert(*v)
    }

    fn bool(&self, v: &Value) -> Result<bool, Error> {
        bool::try_convert(*v)
    }

    fn items(&self, v: &Value) -> Result<Vec<Value>, Error> {
        let array = RArray::try_convert(*v)?;
        // SAFETY: the slice is copied out before any Ruby call can run and
        // move or collect the backing array.
        Ok(unsafe { array.as_slice() }.to_vec())
    }

    fn pair(&self, v: &Value) -> Result<(String, String), Error> {
        let malformed = || native_error(self.0, "metadata entry must be a [key, value] Array");
        let pair = RArray::from_value(*v).ok_or_else(malformed)?;
        let pair: Vec<String> = pair.to_vec()?;
        let mut it = pair.into_iter();
        match (it.next(), it.next(), it.next()) {
            (Some(key), Some(value), None) => Ok((key, value)),
            _ => Err(malformed()),
        }
    }
}

/// The bytes behind a payload. Ruby has no borrowed-buffer type, so a payload
/// is a binary `String` that owns them.
fn read_bytes(ruby: &Ruby, what: &str, value: Value) -> Result<Vec<u8>, Error> {
    let s = RString::from_value(value)
        .ok_or_else(|| native_error(ruby, format!("{what} must be a String")))?;
    Ok(unsafe { s.as_slice() }.to_vec())
}

// ---------------------------------------------------------------------------
// The entry points. Every one below corresponds to exactly one C entry point.
// ---------------------------------------------------------------------------

/// The activation schema version this native library speaks.
fn schema_version() -> u32 {
    crate::flz_core_schema_version()
}

/// Create the process context, negotiating the schema version. Raises
/// `runtime.schema_mismatch` naming both versions, before any controller
/// connection is attempted.
fn context_new(shim_schema_version: u32) -> Result<(), Error> {
    let ruby = ruby();
    if !CONTEXT.load(Ordering::Acquire).is_null() {
        return Err(native_error(
            &ruby,
            "flz_core context already created for this process",
        ));
    }
    let ctx = veneer::context_new(shim_schema_version).map_err(|r| raise(&ruby, r))?;
    CONTEXT.store(ctx, Ordering::Release);
    Ok(())
}

/// Destroy the process context. The host's threads must already be stopped.
fn context_free() {
    let ctx = CONTEXT.swap(std::ptr::null_mut(), Ordering::AcqRel);
    if !ctx.is_null() {
        veneer::context_free(CtxPtr::of(ctx));
    }
}

/// Declare one host class from its JSON declaration. Raises the core's refusal.
fn declare_class(json: RString) -> Result<(), Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    let json = unsafe { json.as_slice() }.to_vec();
    veneer::declare_class(ctx, &json).map_err(|r| raise(&ruby, r))
}

/// Judge one host class declaration without registering it: the derived
/// declaration as JSON, or the core's error document raised, whose
/// `details.issues` lists every issue. The gem's `Graph.validate` is this call
/// and nothing else.
fn check_class(json: RString) -> Result<String, Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    let json = unsafe { json.as_slice() }.to_vec();
    match veneer::check_class(ctx, &json) {
        Ok(derived) => Ok(String::from_utf8_lossy(&derived).into_owned()),
        Err(refusal) => Err(raise(&ruby, refusal)),
    }
}

/// Connect to the controller and start the runner loop. Blocking: the ABI call
/// waits up to ten seconds for the connect confirmation.
fn start(namespace: String) -> Result<(), Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    let refused = |r| raise(&ruby, r);
    let namespace = veneer::c_string("namespace", &namespace).map_err(refused)?;
    without_gvl(move || veneer::start(ctx, &namespace)).map_err(refused)
}

/// Start the declared class as a local run: no controller, no socket. The
/// alternative to [`start`], never an addition. The host keeps driving its own
/// poll loop, which is what runs the tasklets.
fn local_start(factory_id: String, operator_id: String) -> Result<(), Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    let refused = |r| raise(&ruby, r);
    let factory = veneer::c_string("factory id", &factory_id).map_err(refused)?;
    let operator = veneer::c_string("operator id", &operator_id).map_err(refused)?;
    veneer::local_start(ctx, &factory, &operator).map_err(refused)
}

/// Queue the declared sink `channel` for [`local_next_output`]. A channel the
/// declaration does not carry is refused here, by name.
fn local_on_sink(channel: String) -> Result<(), Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    let refused = |r| raise(&ruby, r);
    let channel = veneer::c_string("sink channel", &channel).map_err(refused)?;
    veneer::local_on_sink(ctx, &channel).map_err(refused)
}

/// Feed one value's wire bytes to a declared entry channel. Blocking: the
/// entry path parks while the consumer's mailbox is full.
fn local_feed(channel: String, payload: RString) -> Result<(), Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    let refused = |r| raise(&ruby, r);
    let channel = veneer::c_string("entry channel", &channel).map_err(refused)?;
    let payload = read_bytes(&ruby, "payload", payload.as_value())?;
    without_gvl(move || veneer::local_feed(ctx, &channel, &payload)).map_err(refused)
}

/// The next value a sink released as the output Hash this SDK already builds,
/// or `nil` on timeout. Blocking for the whole timeout.
fn local_next_output(timeout_ms: u64) -> Result<Value, Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    let released = without_gvl(move || veneer::local_next_output(ctx, timeout_ms))
        .map_err(|r| raise(&ruby, r))?;
    match released {
        Some(encoded) => {
            let output = flz_core::schema::decode_output(&encoded)
                .map_err(|e| native_error(&ruby, format!("output failed to decode: {e}")))?;
            shape::sink_output(&Values(&ruby), output)
        }
        None => Ok(ruby.qnil().as_value()),
    }
}

/// Park until the graph has nothing left to do. A timeout is a refusal, never
/// a silent success.
fn local_run_until_idle(timeout_ms: u64) -> Result<(), Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    without_gvl(move || veneer::local_run_until_idle(ctx, timeout_ms)).map_err(|r| raise(&ruby, r))
}

/// End the local run and release its Cells. A context holding none is a
/// no-op, so a host's close path needs no test of its own.
fn local_stop() -> Result<(), Error> {
    let ruby = ruby();
    veneer::local_stop(context(&ruby)?);
    Ok(())
}

/// Block up to `timeout_ms` for one activation. Returns the activation Hash,
/// `nil` on timeout, or `:shutting_down` once every poller has been released.
fn poll(timeout_ms: u64) -> Result<Value, Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    match without_gvl(move || veneer::poll(ctx, timeout_ms)).map_err(|r| raise(&ruby, r))? {
        Polled::Activation(act) => shape::activation(&Values(&ruby), act),
        Polled::Timeout => Ok(ruby.qnil().as_value()),
        Polled::ShuttingDown => Ok(ruby.sym_new("shutting_down").as_value()),
    }
}

/// Park up to `timeout_ms` until the core observes starvation. Answers the
/// ABI's three-way status directly (`1` observed, `0` timeout, `2` shutting
/// down), so a supervisor loop allocates nothing per park.
fn wait_starvation(timeout_ms: u64) -> Result<i32, Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    without_gvl(move || veneer::wait_starvation(ctx, timeout_ms)).map_err(|r| raise(&ruby, r))
}

/// Answer one activation. Blocking, and this is the entry point the GVL rule
/// exists for on the completion side: under the default `DropNothing` overflow
/// policy the routing push parks until the consumer's mailbox drains, and the
/// consumer drains only when its handler runs on another Ruby thread.
fn complete(result: RHash) -> Result<(), Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    let result = shape::result(&Values(&ruby), &result)?;
    without_gvl(move || veneer::complete(ctx, &result)).map_err(|r| raise(&ruby, r))
}

/// Abandon a polled activation: re-hand its untouched items and release its
/// cell. Non-blocking by design — the dispatcher runs it from an `ensure`,
/// where anything that can park is one more thing that can go wrong.
fn abandon(token: u64) -> Result<(), Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    veneer::abandon(ctx, token).map_err(|r| raise(&ruby, r))
}

/// Wake every blocked poller and stop the runner loop.
fn shutdown() -> Result<(), Error> {
    let ruby = ruby();
    veneer::shutdown(context(&ruby)?);
    Ok(())
}

/// Watch standard input and end the process on end of file, on the core's own
/// thread. The host reads descriptor 0 nowhere.
fn watch_stdin_eof(grace_ms: u32) -> Result<(), Error> {
    let ruby = ruby();
    context(&ruby)?;
    veneer::watch_stdin_eof(grace_ms);
    Ok(())
}

/// Request a spawn from the live activation `token`; answers the handle number
/// at once. An empty `id` asks the controller to generate one. Never waits, so
/// it holds the GVL.
fn spawn_request(
    token: u64,
    factory_id: String,
    id: String,
    payload: RString,
    if_running: String,
    replay: bool,
    offset: Option<u64>,
) -> Result<u64, Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    let refused = |r| raise(&ruby, r);
    let factory = veneer::c_string("factory id", &factory_id).map_err(refused)?;
    let id = veneer::c_string("id", &id).map_err(refused)?;
    let mode = veneer::c_string("if_running", &if_running).map_err(refused)?;
    // SAFETY: the GVL is held for the whole call, so nothing can move or free
    // the string's bytes while the core copies them.
    let payload = unsafe { payload.as_slice() };
    veneer::spawn_request(ctx, token, &factory, &id, payload, &mode, replay, offset)
        .map_err(refused)
}

/// Read a spawn handle: `nil` while pending, otherwise its answer as JSON. A
/// nonzero timeout waits with the GVL released.
fn spawn_result(handle: u64, timeout_ms: u64) -> Result<Option<String>, Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    let answer = if timeout_ms == 0 {
        veneer::spawn_result(ctx, handle, 0)
    } else {
        without_gvl(move || veneer::spawn_result(ctx, handle, timeout_ms))
    };
    answer.map_err(|r| raise(&ruby, r))
}

/// Administer the namespace this connection proved; answers the published
/// function's own result as JSON. One module function for the whole surface,
/// so a further verb is Ruby and no native work. Blocking: the ABI call waits
/// up to thirty seconds.
fn admin_call(verb: String, args: String) -> Result<String, Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    let refused = |r| raise(&ruby, r);
    let verb = veneer::c_string("verb", &verb).map_err(refused)?;
    let args = veneer::c_string("args", &args).map_err(refused)?;
    without_gvl(move || veneer::admin_call(ctx, &verb, &args)).map_err(refused)
}

/// The core's monotonic clock in nanoseconds.
fn monotonic_nanos() -> u64 {
    crate::flz_core_monotonic_nanos()
}

/// The context's last recorded error, or `nil`.
fn last_error() -> Option<String> {
    let ptr = CONTEXT.load(Ordering::Acquire);
    if ptr.is_null() {
        return None;
    }
    veneer::last_error(CtxPtr::of(ptr))
}

/// The JSON error document the catalogue renders for `code` and `details`, a
/// JSON object: the document `last_error` carries for a refusal the core
/// makes, for one the gem makes itself.
fn error_document(code: String, details: String) -> Result<String, Error> {
    crate::render_error_document(&code, &details).map_err(|e| native_error(&ruby(), e))
}

/// Every function this extension exports, with the C ABI entry point behind
/// it: `[name, abi_entry_point, blocking]`.
///
/// Two properties are asserted against this table rather than reviewed, so
/// neither can lapse by omission when an export is added later:
///
/// * the veneer adds no ABI surface — every export names an existing entry
///   point, and this veneer has no host bookkeeping of its own because the
///   Ruby host's threads are Ruby's;
/// * every export marked blocking runs inside [`without_gvl`].
fn native_surface() -> Result<RArray, Error> {
    let ruby = ruby();
    let out = ruby.ary_new_capa(NATIVE_SURFACE.len());
    for (name, abi) in NATIVE_SURFACE {
        let row = ruby.ary_new_capa(3);
        row.push(*name)?;
        row.push(*abi)?;
        row.push(crate::veneer::BLOCKING_ENTRY_POINTS.contains(abi))?;
        out.push(row)?;
    }
    Ok(out)
}

/// The table `native_surface` renders and the Rust-side test asserts against:
/// `(name, abi_entry_point)`, blocking derived from
/// [`crate::veneer::BLOCKING_ENTRY_POINTS`].
pub const NATIVE_SURFACE: &[(&str, &str)] = &[
    ("schema_version", "flz_core_schema_version"),
    ("context_new", "flz_core_context_new"),
    ("context_free", "flz_core_context_free"),
    ("declare_class", "flz_core_declare_class"),
    ("check_class", "flz_core_check_class"),
    ("start", "flz_core_start"),
    ("local_start", "flz_core_local_start"),
    ("local_on_sink", "flz_core_local_on_sink"),
    ("local_feed", "flz_core_local_feed"),
    ("local_next_output", "flz_core_local_next_output"),
    ("local_run_until_idle", "flz_core_local_run_until_idle"),
    ("local_stop", "flz_core_local_stop"),
    ("poll", "flz_core_poll"),
    ("wait_starvation", "flz_core_wait_starvation"),
    ("complete", "flz_core_complete"),
    ("abandon", "flz_core_abandon"),
    ("shutdown", "flz_core_shutdown"),
    ("watch_stdin_eof", "flz_core_watch_stdin_eof"),
    ("spawn_request", "flz_core_spawn_request"),
    ("spawn_result", "flz_core_spawn_result"),
    ("admin_call", "flz_core_admin_call"),
    ("monotonic_nanos", "flz_core_monotonic_nanos"),
    ("last_error", "flz_core_last_error"),
    ("native_surface", "flz_core_schema_version"),
    ("error_document", "flz_core_error_document"),
];

/// Define `Flezha::Native`. Named `Init_flz_core_ffi` so `require
/// "flz_core_ffi"` finds it. Its failures raise `Flezha::Error`, which the gem
/// defines before it loads this extension.
#[magnus::init]
pub fn init(ruby: &Ruby) -> Result<(), Error> {
    let flezha = ruby.define_module("Flezha")?;
    let native = flezha.define_module("Native")?;

    native.define_module_function("schema_version", function!(schema_version, 0))?;
    native.define_module_function("context_new", function!(context_new, 1))?;
    native.define_module_function("context_free", function!(context_free, 0))?;
    native.define_module_function("declare_class", function!(declare_class, 1))?;
    native.define_module_function("check_class", function!(check_class, 1))?;
    native.define_module_function("start", function!(start, 1))?;
    native.define_module_function("local_start", function!(local_start, 2))?;
    native.define_module_function("local_on_sink", function!(local_on_sink, 1))?;
    native.define_module_function("local_feed", function!(local_feed, 2))?;
    native.define_module_function("local_next_output", function!(local_next_output, 1))?;
    native.define_module_function("local_run_until_idle", function!(local_run_until_idle, 1))?;
    native.define_module_function("local_stop", function!(local_stop, 0))?;
    native.define_module_function("poll", function!(poll, 1))?;
    native.define_module_function("wait_starvation", function!(wait_starvation, 1))?;
    native.define_module_function("complete", function!(complete, 1))?;
    native.define_module_function("abandon", function!(abandon, 1))?;
    native.define_module_function("shutdown", function!(shutdown, 0))?;
    native.define_module_function("watch_stdin_eof", function!(watch_stdin_eof, 1))?;
    native.define_module_function("spawn_request", function!(spawn_request, 7))?;
    native.define_module_function("spawn_result", function!(spawn_result, 2))?;
    native.define_module_function("admin_call", function!(admin_call, 2))?;
    native.define_module_function("monotonic_nanos", function!(monotonic_nanos, 0))?;
    native.define_module_function("last_error", function!(last_error, 0))?;
    native.define_module_function("native_surface", function!(native_surface, 0))?;
    native.define_module_function("error_document", function!(error_document, 2))?;
    Ok(())
}
