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
    function, Error, Exception, IntoValue, RArray, RClass, RHash, RString, Ruby, Symbol,
    TryConvert, Value,
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
fn context() -> Result<CtxPtr, Error> {
    let ptr = CONTEXT.load(Ordering::Acquire);
    if ptr.is_null() {
        return Err(native_error(
            &ruby(),
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
    let ctx = veneer::context_new(shim_schema_version)?;
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
    let ctx = context()?;
    let json = unsafe { json.as_slice() }.to_vec();
    veneer::declare_class(ctx, &json).map_err(Error::from)
}

/// Judge one host class declaration without registering it: the derived
/// declaration as JSON, or the core's error document raised, whose
/// `details.issues` lists every issue. The gem's `Graph.validate` is this call
/// and nothing else.
fn check_class(json: RString) -> Result<String, Error> {
    let ctx = context()?;
    let json = unsafe { json.as_slice() }.to_vec();
    let derived = veneer::check_class(ctx, &json)?;
    Ok(String::from_utf8_lossy(&derived).into_owned())
}

/// Undeclare a class; after start, waits for the runner's live-instance check.
fn undeclare_class(factory_id: String) -> Result<(), Error> {
    let ctx = context()?;
    let factory_id = veneer::c_string("factory id", &factory_id)?;
    without_gvl(move || veneer::undeclare_class(ctx, &factory_id)).map_err(Error::from)
}

/// Connect to the controller and start the runner loop. Blocking: the ABI call
/// waits up to `FLEZHA_RUNNER_START_WAIT_MS` for the confirmation.
fn start(namespace: String) -> Result<(), Error> {
    let ctx = context()?;
    let namespace = veneer::c_string("namespace", &namespace)?;
    without_gvl(move || veneer::start(ctx, &namespace)).map_err(Error::from)
}

/// Start the declared class as a local run: no controller, no socket. The
/// alternative to [`start`], never an addition. The host keeps driving its own
/// poll loop, which is what runs the tasklets.
fn local_start(factory_id: String, relay_id: String) -> Result<(), Error> {
    let ctx = context()?;
    let factory = veneer::c_string("factory id", &factory_id)?;
    let relay = veneer::c_string("relay id", &relay_id)?;
    veneer::local_start(ctx, &factory, &relay).map_err(Error::from)
}

/// Queue the declared sink `channel` for [`local_next_output`]. A channel the
/// declaration does not carry is refused here, by name.
fn local_on_sink(channel: String) -> Result<(), Error> {
    let ctx = context()?;
    let channel = veneer::c_string("output channel", &channel)?;
    veneer::local_on_sink(ctx, &channel).map_err(Error::from)
}

/// Feed one value's wire bytes to a declared entry channel. Blocking: the
/// entry path parks while the consumer's mailbox is full.
fn local_feed(channel: String, payload: RString) -> Result<(), Error> {
    let ruby = ruby();
    let ctx = context()?;
    let channel = veneer::c_string("entry channel", &channel)?;
    let payload = read_bytes(&ruby, "payload", payload.as_value())?;
    without_gvl(move || veneer::local_feed(ctx, &channel, &payload)).map_err(Error::from)
}

/// The next value a sink released as the output Hash this SDK already builds,
/// or `nil` on timeout. Blocking for the whole timeout.
fn local_next_output(timeout_ms: u64) -> Result<Value, Error> {
    let ruby = ruby();
    let ctx = context()?;
    let released = without_gvl(move || veneer::local_next_output(ctx, timeout_ms))?;
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
    let ctx = context()?;
    without_gvl(move || veneer::local_run_until_idle(ctx, timeout_ms)).map_err(Error::from)
}

/// End the local run and release its Cells. A context holding none is a
/// no-op, so a host's close path needs no test of its own.
fn local_stop() -> Result<(), Error> {
    veneer::local_stop(context()?);
    Ok(())
}

/// Block up to `timeout_ms` for one activation. Returns the activation Hash,
/// `nil` on timeout, or `:shutting_down` once every poller has been released.
fn poll(timeout_ms: u64) -> Result<Value, Error> {
    let ruby = ruby();
    let ctx = context()?;
    match without_gvl(move || veneer::poll(ctx, timeout_ms))? {
        Polled::Activation(act) => shape::activation(&Values(&ruby), act),
        Polled::Timeout => Ok(ruby.qnil().as_value()),
        Polled::ShuttingDown => Ok(ruby.sym_new("shutting_down").as_value()),
    }
}

/// Open the context's poll-worker pool over this host's default base:
/// `[base, ceiling, poll_timeout_ms, retire_ms, drain_ms]`.
fn pool_open(default_base: u32) -> Result<Vec<u32>, Error> {
    Ok(veneer::pool_open(context()?, default_base)?.to_vec())
}

/// The pool's next decision as the ABI's status, at most a second away.
fn pool_supervise() -> Result<i32, Error> {
    let ctx = context()?;
    without_gvl(move || veneer::pool_supervise(ctx)).map_err(Error::from)
}

fn pool_left() -> Result<(), Error> {
    Ok(veneer::pool_left(context()?)?)
}

/// Answer one activation. Blocking, and this is the entry point the GVL rule
/// exists for on the completion side: under the default `DropNothing` overflow
/// policy the routing push parks until the consumer's mailbox drains, and the
/// consumer drains only when its handler runs on another Ruby thread.
fn complete(result: RHash) -> Result<(), Error> {
    let ruby = ruby();
    let ctx = context()?;
    let result = shape::result(&Values(&ruby), &result)?;
    without_gvl(move || veneer::complete(ctx, &result)).map_err(Error::from)
}

/// Abandon a polled activation: re-hand its untouched items and release its
/// cell. Non-blocking by design — the dispatcher runs it from an `ensure`,
/// where anything that can park is one more thing that can go wrong.
fn abandon(token: u64) -> Result<(), Error> {
    let ctx = context()?;
    veneer::abandon(ctx, token).map_err(Error::from)
}

/// Wake every blocked poller and stop the runner loop.
fn shutdown() -> Result<(), Error> {
    veneer::shutdown(context()?);
    Ok(())
}

/// Watch standard input and end the process on end of file, on the core's own
/// thread. The host reads descriptor 0 nowhere.
fn watch_stdin_eof() -> Result<(), Error> {
    context()?;
    veneer::watch_stdin_eof();
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
    let ctx = context()?;
    let factory = veneer::c_string("factory id", &factory_id)?;
    let id = veneer::c_string("id", &id)?;
    let mode = veneer::c_string("if_running", &if_running)?;
    // SAFETY: the GVL is held for the whole call, so nothing can move or free
    // the string's bytes while the core copies them.
    let payload = unsafe { payload.as_slice() };
    veneer::spawn_request(ctx, token, &factory, &id, payload, &mode, replay, offset)
        .map_err(Error::from)
}

/// Send a part of the live activation `token`'s work item `item`. Never
/// waits, so it holds the GVL.
fn part(token: u64, item: u32, payload: RString) -> Result<(), Error> {
    let ctx = context()?;
    // SAFETY: the GVL is held for the whole call, so nothing can move or free
    // the string's bytes while the core copies them.
    let payload = unsafe { payload.as_slice() };
    veneer::part(ctx, token, item, payload).map_err(Error::from)
}

/// Begin `ctx.effect(name)` in the live activation `token`'s work item `item`:
/// `[:kept, result]`, `[:run, nil]` or `[:unrecorded, nil]`. Never waits, so it
/// holds the GVL.
fn effect_begin(token: u64, item: u32, name: String) -> Result<(Symbol, Option<RString>), Error> {
    let ruby = ruby();
    let ctx = context()?;
    let name = veneer::c_string("effect name", &name)?;
    Ok(match veneer::effect_begin(ctx, token, item, &name)? {
        veneer::EffectStart::Kept(result) => {
            (ruby.to_symbol("kept"), Some(ruby.str_from_slice(&result)))
        }
        veneer::EffectStart::Run => (ruby.to_symbol("run"), None),
        veneer::EffectStart::Unrecorded => (ruby.to_symbol("unrecorded"), None),
    })
}

/// Keep the encoded result of the function a `run` begin ran, waiting with the
/// GVL released for the controller: `nil` to return the call's own result, or
/// the result the controller kept first.
fn effect_end(token: u64, item: u32, payload: RString) -> Result<Option<RString>, Error> {
    let ruby = ruby();
    let ctx = context()?;
    // Copied while the GVL is held: the wait below releases it.
    let payload = unsafe { payload.as_slice() }.to_vec();
    let first = without_gvl(move || veneer::effect_end(ctx, token, item, &payload))?;
    Ok(first.map(|first| ruby.str_from_slice(&first)))
}

/// End a begun effect without keeping anything. Never waits.
fn effect_release(token: u64, item: u32) -> Result<(), Error> {
    veneer::effect_release(context()?, token, item);
    Ok(())
}

/// Read a spawn handle: `nil` while pending, otherwise its answer as JSON. A
/// nonzero timeout waits with the GVL released.
fn spawn_result(handle: u64, timeout_ms: u64) -> Result<Option<String>, Error> {
    let ctx = context()?;
    let answer = if timeout_ms == 0 {
        veneer::spawn_result(ctx, handle, 0)
    } else {
        without_gvl(move || veneer::spawn_result(ctx, handle, timeout_ms))
    };
    answer.map_err(Error::from)
}

/// Administer the namespace this connection proved; answers the published
/// function's own result as JSON. One module function for the whole surface,
/// so a further verb is Ruby and no native work. Blocking: the ABI call waits
/// up to thirty seconds.
fn admin_call(verb: String, args: String) -> Result<String, Error> {
    let ctx = context()?;
    let verb = veneer::c_string("verb", &verb)?;
    let args = veneer::c_string("args", &args)?;
    without_gvl(move || veneer::admin_call(ctx, &verb, &args)).map_err(Error::from)
}

/// Send one message, through the controller or locally, answering as JSON:
/// the verdict, or `null` for a local send. Blocking: the controller's answer
/// takes a round trip, and a local send parks while the consumer's mailbox is
/// full.
fn send_message(
    relay: String,
    channel: String,
    message: RString,
    msg_id: Option<String>,
    key: Option<String>,
    local: bool,
) -> Result<String, Error> {
    let ruby = ruby();
    let ctx = context()?;
    let relay = veneer::c_string("relay", &relay)?;
    let channel = veneer::c_string("entry channel", &channel)?;
    let msg_id = veneer::c_string("message id", msg_id.as_deref().unwrap_or_default())?;
    let key = veneer::c_string("key", key.as_deref().unwrap_or_default())?;
    let message = read_bytes(&ruby, "message", message.as_value())?;
    without_gvl(move || veneer::send_message(ctx, &relay, &channel, &message, &msg_id, &key, local))
        .map_err(Error::from)
}

/// The ids of the relay instances bound on this runner.
fn relays() -> Result<Vec<String>, Error> {
    let ctx = context()?;
    veneer::relays(ctx).map_err(Error::from)
}

/// The core's monotonic clock in nanoseconds.
fn monotonic_nanos() -> u64 {
    crate::flz_core_monotonic_nanos()
}

/// A knob's text parsed by the core's rule for `kind`; `nil` when it is no
/// value of that kind.
fn knob(kind: String, raw: String) -> Result<Option<i64>, Error> {
    veneer::knob(&kind, &raw).map_err(Error::from)
}

/// The core's notices held since the last take, as `[level, target, text]`;
/// the first take starts the holding.
fn take_notices() -> Vec<(String, String, String)> {
    let notices = veneer::take_notices().into_iter();
    notices.map(|n| (n.level, n.target, n.text)).collect()
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

/// A `Response` payload as the core reads it: `[target, created, request,
/// value, error]`, exactly one of the binary `value` and the JSON error
/// document `error` set.
fn response_decode(payload: RString) -> Result<ResponseRow, Error> {
    let ruby = ruby();
    // SAFETY: the core reads the payload before `read` runs any Ruby.
    let parts = veneer::response_decode(unsafe { payload.as_slice() }, |r| {
        let (request, value) = (ruby.str_from_slice(r.request), r.result.ok());
        let value = value.map(|value| ruby.str_from_slice(value));
        let error = r.result.err().map(|error| ruby.str_new(error));
        (ruby.str_new(r.target), r.created, request, value, error)
    });
    parts.map_err(Error::from)
}

type ResponseRow = (RString, bool, RString, Option<RString>, Option<RString>);

/// Every function this extension exports, with the C ABI entry point behind
/// it: `[name, abi_entry_point, blocking]`, rendered from
/// [`crate::veneer::SURFACE`] under the entry's own name.
///
/// The veneer adds no ABI surface: every export names an existing entry
/// point, and this veneer has no host bookkeeping of its own because the Ruby
/// host's threads are Ruby's. Every export marked blocking runs inside
/// [`without_gvl`].
fn native_surface() -> Result<RArray, Error> {
    let ruby = ruby();
    let out = ruby.ary_new();
    let host = [("native_surface", "flz_core_schema_version", false)];
    let veneer = crate::veneer::glue_entries().map(|e| (e.name, e.abi, e.blocking));
    for (name, abi, blocking) in veneer.chain(host) {
        let row = ruby.ary_new_capa(3);
        row.push(name)?;
        row.push(abi)?;
        row.push(blocking)?;
        out.push(row)?;
    }
    Ok(out)
}

/// Define `Flezha::Native`. Named `Init_flz_core_ffi` so `require
/// "flz_core_ffi"` finds it. Its failures raise `Flezha::Error`, which the gem
/// defines before it loads this extension.
#[magnus::init]
pub fn init(ruby: &Ruby) -> Result<(), Error> {
    let flezha = ruby.define_module("Flezha")?;
    let native = flezha.define_module("Native")?;
    native.const_set("WAIT_CORE_BOUND", crate::FLZ_WAIT_CORE_BOUND)?;

    native.define_module_function("schema_version", function!(schema_version, 0))?;
    native.define_module_function("context_new", function!(context_new, 1))?;
    native.define_module_function("context_free", function!(context_free, 0))?;
    native.define_module_function("declare_class", function!(declare_class, 1))?;
    native.define_module_function("undeclare_class", function!(undeclare_class, 1))?;
    native.define_module_function("check_class", function!(check_class, 1))?;
    native.define_module_function("start", function!(start, 1))?;
    native.define_module_function("local_start", function!(local_start, 2))?;
    native.define_module_function("local_on_sink", function!(local_on_sink, 1))?;
    native.define_module_function("local_feed", function!(local_feed, 2))?;
    native.define_module_function("local_next_output", function!(local_next_output, 1))?;
    native.define_module_function("local_run_until_idle", function!(local_run_until_idle, 1))?;
    native.define_module_function("local_stop", function!(local_stop, 0))?;
    native.define_module_function("poll", function!(poll, 1))?;
    native.define_module_function("pool_open", function!(pool_open, 1))?;
    native.define_module_function("pool_supervise", function!(pool_supervise, 0))?;
    native.define_module_function("pool_left", function!(pool_left, 0))?;
    native.define_module_function("complete", function!(complete, 1))?;
    native.define_module_function("abandon", function!(abandon, 1))?;
    native.define_module_function("shutdown", function!(shutdown, 0))?;
    native.define_module_function("watch_stdin_eof", function!(watch_stdin_eof, 0))?;
    native.define_module_function("spawn_request", function!(spawn_request, 7))?;
    native.define_module_function("spawn_result", function!(spawn_result, 2))?;
    native.define_module_function("part", function!(part, 3))?;
    native.define_module_function("effect_begin", function!(effect_begin, 3))?;
    native.define_module_function("effect_end", function!(effect_end, 3))?;
    native.define_module_function("effect_release", function!(effect_release, 2))?;
    native.define_module_function("admin_call", function!(admin_call, 2))?;
    native.define_module_function("send_message", function!(send_message, 6))?;
    native.define_module_function("relays", function!(relays, 0))?;
    native.define_module_function("monotonic_nanos", function!(monotonic_nanos, 0))?;
    native.define_module_function("knob", function!(knob, 2))?;
    native.define_module_function("take_notices", function!(take_notices, 0))?;
    native.define_module_function("last_error", function!(last_error, 0))?;
    native.define_module_function("native_surface", function!(native_surface, 0))?;
    native.define_module_function("error_document", function!(error_document, 2))?;
    native.define_module_function("response_decode", function!(response_decode, 1))?;
    Ok(())
}
