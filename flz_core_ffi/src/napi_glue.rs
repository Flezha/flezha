//! N-API bindings over this crate's C ABI for the Node SDK's native binding,
//! one export per C entry point.
//!
//! A `worker_thread` is a separate isolate with its own module instance, so
//! the one context lives in [`CONTEXT`], a process global every isolate
//! reaches. The event-loop rule is [`off_loop`]: `flz_core_poll` is never
//! exported (the fetchers in [`crate::napi_host`] drive it), and every other
//! blocking entry point answers a promise resolved from the host's own
//! growable pool — under the default overflow policy a completion parks
//! until the consumer's mailbox drains, and the consumer drains only when its
//! handler runs on an event loop. Payload `Vec`s move into `Buffer`s without
//! copying.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicPtr, Ordering};

use napi::bindgen_prelude::*;
use napi::ValueType;
use napi_derive::napi;

use crate::veneer::{self, shape, CtxPtr, Refusal};
use crate::FlzContext;

/// The one context this process holds. Null until `contextNew`.
static CONTEXT: AtomicPtr<FlzContext> = AtomicPtr::new(std::ptr::null_mut());

/// The one mapping of a refusal onto a JavaScript error: a value the binding
/// could not map is an argument error, everything else a generic failure.
impl From<Refusal> for Error {
    fn from(refusal: Refusal) -> Self {
        match refusal {
            Refusal::Core(message) | Refusal::Schema(message) => {
                Error::new(Status::GenericFailure, message)
            }
            Refusal::Shape(message) => Error::new(Status::InvalidArg, message),
        }
    }
}

/// The live context, or an error naming what the caller skipped.
pub(crate) fn context() -> Result<CtxPtr> {
    let ptr = CONTEXT.load(Ordering::Acquire);
    if ptr.is_null() {
        return Err(Error::new(
            Status::InvalidArg,
            "flz_core context not created: call contextNew first",
        ));
    }
    Ok(CtxPtr::of(ptr))
}

/// Run one blocking ABI call off the event loop, and answer a promise with it.
///
/// The single place the event-loop rule is implemented. Every entry point that
/// can park goes through here, so "no blocking core call runs on a JavaScript
/// thread" is a property of one function rather than of four correct copies —
/// and the pool the work lands on is the growable one, which is what keeps a
/// parked completion from exhausting it.
fn off_loop<'a, T, F>(env: &'a Env, ctx: CtxPtr, work: F) -> Result<Object<'a>>
where
    T: ToNapiValue + 'static,
    F: FnOnce(CtxPtr) -> Result<T> + Send + 'static,
{
    let (deferred, promise) = env.create_deferred::<T, _>()?;
    crate::napi_host::blocking_pool().submit(move || match work(ctx) {
        Ok(value) => deferred.resolve(move |_| Ok(value)),
        Err(failure) => deferred.reject(failure),
    });
    Ok(promise)
}

// ---------------------------------------------------------------------------
// The decoded activation, as JavaScript sees it.
//
// The veneer decodes postcard on the Rust side (`schema::decode_activation`)
// so the SDK never carries a second implementation of the wire contract. The
// shape is flat with a `kind` discriminator rather than a union, because napi
// object mapping has no sum type and a discriminated record is what JavaScript
// switches on anyway. Which key carries which field is `veneer::shape`'s; this
// module says only how JavaScript builds and reads a value, in camelCase.
//
// The values are built on the JavaScript thread, inside the threadsafe
// function's callback: a fetcher thread has no `Env`, and the objects had to
// be built there in any case.
// ---------------------------------------------------------------------------

/// One property of an object, absent when the key is missing or carries
/// `undefined` / `null`.
///
/// Deliberately not `Object::get`, which is four N-API calls per read: it
/// mints a JavaScript string for the key (`napi_create_string_utf8`), reads
/// through `napi_get_property`, and calls `napi_typeof` — and the caller then
/// needs its own `napi_typeof` to tell an absent key from an explicit `null`.
/// `napi_get_named_property` takes the key as a C string, which V8 interns, so
/// one read plus one `napi_typeof` answers the same question.
fn named<'env>(map: &Object<'env>, key: &str) -> Result<Option<Unknown<'env>>> {
    let found: Unknown<'env> = map.get_named_property_unchecked(key)?;
    Ok(match found.get_type()? {
        ValueType::Undefined | ValueType::Null => None,
        _ => Some(found),
    })
}

/// A `BigInt` argument as the `u64` the ABI takes. Also what the shape's
/// numeric reader answers with, so an out-of-range integer is refused in one
/// place whether it arrives as an argument or inside an answer.
fn from_big(what: &str, v: &BigInt) -> Result<u64> {
    let (_, value, lossless) = v.get_u64();
    if !lossless {
        return Err(Error::new(
            Status::InvalidArg,
            format!("{what} does not fit an unsigned 64-bit integer"),
        ));
    }
    Ok(value)
}

/// The JavaScript half of [`shape::Sink`] and [`shape::Source`]: a handle over
/// the environment of the loop the value belongs to, not state.
#[derive(Clone, Copy)]
pub struct Values<'env>(Env, PhantomData<&'env ()>);

impl<'env> Values<'env> {
    pub fn of(env: Env) -> Self {
        Self(env, PhantomData)
    }

    /// One JavaScript value, type-erased so every `Sink` method answers the
    /// same type.
    fn erased<T: ToNapiValue>(&self, value: T) -> Result<Unknown<'env>> {
        let raw = unsafe { T::to_napi_value(self.0.raw(), value)? };
        unsafe { Unknown::from_napi_value(self.0.raw(), raw) }
    }

    fn read<T: FromNapiValue>(&self, value: &Unknown<'env>) -> Result<T> {
        unsafe { T::from_napi_value(self.0.raw(), value.raw()) }
    }

    fn refused<T>(what: &str, expected: &str) -> Result<T> {
        Err(Error::new(
            Status::InvalidArg,
            format!("{what} must be {expected}"),
        ))
    }
}

impl<'env> shape::Sink for Values<'env> {
    type Value = Unknown<'env>;
    type Map = Object<'env>;
    type List = Vec<Unknown<'env>>;
    type Error = Error;

    const CAMEL: bool = true;

    fn map(&self) -> Result<Object<'env>> {
        Object::new(&self.0)
    }

    fn set(&self, map: &mut Object<'env>, key: shape::Key, value: Unknown<'env>) -> Result<()> {
        map.set(key.spell(<Self as shape::Sink>::CAMEL), value)
    }

    fn finish_map(&self, map: Object<'env>) -> Result<Unknown<'env>> {
        unsafe { Unknown::from_napi_value(self.0.raw(), map.raw()) }
    }

    fn list(&self, capacity: usize) -> Result<Vec<Unknown<'env>>> {
        Ok(Vec::with_capacity(capacity))
    }

    fn push(&self, list: &mut Vec<Unknown<'env>>, value: Unknown<'env>) -> Result<()> {
        list.push(value);
        Ok(())
    }

    fn finish_list(&self, list: Vec<Unknown<'env>>) -> Result<Unknown<'env>> {
        self.erased(Array::from_vec(&self.0, list)?)
    }

    /// The payload `Vec` moves into the `Buffer`, so it reaches the handler
    /// with no copy beyond the one postcard already made.
    fn bytes(&self, v: Vec<u8>) -> Result<Unknown<'env>> {
        self.erased(Buffer::from(v))
    }

    fn text(&self, v: String) -> Result<Unknown<'env>> {
        self.erased(v)
    }

    fn word(&self, v: &'static str) -> Result<Unknown<'env>> {
        self.erased(v)
    }

    fn pair(&self, key: String, value: String) -> Result<Unknown<'env>> {
        let mut out = Object::new(&self.0)?;
        out.set(shape::KEY.camel, key)?;
        out.set(shape::VALUE.camel, value)?;
        self.finish_map(out)
    }

    fn u64(&self, v: u64) -> Result<Unknown<'env>> {
        self.erased(BigInt::from(v))
    }

    fn u32(&self, v: u32) -> Result<Unknown<'env>> {
        self.erased(v)
    }

    fn bool(&self, v: bool) -> Result<Unknown<'env>> {
        self.erased(v)
    }
}

impl<'env> shape::Source for Values<'env> {
    type Value = Unknown<'env>;
    type Map = Object<'env>;
    type Error = Error;

    const CAMEL: bool = true;

    fn get(&self, map: &Object<'env>, key: shape::Key) -> Result<Option<Unknown<'env>>> {
        named(map, key.spell(<Self as shape::Source>::CAMEL))
    }

    fn map(&self, v: &Unknown<'env>, what: &str) -> Result<Object<'env>> {
        self.read(v).or_else(|_| Self::refused(what, "an object"))
    }

    fn text(&self, v: &Unknown<'env>) -> Result<String> {
        self.read(v)
    }

    fn bytes(&self, v: &Unknown<'env>, what: &str) -> Result<Vec<u8>> {
        match self.read::<Buffer>(v) {
            Ok(buffer) => Ok(buffer.to_vec()),
            Err(_) => Self::refused(what, "a Buffer"),
        }
    }

    fn u64(&self, v: &Unknown<'env>, what: &str) -> Result<u64> {
        from_big(what, &self.read::<BigInt>(v)?)
    }

    fn u32(&self, v: &Unknown<'env>) -> Result<u32> {
        self.read(v)
    }

    fn bool(&self, v: &Unknown<'env>) -> Result<bool> {
        self.read(v)
    }

    fn items(&self, v: &Unknown<'env>) -> Result<Vec<Unknown<'env>>> {
        let array = self.read::<Array<'env>>(v)?;
        let mut out = Vec::with_capacity(array.len() as usize);
        for index in 0..array.len() {
            match array.get::<Unknown<'env>>(index)? {
                Some(value) => out.push(value),
                None => return Self::refused("a list entry", "present"),
            }
        }
        Ok(out)
    }

    fn pair(&self, v: &Unknown<'env>) -> Result<(String, String)> {
        let entry = self.map(v, "a metadata entry")?;
        match (
            named(&entry, shape::KEY.camel)?,
            named(&entry, shape::VALUE.camel)?,
        ) {
            (Some(key), Some(value)) => Ok((self.read(&key)?, self.read(&value)?)),
            _ => Self::refused("a metadata entry", "a { key, value } pair"),
        }
    }
}

// ---------------------------------------------------------------------------
// The entry points. Every one below corresponds to exactly one C entry point.
// ---------------------------------------------------------------------------

/// The activation schema version this native binary speaks.
#[napi]
pub fn schema_version() -> u32 {
    crate::flz_core_schema_version()
}

/// Create the process context, negotiating the schema version. Throws naming
/// both versions on a mismatch, before any controller connection is attempted.
#[napi]
pub fn context_new(shim_schema_version: u32) -> Result<()> {
    if !CONTEXT.load(Ordering::Acquire).is_null() {
        return Err(Error::new(
            Status::InvalidArg,
            "flz_core context already created for this process",
        ));
    }
    let ctx = veneer::context_new(shim_schema_version)?;
    CONTEXT.store(ctx, Ordering::Release);
    Ok(())
}

/// Destroy the process context. The host's threads must already be stopped.
#[napi]
pub fn context_free() {
    let ctx = CONTEXT.swap(std::ptr::null_mut(), Ordering::AcqRel);
    if !ctx.is_null() {
        veneer::context_free(CtxPtr::of(ctx));
    }
}

/// Declare one host class from its JSON declaration. Throws the core's refusal.
#[napi]
pub fn declare_class(json: Buffer) -> Result<()> {
    Ok(veneer::declare_class(context()?, &json)?)
}

/// Connect to the controller and start the runner loop. Asynchronous: the ABI
/// call waits up to ten seconds for the connect confirmation, which is ten
/// seconds an event loop may not spend inside native code.
#[napi(ts_return_type = "Promise<void>")]
pub fn start<'a>(env: &'a Env, namespace: String) -> Result<Object<'a>> {
    let namespace = veneer::c_string("namespace", &namespace)?;
    off_loop(env, context()?, move |ctx| {
        Ok(veneer::start(ctx, &namespace)?)
    })
}

/// Answer one activation. Asynchronous, and this is the entry point the rule
/// exists for: under the default `DropNothing` overflow policy the routing
/// push parks until the consumer's mailbox drains, and the consumer drains
/// only when its handler runs on an event loop.
#[napi(ts_return_type = "Promise<void>")]
pub fn complete<'a>(env: &'a Env, result: Object<'a>) -> Result<Object<'a>> {
    let result = shape::result(&Values::of(*env), &result)?;
    let token = result.token;
    off_loop(env, context()?, move |ctx| {
        let verdict = veneer::complete(ctx, &result);
        crate::napi_host::release_token(token);
        Ok(verdict?)
    })
}

/// Abandon a polled activation: re-hand its untouched items and release its
/// entity. Synchronous by design — the answer guard runs it from a `finally`
/// and from a dying worker's cleanup, where a promise would be one more thing
/// that can be dropped.
#[napi]
pub fn abandon(token: BigInt) -> Result<()> {
    let ctx = context()?;
    let token = from_big("token", &token)?;
    crate::napi_host::release_token(token);
    Ok(veneer::abandon(ctx, token)?)
}

/// Wake every blocked poller and stop the runner loop.
#[napi]
pub fn shutdown() -> Result<()> {
    veneer::shutdown(context()?);
    Ok(())
}

/// Emit one output payload from a live workflow run-sugar body. Non-blocking
/// in the core (an unbounded coordinator send), so synchronous here.
#[napi]
pub fn workflow_emit(instance_id: String, payload: Buffer) -> Result<()> {
    let ctx = context()?;
    let instance = veneer::c_string("instance id", &instance_id)?;
    Ok(veneer::workflow_emit(ctx, &instance, &payload)?)
}

/// The next inbox item of a live workflow body, or `null` on timeout.
/// Asynchronous: it parks for the whole timeout when the inbox is empty.
#[napi(ts_return_type = "Promise<Buffer | null>")]
pub fn workflow_recv<'a>(env: &'a Env, instance_id: String, timeout_ms: u32) -> Result<Object<'a>> {
    let instance = veneer::c_string("instance id", &instance_id)?;
    off_loop(env, context()?, move |ctx| {
        Ok(veneer::workflow_recv(ctx, &instance, u64::from(timeout_ms))?.map(Buffer::from))
    })
}

/// Whether the named workflow body's cancellation token has tripped.
#[napi]
pub fn workflow_cancelled(instance_id: String) -> Result<bool> {
    let ctx = context()?;
    let instance = veneer::c_string("instance id", &instance_id)?;
    Ok(veneer::workflow_cancelled(ctx, &instance))
}

/// Spawn-or-deliver a workflow instance; resolves with the controller's
/// outcome string. Asynchronous: the ABI call waits up to thirty seconds.
#[napi(ts_return_type = "Promise<string>")]
pub fn spawn_workflow<'a>(
    env: &'a Env,
    instance_id: String,
    factory_id: String,
    payload: Buffer,
    if_running: String,
) -> Result<Object<'a>> {
    let instance = veneer::c_string("instance id", &instance_id)?;
    let factory = veneer::c_string("factory id", &factory_id)?;
    let mode = veneer::c_string("ifRunning", &if_running)?;
    let payload = payload.to_vec();
    off_loop(env, context()?, move |ctx| {
        Ok(veneer::spawn_workflow(
            ctx, &instance, &factory, &payload, &mode,
        )?)
    })
}

/// The core's monotonic clock in nanoseconds.
#[napi]
pub fn monotonic_nanos() -> BigInt {
    BigInt::from(crate::flz_core_monotonic_nanos())
}

/// When the named entry channel last received a frame, on the same clock;
/// `0n` when no frame ever has.
#[napi]
pub fn entry_last_delivery_nanos(channel: String) -> Result<BigInt> {
    let channel = veneer::c_string("channel", &channel)?;
    Ok(BigInt::from(veneer::entry_last_delivery_nanos(&channel)))
}

/// One answer from `idleWatchPoll`: the ABI's status and the episode's gap.
#[napi(object)]
pub struct JsIdleEpisode {
    /// `0` no episode, `1` idle, `2` resumed, `-1` an id no watch has.
    pub status: i32,
    /// The episode's gap in nanoseconds; `0n` with no episode.
    pub gap_nanos: BigInt,
}

/// Register an episode latch over the named entry channel in the core; the
/// tick that advances it is this host's own timer.
#[napi]
pub fn idle_watch_new(channel: String, threshold_nanos: BigInt) -> Result<BigInt> {
    let channel = veneer::c_string("channel", &channel)?;
    let threshold = from_big("thresholdNanos", &threshold_nanos)?;
    Ok(BigInt::from(veneer::idle_watch_new(&channel, threshold)))
}

/// Advance a watch's latch to now.
#[napi]
pub fn idle_watch_poll(id: BigInt) -> Result<JsIdleEpisode> {
    let (status, gap) = veneer::idle_watch_poll(from_big("id", &id)?);
    Ok(JsIdleEpisode {
        status,
        gap_nanos: BigInt::from(gap),
    })
}

/// Drop a watch.
#[napi]
pub fn idle_watch_free(id: BigInt) -> Result<()> {
    veneer::idle_watch_free(from_big("id", &id)?);
    Ok(())
}

/// The context's last recorded error, or `null`.
#[napi]
pub fn last_error() -> Option<String> {
    let ptr = CONTEXT.load(Ordering::Acquire);
    if ptr.is_null() {
        return None;
    }
    veneer::last_error(CtxPtr::of(ptr))
}

/// Register the host's log callback: the one host function native threads
/// ever call, for this host's own diagnostics.
#[napi]
pub fn set_log_callback(callback: Option<Function<crate::napi_host::JsLogLine, ()>>) -> Result<()> {
    crate::napi_host::set_log_callback(callback)
}

/// One exported function, and the C ABI entry point behind it.
#[napi(object)]
pub struct JsNativeExport {
    /// The JavaScript name.
    pub name: String,
    /// The C ABI entry point it reaches, or `null` for host bookkeeping that
    /// reaches none.
    pub abi: Option<String>,
    /// Whether that entry point can block. Every one of these is a promise.
    pub blocking: bool,
}

/// Every function this addon exports, with the C ABI entry point behind it.
///
/// Two properties are asserted against this table rather than reviewed, so
/// neither can lapse by omission when an export is added later:
///
/// * the veneer adds no ABI surface — every export names an existing entry
///   point or is host bookkeeping that reaches none;
/// * no blocking entry point is synchronously reachable — `poll` and
///   `waitStarvation` are absent from the exports entirely, and every export
///   marked blocking returns a promise.
#[napi]
pub fn native_surface() -> Vec<JsNativeExport> {
    // The veneer: one export, one C entry point, blocking as the ABI says.
    fn veneer(name: &str, abi: &str) -> JsNativeExport {
        host(name, Some(abi), crate::BLOCKING_ENTRY_POINTS.contains(&abi))
    }
    fn host(name: &str, abi: Option<&str>, blocking: bool) -> JsNativeExport {
        JsNativeExport {
            name: name.to_string(),
            abi: abi.map(str::to_string),
            blocking,
        }
    }
    vec![
        veneer("schemaVersion", "flz_core_schema_version"),
        veneer("contextNew", "flz_core_context_new"),
        veneer("contextFree", "flz_core_context_free"),
        veneer("declareClass", "flz_core_declare_class"),
        veneer("start", "flz_core_start"),
        veneer("complete", "flz_core_complete"),
        veneer("abandon", "flz_core_abandon"),
        veneer("shutdown", "flz_core_shutdown"),
        veneer("workflowEmit", "flz_core_workflow_emit"),
        veneer("workflowRecv", "flz_core_workflow_recv"),
        veneer("workflowCancelled", "flz_core_workflow_cancelled"),
        veneer("spawnWorkflow", "flz_core_spawn_workflow"),
        veneer("monotonicNanos", "flz_core_monotonic_nanos"),
        veneer(
            "entryLastDeliveryNanos",
            "flz_core_entry_last_delivery_nanos",
        ),
        veneer("idleWatchNew", "flz_core_idle_watch_new"),
        veneer("idleWatchPoll", "flz_core_idle_watch_poll"),
        veneer("idleWatchFree", "flz_core_idle_watch_free"),
        veneer("lastError", "flz_core_last_error"),
        host("setLogCallback", None, false),
        // The host's threads. `startHost` is how `flz_core_poll` is reached at
        // all: it is driven from native fetchers and never exported, because
        // an event loop inside a blocking poll is an event loop running
        // nothing else.
        host("startHost", Some("flz_core_poll"), false),
        host("stopHost", Some("flz_core_poll"), true),
        host("registerDispatch", None, false),
        host("unregisterDispatch", None, false),
        host("dispatchLoopCount", None, false),
        host("hostStats", None, false),
        host("setHostDrainedCallback", None, false),
        host("watchStdinEof", Some("flz_core_shutdown"), false),
        host("nativeSurface", None, false),
    ]
}
