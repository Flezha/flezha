//! JNI bindings over this crate's C ABI for the Java SDK's `FlzCore` class,
//! one export per C entry point. This module owns the byte-level crossing:
//! Java decodes postcard itself, so activations and payloads cross as fresh
//! `byte[]` copies read out of the core's buffer before it is freed, and
//! Java-owned arrays are copied in before the call returns. A refusal maps
//! once, in [`throw`], onto the SDK's `FlezhaException`, built from the JSON
//! error document.

use std::any::Any;
use std::ffi::CString;

use jni::errors::ErrorPolicy;
use jni::objects::{JByteArray, JClass, JIntArray, JString, JThrowable, JValue};
use jni::strings::JNIString;
use jni::sys::{jboolean, jint, jlong};
use jni::{jni_sig, jni_str, Env, EnvUnowned};

use crate::veneer::{self, with_buffer, CtxPtr, Refusal};
use crate::{FlzContext, FLZ_POLL_ACTIVATION, FLZ_POLL_SHUTTING_DOWN, FLZ_POLL_TIMEOUT};

/// Throw `FlezhaException.fromDocument(document)`. A value the binding could
/// not map carries no document of its own, so it is `internal.uncatalogued`
/// naming it; `RuntimeException` is the fallback off the SDK classpath.
fn throw(env: &mut Env, refusal: &Refusal) {
    let document = match refusal {
        Refusal::Shape(text) => flz_core::FlezhaError::with(
            flz_transport::errors::UNCATALOGUED,
            [("reason", text.as_str().into())],
        )
        .json(),
        Refusal::Core(document) | Refusal::Schema(document) => document.clone(),
    };
    let exception = (|| -> jni::errors::Result<JThrowable> {
        let text = env.new_string(&document)?;
        let exception = env
            .call_static_method(
                jni_str!("com/flezha/sdk/FlezhaException"),
                jni_str!("fromDocument"),
                jni_sig!("(Ljava/lang/String;)Lcom/flezha/sdk/FlezhaException;"),
                &[JValue::Object(&text)],
            )?
            .l()?;
        env.cast_local::<JThrowable>(exception)
    })();
    // A throw that took answers `JavaException`: the exception is now pending.
    let thrown = exception.and_then(|exception| env.throw(exception));
    if !matches!(thrown, Err(jni::errors::Error::JavaException)) {
        env.exception_clear();
        let _ = env.throw_new(
            jni_str!("java/lang/RuntimeException"),
            JNIString::from(document),
        );
    }
}

impl From<jni::errors::Error> for Refusal {
    fn from(e: jni::errors::Error) -> Self {
        Refusal::Shape(e.to_string())
    }
}

/// How every export resolves its outcome: a refusal is thrown by [`throw`],
/// and so is a panic, as a shape refusal naming it, rather than unwinding
/// into the JVM.
struct Throw;

impl<T: Default> ErrorPolicy<T, Refusal> for Throw {
    type Captures<'unowned_env_local: 'native_method, 'native_method> = ();

    fn on_error<'unowned_env_local: 'native_method, 'native_method>(
        env: &mut Env<'unowned_env_local>,
        _: &mut (),
        refusal: Refusal,
    ) -> jni::errors::Result<T> {
        throw(env, &refusal);
        Ok(T::default())
    }

    fn on_panic<'unowned_env_local: 'native_method, 'native_method>(
        env: &mut Env<'unowned_env_local>,
        _: &mut (),
        payload: Box<dyn Any + Send + 'static>,
    ) -> jni::errors::Result<T> {
        let what = flz_core::util::format_panic(payload);
        throw(env, &Refusal::Shape(format!("native panic: {what}")));
        Ok(T::default())
    }
}

fn ctx_of(ctx: jlong) -> CtxPtr {
    CtxPtr::of(ctx as *mut FlzContext)
}

fn c_string(env: &Env, what: &str, value: &JString) -> Result<CString, Refusal> {
    let value = value
        .try_to_string(env)
        .map_err(|e| Refusal::Shape(format!("{what} unreadable: {e}")))?;
    veneer::c_string(what, &value)
}

fn bytes_of(env: &Env, what: &str, array: &JByteArray) -> Result<Vec<u8>, Refusal> {
    env.convert_byte_array(array)
        .map_err(|e| Refusal::Shape(format!("{what} unreadable: {e}")))
}

/// A fresh Java `byte[]` holding `bytes`; `what` names them in a failure.
fn byte_array<'l>(env: &mut Env<'l>, what: &str, bytes: &[u8]) -> Result<JByteArray<'l>, Refusal> {
    env.byte_array_from_slice(bytes)
        .map_err(|e| Refusal::Shape(format!("{what} copy failed: {e}")))
}

/// The activation schema version this native library speaks.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_schemaVersion(
    _env: EnvUnowned,
    _class: JClass,
) -> jint {
    crate::flz_core_schema_version() as jint
}

/// Create the context, negotiating the schema version. Throws (naming both
/// versions) instead of returning a null handle.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_contextNew(
    mut env: EnvUnowned,
    _class: JClass,
    shim_schema_version: jint,
) -> jlong {
    env.with_env(|_| veneer::context_new(shim_schema_version as u32).map(|ctx| ctx as jlong))
        .resolve::<Throw>()
}

/// Start the declared class as a local run: no controller, no socket. The
/// JVM keeps driving `poll`/`complete`, which is what runs the tasklets.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_localStart(
    mut env: EnvUnowned,
    _class: JClass,
    ctx: jlong,
    factory_id: JString,
    relay_id: JString,
) {
    env.with_env(|env| {
        let factory = c_string(env, "factory id", &factory_id)?;
        let relay = c_string(env, "relay id", &relay_id)?;
        veneer::local_start(ctx_of(ctx), &factory, &relay)
    })
    .resolve::<Throw>()
}

/// Queue the sink named `channel` for `localNextOutput`.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_localOnSink(
    mut env: EnvUnowned,
    _class: JClass,
    ctx: jlong,
    channel: JString,
) {
    env.with_env(|env| {
        let channel = c_string(env, "output channel", &channel)?;
        veneer::local_on_sink(ctx_of(ctx), &channel)
    })
    .resolve::<Throw>()
}

/// Feed one value's wire bytes to a declared entry channel.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_localFeed(
    mut env: EnvUnowned,
    _class: JClass,
    ctx: jlong,
    channel: JString,
    payload: JByteArray,
) {
    env.with_env(|env| {
        let channel = c_string(env, "entry channel", &channel)?;
        let bytes = bytes_of(env, "entry payload", &payload)?;
        veneer::local_feed(ctx_of(ctx), &channel, &bytes)
    })
    .resolve::<Throw>()
}

/// One encoded `Output` for a value the graph released on a sink, or null
/// when none arrived within `timeoutMs`.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_localNextOutput<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass,
    ctx: jlong,
    timeout_ms: jlong,
) -> JByteArray<'l> {
    env.with_env(|env| {
        let output = veneer::local_next_output(ctx_of(ctx), timeout_ms.max(0) as u64)?;
        bytes_answer(env, "output", output)
    })
    .resolve::<Throw>()
}

/// Block until the graph is idle; a timeout throws rather than returning.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_localRunUntilIdle(
    mut env: EnvUnowned,
    _class: JClass,
    ctx: jlong,
    timeout_ms: jlong,
) {
    env.with_env(|_| veneer::local_run_until_idle(ctx_of(ctx), timeout_ms.max(0) as u64))
        .resolve::<Throw>()
}

/// End the local run; a context with none is a no-op.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_localStop(
    _env: EnvUnowned,
    _class: JClass,
    ctx: jlong,
) {
    veneer::local_stop(ctx_of(ctx));
}

/// Declare one host class from its JSON declaration. Throws the refusal.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_declareClass(
    mut env: EnvUnowned,
    _class: JClass,
    ctx: jlong,
    json: JByteArray,
) {
    env.with_env(|env| {
        let bytes = bytes_of(env, "declaration bytes", &json)?;
        veneer::declare_class(ctx_of(ctx), &bytes)
    })
    .resolve::<Throw>()
}

/// Undeclare one host class; after start, waits for the runner's
/// live-instance check. Throws the refusal.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_undeclareClass(
    mut env: EnvUnowned,
    _class: JClass,
    ctx: jlong,
    factory_id: JString,
) {
    env.with_env(|env| {
        let factory_id = c_string(env, "factory id", &factory_id)?;
        veneer::undeclare_class(ctx_of(ctx), &factory_id)
    })
    .resolve::<Throw>()
}

/// Judge a host class declaration without registering it: the derived
/// declaration's JSON bytes, or a throw whose `details.issues` names every
/// refusal.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_checkClass<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass,
    ctx: jlong,
    json: JByteArray,
) -> JByteArray<'l> {
    env.with_env(|env| {
        let submitted = bytes_of(env, "declaration bytes", &json)?;
        let derived = veneer::check_class(ctx_of(ctx), &submitted)?;
        byte_array(env, "derived declaration", &derived)
    })
    .resolve::<Throw>()
}

/// Connect to the controller and start the runner loop. Throws on failure.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_start(
    mut env: EnvUnowned,
    _class: JClass,
    ctx: jlong,
    namespace: JString,
) {
    env.with_env(|env| {
        let namespace = c_string(env, "namespace", &namespace)?;
        veneer::start(ctx_of(ctx), &namespace)
    })
    .resolve::<Throw>()
}

/// Block up to `timeout_ms` for one activation. `null` = timeout; a
/// zero-length array = shutting down (an encoded activation is never empty);
/// otherwise the postcard-encoded activation bytes.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_poll<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass,
    ctx: jlong,
    timeout_ms: jlong,
) -> JByteArray<'l> {
    let ctx = ctx_of(ctx);
    env.with_env(|env| {
        let (status, copied) = with_buffer(
            &[FLZ_POLL_ACTIVATION],
            |buf, len| unsafe {
                crate::flz_core_poll(ctx.get(), timeout_ms.max(0) as u64, buf, len)
            },
            |bytes| byte_array(env, "activation", bytes),
        );
        match (status, copied) {
            (FLZ_POLL_ACTIVATION, Some(array)) => array,
            (FLZ_POLL_TIMEOUT, _) => Ok(JByteArray::default()),
            (FLZ_POLL_SHUTTING_DOWN, _) => Ok(env.byte_array_from_slice(&[]).unwrap_or_default()),
            _ => Err(Refusal::Core(veneer::last_error_of(ctx))),
        }
    })
    .resolve::<Throw>()
}

/// Open the context's poll-worker pool and write its policy into `policy`:
/// base, ceiling, poll timeout, retire time and drain bound, in that order.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_poolOpen(
    mut env: EnvUnowned,
    _class: JClass,
    ctx: jlong,
    default_base: jint,
    policy: JIntArray,
) {
    env.with_env(|env| {
        let p = veneer::pool_open(ctx_of(ctx), default_base.max(1) as u32)?;
        policy
            .set_region(env, 0, &p.map(|f| f as jint))
            .map_err(|e| Refusal::Shape(format!("pool policy unwritable: {e}")))
    })
    .resolve::<Throw>()
}

/// The pool's next decision as the ABI's status, so a supervisor loop costs
/// one JNI call per answer and allocates nothing.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_poolSupervise(
    mut env: EnvUnowned,
    _class: JClass,
    ctx: jlong,
) -> jint {
    // A refusal answers the policy's default 0, which must read as idle.
    const _: () = assert!(crate::FLZ_POOL_IDLE == 0);
    env.with_env(|_| veneer::pool_supervise(ctx_of(ctx)))
        .resolve::<Throw>()
}

/// Give back one seat of the pool: a fetcher that left, or one that never started.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_poolLeft(
    mut env: EnvUnowned,
    _class: JClass,
    ctx: jlong,
) {
    env.with_env(|_| veneer::pool_left(ctx_of(ctx)))
        .resolve::<Throw>()
}

/// Complete an activation with a postcard-encoded `ActivationResult`.
/// Accepted from any thread. Throws on a decode/apply refusal.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_complete(
    mut env: EnvUnowned,
    _class: JClass,
    ctx: jlong,
    result: JByteArray,
) {
    let ctx = ctx_of(ctx);
    env.with_env(|env| {
        let bytes = bytes_of(env, "result bytes", &result)?;
        let status = unsafe { crate::flz_core_complete(ctx.get(), bytes.as_ptr(), bytes.len()) };
        veneer::verdict(ctx, status)
    })
    .resolve::<Throw>()
}

/// Abandon a polled activation by token: re-hand its untouched items and
/// release its cell. Java's `finally` calls it when a throwable escaped the
/// dispatch, so an unanswered activation can never retire a cell.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_abandon(
    mut env: EnvUnowned,
    _class: JClass,
    ctx: jlong,
    token: jlong,
) {
    env.with_env(|_| veneer::abandon(ctx_of(ctx), token as u64))
        .resolve::<Throw>()
}

/// A string answer as a Java `String`, `null` for none.
fn string_answer<'l>(env: &mut Env<'l>, answer: Option<String>) -> Result<JString<'l>, Refusal> {
    answer.map_or(Ok(JString::default()), |text| Ok(env.new_string(text)?))
}

/// A byte answer as a Java `byte[]`, `null` for none.
fn bytes_answer<'l>(
    env: &mut Env<'l>,
    what: &str,
    answer: Option<Vec<u8>>,
) -> Result<JByteArray<'l>, Refusal> {
    answer.map_or(Ok(JByteArray::default()), |bytes| {
        byte_array(env, what, &bytes)
    })
}

/// Request a spawn from the live activation `token`; returns the handle number
/// at once. An empty `id` asks the controller to generate one. Throws only for
/// an unreadable argument or a token that is not outstanding.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_spawnRequest(
    mut env: EnvUnowned,
    _class: JClass,
    ctx: jlong,
    token: jlong,
    factory_id: JString,
    id: JString,
    payload: JByteArray,
    if_running: JString,
    replay: jboolean,
    offset: jlong,
) -> jlong {
    env.with_env(|env| {
        let unreadable = |_| Refusal::Shape("spawn arguments unreadable".to_string());
        let factory = c_string(env, "factory id", &factory_id).map_err(unreadable)?;
        let id = c_string(env, "id", &id).map_err(unreadable)?;
        let mode = c_string(env, "if_running", &if_running).map_err(unreadable)?;
        let bytes = bytes_of(env, "payload bytes", &payload)?;
        let offset = u64::try_from(offset).ok();
        veneer::spawn_request(
            ctx_of(ctx),
            token as u64,
            &factory,
            &id,
            &bytes,
            &mode,
            replay,
            offset,
        )
        .map(|handle| handle as jlong)
    })
    .resolve::<Throw>()
}

/// Send a part of the invocation of the live activation `token`'s work item
/// `item`; never waits. Throws for `part.no_output` and
/// `part.outside_invocation`.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_part(
    mut env: EnvUnowned,
    _class: JClass,
    ctx: jlong,
    token: jlong,
    item: jint,
    payload: JByteArray,
) {
    env.with_env(|env| {
        let bytes = bytes_of(env, "part payload", &payload)?;
        veneer::part(ctx_of(ctx), token as u64, item as u32, &bytes)
    })
    .resolve::<Throw>()
}

/// Begin `ctx.effect(name)` in the live activation `token`'s work item `item`.
/// The first byte is the status (`FLZ_EFFECT_RUN`, `FLZ_EFFECT_KEPT`,
/// `FLZ_EFFECT_UNRECORDED`) and the rest, for a kept one, its result. Throws
/// for a refused call.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_effectBegin<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass,
    ctx: jlong,
    token: jlong,
    item: jint,
    name: JString,
) -> JByteArray<'l> {
    env.with_env(|env| {
        let name = c_string(env, "effect name", &name)
            .map_err(|_| Refusal::Shape("effect name unreadable".to_string()))?;
        let tagged = match veneer::effect_begin(ctx_of(ctx), token as u64, item as u32, &name)? {
            veneer::EffectStart::Kept(result) => {
                let mut tagged = vec![crate::FLZ_EFFECT_KEPT as u8];
                tagged.extend(result);
                tagged
            }
            veneer::EffectStart::Run => vec![crate::FLZ_EFFECT_RUN as u8],
            veneer::EffectStart::Unrecorded => vec![crate::FLZ_EFFECT_UNRECORDED as u8],
        };
        byte_array(env, "result", &tagged)
    })
    .resolve::<Throw>()
}

/// Keep the encoded result of the function a run begin ran, waiting for the
/// controller: `null` to return the call's own result, or the result the
/// controller kept first. Throws for `effect.too_large` and
/// `effect.not_recorded`.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_effectEnd<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass,
    ctx: jlong,
    token: jlong,
    item: jint,
    payload: JByteArray,
) -> JByteArray<'l> {
    env.with_env(|env| {
        let bytes = bytes_of(env, "effect result", &payload)?;
        let first = veneer::effect_end(ctx_of(ctx), token as u64, item as u32, &bytes)?;
        bytes_answer(env, "result", first)
    })
    .resolve::<Throw>()
}

/// End a begun effect without keeping anything.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_effectRelease(
    _env: EnvUnowned,
    _class: JClass,
    ctx: jlong,
    token: jlong,
    item: jint,
) {
    veneer::effect_release(ctx_of(ctx), token as u64, item as u32);
}

/// Read a spawn handle, waiting up to `timeoutMs`: `null` while pending,
/// otherwise its answer as JSON. Throws for a released handle.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_spawnResult<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass,
    ctx: jlong,
    handle: jlong,
    timeout_ms: jlong,
) -> JString<'l> {
    env.with_env(|env| {
        let answer = veneer::spawn_result(ctx_of(ctx), handle as u64, timeout_ms.max(0) as u64)?;
        string_answer(env, answer)
    })
    .resolve::<Throw>()
}

/// Administer the namespace this connection proved: the published function's
/// answer as JSON. One export for the whole surface, so a further verb is a
/// Java method and no JNI. Throws on refusal / no control plane / timeout.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_adminCall<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass,
    ctx: jlong,
    verb: JString,
    args: JString,
) -> JString<'l> {
    env.with_env(|env| {
        let verb = c_string(env, "verb", &verb)?;
        let args = c_string(env, "args", &args)?;
        let answer = veneer::admin_call(ctx_of(ctx), &verb, &args)?;
        string_answer(env, Some(answer))
    })
    .resolve::<Throw>()
}

/// Send one message, through the controller or locally, answering as JSON:
/// the verdict, or `null` for a local send. An empty id or key is none.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_sendMessage<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass,
    ctx: jlong,
    relay: JString,
    channel: JString,
    message: JByteArray,
    msg_id: JString,
    key: JString,
    local: jboolean,
) -> JString<'l> {
    env.with_env(|env| {
        let relay = c_string(env, "relay", &relay)?;
        let channel = c_string(env, "entry channel", &channel)?;
        let msg_id = c_string(env, "message id", &msg_id)?;
        let key = c_string(env, "key", &key)?;
        let message = bytes_of(env, "message", &message)?;
        let verdict = veneer::send_message(
            ctx_of(ctx),
            &relay,
            &channel,
            &message,
            &msg_id,
            &key,
            local,
        )?;
        string_answer(env, Some(verdict))
    })
    .resolve::<Throw>()
}

/// The ids of the relay instances bound on this runner, as a JSON array.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_relays<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass,
    ctx: jlong,
) -> JString<'l> {
    env.with_env(|env| {
        let ids = veneer::relays(ctx_of(ctx))?;
        string_answer(env, Some(serde_json::json!(ids).to_string()))
    })
    .resolve::<Throw>()
}

/// The core's notices held since the last take, as a JSON array of
/// `{"level", "target", "text"}`; the first take starts the holding.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_takeNotices<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass,
) -> JString<'l> {
    env.with_env(|env| {
        let notices = serde_json::json!(veneer::take_notices()).to_string();
        string_answer(env, Some(notices))
    })
    .resolve::<Throw>()
}

/// The core's monotonic clock in nanoseconds.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_monotonicNanos(
    _env: EnvUnowned,
    _class: JClass,
) -> jlong {
    crate::flz_core_monotonic_nanos() as jlong
}

/// A knob's text parsed by the core's rule for `kind`, as its decimal text;
/// null when it is no value of that kind.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_knob<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass,
    kind: JString,
    raw: JString,
) -> JString<'l> {
    env.with_env(|env| {
        let answer = match (kind.try_to_string(env), raw.try_to_string(env)) {
            (Ok(kind), Ok(raw)) => veneer::knob(&kind, &raw)?.map(|v| v.to_string()),
            _ => return Err(Refusal::Shape("a knob argument is unreadable".into())),
        };
        string_answer(env, answer)
    })
    .resolve::<Throw>()
}

/// The JSON error document the catalogue renders for `code` and the JSON
/// object `detailsJson`: a refusal the SDK makes itself.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_errorDocument<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass,
    code: JString,
    details_json: JString,
) -> JString<'l> {
    env.with_env(|env| {
        let document = match (code.try_to_string(env), details_json.try_to_string(env)) {
            (Ok(code), Ok(details)) => crate::render_error_document(&code, &details),
            _ => Err("an error document argument is unreadable".into()),
        };
        string_answer(env, Some(document.map_err(Refusal::Shape)?))
    })
    .resolve::<Throw>()
}

/// A `Response` channel payload as the core's decoder reads it: its target,
/// request and result back to back, with `[target length, request length,
/// result length, created, answered]` written into `parts`. Throws
/// `call.malformed_response`.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_responseDecode<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass,
    payload: JByteArray,
    parts: JIntArray,
) -> JByteArray<'l> {
    env.with_env(|env| {
        let payload = bytes_of(env, "Response payload", &payload)?;
        let (mut out, mut decoded) = (vec![0; payload.len()], [0; 5]);
        veneer::response_into(&payload, &mut out, &mut decoded)?;
        parts
            .set_region(env, 0, &decoded.map(|part| part as jint))
            .map_err(|e| Refusal::Shape(format!("Response parts unwritable: {e}")))?;
        out.truncate(decoded[..3].iter().sum());
        byte_array(env, "result", &out)
    })
    .resolve::<Throw>()
}

/// Wake every blocked poller and stop the runner loop.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_shutdown(
    _env: EnvUnowned,
    _class: JClass,
    ctx: jlong,
) {
    veneer::shutdown(ctx_of(ctx));
}

/// Watch standard input and end the process on end of file, on the core's own
/// thread, as the SIGTERM handler ends it.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_watchStdinEof(
    _env: EnvUnowned,
    _class: JClass,
) {
    veneer::watch_stdin_eof();
}

/// Destroy the context.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_contextFree(
    _env: EnvUnowned,
    _class: JClass,
    ctx: jlong,
) {
    veneer::context_free(ctx_of(ctx));
}
