//! JNI bindings over this crate's C ABI for the Java SDK's `FlzCore` class,
//! one export per C entry point. This module owns the byte-level crossing:
//! Java decodes postcard itself, so activations and payloads cross as fresh
//! `byte[]` copies read out of the core's buffer before it is freed, and
//! Java-owned arrays are copied in before the call returns. A refusal maps
//! once, in [`throw`], onto the SDK's `FlezhaException`, built from the JSON
//! error document.

use std::ffi::CString;

use jni::objects::{JByteArray, JClass, JIntArray, JString, JThrowable, JValue};
use jni::sys::{jboolean, jbyteArray, jint, jlong, jstring};
use jni::JNIEnv;

use crate::veneer::{self, with_buffer, CtxPtr, Refusal};
use crate::{FlzContext, FLZ_POLL_ACTIVATION, FLZ_POLL_SHUTTING_DOWN, FLZ_POLL_TIMEOUT};

const EXCEPTION: &str = "com/flezha/sdk/FlezhaException";
const FROM_DOCUMENT: &str = "(Ljava/lang/String;)Lcom/flezha/sdk/FlezhaException;";

/// Throw `FlezhaException.fromDocument(document)`. A value the binding could
/// not map carries no document of its own, so it is `internal.uncatalogued`
/// naming it; `RuntimeException` is the fallback off the SDK classpath.
fn throw(env: &mut JNIEnv, refusal: &Refusal) {
    let document = match refusal {
        Refusal::Shape(text) => flz_core::FlezhaError::with(
            flz_transport::errors::UNCATALOGUED,
            [("reason", text.as_str().into())],
        )
        .json(),
        Refusal::Core(document) | Refusal::Schema(document) => document.clone(),
    };
    let thrown = (|| -> jni::errors::Result<()> {
        let text = env.new_string(&document)?;
        let exception = env
            .call_static_method(
                EXCEPTION,
                "fromDocument",
                FROM_DOCUMENT,
                &[JValue::Object(&text)],
            )?
            .l()?;
        env.throw(JThrowable::from(exception))
    })();
    if thrown.is_err() {
        let _ = env.exception_clear();
        let _ = env.throw_new("java/lang/RuntimeException", document);
    }
}

fn report(env: &mut JNIEnv, outcome: Result<(), Refusal>) {
    if let Err(refusal) = outcome {
        throw(env, &refusal);
    }
}

fn ctx_of(ctx: jlong) -> CtxPtr {
    CtxPtr::of(ctx as *mut FlzContext)
}

fn c_string(env: &mut JNIEnv, what: &str, value: &JString) -> Result<CString, Refusal> {
    let value: String = env
        .get_string(value)
        .map_err(|e| Refusal::Shape(format!("{what} unreadable: {e}")))?
        .into();
    veneer::c_string(what, &value)
}

fn bytes_of(env: &mut JNIEnv, what: &str, array: &JByteArray) -> Result<Vec<u8>, Refusal> {
    env.convert_byte_array(array)
        .map_err(|e| Refusal::Shape(format!("{what} unreadable: {e}")))
}

/// The activation schema version this native library speaks.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_schemaVersion(
    _env: JNIEnv,
    _class: JClass,
) -> jint {
    crate::flz_core_schema_version() as jint
}

/// Create the context, negotiating the schema version. Throws (naming both
/// versions) instead of returning a null handle.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_contextNew(
    mut env: JNIEnv,
    _class: JClass,
    shim_schema_version: jint,
) -> jlong {
    match veneer::context_new(shim_schema_version as u32) {
        Ok(ctx) => ctx as jlong,
        Err(refusal) => {
            throw(&mut env, &refusal);
            0
        }
    }
}

/// Start the declared class as a local run: no controller, no socket. The
/// JVM keeps driving `poll`/`complete`, which is what runs the tasklets.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_localStart(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    factory_id: JString,
    relay_id: JString,
) {
    let ctx = ctx_of(ctx);
    let outcome = c_string(&mut env, "factory id", &factory_id).and_then(|factory| {
        c_string(&mut env, "relay id", &relay_id)
            .and_then(|relay| veneer::local_start(ctx, &factory, &relay))
    });
    report(&mut env, outcome);
}

/// Queue the sink named `channel` for `localNextOutput`.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_localOnSink(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    channel: JString,
) {
    let outcome = c_string(&mut env, "output channel", &channel)
        .and_then(|channel| veneer::local_on_sink(ctx_of(ctx), &channel));
    report(&mut env, outcome);
}

/// Feed one value's wire bytes to a declared entry channel.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_localFeed(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    channel: JString,
    payload: JByteArray,
) {
    let ctx = ctx_of(ctx);
    let outcome = c_string(&mut env, "entry channel", &channel).and_then(|channel| {
        bytes_of(&mut env, "entry payload", &payload)
            .and_then(|bytes| veneer::local_feed(ctx, &channel, &bytes))
    });
    report(&mut env, outcome);
}

/// One encoded `Output` for a value the graph released on a sink, or null
/// when none arrived within `timeoutMs`.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_localNextOutput(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    timeout_ms: jlong,
) -> jbyteArray {
    match veneer::local_next_output(ctx_of(ctx), timeout_ms.max(0) as u64) {
        Ok(Some(bytes)) => match env.byte_array_from_slice(&bytes) {
            Ok(array) => array.into_raw(),
            Err(e) => {
                throw(
                    &mut env,
                    &Refusal::Shape(format!("output copy failed: {e}")),
                );
                std::ptr::null_mut()
            }
        },
        Ok(None) => std::ptr::null_mut(),
        Err(refusal) => {
            throw(&mut env, &refusal);
            std::ptr::null_mut()
        }
    }
}

/// Block until the graph is idle; a timeout throws rather than returning.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_localRunUntilIdle(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    timeout_ms: jlong,
) {
    let outcome = veneer::local_run_until_idle(ctx_of(ctx), timeout_ms.max(0) as u64);
    report(&mut env, outcome);
}

/// End the local run; a context with none is a no-op.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_localStop(
    _env: JNIEnv,
    _class: JClass,
    ctx: jlong,
) {
    veneer::local_stop(ctx_of(ctx));
}

/// Declare one host class from its JSON declaration. Throws the refusal.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_declareClass(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    json: JByteArray,
) {
    let outcome = bytes_of(&mut env, "declaration bytes", &json)
        .and_then(|bytes| veneer::declare_class(ctx_of(ctx), &bytes));
    report(&mut env, outcome);
}

/// Undeclare one host class; after start, waits for the runner's
/// live-instance check. Throws the refusal.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_undeclareClass(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    factory_id: JString,
) {
    let outcome = c_string(&mut env, "factory id", &factory_id)
        .and_then(|factory_id| veneer::undeclare_class(ctx_of(ctx), &factory_id));
    report(&mut env, outcome);
}

/// Judge a host class declaration without registering it: the derived
/// declaration's JSON bytes, or a throw whose `details.issues` names every
/// refusal.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_checkClass(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    json: JByteArray,
) -> jbyteArray {
    let ctx = ctx_of(ctx);
    let submitted = match bytes_of(&mut env, "declaration bytes", &json) {
        Ok(bytes) => bytes,
        Err(refusal) => {
            throw(&mut env, &refusal);
            return std::ptr::null_mut();
        }
    };
    match veneer::check_class(ctx, &submitted) {
        Ok(derived) => match env.byte_array_from_slice(&derived) {
            Ok(array) => array.into_raw(),
            Err(e) => {
                throw(
                    &mut env,
                    &Refusal::Shape(format!("derived declaration copy failed: {e}")),
                );
                std::ptr::null_mut()
            }
        },
        Err(refusal) => {
            throw(&mut env, &refusal);
            std::ptr::null_mut()
        }
    }
}

/// Connect to the controller and start the runner loop. Throws on failure.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_start(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    namespace: JString,
) {
    let outcome = c_string(&mut env, "namespace", &namespace)
        .and_then(|namespace| veneer::start(ctx_of(ctx), &namespace));
    report(&mut env, outcome);
}

/// Block up to `timeout_ms` for one activation. `null` = timeout; a
/// zero-length array = shutting down (an encoded activation is never empty);
/// otherwise the postcard-encoded activation bytes.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_poll(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    timeout_ms: jlong,
) -> jbyteArray {
    let ctx = ctx_of(ctx);
    let (status, copied) = with_buffer(
        &[FLZ_POLL_ACTIVATION],
        |buf, len| unsafe { crate::flz_core_poll(ctx.get(), timeout_ms.max(0) as u64, buf, len) },
        |bytes| env.byte_array_from_slice(bytes),
    );
    match (status, copied) {
        (FLZ_POLL_ACTIVATION, Some(Ok(array))) => array.into_raw(),
        (FLZ_POLL_ACTIVATION, Some(Err(e))) => {
            throw(
                &mut env,
                &Refusal::Shape(format!("activation copy failed: {e}")),
            );
            std::ptr::null_mut()
        }
        (FLZ_POLL_TIMEOUT, _) => std::ptr::null_mut(),
        (FLZ_POLL_SHUTTING_DOWN, _) => match env.byte_array_from_slice(&[]) {
            Ok(array) => array.into_raw(),
            Err(_) => std::ptr::null_mut(),
        },
        _ => {
            throw(&mut env, &Refusal::Core(veneer::last_error_of(ctx)));
            std::ptr::null_mut()
        }
    }
}

/// Open the context's poll-worker pool and write its policy into `policy`:
/// base, ceiling, poll timeout, retire time and drain bound, in that order.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_poolOpen(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    default_base: jint,
    policy: JIntArray,
) {
    let opened = veneer::pool_open(ctx_of(ctx), default_base.max(1) as u32).and_then(|p| {
        env.set_int_array_region(&policy, 0, &p.map(|f| f as jint))
            .map_err(|e| Refusal::Shape(format!("pool policy unwritable: {e}")))
    });
    report(&mut env, opened);
}

/// The pool's next decision as the ABI's status, so a supervisor loop costs
/// one JNI call per answer and allocates nothing.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_poolSupervise(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
) -> jint {
    veneer::pool_supervise(ctx_of(ctx)).unwrap_or_else(|refusal| {
        throw(&mut env, &refusal);
        crate::FLZ_POOL_IDLE
    })
}

/// Give back one seat of the pool: a fetcher that left, or one that never started.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_poolLeft(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
) {
    report(&mut env, veneer::pool_left(ctx_of(ctx)));
}

/// Complete an activation with a postcard-encoded `ActivationResult`.
/// Accepted from any thread. Throws on a decode/apply refusal.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_complete(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    result: JByteArray,
) {
    let ctx = ctx_of(ctx);
    let outcome = bytes_of(&mut env, "result bytes", &result).and_then(|bytes| {
        let status = unsafe { crate::flz_core_complete(ctx.get(), bytes.as_ptr(), bytes.len()) };
        veneer::verdict(ctx, status)
    });
    report(&mut env, outcome);
}

/// Abandon a polled activation by token: re-hand its untouched items and
/// release its cell. Java's `finally` calls it when a throwable escaped the
/// dispatch, so an unanswered activation can never retire a cell.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_abandon(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    token: jlong,
) {
    report(&mut env, veneer::abandon(ctx_of(ctx), token as u64));
}

type SpawnArgs = (CString, CString, Vec<u8>, CString);

fn spawn_args(
    env: &mut JNIEnv,
    factory_id: &JString,
    id: &JString,
    payload: &JByteArray,
    if_running: &JString,
) -> Result<SpawnArgs, Refusal> {
    let unreadable = |_| Refusal::Shape("spawn arguments unreadable".to_string());
    let factory = c_string(env, "factory id", factory_id).map_err(unreadable)?;
    let id = c_string(env, "id", id).map_err(unreadable)?;
    let mode = c_string(env, "if_running", if_running).map_err(unreadable)?;
    let bytes = bytes_of(env, "payload bytes", payload)?;
    Ok((factory, id, bytes, mode))
}

/// A string answer as a Java `String`, `null` for none; a refusal is thrown
/// and answers `null`.
fn string_or_throw(env: &mut JNIEnv, answer: Result<Option<String>, Refusal>) -> jstring {
    match answer {
        Ok(Some(text)) => env
            .new_string(text)
            .map_or(std::ptr::null_mut(), JString::into_raw),
        Ok(None) => std::ptr::null_mut(),
        Err(refusal) => {
            throw(env, &refusal);
            std::ptr::null_mut()
        }
    }
}

/// Request a spawn from the live activation `token`; returns the handle number
/// at once. An empty `id` asks the controller to generate one. Throws only for
/// an unreadable argument or a token that is not outstanding.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_spawnRequest<'l>(
    mut env: JNIEnv<'l>,
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
    let ctx = ctx_of(ctx);
    let offset = u64::try_from(offset).ok();
    let handle = spawn_args(&mut env, &factory_id, &id, &payload, &if_running).and_then(
        |(factory, id, bytes, mode)| {
            veneer::spawn_request(
                ctx,
                token as u64,
                &factory,
                &id,
                &bytes,
                &mode,
                replay != 0,
                offset,
            )
        },
    );
    match handle {
        Ok(handle) => handle as jlong,
        Err(refusal) => {
            throw(&mut env, &refusal);
            0
        }
    }
}

/// Send a part of the invocation of the live activation `token`'s work item
/// `item`; never waits. Throws for `part.no_output` and
/// `part.outside_invocation`.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_part<'l>(
    mut env: JNIEnv<'l>,
    _class: JClass,
    ctx: jlong,
    token: jlong,
    item: jint,
    payload: JByteArray,
) {
    let outcome = bytes_of(&mut env, "part payload", &payload)
        .and_then(|bytes| veneer::part(ctx_of(ctx), token as u64, item as u32, &bytes));
    report(&mut env, outcome);
}

/// A byte answer as a Java `byte[]`, `null` for none; a refusal is thrown and
/// answers `null`.
fn bytes_or_throw(env: &mut JNIEnv, answer: Result<Option<Vec<u8>>, Refusal>) -> jbyteArray {
    let copied = answer.and_then(|bytes| {
        bytes
            .map(|bytes| env.byte_array_from_slice(&bytes))
            .transpose()
            .map_err(|e| Refusal::Shape(format!("result copy failed: {e}")))
    });
    match copied {
        Ok(Some(array)) => array.into_raw(),
        Ok(None) => std::ptr::null_mut(),
        Err(refusal) => {
            throw(env, &refusal);
            std::ptr::null_mut()
        }
    }
}

/// Begin `ctx.effect(name)` in the live activation `token`'s work item `item`.
/// The first byte is the status (`FLZ_EFFECT_RUN`, `FLZ_EFFECT_KEPT`,
/// `FLZ_EFFECT_UNRECORDED`) and the rest, for a kept one, its result. Throws
/// for a refused call.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_effectBegin<'l>(
    mut env: JNIEnv<'l>,
    _class: JClass,
    ctx: jlong,
    token: jlong,
    item: jint,
    name: JString,
) -> jbyteArray {
    let started = c_string(&mut env, "effect name", &name)
        .map_err(|_| Refusal::Shape("effect name unreadable".to_string()))
        .and_then(|name| veneer::effect_begin(ctx_of(ctx), token as u64, item as u32, &name));
    let tagged = started.map(|start| {
        Some(match start {
            veneer::EffectStart::Kept(result) => {
                let mut tagged = vec![crate::FLZ_EFFECT_KEPT as u8];
                tagged.extend(result);
                tagged
            }
            veneer::EffectStart::Run => vec![crate::FLZ_EFFECT_RUN as u8],
            veneer::EffectStart::Unrecorded => vec![crate::FLZ_EFFECT_UNRECORDED as u8],
        })
    });
    bytes_or_throw(&mut env, tagged)
}

/// Keep the encoded result of the function a run begin ran, waiting for the
/// controller: `null` to return the call's own result, or the result the
/// controller kept first. Throws for `effect.too_large` and
/// `effect.not_recorded`.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_effectEnd<'l>(
    mut env: JNIEnv<'l>,
    _class: JClass,
    ctx: jlong,
    token: jlong,
    item: jint,
    payload: JByteArray,
) -> jbyteArray {
    let first = bytes_of(&mut env, "effect result", &payload)
        .and_then(|bytes| veneer::effect_end(ctx_of(ctx), token as u64, item as u32, &bytes));
    bytes_or_throw(&mut env, first)
}

/// End a begun effect without keeping anything.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_effectRelease(
    _env: JNIEnv,
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
    mut env: JNIEnv<'l>,
    _class: JClass,
    ctx: jlong,
    handle: jlong,
    timeout_ms: jlong,
) -> jstring {
    let answer = veneer::spawn_result(ctx_of(ctx), handle as u64, timeout_ms.max(0) as u64);
    string_or_throw(&mut env, answer)
}

/// Administer the namespace this connection proved: the published function's
/// answer as JSON. One export for the whole surface, so a further verb is a
/// Java method and no JNI. Throws on refusal / no control plane / timeout.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_adminCall<'l>(
    mut env: JNIEnv<'l>,
    _class: JClass,
    ctx: jlong,
    verb: JString,
    args: JString,
) -> jstring {
    let ctx = ctx_of(ctx);
    let result = c_string(&mut env, "verb", &verb)
        .and_then(|verb| c_string(&mut env, "args", &args).map(|args| (verb, args)))
        .and_then(|(verb, args)| veneer::admin_call(ctx, &verb, &args));
    string_or_throw(&mut env, result.map(Some))
}

/// Send one message, through the controller or locally, answering as JSON:
/// the verdict, or `null` for a local send. An empty id or key is none.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_sendMessage<'l>(
    mut env: JNIEnv<'l>,
    _class: JClass,
    ctx: jlong,
    relay: JString,
    channel: JString,
    message: JByteArray,
    msg_id: JString,
    key: JString,
    local: jboolean,
) -> jstring {
    let ctx = ctx_of(ctx);
    let answer = (|| {
        let relay = c_string(&mut env, "relay", &relay)?;
        let channel = c_string(&mut env, "entry channel", &channel)?;
        let msg_id = c_string(&mut env, "message id", &msg_id)?;
        let key = c_string(&mut env, "key", &key)?;
        let message = bytes_of(&mut env, "message", &message)?;
        veneer::send_message(ctx, &relay, &channel, &message, &msg_id, &key, local != 0)
    })();
    string_or_throw(&mut env, answer.map(Some))
}

/// The ids of the relay instances bound on this runner, as a JSON array.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_relays<'l>(
    mut env: JNIEnv<'l>,
    _class: JClass,
    ctx: jlong,
) -> jstring {
    let ids = veneer::relays(ctx_of(ctx)).map(|ids| Some(serde_json::json!(ids).to_string()));
    string_or_throw(&mut env, ids)
}

/// The core's notices held since the last take, as a JSON array of
/// `{"level", "target", "text"}`; the first take starts the holding.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_takeNotices<'l>(
    mut env: JNIEnv<'l>,
    _class: JClass,
) -> jstring {
    let notices = serde_json::json!(veneer::take_notices()).to_string();
    string_or_throw(&mut env, Ok(Some(notices)))
}

/// The core's monotonic clock in nanoseconds.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_monotonicNanos(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    crate::flz_core_monotonic_nanos() as jlong
}

/// A knob's text parsed by the core's rule for `kind`, as its decimal text;
/// null when it is no value of that kind.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_knob<'l>(
    mut env: JNIEnv<'l>,
    _class: JClass,
    kind: JString,
    raw: JString,
) -> jstring {
    let read = |env: &mut JNIEnv, value: &JString| env.get_string(value).map(String::from);
    let answer = match (read(&mut env, &kind), read(&mut env, &raw)) {
        (Ok(kind), Ok(raw)) => veneer::knob(&kind, &raw).map(|v| v.map(|v| v.to_string())),
        _ => Err(Refusal::Shape("a knob argument is unreadable".into())),
    };
    string_or_throw(&mut env, answer)
}

/// The JSON error document the catalogue renders for `code` and the JSON
/// object `detailsJson`: a refusal the SDK makes itself.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_errorDocument<'l>(
    mut env: JNIEnv<'l>,
    _class: JClass,
    code: JString,
    details_json: JString,
) -> jstring {
    let read = |env: &mut JNIEnv, value: &JString| env.get_string(value).map(String::from);
    let document = match (read(&mut env, &code), read(&mut env, &details_json)) {
        (Ok(code), Ok(details)) => crate::render_error_document(&code, &details),
        _ => Err("an error document argument is unreadable".into()),
    };
    string_or_throw(&mut env, document.map(Some).map_err(Refusal::Shape))
}

/// A `Response` channel payload as the core's decoder reads it: its target,
/// request and result back to back, with `[target length, request length,
/// result length, created, answered]` written into `parts`. Throws
/// `call.malformed_response`.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_responseDecode<'l>(
    mut env: JNIEnv<'l>,
    _class: JClass,
    payload: JByteArray,
    parts: JIntArray,
) -> jbyteArray {
    let decoded = bytes_of(&mut env, "Response payload", &payload).and_then(|payload| {
        let (mut out, mut decoded) = (vec![0; payload.len()], [0; 5]);
        veneer::response_into(&payload, &mut out, &mut decoded)?;
        let unwritable = |e| Refusal::Shape(format!("Response parts unwritable: {e}"));
        env.set_int_array_region(&parts, 0, &decoded.map(|part| part as jint))
            .map_err(unwritable)?;
        out.truncate(decoded[..3].iter().sum());
        Ok(Some(out))
    });
    bytes_or_throw(&mut env, decoded)
}

/// Wake every blocked poller and stop the runner loop.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_shutdown(
    _env: JNIEnv,
    _class: JClass,
    ctx: jlong,
) {
    veneer::shutdown(ctx_of(ctx));
}

/// Watch standard input and end the process on end of file, on the core's own
/// thread, as the SIGTERM handler ends it.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_watchStdinEof(
    _env: JNIEnv,
    _class: JClass,
) {
    veneer::watch_stdin_eof();
}

/// Destroy the context.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_contextFree(
    _env: JNIEnv,
    _class: JClass,
    ctx: jlong,
) {
    veneer::context_free(ctx_of(ctx));
}
