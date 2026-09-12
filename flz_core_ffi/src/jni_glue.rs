//! JNI bindings over this crate's C ABI for the Java SDK's `FlzCore` class,
//! one export per C entry point. This module owns the byte-level crossing:
//! Java decodes postcard itself, so activations and payloads cross as fresh
//! `byte[]` copies read out of the core's buffer before it is freed, and
//! Java-owned arrays are copied in before the call returns. A refusal maps
//! once, in [`throw`], onto `FlezhaNativeException` (`RuntimeException` off
//! the SDK classpath); [`throw_with_body`] is the same exception carrying the
//! answer an entry point handed out alongside its refusal.

use std::ffi::CString;

use jni::objects::{JByteArray, JClass, JObject, JString, JThrowable, JValue};
use jni::sys::{jboolean, jbyteArray, jint, jlong};
use jni::JNIEnv;

use crate::veneer::{self, with_buffer, CtxPtr, Refusal};
use crate::{FlzContext, FLZ_POLL_ACTIVATION, FLZ_POLL_SHUTTING_DOWN, FLZ_POLL_TIMEOUT};

const NATIVE_EXCEPTION: &str = "com/flezha/sdk/native_/FlezhaNativeException";

fn throw(env: &mut JNIEnv, refusal: &Refusal) {
    let message = refusal.message();
    let thrown = env
        .find_class(NATIVE_EXCEPTION)
        .and_then(|class| env.throw_new(class, message));
    if thrown.is_err() {
        let _ = env.throw_new("java/lang/RuntimeException", message);
    }
}

/// Throw the same exception with the answer the core handed out beside the
/// refusal — the topology check's verdict JSON. The message is unchanged; the
/// body is what lets the SDK read the refusal names as data instead of out of
/// the rendered sentence.
fn throw_with_body(env: &mut JNIEnv, refusal: &Refusal, body: &[u8]) {
    let carried = (|| -> jni::errors::Result<()> {
        let message: JObject = env.new_string(refusal.message())?.into();
        let body: JObject = env.byte_array_from_slice(body)?.into();
        let exception = env.new_object(
            NATIVE_EXCEPTION,
            "(Ljava/lang/String;[B)V",
            &[JValue::Object(&message), JValue::Object(&body)],
        )?;
        env.throw(JThrowable::from(exception))
    })();
    if carried.is_err() {
        throw(env, refusal);
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
/// JVM keeps driving `poll`/`complete`, which is what runs the procedures.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_localStart(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    factory_id: JString,
    operative_id: JString,
) {
    let ctx = ctx_of(ctx);
    let outcome = c_string(&mut env, "factory id", &factory_id).and_then(|factory| {
        c_string(&mut env, "operative id", &operative_id)
            .and_then(|operative| veneer::local_start(ctx, &factory, &operative))
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
    let ctx = ctx_of(ctx);
    let outcome = c_string(&mut env, "sink channel", &channel)
        .and_then(|channel| veneer::local_on_sink(ctx, &channel));
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
    let ctx = ctx_of(ctx);
    match veneer::local_next_output(ctx, timeout_ms.max(0) as u64) {
        Ok(Some(bytes)) => match env.byte_array_from_slice(&bytes) {
            Ok(array) => array.into_raw(),
            Err(e) => {
                throw(
                    &mut env,
                    &Refusal::Shape(format!("sink output copy failed: {e}")),
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
    let ctx = ctx_of(ctx);
    let outcome = veneer::local_run_until_idle(ctx, timeout_ms.max(0) as u64);
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
    let ctx = ctx_of(ctx);
    let outcome = bytes_of(&mut env, "declaration bytes", &json)
        .and_then(|bytes| veneer::declare_class(ctx, &bytes));
    report(&mut env, outcome);
}

/// Judge a host class declaration without registering it: the derived
/// declaration's JSON bytes, or a throw carrying the core's rendered verdict
/// AND the verdict's own JSON, which names every refusal.
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
        Err((refusal, verdict)) => {
            throw_with_body(&mut env, &refusal, &verdict);
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
    let ctx = ctx_of(ctx);
    let outcome = c_string(&mut env, "namespace", &namespace)
        .and_then(|namespace| veneer::start(ctx, &namespace));
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

/// Park up to `timeout_ms` until the core observes starvation. Answers the
/// ABI's three-way status directly (`1` observed, `0` timeout, `2` shutting
/// down) rather than an object, so a supervisor loop costs one JNI call per
/// park and allocates nothing.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_waitStarvation(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    timeout_ms: jlong,
) -> jint {
    match veneer::wait_starvation(ctx_of(ctx), timeout_ms.max(0) as u64) {
        Ok(status) => status,
        Err(refusal) => {
            throw(&mut env, &refusal);
            FLZ_POLL_TIMEOUT
        }
    }
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
/// release its entity. Java's `finally` calls it when a throwable escaped the
/// dispatch, so an unanswered activation can never retire an entity.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_abandon(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    token: jlong,
) {
    report(&mut env, veneer::abandon(ctx_of(ctx), token as u64));
}

/// Emit one output payload from a live workflow run-sugar body.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_workflowEmit(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    instance_id: JString,
    payload: JByteArray,
) {
    let ctx = ctx_of(ctx);
    let outcome = c_string(&mut env, "instance id", &instance_id).and_then(|instance| {
        let bytes = bytes_of(&mut env, "payload bytes", &payload)?;
        veneer::workflow_emit(ctx, &instance, &bytes)
    });
    report(&mut env, outcome);
}

/// Block up to `timeout_ms` for the next inbox item of a live workflow
/// run-sugar body. `null` = timeout; throws when the body is gone or the
/// inbox closed.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_workflowRecv(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    instance_id: JString,
    timeout_ms: jlong,
) -> jbyteArray {
    let ctx = ctx_of(ctx);
    let instance = match c_string(&mut env, "instance id", &instance_id) {
        Ok(instance) => instance,
        Err(refusal) => {
            throw(&mut env, &refusal);
            return std::ptr::null_mut();
        }
    };
    let (status, copied) = with_buffer(
        &[FLZ_POLL_ACTIVATION],
        |buf, len| unsafe {
            crate::flz_core_workflow_recv(
                ctx.get(),
                instance.as_ptr(),
                timeout_ms.max(0) as u64,
                buf,
                len,
            )
        },
        |bytes| env.byte_array_from_slice(bytes),
    );
    match (status, copied) {
        (FLZ_POLL_ACTIVATION, Some(Ok(array))) => array.into_raw(),
        (FLZ_POLL_ACTIVATION, Some(Err(e))) => {
            throw(
                &mut env,
                &Refusal::Shape(format!("inbox payload copy failed: {e}")),
            );
            std::ptr::null_mut()
        }
        (FLZ_POLL_TIMEOUT, _) => std::ptr::null_mut(),
        _ => {
            throw(&mut env, &Refusal::Core(veneer::last_error_of(ctx)));
            std::ptr::null_mut()
        }
    }
}

/// Whether the named workflow body's cancellation token has tripped.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_workflowCancelled(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    instance_id: JString,
) -> jboolean {
    match c_string(&mut env, "instance id", &instance_id) {
        Ok(instance) => veneer::workflow_cancelled(ctx_of(ctx), &instance) as jboolean,
        Err(_) => 1,
    }
}

type SpawnArgs = (CString, CString, Vec<u8>, CString);

fn spawn_args(
    env: &mut JNIEnv,
    instance_id: &JString,
    factory_id: &JString,
    payload: &JByteArray,
    if_running: &JString,
) -> Result<SpawnArgs, Refusal> {
    let unreadable = |_| Refusal::Shape("spawn arguments unreadable".to_string());
    let instance = c_string(env, "instance id", instance_id).map_err(unreadable)?;
    let factory = c_string(env, "factory id", factory_id).map_err(unreadable)?;
    let mode = c_string(env, "if_running", if_running).map_err(unreadable)?;
    let bytes = bytes_of(env, "payload bytes", payload)?;
    Ok((instance, factory, bytes, mode))
}

/// Spawn-or-deliver a workflow instance; returns the controller's outcome
/// string. Throws on refusal / no control plane / timeout.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_spawnWorkflow<'l>(
    mut env: JNIEnv<'l>,
    _class: JClass,
    ctx: jlong,
    instance_id: JString,
    factory_id: JString,
    payload: JByteArray,
    if_running: JString,
) -> jni::sys::jstring {
    let ctx = ctx_of(ctx);
    let outcome = spawn_args(&mut env, &instance_id, &factory_id, &payload, &if_running).and_then(
        |(instance, factory, bytes, mode)| {
            veneer::spawn_workflow(ctx, &instance, &factory, &bytes, &mode)
        },
    );
    match outcome {
        Ok(outcome) => match env.new_string(outcome) {
            Ok(s) => s.into_raw(),
            Err(_) => std::ptr::null_mut(),
        },
        Err(refusal) => {
            throw(&mut env, &refusal);
            std::ptr::null_mut()
        }
    }
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
) -> jni::sys::jstring {
    let ctx = ctx_of(ctx);
    let result = c_string(&mut env, "verb", &verb)
        .and_then(|verb| c_string(&mut env, "args", &args).map(|args| (verb, args)))
        .and_then(|(verb, args)| veneer::admin_call(ctx, &verb, &args));
    match result {
        Ok(json) => match env.new_string(json) {
            Ok(s) => s.into_raw(),
            Err(_) => std::ptr::null_mut(),
        },
        Err(refusal) => {
            throw(&mut env, &refusal);
            std::ptr::null_mut()
        }
    }
}

/// The core's monotonic clock in nanoseconds.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_monotonicNanos(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    crate::flz_core_monotonic_nanos() as jlong
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

/// Destroy the context.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_contextFree(
    _env: JNIEnv,
    _class: JClass,
    ctx: jlong,
) {
    veneer::context_free(ctx_of(ctx));
}

/// The table `FlzCore.java`'s `static native` declarations are asserted
/// against: `(java_name, abi_entry_point)`, in declaration order.
///
/// Every other host binding carries one — `napi_glue::native_surface`,
/// `python_glue::NATIVE_SURFACE`, `ruby_glue::NATIVE_SURFACE`, and PHP's
/// `cdef` block, which `AbiSurfaceTest.php` compares against `lib.rs` in both
/// directions. Java was the one host whose sixteen-and-then-some hand-written
/// declarations were compared with the JNI exports by nobody, so an export
/// added on one side and not the other surfaced as a missing method at the
/// first call rather than as a red build.
///
/// The assertion lives in `tests/abi/veneer_jni.rs`.
pub const NATIVE_SURFACE: &[(&str, &str)] = &[
    ("schemaVersion", "flz_core_schema_version"),
    ("contextNew", "flz_core_context_new"),
    ("declareClass", "flz_core_declare_class"),
    ("checkClass", "flz_core_check_class"),
    ("start", "flz_core_start"),
    ("localStart", "flz_core_local_start"),
    ("localOnSink", "flz_core_local_on_sink"),
    ("localFeed", "flz_core_local_feed"),
    ("localNextOutput", "flz_core_local_next_output"),
    ("localRunUntilIdle", "flz_core_local_run_until_idle"),
    ("localStop", "flz_core_local_stop"),
    ("poll", "flz_core_poll"),
    ("complete", "flz_core_complete"),
    ("waitStarvation", "flz_core_wait_starvation"),
    ("abandon", "flz_core_abandon"),
    ("workflowEmit", "flz_core_workflow_emit"),
    ("workflowRecv", "flz_core_workflow_recv"),
    ("workflowCancelled", "flz_core_workflow_cancelled"),
    ("spawnWorkflow", "flz_core_spawn_workflow"),
    ("adminCall", "flz_core_admin_call"),
    ("monotonicNanos", "flz_core_monotonic_nanos"),
    ("shutdown", "flz_core_shutdown"),
    ("contextFree", "flz_core_context_free"),
];
