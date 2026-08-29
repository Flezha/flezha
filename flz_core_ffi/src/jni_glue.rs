//! JNI bindings over this crate's C ABI for the Java SDK's `FlzCore` class,
//! one export per C entry point. This module owns the byte-level crossing:
//! Java decodes postcard itself, so activations and payloads cross as fresh
//! `byte[]` copies read out of the core's buffer before it is freed, and
//! Java-owned arrays are copied in before the call returns. A refusal maps
//! once, in [`throw`], onto `FlezhaNativeException` (`RuntimeException` off
//! the SDK classpath).

use std::ffi::CString;

use jni::objects::{JByteArray, JClass, JString};
use jni::sys::{jboolean, jbyteArray, jint, jlong};
use jni::JNIEnv;

use crate::veneer::{self, with_buffer, CtxPtr, Refusal};
use crate::{FlzContext, FLZ_POLL_ACTIVATION, FLZ_POLL_SHUTTING_DOWN, FLZ_POLL_TIMEOUT};

fn throw(env: &mut JNIEnv, refusal: &Refusal) {
    let message = refusal.message();
    let thrown = env
        .find_class("com/flezha/sdk/native_/FlezhaNativeException")
        .and_then(|class| env.throw_new(class, message));
    if thrown.is_err() {
        let _ = env.throw_new("java/lang/RuntimeException", message);
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
        FLZ_POLL_ACTIVATION,
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
        FLZ_POLL_ACTIVATION,
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

/// The core's monotonic clock in nanoseconds — the reading
/// [`Java_com_flezha_sdk_native_1_FlzCore_entryLastDeliveryNanos`] is measured
/// against.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_monotonicNanos(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    crate::flz_core_monotonic_nanos() as jlong
}

/// When the named entry channel last received a frame, on the same monotonic
/// clock; `0` when no frame ever has.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_entryLastDeliveryNanos(
    mut env: JNIEnv,
    _class: JClass,
    channel: JString,
) -> jlong {
    match c_string(&mut env, "channel", &channel) {
        Ok(channel) => veneer::entry_last_delivery_nanos(&channel) as jlong,
        Err(_) => 0,
    }
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
