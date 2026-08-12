//! JNI bindings over this crate's own C ABI, for the Java SDK's `FlzCore`
//! class (`com.flezha.sdk.native_.FlzCore`). One-to-one with the C entry
//! points — the JNI layer adds nothing the ABI does not have, so a Java-only
//! side door cannot grow here.
//!
//! ## Buffer ownership
//! Every `byte[]` returned to Java is copied from the native buffer into a
//! fresh Java array and the native buffer is freed through
//! [`crate::flz_core_buf_free`] before the call returns. Inbound `byte[]`
//! arrays are short-lived copies — the JVM keeps ownership.
//!
//! ## Error model
//! Failures throw `com.flezha.sdk.native_.FlezhaNativeException` (falling
//! back to `java.lang.RuntimeException` off the SDK classpath) with the
//! message from the context's last-error slot.

use std::ffi::{CStr, CString};

use jni::objects::{JByteArray, JClass, JString};
use jni::sys::{jboolean, jbyteArray, jint, jlong};
use jni::JNIEnv;

use crate::{FlzContext, FLZ_OK, FLZ_POLL_ACTIVATION, FLZ_POLL_SHUTTING_DOWN, FLZ_POLL_TIMEOUT};

fn throw(env: &mut JNIEnv, message: &str) {
    let thrown = env
        .find_class("com/flezha/sdk/native_/FlezhaNativeException")
        .and_then(|class| env.throw_new(class, message));
    if thrown.is_err() {
        let _ = env.throw_new("java/lang/RuntimeException", message);
    }
}

fn last_error_of(ctx: *const FlzContext) -> String {
    let ptr = unsafe { crate::flz_core_last_error(ctx) };
    if ptr.is_null() {
        "unknown native failure".to_string()
    } else {
        unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned()
    }
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
    let ctx = crate::flz_core_context_new(shim_schema_version as u32);
    if ctx.is_null() {
        let ptr = crate::flz_core_init_error();
        let message = if ptr.is_null() {
            "flz_core context creation failed".to_string()
        } else {
            unsafe { CStr::from_ptr(ptr) }
                .to_string_lossy()
                .into_owned()
        };
        throw(&mut env, &message);
        return 0;
    }
    ctx as jlong
}

/// Declare one host class from its JSON declaration. Throws the refusal.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_declareClass(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    json: JByteArray,
) {
    let ctx = ctx as *mut FlzContext;
    let bytes = match env.convert_byte_array(&json) {
        Ok(b) => b,
        Err(e) => return throw(&mut env, &format!("declaration bytes unreadable: {e}")),
    };
    let status = unsafe { crate::flz_core_declare_class(ctx, bytes.as_ptr(), bytes.len()) };
    if status != FLZ_OK {
        throw(&mut env, &last_error_of(ctx));
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
    let ctx = ctx as *mut FlzContext;
    let namespace: String = match env.get_string(&namespace) {
        Ok(s) => s.into(),
        Err(e) => return throw(&mut env, &format!("namespace unreadable: {e}")),
    };
    let c_namespace = match CString::new(namespace) {
        Ok(c) => c,
        Err(_) => return throw(&mut env, "namespace contains NUL"),
    };
    let status = unsafe { crate::flz_core_start(ctx, c_namespace.as_ptr()) };
    if status != FLZ_OK {
        throw(&mut env, &last_error_of(ctx));
    }
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
    let ctx = ctx as *mut FlzContext;
    let mut buf: *mut u8 = std::ptr::null_mut();
    let mut len: usize = 0;
    let status = unsafe { crate::flz_core_poll(ctx, timeout_ms.max(0) as u64, &mut buf, &mut len) };
    match status {
        FLZ_POLL_ACTIVATION => {
            let bytes = unsafe { std::slice::from_raw_parts(buf, len) };
            let out = env.byte_array_from_slice(bytes);
            unsafe { crate::flz_core_buf_free(buf, len) };
            match out {
                Ok(array) => array.into_raw(),
                Err(e) => {
                    throw(&mut env, &format!("activation copy failed: {e}"));
                    std::ptr::null_mut()
                }
            }
        }
        FLZ_POLL_TIMEOUT => std::ptr::null_mut(),
        FLZ_POLL_SHUTTING_DOWN => match env.byte_array_from_slice(&[]) {
            Ok(array) => array.into_raw(),
            Err(_) => std::ptr::null_mut(),
        },
        _ => {
            throw(&mut env, &last_error_of(ctx));
            std::ptr::null_mut()
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
    let ctx = ctx as *mut FlzContext;
    let bytes = match env.convert_byte_array(&result) {
        Ok(b) => b,
        Err(e) => return throw(&mut env, &format!("result bytes unreadable: {e}")),
    };
    let status = unsafe { crate::flz_core_complete(ctx, bytes.as_ptr(), bytes.len()) };
    if status != FLZ_OK {
        throw(&mut env, &last_error_of(ctx));
    }
}

/// Emit one output payload from a live bounded run-sugar body.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_boundedEmit(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    instance_id: JString,
    payload: JByteArray,
) {
    let ctx = ctx as *mut FlzContext;
    let instance: String = match env.get_string(&instance_id) {
        Ok(s) => s.into(),
        Err(e) => return throw(&mut env, &format!("instance id unreadable: {e}")),
    };
    let bytes = match env.convert_byte_array(&payload) {
        Ok(b) => b,
        Err(e) => return throw(&mut env, &format!("payload bytes unreadable: {e}")),
    };
    let c_instance = match CString::new(instance) {
        Ok(c) => c,
        Err(_) => return throw(&mut env, "instance id contains NUL"),
    };
    let status = unsafe {
        crate::flz_core_bounded_emit(ctx, c_instance.as_ptr(), bytes.as_ptr(), bytes.len())
    };
    if status != FLZ_OK {
        throw(&mut env, &last_error_of(ctx));
    }
}

/// Block up to `timeout_ms` for the next inbox item of a live bounded
/// run-sugar body. `null` = timeout; throws when the body is gone or the
/// inbox closed.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_boundedRecv(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    instance_id: JString,
    timeout_ms: jlong,
) -> jbyteArray {
    let ctx = ctx as *mut FlzContext;
    let instance: String = match env.get_string(&instance_id) {
        Ok(s) => s.into(),
        Err(e) => {
            throw(&mut env, &format!("instance id unreadable: {e}"));
            return std::ptr::null_mut();
        }
    };
    let c_instance = match CString::new(instance) {
        Ok(c) => c,
        Err(_) => {
            throw(&mut env, "instance id contains NUL");
            return std::ptr::null_mut();
        }
    };
    let mut buf: *mut u8 = std::ptr::null_mut();
    let mut len: usize = 0;
    let status = unsafe {
        crate::flz_core_bounded_recv(
            ctx,
            c_instance.as_ptr(),
            timeout_ms.max(0) as u64,
            &mut buf,
            &mut len,
        )
    };
    match status {
        FLZ_POLL_ACTIVATION => {
            let bytes = unsafe { std::slice::from_raw_parts(buf, len) };
            let out = env.byte_array_from_slice(bytes);
            unsafe { crate::flz_core_buf_free(buf, len) };
            match out {
                Ok(array) => array.into_raw(),
                Err(e) => {
                    throw(&mut env, &format!("inbox payload copy failed: {e}"));
                    std::ptr::null_mut()
                }
            }
        }
        FLZ_POLL_TIMEOUT => std::ptr::null_mut(),
        _ => {
            throw(&mut env, &last_error_of(ctx));
            std::ptr::null_mut()
        }
    }
}

/// Whether the named bounded body's cancellation token has tripped.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_boundedCancelled(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    instance_id: JString,
) -> jboolean {
    let ctx = ctx as *const FlzContext;
    let instance: String = match env.get_string(&instance_id) {
        Ok(s) => s.into(),
        Err(_) => return 1,
    };
    let c_instance = match CString::new(instance) {
        Ok(c) => c,
        Err(_) => return 1,
    };
    unsafe { crate::flz_core_bounded_cancelled(ctx, c_instance.as_ptr()) as jboolean }
}

/// Spawn-or-deliver a bounded instance; returns the controller's outcome
/// string. Throws on refusal / no control plane / timeout.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_spawnBounded<'l>(
    mut env: JNIEnv<'l>,
    _class: JClass,
    ctx: jlong,
    instance_id: JString,
    factory_id: JString,
    payload: JByteArray,
    if_running: JString,
) -> jni::sys::jstring {
    let ctx = ctx as *mut FlzContext;
    let get = |env: &mut JNIEnv, s: &JString| -> Option<CString> {
        let v: String = env.get_string(s).ok()?.into();
        CString::new(v).ok()
    };
    let (Some(instance), Some(factory), Some(mode)) = (
        get(&mut env, &instance_id),
        get(&mut env, &factory_id),
        get(&mut env, &if_running),
    ) else {
        throw(&mut env, "spawn arguments unreadable");
        return std::ptr::null_mut();
    };
    let bytes = match env.convert_byte_array(&payload) {
        Ok(b) => b,
        Err(e) => {
            throw(&mut env, &format!("payload bytes unreadable: {e}"));
            return std::ptr::null_mut();
        }
    };
    let mut out: *mut u8 = std::ptr::null_mut();
    let mut out_len: usize = 0;
    let status = unsafe {
        crate::flz_core_spawn_bounded(
            ctx,
            instance.as_ptr(),
            factory.as_ptr(),
            bytes.as_ptr(),
            bytes.len(),
            mode.as_ptr(),
            &mut out,
            &mut out_len,
        )
    };
    if status != FLZ_OK {
        throw(&mut env, &last_error_of(ctx));
        return std::ptr::null_mut();
    }
    let outcome = unsafe { std::slice::from_raw_parts(out, out_len) }.to_vec();
    unsafe { crate::flz_core_buf_free(out, out_len) };
    match env.new_string(String::from_utf8_lossy(&outcome)) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Wake every blocked poller and stop the runner loop.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_shutdown(
    _env: JNIEnv,
    _class: JClass,
    ctx: jlong,
) {
    unsafe { crate::flz_core_shutdown(ctx as *mut FlzContext) };
}

/// Destroy the context.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_FlzCore_contextFree(
    _env: JNIEnv,
    _class: JClass,
    ctx: jlong,
) {
    unsafe { crate::flz_core_context_free(ctx as *mut FlzContext) };
}
