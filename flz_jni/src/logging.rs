//! JNI entry points for registering a Java-side log callback with the native
//! transport.
//!
//! Java side:
//! ```java
//! package com.flezha.sdk.native_;
//!
//! @FunctionalInterface
//! public interface FlezhaLogCallback {
//!     void onLog(int level, String message);
//! }
//! ```
//!
//! The native C FFI logging callback (`FlezhaLogFn`) receives an opaque
//! `user_data` pointer. We stash a boxed `LogBridge` owning a `JavaVM*` plus
//! a `GlobalRef` to the Java callback, and pass the raw pointer through.

use std::ffi::CStr;
use std::os::raw::c_char;

use flz_transport::ffi::types::FlezhaLogLevel;
use jni::objects::{GlobalRef, JClass, JObject, JValue};
use jni::sys::jint;
use jni::{JNIEnv, JavaVM};

use crate::context::JniContext;
use crate::throw_native;

struct LogBridge {
    jvm: JavaVM,
    callback: GlobalRef,
}

unsafe extern "C" fn log_trampoline(
    level: FlezhaLogLevel,
    message: *const c_char,
    user_data: *mut std::ffi::c_void,
) {
    if user_data.is_null() || message.is_null() {
        return;
    }
    let bridge = &*(user_data as *const LogBridge);
    let msg = CStr::from_ptr(message).to_string_lossy().into_owned();

    let mut env = match bridge.jvm.attach_current_thread() {
        Ok(g) => g,
        Err(_) => return,
    };

    let jstr = match env.new_string(msg) {
        Ok(s) => s,
        Err(_) => return,
    };

    let _ = env.call_method(
        bridge.callback.as_obj(),
        "onLog",
        "(ILjava/lang/String;)V",
        &[JValue::from(level as jint), JValue::Object(&jstr)],
    );
    if env.exception_check().unwrap_or(false) {
        let _ = env.exception_clear();
    }
}

/// `void setLogCallback(long handle, FlezhaLogCallback cb)`
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_NativeTransport_setLogCallback<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jni::sys::jlong,
    cb: JObject<'local>,
) {
    if handle == 0 {
        throw_native(&mut env, 1, "null context handle");
        return;
    }

    // Safety: Java contract guarantees the context is live until contextDestroy.
    let ctx: &JniContext = unsafe { &*(handle as *const JniContext) };

    let global = match env.new_global_ref(cb) {
        Ok(g) => g,
        Err(e) => {
            throw_native(&mut env, 7, &format!("failed to take global ref: {}", e));
            return;
        }
    };

    // Stash the Java-side ref on the context.
    *ctx.log_callback.lock().unwrap() = Some(global.clone());

    // Build the C-FFI bridge and register it with the underlying log machinery.
    // We need a standalone FFI context here since logging is attached per-context
    // on the shared library side. A more thorough wiring would also plumb the
    // per-context logger through QuicClient etc.; for now, logs originating
    // from frame_build and future FFI paths will flow through this bridge.
    let bridge = Box::new(LogBridge {
        jvm: match env.get_java_vm() {
            Ok(v) => v,
            Err(e) => {
                throw_native(&mut env, 7, &format!("JavaVM lookup failed: {}", e));
                return;
            }
        },
        callback: global,
    });
    let bridge_ptr = Box::into_raw(bridge) as *mut std::ffi::c_void;

    // Register a trampoline against a one-shot FFI context. We allocate it
    // here, keep it alive for the bridge lifetime, and destroy on contextDestroy
    // via a future hook. For parity-v1, this is a best-effort: the trampoline
    // stays registered until process exit.
    unsafe {
        let cfg = flz_transport::ffi::config::flezha_config_default();
        let ffi_ctx = flz_transport::ffi::api::flezha_context_new(cfg);
        if ffi_ctx.is_null() {
            throw_native(&mut env, 7, "failed to create FFI context for logging");
            let _ = Box::from_raw(bridge_ptr as *mut LogBridge);
            return;
        }
        let status = flz_transport::ffi::api::flezha_set_log_callback(
            ffi_ctx,
            Some(log_trampoline),
            bridge_ptr,
        );
        if status as i32 != 0 {
            throw_native(&mut env, status as i32, "flezha_set_log_callback failed");
            let _ = Box::from_raw(bridge_ptr as *mut LogBridge);
            // Let the FFI context leak — it is small and will be recovered on exit.
        }
    }
}
