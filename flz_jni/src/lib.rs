//! JNI bindings exposing `flz_transport`'s C FFI to the Flezha Java SDK.
//!
//! Entry points follow the JNI naming convention
//! `Java_com_flezha_sdk_native_NativeTransport_*` and delegate to the
//! C FFI surface in `flz_transport::ffi`.
//!
//! ## Buffer ownership
//! Every `byte[]` returned to Java is copied from the native buffer into a
//! fresh Java array before the native allocation is freed. Inbound `byte[]`
//! arrays from Java are always short-lived copies — the JVM keeps ownership.
//!
//! ## Callbacks
//! The server-mode data-plane callbacks are surfaced to Java through a single
//! dispatcher object registered at context creation time. The dispatcher
//! holds a `JavaVM*` + a global `jobject` reference to the Java callback
//! handler and uses `AttachCurrentThread` from native-owned threads before
//! invoking Java methods. See `callbacks.rs`.
//!
//! ## Error model
//! Non-OK `FlezhaStatus` values are translated into `FlezhaNativeException`
//! on the Java side via the `throw_native` helper, with the message pulled
//! from `flezha_last_error()`.

mod callbacks;
mod context;
mod frame;
mod logging;
mod version;

use jni::JNIEnv;

/// Throw `com.flezha.sdk.native_.FlezhaNativeException` with the given message.
///
/// Returns unit; the caller should `return` immediately after calling this
/// (JNI does not honor exceptions mid-function).
pub(crate) fn throw_native(env: &mut JNIEnv, status_code: i32, message: &str) {
    let exception_class = match env.find_class("com/flezha/sdk/native_/FlezhaNativeException") {
        Ok(class) => class,
        Err(_) => {
            // Fallback: if our custom class is not on the classpath, throw a runtime exception.
            let _ = env.throw_new("java/lang/RuntimeException", message);
            return;
        }
    };

    let msg_jstr = match env.new_string(message) {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new(exception_class, "(failed to build JNI exception message)");
            return;
        }
    };

    let status_obj = jni::objects::JValue::from(status_code);
    let msg_obj = jni::objects::JValue::Object(&msg_jstr);

    match env.new_object(
        &exception_class,
        "(ILjava/lang/String;)V",
        &[status_obj, msg_obj],
    ) {
        Ok(obj) => {
            let _ = env.throw(jni::objects::JThrowable::from(obj));
        }
        Err(_) => {
            let _ = env.throw_new(exception_class, message);
        }
    }
}

/// Convert a `FlezhaStatus` value returned by the C FFI into an optional
/// error. Returns `Ok(())` for `FlezhaStatus::Ok`, otherwise `Err((code, msg))`.
#[allow(dead_code)]
pub(crate) fn status_to_result(status: i32) -> Result<(), (i32, String)> {
    if status == 0 {
        Ok(())
    } else {
        let msg = unsafe {
            let ptr = flz_transport::ffi::error::flezha_last_error();
            if ptr.is_null() {
                String::from("(no error message)")
            } else {
                std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned()
            }
        };
        Err((status, msg))
    }
}
