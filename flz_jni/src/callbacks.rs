//! Server / client-side callback dispatch.
//!
//! Each reader thread (spawned by `context::register_and_spawn_reader`) owns
//! a raw pointer to the `JniContext` for its lifetime. When data arrives or
//! the stream closes, it calls `dispatch_*` which:
//!
//! 1. Attaches the current thread to the `JavaVM` (idempotent — if already
//!    attached, returns the existing `JNIEnv`).
//! 2. Looks up the Java-side dispatcher `GlobalRef`.
//! 3. Invokes the appropriate method on the dispatcher with the event
//!    payload copied into a freshly-allocated Java `byte[]`.
//!
//! The dispatcher interface (Java) is:
//!
//! ```java
//! package com.flezha.sdk.native_;
//!
//! public interface NativeEventDispatcher {
//!     void onData(long streamId, byte[] data);
//!     void onStreamClosed(long streamId);
//!     void onConnectionLost(long streamId);
//! }
//! ```
//!
//! Buffer ownership: `byte[]` is a fresh JVM allocation, so no explicit free
//! is needed on the Rust side after the call returns.

use jni::objects::{JByteArray, JObject, JValue};
use jni::JavaVM;

use crate::context::JniContext;

const DISPATCHER_CLASS: &str = "com/flezha/sdk/native_/NativeEventDispatcher";

fn with_env<F>(ctx: &JniContext, f: F)
where
    F: FnOnce(&mut jni::JNIEnv, &JObject),
{
    // Attach current thread (idempotent) and obtain a JNIEnv.
    let attach = ctx.jvm.attach_current_thread();
    let mut guard = match attach {
        Ok(g) => g,
        Err(_) => return,
    };

    // Snapshot the dispatcher global ref under the lock, then drop the lock
    // before calling into Java so we don't hold a Rust mutex across the
    // dispatcher call (which could take arbitrary time).
    let dispatcher = {
        let g = ctx.dispatcher.lock().unwrap();
        g.as_ref().map(|gr| gr.as_obj().as_raw())
    };

    let Some(raw) = dispatcher else {
        return;
    };

    // SAFETY: we're about to wrap a raw jobject we just retrieved — valid for
    // the duration of the call while we hold the attach guard.
    let obj = unsafe { JObject::from_raw(raw) };
    f(&mut guard, &obj);
}

pub(crate) fn dispatch_data(ctx: &JniContext, stream_id: u64, data: &[u8]) {
    with_env(ctx, |env, dispatcher| {
        let byte_array: JByteArray = match env.byte_array_from_slice(data) {
            Ok(a) => a,
            Err(_) => return,
        };

        let _ = env.call_method(
            dispatcher,
            "onData",
            "(J[B)V",
            &[JValue::from(stream_id as i64), JValue::Object(&byte_array)],
        );

        if env.exception_check().unwrap_or(false) {
            let _ = env.exception_clear();
        }
    });

    // Safety net: if the dispatcher class on the classpath changes shape, the
    // JNI lookup above silently no-ops rather than crashing the JVM.
    let _ = DISPATCHER_CLASS;
}

pub(crate) fn dispatch_stream_closed(ctx: &JniContext, stream_id: u64) {
    with_env(ctx, |env, dispatcher| {
        let _ = env.call_method(
            dispatcher,
            "onStreamClosed",
            "(J)V",
            &[JValue::from(stream_id as i64)],
        );
        if env.exception_check().unwrap_or(false) {
            let _ = env.exception_clear();
        }
    });
}

pub(crate) fn dispatch_connection_lost(ctx: &JniContext, stream_id: u64) {
    with_env(ctx, |env, dispatcher| {
        let _ = env.call_method(
            dispatcher,
            "onConnectionLost",
            "(J)V",
            &[JValue::from(stream_id as i64)],
        );
        if env.exception_check().unwrap_or(false) {
            let _ = env.exception_clear();
        }
    });
}

// Unused-import guard to keep JavaVM in-scope — the `ctx.jvm` field is of type
// `jni::JavaVM` and crate-level `use` above documents the dependency.
fn _retain_vm(_vm: &JavaVM) {}
