//! JNI entry points for `flezha_frame_build` / `flezha_frame_free`.
//!
//! Frame build allocates a native buffer via the C FFI, copies it into a
//! Java `byte[]`, then frees the native buffer before returning. Java code
//! never sees a raw native pointer — the buffer lifetime is fully contained
//! inside the native method.

use std::ffi::CString;
use std::os::raw::c_char;

use jni::objects::{JByteArray, JClass, JString};
use jni::sys::jbyteArray;
use jni::JNIEnv;

use crate::throw_native;

/// `byte[] buildFrame(String payloadId, byte[] message, String metadataJson)`
///
/// `metadataJson` may be null → treated as empty metadata.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_NativeTransport_buildFrame<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    payload_id_jstr: JString<'local>,
    message_arr: JByteArray<'local>,
    metadata_json_jstr: JString<'local>,
) -> jbyteArray {
    let payload_id = match env.get_string(&payload_id_jstr) {
        Ok(s) => s,
        Err(e) => {
            throw_native(&mut env, 1, &format!("payload_id not a string: {}", e));
            return JByteArray::default().into_raw();
        }
    };
    let payload_id_str: String = payload_id.into();

    let payload_cstr = match CString::new(payload_id_str) {
        Ok(c) => c,
        Err(_) => {
            throw_native(&mut env, 1, "payload_id contains null byte");
            return JByteArray::default().into_raw();
        }
    };

    let message_bytes = match env.convert_byte_array(&message_arr) {
        Ok(v) => v,
        Err(e) => {
            throw_native(&mut env, 1, &format!("message byte[] conversion failed: {}", e));
            return JByteArray::default().into_raw();
        }
    };

    let metadata_cstr: Option<CString> = if metadata_json_jstr.is_null() {
        None
    } else {
        match env.get_string(&metadata_json_jstr) {
            Ok(s) => {
                let s: String = s.into();
                if s.is_empty() {
                    None
                } else {
                    match CString::new(s) {
                        Ok(c) => Some(c),
                        Err(_) => {
                            throw_native(&mut env, 1, "metadata_json contains null byte");
                            return JByteArray::default().into_raw();
                        }
                    }
                }
            }
            Err(e) => {
                throw_native(&mut env, 1, &format!("metadata_json not a string: {}", e));
                return JByteArray::default().into_raw();
            }
        }
    };

    let metadata_ptr: *const c_char = metadata_cstr
        .as_ref()
        .map(|c| c.as_ptr())
        .unwrap_or(std::ptr::null());

    let mut out: *mut u8 = std::ptr::null_mut();
    let mut out_len: usize = 0;

    let status = unsafe {
        flz_transport::ffi::api::flezha_frame_build(
            payload_cstr.as_ptr(),
            message_bytes.as_ptr(),
            message_bytes.len(),
            metadata_ptr,
            &mut out as *mut *mut u8,
            &mut out_len as *mut usize,
        )
    };

    if status as i32 != 0 {
        let err = unsafe {
            let p = flz_transport::ffi::error::flezha_last_error();
            if p.is_null() {
                "frame_build failed".to_string()
            } else {
                std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
            }
        };
        throw_native(&mut env, status as i32, &err);
        return JByteArray::default().into_raw();
    }

    // Copy native buffer into a Java byte[], then free.
    let slice = unsafe { std::slice::from_raw_parts(out, out_len) };
    let java_arr = match env.byte_array_from_slice(slice) {
        Ok(a) => a,
        Err(e) => {
            unsafe {
                let _ = flz_transport::ffi::api::flezha_frame_free(out, out_len);
            }
            throw_native(&mut env, 7, &format!("byte[] alloc failed: {}", e));
            return JByteArray::default().into_raw();
        }
    };

    unsafe {
        let free_status = flz_transport::ffi::api::flezha_frame_free(out, out_len);
        if free_status as i32 != 0 {
            // Logged only — the Java array is already populated, so we return
            // success but record that the native buffer leaked a little.
            eprintln!("[flz_jni] frame_free returned non-zero status: {}", free_status as i32);
        }
    }

    java_arr.into_raw()
}
