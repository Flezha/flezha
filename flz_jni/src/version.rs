//! JNI entry points for the `flezha_version()` FFI call.

use jni::objects::{JClass, JString};
use jni::sys::jstring;
use jni::JNIEnv;

/// `String NativeTransport.version()`
///
/// Returns the version string embedded in `libflz_transport`.
#[no_mangle]
pub extern "system" fn Java_com_flezha_sdk_native_1_NativeTransport_version<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jstring {
    let ptr = flz_transport::ffi::api::flezha_version();
    if ptr.is_null() {
        return JString::default().into_raw();
    }

    let version_str = unsafe { std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned() };

    match env.new_string(version_str) {
        Ok(jstr) => jstr.into_raw(),
        Err(_) => JString::default().into_raw(),
    }
}
