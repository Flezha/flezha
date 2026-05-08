use std::ffi::CString;

use super::types::{FlezhaLogFn, FlezhaLogLevel};

pub(crate) struct LogState {
    pub callback: FlezhaLogFn,
    pub user_data: *mut std::ffi::c_void,
}

// Safety: user_data is intentionally shared across threads by the caller's contract
unsafe impl Send for LogState {}
unsafe impl Sync for LogState {}

// reason: ctor + log helpers invoked from the C-FFI surface (flezha_set_log_callback)
#[allow(dead_code)]
impl LogState {
    pub fn new() -> Self {
        LogState {
            callback: None,
            user_data: std::ptr::null_mut(),
        }
    }

    pub fn log(&self, level: FlezhaLogLevel, message: &str) {
        if let Some(cb) = self.callback {
            if let Ok(c_msg) = CString::new(message) {
                unsafe {
                    cb(level, c_msg.as_ptr(), self.user_data);
                }
            }
        }
    }
}
