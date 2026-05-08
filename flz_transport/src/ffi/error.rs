use std::cell::RefCell;
use std::ffi::CString;
use std::os::raw::c_char;

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

/// Set the thread-local last error message.
pub(crate) fn set_last_error(msg: impl Into<String>) {
    let msg = msg.into();
    let c_string =
        CString::new(msg).unwrap_or_else(|_| CString::new("(error contained null byte)").unwrap());
    LAST_ERROR.with(|cell| {
        *cell.borrow_mut() = Some(c_string);
    });
}

/// Retrieve the last error message for the current thread.
///
/// Returns a pointer to a null-terminated UTF-8 string. The pointer is valid
/// until the next libflezha call on the same thread. Returns null if no error
/// has been recorded.
#[no_mangle]
pub extern "C" fn flezha_last_error() -> *const c_char {
    LAST_ERROR.with(|cell| match cell.borrow().as_ref() {
        Some(c_string) => c_string.as_ptr(),
        None => std::ptr::null(),
    })
}
