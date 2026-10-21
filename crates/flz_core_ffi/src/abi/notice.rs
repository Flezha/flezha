//! The core's notices over the C ABI, for the host to write through its own
//! log: a callback any thread may call, or a queue the host drains itself.

use std::ffi::{c_char, CString};
use std::sync::Once;
use std::time::Duration;

use flz_transport::notice;

use crate::{hand_out, FLZ_ERR, FLZ_OK};

/// Send every later notice to `callback` as its level (1 info, 2 warn,
/// 3 error, 4 always: the local-mode banner, which a host writes whatever its
/// level filter says) and its text, borrowed for the call. The callback runs
/// on whichever thread wrote the notice, a core thread included. NULL sends
/// them to standard error again.
#[no_mangle]
pub extern "C" fn flz_core_set_log_callback(
    callback: Option<extern "C" fn(level: i32, message: *const c_char)>,
) {
    match callback {
        Some(callback) => notice::set_hook(Box::new(move |level, _target, text| {
            let text = CString::new(text.replace('\0', " ")).unwrap_or_default();
            callback(level as i32, text.as_ptr());
        })),
        None => notice::clear_hook(),
    }
}

/// Every notice held since the last call, as the JSON array
/// `[{"level", "target", "text"}]` with `level` one of `info`, `warn`,
/// `error`, `always`; free with [`crate::flz_core_buf_free`]. The first call
/// starts the holding: from then on a notice waits for the next call rather
/// than going to standard error. At most 1 024 wait; past that the newest are
/// dropped and counted, and the next batch ends with a line saying how many.
///
/// # Safety
/// `out`/`out_len` must be valid writable pointers.
#[no_mangle]
pub unsafe extern "C" fn flz_core_take_notices(out: *mut *mut u8, out_len: *mut usize) -> i32 {
    if out.is_null() || out_len.is_null() {
        return FLZ_ERR;
    }
    static HOLDING: Once = Once::new();
    HOLDING.call_once(|| notice::set_hook(Box::new(notice::push)));
    let batch: Vec<_> = notice::take(Duration::ZERO)
        .into_iter()
        .map(|n| serde_json::json!({"level": n.level.name(), "target": n.target, "text": n.text}))
        .collect();
    hand_out(serde_json::to_vec(&batch).unwrap_or_default(), out, out_len);
    FLZ_OK
}
