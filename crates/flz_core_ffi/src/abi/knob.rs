//! A knob's text parsed by the one rule `flz_transport::env` applies, for a
//! host that looked the variable up itself.

use std::ffi::{c_char, CStr};

use flz_transport::env;

use crate::{FLZ_ERR, FLZ_OK};

/// [`flz_core_knob`]'s answer for text that is no value of its kind: the host
/// keeps its default.
pub const FLZ_KNOB_UNSET: i32 = 1;

/// Parse `raw` as a knob of `kind` (`positive`, `ms`, `int` or `bool`, a flag
/// written as 0 or 1) into `*out`: [`FLZ_OK`], or [`FLZ_KNOB_UNSET`] when the
/// text does not parse. A kind that is none of these answers [`FLZ_ERR`].
/// Context-free.
///
/// # Safety
/// `kind` and `raw` are NUL-terminated strings; `out` is a valid writable pointer.
#[no_mangle]
pub unsafe extern "C" fn flz_core_knob(
    kind: *const c_char,
    raw: *const c_char,
    out: *mut i64,
) -> i32 {
    if kind.is_null() || raw.is_null() || out.is_null() {
        return FLZ_ERR;
    }
    let raw = CStr::from_ptr(raw).to_str().unwrap_or_default();
    let value = match CStr::from_ptr(kind).to_bytes() {
        b"positive" => env::parse_positive(raw).and_then(|n| i64::try_from(n).ok()),
        b"ms" => env::parse_ms(raw).and_then(|n| i64::try_from(n).ok()),
        b"int" => env::parse_int(raw),
        b"bool" => env::parse_bool(raw).map(i64::from),
        _ => return FLZ_ERR,
    };
    match value {
        Some(value) => {
            *out = value;
            FLZ_OK
        }
        None => FLZ_KNOB_UNSET,
    }
}
