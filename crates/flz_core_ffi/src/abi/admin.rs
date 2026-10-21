//! The controller calls a host makes outside an invocation: an admin verb, a
//! send, the relays listing.

use std::ffi::{c_char, CStr};

use crate::{hand_out, Error, FlzContext, FLZ_ERR, FLZ_OK};

/// Administer the namespace this runner's connection proved: one entry for the
/// whole surface, because the verb is data. `verb` names a function of the
/// controller's published consumer surface (`"Relay.start"`) and `args` is
/// a JSON array of that call's arguments other than the namespace, which the
/// controller fills from the connection. The published function's own answer
/// comes back as JSON in `result_out`/`result_len_out` (free with
/// [`flz_core_buf_free`]).
///
/// Adding a verb to the surface therefore costs nothing here and nothing in
/// any host's glue: it is one typed wrapper in the host's own language.
///
/// Blocks for the controller's answer.
///
/// # Safety
/// `ctx` must be live; strings NUL-terminated; buffers readable/writable.
#[no_mangle]
pub unsafe extern "C" fn flz_core_admin_call(
    ctx: *mut FlzContext,
    verb: *const c_char,
    args: *const c_char,
    result_out: *mut *mut u8,
    result_len_out: *mut usize,
) -> i32 {
    live!(mut ctx);
    let (Ok(verb), Ok(args)) = (CStr::from_ptr(verb).to_str(), CStr::from_ptr(args).to_str())
    else {
        return ctx.fail(Error::Utf8("administration arguments"));
    };
    match flz_core::admin::call(verb, args) {
        Ok(result) => {
            hand_out(result.into_bytes(), result_out, result_len_out);
            FLZ_OK
        }
        Err(e) => ctx.fail(e.into()),
    }
}

/// Send `message` to the entry channel `channel` of `relay`, as
/// [`flz_core::admin::send`] does: through the controller, answering its
/// verdict as JSON in `result_out`/`result_len_out` (free with
/// [`flz_core_buf_free`]), or with `local` nonzero in process to a relay
/// bound on this runner, answering `null`. An empty `msg_id` or `key` is none.
/// Blocks for the controller's answer, or while a local consumer's mailbox is
/// full.
///
/// # Safety
/// `ctx` must be live; strings NUL-terminated; `message`/`len` a readable
/// slice (`message` may be null when `len` is 0); buffers writable.
#[no_mangle]
pub unsafe extern "C" fn flz_core_send_message(
    ctx: *mut FlzContext,
    relay: *const c_char,
    channel: *const c_char,
    message: *const u8,
    len: usize,
    msg_id: *const c_char,
    key: *const c_char,
    local: i32,
    result_out: *mut *mut u8,
    result_len_out: *mut usize,
) -> i32 {
    live!(ctx);
    let (Ok(relay), Ok(channel), Ok(msg_id), Ok(key)) = (
        CStr::from_ptr(relay).to_str(),
        CStr::from_ptr(channel).to_str(),
        CStr::from_ptr(msg_id).to_str(),
        CStr::from_ptr(key).to_str(),
    ) else {
        return ctx.fail(Error::Utf8("send arguments"));
    };
    let message = if len == 0 {
        &[][..]
    } else {
        std::slice::from_raw_parts(message, len)
    };
    let (msg_id, key) = (
        (!msg_id.is_empty()).then_some(msg_id),
        (!key.is_empty()).then_some(key),
    );
    match flz_core::admin::send(relay, message, channel, msg_id, key, local != 0) {
        Ok(answer) => {
            hand_out(answer.into_bytes(), result_out, result_len_out);
            FLZ_OK
        }
        Err(e) => ctx.fail(e.into()),
    }
}

/// The ids of the relay instances bound on this process's runner, as a
/// JSON array of strings in `buf_out`/`len_out` (free with
/// [`flz_core_buf_free`]). Never contacts the controller.
///
/// # Safety
/// `ctx` must be live; `buf_out`/`len_out` must be writable.
#[no_mangle]
pub unsafe extern "C" fn flz_core_relays(
    ctx: *mut FlzContext,
    buf_out: *mut *mut u8,
    len_out: *mut usize,
) -> i32 {
    if ctx.is_null() {
        return FLZ_ERR;
    }
    let ids = flz_core::runner::bound().ids();
    hand_out(
        serde_json::to_vec(&ids).unwrap_or_default(),
        buf_out,
        len_out,
    );
    FLZ_OK
}
