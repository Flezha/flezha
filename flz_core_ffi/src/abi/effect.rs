//! A recorded effect's begin, end and release.

use std::ffi::{c_char, CStr};

use crate::{
    hand_out, Counted, Error, FlzContext, Running, FLZ_EFFECT_KEPT, FLZ_EFFECT_RUN,
    FLZ_EFFECT_UNRECORDED, FLZ_OK,
};

/// Begin `ctx.effect(name)` in `item`, the index of a work item of the live
/// activation `token` (any index for a compensation). [`FLZ_EFFECT_KEPT`]: the
/// out buffer holds the result kept for this call, which the host decodes and
/// returns without running the function; free it with [`flz_core_buf_free`].
/// [`FLZ_EFFECT_RUN`]: run the function, then answer [`flz_core_effect_end`]
/// with its encoded result, or [`flz_core_effect_release`] when it raised.
/// [`FLZ_EFFECT_UNRECORDED`]: the invocation took no input-log offset, so the
/// host runs the function, returns its result and answers
/// [`flz_core_effect_release`]. [`FLZ_ERR`]: `effect.bad_name`,
/// `effect.nested` while the item's function is still running, or
/// `effect.outside_invocation` for a token that is not outstanding.
///
/// # Safety
/// `ctx` must be live; `name` NUL-terminated; `out`/`out_len` writable.
#[no_mangle]
pub unsafe extern "C" fn flz_core_effect_begin(
    ctx: *mut FlzContext,
    token: u64,
    item: u32,
    name: *const c_char,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    use flz_core::effect_result;
    live!(ctx);
    let Ok(name) = CStr::from_ptr(name).to_str() else {
        return ctx.fail(Error::Utf8("effect name"));
    };
    if let Err(refusal) = effect_result::check_name(name) {
        return ctx.fail(Error::Refused(refusal));
    }
    let Some(mut outstanding) = ctx.outstanding.get_mut(&token) else {
        return ctx.fail(Error::Refused(effect_result::outside_invocation(name)));
    };
    if outstanding
        .effects
        .iter()
        .any(|(running, _)| *running == item)
    {
        return ctx.fail(Error::Refused(effect_result::nested(name)));
    }
    let Some((offset, phase, attempt)) = outstanding.activation.invocation(item as usize) else {
        return ctx.fail(Error::Refused(effect_result::outside_invocation(name)));
    };
    let counted = Counted::Effect(item, name.to_string());
    let ordinal = outstanding.positions.get(&counted).copied().unwrap_or(0);
    let origin = flz_core::origin::InvocationOrigin {
        cell_identity: outstanding.activation.cell_identity(),
        offset,
    };
    let resolved = effect_result::resolve(origin, phase, name, ordinal, attempt);
    outstanding.positions.insert(counted, resolved.ordinal + 1);
    let identity = resolved.identity;
    if let Some(result) = resolved.kept {
        hand_out(result, out, out_len);
        return FLZ_EFFECT_KEPT;
    }
    let status = if identity.is_empty() {
        FLZ_EFFECT_UNRECORDED
    } else {
        FLZ_EFFECT_RUN
    };
    let running = Running {
        name: name.to_string(),
        identity,
        offset,
    };
    outstanding.effects.push((item, running));
    status
}

/// Keep the encoded result of the function an [`FLZ_EFFECT_RUN`] begin ran,
/// waiting for the controller to keep it. [`FLZ_OK`]: the host returns its own
/// result. [`FLZ_EFFECT_KEPT`]: the out buffer holds the result the controller
/// kept first for this call, which the host decodes and returns instead; free
/// it with [`flz_core_buf_free`]. [`FLZ_ERR`]: `effect.too_large`,
/// `effect.not_recorded`, or `effect.outside_invocation` for an item with no
/// effect begun.
///
/// # Safety
/// `ctx` must be live; `payload`/`payload_len` readable (`payload` may be null
/// when `payload_len` is 0); `out`/`out_len` writable.
#[no_mangle]
pub unsafe extern "C" fn flz_core_effect_end(
    ctx: *mut FlzContext,
    token: u64,
    item: u32,
    payload: *const u8,
    payload_len: usize,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    live!(ctx);
    let taken = ctx.outstanding.get_mut(&token).and_then(|mut outstanding| {
        let running = outstanding.take_effect(item)?;
        Some((running, outstanding.activation.cell_identity().to_vec()))
    });
    let Some((running, cell)) = taken else {
        return ctx.fail(Error::Refused(flz_core::effect_result::outside_invocation(
            "",
        )));
    };
    let payload = if payload_len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(payload, payload_len).to_vec()
    };
    let origin = flz_core::origin::InvocationOrigin {
        cell_identity: &cell,
        offset: running.offset,
    };
    match flz_core::effect_result::keep(origin, &running.name, running.identity, payload) {
        Ok(None) => FLZ_OK,
        Ok(Some(first)) => {
            hand_out(first, out, out_len);
            FLZ_EFFECT_KEPT
        }
        Err(refusal) => ctx.fail(Error::Refused(refusal)),
    }
}

/// End an effect begun in `item` without keeping anything: its function raised,
/// or the begin answered [`FLZ_EFFECT_UNRECORDED`]. A token or item with no
/// effect begun is a no-op, so a host may answer it from a `finally`.
///
/// # Safety
/// `ctx` must be live.
#[no_mangle]
pub unsafe extern "C" fn flz_core_effect_release(
    ctx: *mut FlzContext,
    token: u64,
    item: u32,
) -> i32 {
    live!(ctx);
    if let Some(mut outstanding) = ctx.outstanding.get_mut(&token) {
        outstanding.take_effect(item);
    }
    FLZ_OK
}
