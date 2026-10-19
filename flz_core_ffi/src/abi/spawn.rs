//! A tasklet's spawn request and the reads of its handle.

use std::ffi::{c_char, CStr};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, TryLockError};
use std::time::Duration;

use flz_core::spawn::SpawnOptions;

use crate::{hand_out, Counted, Error, FlzContext, FLZ_ERR, FLZ_OK, FLZ_SPAWN_PENDING};

/// Ask for a steps instance from the live activation `token`: start
/// `factory_id` under `id`, or under an id the controller generates when `id`
/// is empty. `if_running` (`"deliver"`, the default when empty, or `"error"`)
/// applies only to an id that is already running. A nonzero `replay` sends
/// nothing and resolves the handle to `skipped_on_replay` at once. `offset`
/// is the input-log offset of the item the calling tasklet is taking (`-1`
/// for none): with the Cell and the call's position it names the request, so
/// a re-run of that item resolves to what the first run's request did.
///
/// Never waits: `*handle_out`'s answer, a controller's refusal too, is [`flz_core_spawn_result`]'s.
/// [`FLZ_ERR`], sending nothing: bad UTF-8, `if_running` without `id`, or an unknown token.
///
/// # Safety
/// `ctx` must be live; strings NUL-terminated; `payload`/`payload_len`
/// readable; `handle_out` writable.
#[no_mangle]
pub unsafe extern "C" fn flz_core_spawn_request(
    ctx: *mut FlzContext,
    token: u64,
    factory_id: *const c_char,
    id: *const c_char,
    payload: *const u8,
    payload_len: usize,
    if_running: *const c_char,
    replay: i32,
    offset: i64,
    handle_out: *mut u64,
) -> i32 {
    let Some(ctx) = ctx.as_ref() else {
        return FLZ_ERR;
    };
    let (Ok(factory_id), Ok(id), Ok(if_running)) = (
        CStr::from_ptr(factory_id).to_str(),
        CStr::from_ptr(id).to_str(),
        CStr::from_ptr(if_running).to_str(),
    ) else {
        return ctx.fail(Error::Utf8("spawn arguments"));
    };
    let options = match SpawnOptions::from_wire(id, if_running) {
        Ok(options) => options,
        Err(refusal) => return ctx.fail(Error::Refused(refusal)),
    };
    // Held until the handle is recorded, so the activation cannot complete in
    // between and leave the handle unreleased.
    let Some(mut outstanding) = ctx.outstanding.get_mut(&token) else {
        return ctx.fail(Error::UnknownToken(token));
    };
    let payload = if payload_len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(payload, payload_len).to_vec()
    };
    let origin_position = outstanding.next_position(Counted::Spawn(offset));
    let origin = flz_core::origin::InvocationOrigin {
        cell_identity: outstanding.activation.cell_identity(),
        offset: u64::try_from(offset).ok(),
    };
    let spawn = flz_core::spawn::request(
        factory_id.to_string(),
        payload,
        options,
        replay != 0,
        origin,
        origin_position,
    );
    let handle = ctx.last_spawn.fetch_add(1, Ordering::Relaxed) + 1;
    ctx.spawns.insert(handle, Arc::new(Mutex::new(spawn)));
    outstanding.spawns.push(handle);
    *handle_out = handle;
    FLZ_OK
}

/// Read a spawn handle, waiting up to `timeout_ms` (0 never waits,
/// [`FLZ_WAIT_CORE_BOUND`] to the request's deadline) for the controller's
/// answer. [`FLZ_OK`]: `out` holds `{"id","outcome"}`, the id the request
/// resolved to and `spawned`, `delivered` or `skipped_on_replay`; free it with
/// [`flz_core_buf_free`]. [`FLZ_SPAWN_PENDING`]: no answer yet, `out` untouched.
/// [`FLZ_ERR`]: `out` untouched and [`flz_core_last_error`] holds the
/// controller's code (`relay.already_running` or any other refusal's own), the
/// core's (`spawn.timeout`, `spawn.unavailable`) or `runtime.spawn_handle_released`
/// once the handle's activation ended. A resolved handle answers every later
/// read the same.
///
/// # Safety
/// `ctx` must be live; `out`/`out_len` must be valid writable pointers.
#[no_mangle]
pub unsafe extern "C" fn flz_core_spawn_result(
    ctx: *mut FlzContext,
    handle: u64,
    timeout_ms: u64,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    let Some(ctx) = ctx.as_ref() else {
        return FLZ_ERR;
    };
    let Some(spawn) = ctx.spawns.get(&handle).map(|entry| Arc::clone(&entry)) else {
        return ctx.fail(Error::SpawnReleased(handle));
    };
    let mut held = match (timeout_ms, spawn.try_lock()) {
        (_, Ok(held)) => held,
        (_, Err(TryLockError::Poisoned(p))) => p.into_inner(),
        // Another thread's waiting read holds the handle, so no answer is in.
        (0, Err(TryLockError::WouldBlock)) => return FLZ_SPAWN_PENDING,
        (_, Err(TryLockError::WouldBlock)) => spawn.lock().unwrap_or_else(|p| p.into_inner()),
    };
    // A reader that waited for the lock may find the handle already released.
    if !ctx.spawns.contains_key(&handle) {
        return ctx.fail(Error::SpawnReleased(handle));
    }
    let Some(answer) = held.wait_timeout(Duration::from_millis(timeout_ms)) else {
        return FLZ_SPAWN_PENDING;
    };
    match answer {
        Ok(result) => {
            let json = serde_json::json!({"id": result.id, "outcome": result.outcome.as_wire()});
            hand_out(serde_json::to_vec(&json).unwrap_or_default(), out, out_len);
            FLZ_OK
        }
        Err(error) => ctx.fail(Error::Spawn(error)),
    }
}
