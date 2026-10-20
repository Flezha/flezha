//! Polling activations and answering them, the pool of workers that poll,
//! and the calls an outstanding activation makes inside its invocation.

use std::collections::HashMap;
use std::time::Duration;

use flz_core::poll_core::Polled;
use flz_core::poll_pool::{self, Governor, Supervise};
use flz_core::schema;
use flz_core::util::LockRecover;

use crate::{
    hand_out, Error, FlzContext, Outstanding, FLZ_ERR, FLZ_OK, FLZ_POLL_ACTIVATION,
    FLZ_POLL_SHUTTING_DOWN, FLZ_POLL_TIMEOUT,
};

/// Block for up to `timeout_ms` for one activation. On
/// [`FLZ_POLL_ACTIVATION`], `*buf_out`/`*len_out` hold the encoded
/// [`schema::Activation`]; free with [`flz_core_buf_free`].
///
/// # Safety
/// `ctx` must be live; `buf_out`/`len_out` must be valid writable pointers.
#[no_mangle]
pub unsafe extern "C" fn flz_core_poll(
    ctx: *mut FlzContext,
    timeout_ms: u64,
    buf_out: *mut *mut u8,
    len_out: *mut usize,
) -> i32 {
    live!(mut ctx);
    match flz_core::poll_core::global().poll(Duration::from_millis(timeout_ms)) {
        Polled::Timeout => FLZ_POLL_TIMEOUT,
        Polled::ShuttingDown => FLZ_POLL_SHUTTING_DOWN,
        Polled::Activation(act) => {
            let encoded = schema::encode_activation(&flz_core::host::to_schema(&act));
            let outstanding = Outstanding {
                activation: act,
                spawns: Vec::new(),
                positions: HashMap::new(),
                effects: Vec::new(),
                part_sequences: HashMap::new(),
            };
            ctx.outstanding
                .insert(outstanding.activation.token, outstanding);
            hand_out(encoded, buf_out, len_out);
            FLZ_POLL_ACTIVATION
        }
    }
}

/// [`flz_core_pool_supervise`]: nothing to spawn this wait.
pub const FLZ_POOL_IDLE: i32 = 0;
/// Spawn a worker that never retires, into the seat already reserved for it.
pub const FLZ_POOL_SPAWN_BASE: i32 = 1;
/// The pool is over; the supervisor leaves.
pub const FLZ_POOL_STOP: i32 = 2;
/// Spawn a worker that leaves after the retire time of timed-out polls, into
/// the seat already reserved for it.
pub const FLZ_POOL_SPAWN_GROWN: i32 = 3;

/// Open this context's poll-worker pool over the host's own `default_base`
/// and write its policy to `policy_out`: the base, the ceiling, then the poll
/// timeout, retire time and drain bound in milliseconds. The base and the
/// ceiling come from `FLEZHA_WORKER_POOL_SIZE` and `FLEZHA_WORKER_POOL_MAX`.
/// The base seats are taken: the host spawns `base` workers next. A second
/// open replaces the pool.
///
/// # Safety
/// `ctx` must be live; `policy_out` must point at five writable `u32`s.
#[no_mangle]
pub unsafe extern "C" fn flz_core_pool_open(
    ctx: *mut FlzContext,
    default_base: u32,
    policy_out: *mut [u32; 5],
) -> i32 {
    live!(ctx);
    let Some(policy_out) = policy_out.as_mut() else {
        return FLZ_ERR;
    };
    let governor = Governor::from_env(default_base.max(1) as usize);
    let ms = |d: Duration| d.as_millis() as u32;
    let (base, ceiling) = (governor.base as u32, governor.ceiling as u32);
    let retire = ms(governor.retire_after);
    *policy_out = [
        base,
        ceiling,
        ms(poll_pool::POLL_TIMEOUT),
        retire,
        ms(poll_pool::DRAIN_BOUND),
    ];
    *ctx.pool.lock_recover() = Some(governor);
    FLZ_OK
}

/// The pool's next decision, at most the supervisor wait (1 s) away:
/// [`FLZ_POOL_SPAWN_GROWN`], [`FLZ_POOL_SPAWN_BASE`] (a worker that left is
/// replaced), [`FLZ_POOL_IDLE`] or [`FLZ_POOL_STOP`], which a context with no
/// open pool answers too. A spawn answer has reserved its seat; a spawn that
/// fails gives it back through [`flz_core_pool_left`].
///
/// # Safety
/// `ctx` must be a live pointer from [`flz_core_context_new`].
#[no_mangle]
pub unsafe extern "C" fn flz_core_pool_supervise(ctx: *mut FlzContext) -> i32 {
    live!(ctx);
    let Some(governor) = ctx.pool.lock_recover().clone() else {
        return FLZ_POOL_STOP;
    };
    match governor.supervise(flz_core::poll_core::global()) {
        Supervise::Spawn { grown: true } => FLZ_POOL_SPAWN_GROWN,
        Supervise::Spawn { grown: false } => FLZ_POOL_SPAWN_BASE,
        Supervise::Idle => FLZ_POOL_IDLE,
        Supervise::Stop => FLZ_POOL_STOP,
    }
}

/// Give back one seat: a worker that left, or a spawn that failed.
///
/// # Safety
/// `ctx` must be a live pointer from [`flz_core_context_new`].
#[no_mangle]
pub unsafe extern "C" fn flz_core_pool_left(ctx: *mut FlzContext) -> i32 {
    live!(ctx);
    if let Some(governor) = ctx.pool.lock_recover().as_ref() {
        governor.left();
    }
    FLZ_OK
}

/// Complete an activation with an encoded [`schema::ActivationResult`].
/// Accepted from any thread, including one that never polled.
///
/// # Safety
/// `ctx` must be live; `buf`/`len` must describe a readable buffer.
#[no_mangle]
pub unsafe extern "C" fn flz_core_complete(
    ctx: *mut FlzContext,
    buf: *const u8,
    len: usize,
) -> i32 {
    live!(mut ctx);
    let bytes = std::slice::from_raw_parts(buf, len);
    let result = match schema::decode_result(bytes) {
        Ok(r) => r,
        Err(e) => return ctx.fail(Error::ResultDecode(e)),
    };
    let act = match ctx.take_outstanding(result.token) {
        Ok(act) => act,
        Err(status) => return status,
    };
    match flz_core::host::apply_result(flz_core::poll_core::global(), act, result) {
        Ok(()) => FLZ_OK,
        Err(e) => ctx.fail(e.into()),
    }
}

/// Abandon a polled activation: re-hand its untouched items and release its
/// cell, exactly as if it had never been polled. Accepted from any thread.
///
/// The third and last way to answer a handout, and the one the ABI must name
/// explicitly because **no unwind crosses this boundary**: a veneer whose host
/// language throws, or whose thread dies, cannot reach [`flz_core_complete`],
/// and per-cell exclusivity would then retire that cell for the life of
/// the process. A veneer SHALL answer every polled activation exactly once and
/// SHALL make that structural in its own language — a `finally`, a
/// destructor, a scope guard — not a discipline at each return site.
///
/// No retry-ladder attempt is consumed: an abandon is not a failure, it is
/// work that never ran. Only pass a token whose items the veneer did NOT
/// process; abandoning a partly-processed batch would execute those items
/// twice. An unknown or already-answered token is refused with
/// [`FLZ_ERR`] and the reason in [`flz_core_last_error`].
///
/// # Safety
/// `ctx` must be a live pointer from [`flz_core_context_new`].
#[no_mangle]
pub unsafe extern "C" fn flz_core_abandon(ctx: *mut FlzContext, token: u64) -> i32 {
    live!(mut ctx);
    let act = match ctx.take_outstanding(token) {
        Ok(act) => act,
        Err(status) => return status,
    };
    match flz_core::host::abandon_activation(flz_core::poll_core::global(), act) {
        Ok(()) => FLZ_OK,
        Err(e) => ctx.fail(e.into()),
    }
}

/// Send a part of the invocation of `item`, the index of a work item of the
/// live activation `token`, with the payload the host encoded as that
/// tasklet's emit. Never waits. [`FLZ_OK`] for a part that left and for one
/// the drop rules dropped; [`FLZ_ERR`] with `part.no_output`, or with
/// `part.outside_invocation` for a token that is not outstanding or an item
/// its activation does not hold.
///
/// # Safety
/// `ctx` must be live; `payload`/`payload_len` readable.
#[no_mangle]
pub unsafe extern "C" fn flz_core_part(
    ctx: *mut FlzContext,
    token: u64,
    item: u32,
    payload: *const u8,
    payload_len: usize,
) -> i32 {
    live!(ctx);
    let Some(mut outstanding) = ctx.outstanding.get_mut(&token) else {
        return ctx.fail(flz_core::part::outside_invocation(None).into());
    };
    let Outstanding {
        activation,
        part_sequences,
        ..
    } = &mut *outstanding;
    let Some((slot, work)) = activation.host_item(item as usize) else {
        return ctx.fail(flz_core::part::outside_invocation(None).into());
    };
    let payload = if payload_len == 0 {
        &[][..]
    } else {
        std::slice::from_raw_parts(payload, payload_len)
    };
    let sequence = part_sequences.entry(item).or_insert(0);
    match flz_core::part::send(slot, work, sequence, payload) {
        Ok(_) => FLZ_OK,
        Err(refusal) => ctx.fail(refusal.into()),
    }
}
