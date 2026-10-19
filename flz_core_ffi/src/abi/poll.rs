//! Polling activations and answering them, and the calls an outstanding
//! activation makes inside its invocation.

use std::collections::HashMap;
use std::time::Duration;

use flz_core::poll_core::{EngineActivation, Polled, Starvation};
use flz_core::schema;

use crate::{
    hand_out, Error, FlzContext, Outstanding, FLZ_ERR, FLZ_OK, FLZ_POLL_ACTIVATION,
    FLZ_POLL_SHUTTING_DOWN, FLZ_POLL_TIMEOUT, FLZ_STARVATION_OBSERVED,
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

/// Block for up to `timeout_ms` until the core observes starvation: work has
/// been queued (or a deferred wake is due) with no thread inside `poll` and
/// the jobs-underway cap not reached. Answers in the same three-way shape as
/// [`flz_core_poll`] — [`FLZ_STARVATION_OBSERVED`], [`FLZ_POLL_TIMEOUT`], or
/// [`FLZ_POLL_SHUTTING_DOWN`] — so a supervisor loop ends with the runner.
///
/// For hosts whose fetch threads can be occupied by user code: they grow
/// their pool on an observation. A host that hands every activation to a
/// cheap concurrency primitive (virtual threads, goroutines, an event loop)
/// returns to `poll` immediately and never needs to call this.
///
/// # Safety
/// `ctx` must be a live pointer from [`flz_core_context_new`].
#[no_mangle]
pub unsafe extern "C" fn flz_core_wait_starvation(ctx: *mut FlzContext, timeout_ms: u64) -> i32 {
    if ctx.is_null() {
        return FLZ_ERR;
    }
    match flz_core::poll_core::global().wait_starvation(Duration::from_millis(timeout_ms)) {
        Starvation::Observed => FLZ_STARVATION_OBSERVED,
        Starvation::Timeout => FLZ_POLL_TIMEOUT,
        Starvation::ShuttingDown => FLZ_POLL_SHUTTING_DOWN,
    }
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

/// The pending timers of the Cell an outstanding work activation runs, as the
/// core keeps them. A restore hands a host its state and not its timers, so a
/// host that keeps its own copy reads them here once after one. On [`FLZ_OK`],
/// `*buf_out`/`*len_out` hold the postcard `Vec<TimerOp>`, each a
/// registration; free with [`flz_core_buf_free`].
///
/// # Safety
/// `ctx` must be live; `buf_out`/`len_out` must be valid writable pointers.
#[no_mangle]
pub unsafe extern "C" fn flz_core_pending_timers(
    ctx: *mut FlzContext,
    token: u64,
    buf_out: *mut *mut u8,
    len_out: *mut usize,
) -> i32 {
    live!(ctx);
    let ops = match ctx.outstanding.get(&token).as_deref() {
        Some(Outstanding {
            activation:
                EngineActivation {
                    payload: flz_core::poll_core::EnginePayload::Work { slot, .. },
                    ..
                },
            ..
        }) => {
            use flz_core::util::LockRecover;
            slot.timers.lock_recover().as_register_ops()
        }
        _ => return ctx.fail(Error::UnknownToken(token)),
    };
    hand_out(
        postcard::to_stdvec(&ops).expect("timer ops encode"),
        buf_out,
        len_out,
    );
    FLZ_OK
}
