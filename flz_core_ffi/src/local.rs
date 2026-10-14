//! Local mode over the C ABI: the declared graph run in the host's own
//! process, with no controller.
//!
//! Every call here is the C-ABI face of `flz_core::local::LocalRun`. The host
//! keeps driving [`crate::flz_core_poll`] / [`crate::flz_core_complete`] as it
//! always does — its poll loop is what runs the tasklets, and local mode
//! does not touch it. [`crate::flz_core_start`] is the alternative to
//! [`flz_core_local_start`], never an addition: a context either dials a
//! controller or holds a local run.

use std::ffi::{c_char, CStr};
use std::sync::Arc;
use std::time::Duration;

use flz_core::schema;

use crate::{
    hand_out, Error, FlzContext, LocalSession, FLZ_OK, FLZ_POLL_ACTIVATION, FLZ_POLL_TIMEOUT,
};

/// Start the class declared under `factory_id` as a local run named
/// `relay_id`. Refuses a declaration that needs a controller, naming
/// the capability in [`flz_core_last_error`].
///
/// # Safety
/// `ctx` must be live; both strings must be NUL-terminated.
#[no_mangle]
pub unsafe extern "C" fn flz_core_local_start(
    ctx: *mut FlzContext,
    factory_id: *const c_char,
    relay_id: *const c_char,
) -> i32 {
    let ctx = &*ctx;
    if flz_core::poll_core::global().is_shut_down() {
        return ctx.fail(Error::ShutDown);
    }
    let (Ok(factory_id), Ok(relay_id)) = (
        CStr::from_ptr(factory_id).to_str(),
        CStr::from_ptr(relay_id).to_str(),
    ) else {
        return ctx.fail(Error::Utf8("local factory id"));
    };
    let run = match flz_core::local::LocalRun::start(factory_id, relay_id) {
        Ok(run) => run,
        Err(e) => return ctx.fail(Error::Core(e)),
    };
    let (releases, receiver) = std::sync::mpsc::channel();
    *ctx.local.lock().unwrap_or_else(|p| p.into_inner()) = Some(LocalSession {
        run: Arc::new(run),
        releases,
    });
    *ctx.local_outputs.lock().unwrap_or_else(|p| p.into_inner()) = Some(receiver);
    FLZ_OK
}

/// Queue every value released on output `channel` for
/// [`flz_core_local_next_output`]. A sink with no registered interest is
/// reported by the core rather than discarded.
///
/// # Safety
/// `ctx` must be live; `channel` must be NUL-terminated.
#[no_mangle]
pub unsafe extern "C" fn flz_core_local_on_sink(
    ctx: *mut FlzContext,
    channel: *const c_char,
) -> i32 {
    let ctx = &*ctx;
    let Ok(channel) = CStr::from_ptr(channel).to_str() else {
        return ctx.fail(Error::Utf8("local output channel"));
    };
    let (run, releases) = match ctx.local_session() {
        Ok(session) => session,
        Err(e) => return ctx.fail(e),
    };
    let registered = run.on_output(
        channel,
        Arc::new(move |output: flz_core::local::LocalOutput| {
            // The host reads its own language's `Output`, which is the shape
            // it already decodes off a completion. The key rides `flz.key` in
            // the metadata, exactly where a deployed keyed sink carries it.
            let mut metadata: Vec<(String, String)> = output.metadata.into_iter().collect();
            metadata.sort();
            let _ = releases.send(schema::encode_output(&schema::Output {
                target: schema::OutputTarget::Plugin {
                    channel: output.channel,
                },
                key: None,
                event_time: None,
                metadata,
                payload: output.payload,
            }));
        }),
    );
    match registered {
        Ok(()) => FLZ_OK,
        Err(e) => ctx.fail(Error::Core(e)),
    }
}

/// Feed one value's wire bytes to a declared entry channel. Blocks while
/// the consumer's mailbox is full, exactly as the deployed entry path does.
///
/// # Safety
/// `ctx` must be live; `channel` NUL-terminated; `payload`/`len` a readable
/// slice.
#[no_mangle]
pub unsafe extern "C" fn flz_core_local_feed(
    ctx: *mut FlzContext,
    channel: *const c_char,
    payload: *const u8,
    len: usize,
) -> i32 {
    let ctx = &*ctx;
    let Ok(channel) = CStr::from_ptr(channel).to_str() else {
        return ctx.fail(Error::Utf8("local entry channel"));
    };
    let (run, _releases) = match ctx.local_session() {
        Ok(session) => session,
        Err(e) => return ctx.fail(e),
    };
    let bytes = std::slice::from_raw_parts(payload, len).to_vec();
    match run.feed(channel, bytes) {
        Ok(()) => FLZ_OK,
        Err(e) => ctx.fail(Error::Core(e)),
    }
}

/// Hand out the next queued sink output, or [`FLZ_POLL_TIMEOUT`] when none
/// arrives within `timeout_ms`. The buffer is one encoded `schema::Output`,
/// freed by [`flz_core_buf_free`].
///
/// # Safety
/// `ctx` must be live; `buf_out`/`len_out` must be writable.
#[no_mangle]
pub unsafe extern "C" fn flz_core_local_next_output(
    ctx: *mut FlzContext,
    timeout_ms: u64,
    buf_out: *mut *mut u8,
    len_out: *mut usize,
) -> i32 {
    let ctx = &*ctx;
    let guard = ctx.local_outputs.lock().unwrap_or_else(|p| p.into_inner());
    let Some(outputs) = guard.as_ref() else {
        return ctx.fail(Error::NoLocalRun);
    };
    match outputs.recv_timeout(Duration::from_millis(timeout_ms)) {
        Ok(encoded) => {
            hand_out(encoded, buf_out, len_out);
            FLZ_POLL_ACTIVATION
        }
        Err(_) => FLZ_POLL_TIMEOUT,
    }
}

/// Block until the graph has nothing left to do, or `timeout_ms` elapses —
/// which is a refusal, never a silent success.
///
/// # Safety
/// `ctx` must be live.
#[no_mangle]
pub unsafe extern "C" fn flz_core_local_run_until_idle(
    ctx: *mut FlzContext,
    timeout_ms: u64,
) -> i32 {
    let ctx = &*ctx;
    let (run, _releases) = match ctx.local_session() {
        Ok(session) => session,
        Err(e) => return ctx.fail(e),
    };
    match run.run_until_idle(Duration::from_millis(timeout_ms)) {
        Ok(()) => FLZ_OK,
        Err(e) => ctx.fail(Error::Core(e)),
    }
}

/// End the local run and release its Cells. A context with none is a
/// no-op, so a host's close path needs no test of its own.
///
/// Values already released stay readable. Dropping the run drops the sender,
/// so [`flz_core_local_next_output`] hands out what is still queued and then
/// reports a timeout, and a host that drains after stopping loses nothing.
///
/// # Safety
/// `ctx` must be live.
#[no_mangle]
pub unsafe extern "C" fn flz_core_local_stop(ctx: *mut FlzContext) -> i32 {
    let ctx = &*ctx;
    ctx.local.lock().unwrap_or_else(|p| p.into_inner()).take();
    FLZ_OK
}
