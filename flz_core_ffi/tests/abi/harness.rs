//! What the scenario drives the ABI with: the poll/complete pair every phase
//! calls, over the crate's own fake controller.
//!
//! The controller is [`flz_core_ffi::fake_controller::Controller`], the one
//! `flz-fake-controller` drives rather than a copy of it, so a frame it learns to
//! handle is learnt here and in the binary at once. It captures every
//! runner→controller frame and wakes [`Controller::await_frame`] on each one,
//! so a phase waits for the frame it needs rather than for a duration.

use flz_core::schema;
pub use flz_core_ffi::fake_controller::Controller;
use flz_core_ffi::veneer::{last_error_of, CtxPtr};
use flz_core_ffi::*;

/// The controller's answers in a test's voice.
///
/// The module reports a refusal rather than raising one, because
/// `flz-fake-controller` turns it into an exit code and a message an SDK
/// author reads. A phase of this scenario has nothing to do with a refusal but
/// fail, so it gets the short name and the panic here rather than a second
/// method on the module.
pub trait Demanded {
    fn send(&self, reference_id: &str, command: flz_core::pb::command_request::Command);
    fn await_frame<T>(
        &self,
        what: &str,
        probe: impl Fn(&[flz_core_ffi::fake_controller::CapturedFrame]) -> Option<T>,
    ) -> T;
    fn await_reply(&self, what: &str, reference_id: &str) -> flz_core::pb::CommandResponse;
    fn await_success(&self, what: &str, reference_id: &str);
}

impl Demanded for Controller {
    fn send(&self, reference_id: &str, command: flz_core::pb::command_request::Command) {
        self.try_send(reference_id, command)
            .expect("write command frame");
    }

    fn await_frame<T>(
        &self,
        what: &str,
        probe: impl Fn(&[flz_core_ffi::fake_controller::CapturedFrame]) -> Option<T>,
    ) -> T {
        self.try_await_frame(what, probe)
            .unwrap_or_else(|e| panic!("{e}"))
    }

    fn await_reply(&self, what: &str, reference_id: &str) -> flz_core::pb::CommandResponse {
        self.try_await_reply(what, reference_id)
            .unwrap_or_else(|e| panic!("{e}"))
    }

    fn await_success(&self, what: &str, reference_id: &str) {
        assert_eq!(self.await_reply(what, reference_id).status, "success");
    }
}

/// Poll through the ABI, panicking on error; returns (status, decoded).
pub fn abi_poll(ctx: *mut FlzContext, timeout_ms: u64) -> (i32, Option<schema::Activation>) {
    let mut buf: *mut u8 = std::ptr::null_mut();
    let mut len: usize = 0;
    let status = unsafe { flz_core_poll(ctx, timeout_ms, &mut buf, &mut len) };
    if status != FLZ_POLL_ACTIVATION {
        return (status, None);
    }
    let bytes = unsafe { std::slice::from_raw_parts(buf, len) }.to_vec();
    unsafe { flz_core_buf_free(buf, len) };
    let act = schema::decode_activation(&bytes).expect("activation decodes");
    crate::shape::assert_activation_maps(&act);
    (status, Some(act))
}

/// Map a result through the shape and assert it changed nothing.
pub fn through_veneers(result: &schema::ActivationResult) -> schema::ActivationResult {
    crate::shape::through_veneer(result)
}

pub fn abi_complete(ctx: *mut FlzContext, result: &schema::ActivationResult) {
    abi_complete_mapped(ctx, &through_veneers(result));
}

/// Hand an already-mapped result to the ABI. Callable from any thread.
pub fn abi_complete_mapped(ctx: *mut FlzContext, result: &schema::ActivationResult) {
    let bytes = schema::encode_result(result);
    let status = unsafe { flz_core_complete(ctx, bytes.as_ptr(), bytes.len()) };
    if status != FLZ_OK {
        panic!("complete failed: {}", last_error(ctx));
    }
}

/// The activation's input payloads, in batch order.
pub fn work_payloads(act: &schema::Activation) -> Vec<Vec<u8>> {
    let schema::ActivationKind::Work(items) = &act.kind else {
        panic!("expected a work batch");
    };
    items
        .iter()
        .map(|item| match item {
            schema::WorkItem::Input(input) => input.payload.clone(),
            other => panic!("expected inputs, got {other:?}"),
        })
        .collect()
}

/// The last error the context recorded, read the way a binding reads it.
pub fn last_error(ctx: *mut FlzContext) -> String {
    last_error_of(CtxPtr::of(ctx))
}

pub fn ok_effect(consumed_offset: Option<u64>, timers: Vec<schema::TimerOp>) -> schema::ItemEffect {
    schema::ItemEffect {
        verdict: schema::Verdict::Ok,
        changes: vec![],
        outputs: vec![],
        consumed_offset,
        event_time: 0,
        timers,
        checkpoint: false,
        dlq: None,
        complete: None,
    }
}
