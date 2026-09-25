use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use crossbeam::channel::Sender;

use crate::operator::instance::OperatorInstance;

/// The coordinator's message channel, installed once by `coordinator::start`.
static RECONNECT_TRIGGER: OnceLock<Mutex<Sender<RunnerMessage>>> = OnceLock::new();

pub fn install_reconnect_trigger(otx: Sender<RunnerMessage>) {
    crate::state_sink::install_reconnect_hook(request_reconnect);
    // Replace on re-install: a runner restarted after a prior one closed must
    // not leave the trigger pointing at the dead coordinator's channel.
    match RECONNECT_TRIGGER.get() {
        Some(lock) => *lock.lock().unwrap() = otx,
        None => {
            let _ = RECONNECT_TRIGGER.set(Mutex::new(otx));
        }
    }
}

/// Send to the coordinator from outside its thread; a no-op when none is
/// installed.
pub fn coordinator_send(msg: RunnerMessage) {
    let _ = coordinator_try_send(msg);
}

/// [`coordinator_send`] that says whether the message reached a live
/// coordinator, for callers that would otherwise wait for a reply forever.
pub fn coordinator_try_send(msg: RunnerMessage) -> bool {
    match RECONNECT_TRIGGER.get() {
        Some(lock) => match lock.lock() {
            Ok(tx) => tx.send(msg).is_ok(),
            Err(_) => false,
        },
        None => false,
    }
}

/// Ask the coordinator for a reconnect; `reason` per [`RunnerMessage::Reconnect`].
pub fn request_reconnect(reason: &'static str) {
    coordinator_send(RunnerMessage::Reconnect(reason));
}

/// `false` only in a transport-less run, which must not wait for replies.
pub fn coordinator_installed() -> bool {
    RECONNECT_TRIGGER.get().is_some()
}

/// An SDK-initiated request under the caller's `reference_id`, which the
/// controller's reply echoes to wake the registered waiter.
pub fn emit_control_request(reference_id: String, command: crate::pb::command_request::Command) {
    coordinator_send(RunnerMessage::control_request(reference_id, command));
}

/// A bounded operator's `BoundedCompleted`, so the controller deregisters it.
pub fn emit_bounded_completed(operator_id: String, status: String, error: String, result: Vec<u8>) {
    coordinator_send(RunnerMessage::EmitBoundedCompleted {
        operator_id,
        status,
        error,
        result,
    });
}

/// Serializes every test that mutates process-global runner state (the env
/// knobs, the coordinator trigger); poison-tolerant so one failure does not
/// cascade.
static ENV_TEST_LOCK: Mutex<()> = Mutex::new(());

#[doc(hidden)]
pub fn env_test_guard() -> std::sync::MutexGuard<'static, ()> {
    ENV_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Control-plane messages routed through the coordinator; data-plane
/// traffic never comes here.
pub enum RunnerMessage {
    HandleCommandAction(String, Vec<u8>, HashMap<String, String>),
    ReturnOperators,
    /// Drive a reconnect; the reason is one of `control_stream_broken |
    /// read_error | write_error | keepalive_timeout | controller_requested |
    /// broken_pipe_flood`.
    Reconnect(&'static str),
    /// The keepalive timer's tick: a ping on the current control stream.
    SendControlKeepalive,
    /// The heartbeat timer's tick: a heartbeat on every secondary stream.
    SendDataHeartbeat,
    /// A bounded operator reached an exit trigger.
    EmitBoundedCompleted {
        operator_id: String,
        status: String,
        error: String,
        result: Vec<u8>,
    },
    /// An SDK-initiated request, written verbatim under the caller's
    /// `reference_id`.
    EmitControlRequest {
        reference_id: String,
        command: Box<crate::pb::command_request::Command>,
    },
    /// A post-start remove, on the loop so the live-instance check is
    /// serialized with `AddOperator`; answered by
    /// [`ClientResponseMessage::RemoveFactoryResult`].
    RemoveFactory {
        factory_id: String,
    },
}

impl RunnerMessage {
    /// An SDK-initiated request under the caller's `reference_id`. The command
    /// is boxed so it does not size every other message on the channel.
    pub fn control_request(
        reference_id: String,
        command: crate::pb::command_request::Command,
    ) -> Self {
        RunnerMessage::EmitControlRequest {
            reference_id,
            command: Box::new(command),
        }
    }
}

pub enum RunnerStatus {
    Starting,
    Running,
    Stopped,
}

pub enum ClientResponseMessage {
    RunnerIdConfirmation,
    ReturnOperators(HashMap<String, Arc<OperatorInstance>>),
    /// `Err(n)`: `n` live instances exist and the class stays defined.
    RemoveFactoryResult(Result<(), usize>),
}

pub use crate::coordinator::{live_instance, start, RunnerNode};
pub use crate::routing::OperatorDispatchHandle;
