//! Telling the host a Cell or a relay left this runner, so it drops the state
//! it holds for them. The invariant: no activation for the same name reaches
//! the host before it answered the leave, and the leave reaches it only once
//! nothing handed out for what left is still running.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use crate::cell::CellSlot;
use crate::poll_core::{CommandReply, EnginePayload};
use crate::relay::instance::RelayInstance;
use crate::schema;
use crate::util::LockRecover;

/// Relay leaves the host has not answered yet, by relay id.
static PENDING: LazyLock<Mutex<HashMap<String, Arc<PendingLeave>>>> =
    LazyLock::new(Default::default);

/// A command frame as the control stream carried it: payload id, bytes.
pub type HeldFrame = (String, Vec<u8>);

/// A relay's leave until the host answers it; a start of the same id is held
/// for that answer ([`crate::command`]).
pub struct PendingLeave {
    /// The starts held behind the leave; `None` once it is answered.
    held: Mutex<Option<Vec<HeldFrame>>>,
}

impl PendingLeave {
    pub(super) fn new() -> Self {
        Self {
            held: Mutex::new(Some(Vec::new())),
        }
    }

    /// Hold a start until the host answered the leave, or the poll core
    /// dropped it; the frame comes back when that already happened.
    pub fn hold(&self, frame: HeldFrame) -> Option<HeldFrame> {
        match self.held.lock_recover().as_mut() {
            Some(held) => {
                held.push(frame);
                None
            }
            None => Some(frame),
        }
    }

    /// Hand every held start back to the coordinator, in arrival order.
    pub(super) fn answer(&self) {
        for (payload_id, message) in self.held.lock_recover().take().unwrap_or_default() {
            crate::runner::coordinator_send(crate::runner::RunnerMessage::HandleCommandAction(
                payload_id,
                message,
                HashMap::new(),
            ));
        }
    }
}

/// The leave of `relay` the host has not answered yet, if one is pending.
pub fn pending_leave(relay: &str) -> Option<Arc<PendingLeave>> {
    PENDING.lock_recover().get(relay).cloned()
}

/// The leave pending under `relay`, begun now when none is: a stop that waits
/// for the relay to drain and the host's leave after it share one, so a start
/// of the same id is held from the first until the last is done.
pub(crate) fn begin_leave(relay: &str) -> Arc<PendingLeave> {
    let mut pending = PENDING.lock_recover();
    Arc::clone(
        pending
            .entry(relay.to_string())
            .or_insert_with(|| Arc::new(PendingLeave::new())),
    )
}

/// End the leave pending under `relay`, if any, running the starts it held.
pub(crate) fn end_leave(relay: &str) {
    let ended = PENDING.lock_recover().remove(relay);
    if let Some(pending) = ended {
        pending.answer();
    }
}

/// Tell the host an evicted Cell left, and wait for its answer: the caller
/// holds the Cell's gates and removes its key only after, so a Cell minted
/// again under the key is never handed work before the host dropped the old
/// state. A leave no host has taken when the relay stops is withdrawn: the
/// relay's own leave, sent once its sweeps are done, covers the Cell. MUST
/// run under the cell gate.
pub fn cell_left_locked(slot: &Arc<CellSlot>) {
    let leave = schema::ActivationKind::Leave { whole_relay: false };
    let covered = (
        &*slot.relay_stopped,
        schema::ActivationOutcome::StateApplied,
    );
    let wait = crate::poll_core::HOST_COMMAND_TIMEOUT;
    let outcome = crate::poll_core::global()
        .host_command_within(slot, leave, wait, Some(covered))
        .unwrap_or_else(|err| {
            schema::ActivationOutcome::Failed(schema::WorkError {
                message: err.to_string(),
                non_retryable: false,
            })
        });
    leave_answered(&slot.instance_id, outcome);
}

/// Tell the host a relay left this runner (stopped, finished or migrated
/// away), once no activation handed out for it is still running: a host write
/// landing after the leave would stay for good. Until the host answers, the
/// leave is pending under the relay's id. A typed relay's state went with its
/// slots, so the leave its stop began ends here.
pub fn relay_left(instance: &RelayInstance) {
    let topology = &instance.topology;
    let relay = instance.instance_id.to_string();
    if !topology.tasklets.iter().any(|t| t.host_facts.is_some()) {
        return end_leave(&relay);
    }
    let pending = begin_leave(&relay);
    let target = schema::CellRef {
        relay: relay.clone(),
        tasklet: 0,
        cell_key: None,
    };
    let reply = CommandReply::new(move |outcome| {
        leave_answered(&relay, outcome);
        let mut pending_by_relay = PENDING.lock_recover();
        if pending_by_relay
            .get(&relay)
            .is_some_and(|p| Arc::ptr_eq(p, &pending))
        {
            pending_by_relay.remove(&relay);
        }
        drop(pending_by_relay);
        pending.answer();
    });
    let leave = EnginePayload::Leave {
        target,
        factory: topology.factory_id,
        reply,
    };
    let core = crate::poll_core::global();
    // The relay is stopped, so no slot is handed out again; one still running
    // is waited out off this thread.
    let running: Vec<Arc<CellSlot>> = instance
        .all_slots()
        .into_iter()
        .filter(|slot| {
            core.try_with_cell_exclusive(slot, Duration::ZERO, || ())
                .is_none()
        })
        .collect();
    if running.is_empty() {
        core.submit_direct(leave);
        return;
    }
    // Unspawned, the leave is dropped: its reply reports it refused and
    // releases any start held behind it.
    let _ = std::thread::Builder::new()
        .name("flezha-leave".to_string())
        .spawn(move || {
            for slot in &running {
                core.with_cell_exclusive(slot, || ());
            }
            core.submit_direct(leave);
        });
}

/// A refused leave is only reported: the host may still hold the state.
fn leave_answered(relay: &str, outcome: schema::ActivationOutcome) {
    if let schema::ActivationOutcome::Failed(err) = outcome {
        flz_transport::notice!(
            Warn,
            "the host refused a leave of relay {relay}; it may still hold the state: {}",
            err.message
        );
    }
}
