//! What tests reach of the poll core without handing out work that other
//! tests hold.

use std::sync::Arc;

use super::{EngineActivation, EnginePayload, PollCore};
use crate::cell::CellSlot;
use crate::schema::ActivationKind;
use crate::util::LockRecover;

impl PollCore {
    /// What a poll pass does first, without handing out work: for a test
    /// that must not drain Cells other tests hold.
    pub(crate) fn promote_due_wakes(&self) {
        self.promote_due();
    }

    /// The first queued leave for `relay`; nothing else queued is touched.
    pub(crate) fn take_leave(&self, relay: &str) -> Option<EngineActivation> {
        self.take_direct(|payload| match payload {
            EnginePayload::Leave { target, .. } => target.relay == relay,
            EnginePayload::Command {
                slot,
                kind: ActivationKind::Leave { .. },
                ..
            } => *slot.instance_id == *relay,
            _ => false,
        })
    }

    /// The first queued snapshot request for `cell`; nothing else queued is
    /// touched.
    pub(crate) fn take_snapshot_request(&self, cell: &Arc<CellSlot>) -> Option<EngineActivation> {
        self.take_direct(|payload| {
            matches!(payload, EnginePayload::Command {
                slot,
                kind: ActivationKind::SnapshotRequest,
                ..
            } if Arc::ptr_eq(slot, cell))
        })
    }

    fn take_direct(&self, pick: impl Fn(&EnginePayload) -> bool) -> Option<EngineActivation> {
        let mut direct = self.direct.lock_recover();
        let at = direct.iter().position(|act| pick(&act.payload))?;
        direct.remove(at)
    }
}
