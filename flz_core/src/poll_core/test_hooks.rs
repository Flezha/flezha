//! What tests reach of the poll core without handing out work that other
//! tests hold.

use std::sync::Arc;

use super::{EngineActivation, EnginePayload, PollCore};
use crate::cell::CellSlot;
use crate::schema::ActivationKind;

impl PollCore {
    /// What a poll pass does first, without handing out work: for a test
    /// that must not drain Cells other tests hold.
    pub(crate) fn promote_due_wakes(&self) {
        self.promote_due();
    }

    /// [`Self::promote_due_wakes`] with the wall and monotonic clocks reading
    /// `wall` and `mono`.
    pub(crate) fn promote_due_at(&self, wall: u64, mono: u64) {
        self.promote_due_by(&|lane| match lane {
            super::wake::Lane::Wall => wall,
            super::wake::Lane::Mono => mono,
        });
    }

    /// The first queued leave for `relay`; nothing else queued is touched.
    pub(crate) fn take_leave(&self, relay: &str) -> Option<EngineActivation> {
        self.take_direct(|act| match &act.payload {
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
        self.take_direct(|act| {
            matches!(&act.payload, EnginePayload::Command {
                slot,
                kind: ActivationKind::SnapshotRequest,
                ..
            } if Arc::ptr_eq(slot, cell))
        })
    }
}
