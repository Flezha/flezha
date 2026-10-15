//! What tests reach of the poll core without handing out work that other
//! tests hold.

use super::{EngineActivation, EnginePayload, PollCore};
use crate::util::LockRecover;

impl PollCore {
    /// What a poll pass does first, without handing out work: for a test
    /// that must not drain Cells other tests hold.
    pub(crate) fn promote_due_wakes(&self) {
        self.promote_due();
    }

    /// The first queued leave for `relay`; nothing else queued is touched.
    pub(crate) fn take_leave(&self, relay: &str) -> Option<EngineActivation> {
        use crate::schema::ActivationKind::Leave;
        let mut direct = self.direct.lock_recover();
        let at = direct.iter().position(|act| match &act.payload {
            EnginePayload::Leave { target, .. } => target.relay == relay,
            EnginePayload::Command {
                slot,
                kind: Leave { .. },
                ..
            } => *slot.instance_id == *relay,
            _ => false,
        })?;
        direct.remove(at)
    }
}
