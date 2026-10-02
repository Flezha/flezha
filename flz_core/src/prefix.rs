//! An instance's processed prefix (`durable-input-log`): the highest input-log
//! offset `P` at or below which every input the instance took has been
//! processed and every record it produced is on the data stream. An input holds
//! `P` below it from the moment it enters a Cell's mailbox until the record it
//! settled into has been written; a Cell with nothing pending holds nothing.
//! The flusher reports `P` behind those records and never through the
//! reconnect ring, and the controller confirms it once they are fsynced.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::util::LockRecover;

/// The payload id of the marker; its frame is a Cell frame whose identity
/// names the instance and whose seq carries `P`.
pub const PROCESSED_PREFIX_PAYLOAD_ID: &str = "processed_prefix";

pub struct PrefixLedger {
    instance_id: Arc<str>,
    state: Mutex<Ledger>,
    /// Set by every hold, release and restore; a report skips a ledger that
    /// has not moved since the last one.
    moved: AtomicBool,
}

#[derive(Default)]
struct Ledger {
    /// Offset -> inputs of it still pending anywhere in the instance.
    pending: BTreeMap<u64, u32>,
    highest: Option<u64>,
    reported: Option<u64>,
    /// Set by a restore: the re-forward of what it discarded is still ahead
    /// of this offset on the wire, so nothing is reported until an input
    /// above it arrives.
    catch_up_above: Option<u64>,
}

/// One input's claim on its instance's prefix; dropping the last clone
/// releases it.
pub struct Hold {
    ledger: Arc<PrefixLedger>,
    offset: u64,
}

impl Drop for Hold {
    fn drop(&mut self) {
        self.ledger.release(self.offset);
    }
}

impl PrefixLedger {
    pub fn new(instance_id: Arc<str>) -> Arc<Self> {
        Arc::new(Self {
            instance_id,
            state: Mutex::new(Ledger::default()),
            moved: AtomicBool::new(false),
        })
    }

    /// Hold the prefix below `offset` for one input.
    pub fn hold(self: &Arc<Self>, offset: u64) -> Arc<Hold> {
        {
            let mut ledger = self.state.lock_recover();
            *ledger.pending.entry(offset).or_insert(0) += 1;
            ledger.highest = ledger.highest.max(Some(offset));
            if ledger.catch_up_above.is_some_and(|above| offset > above) {
                ledger.catch_up_above = None;
            }
        }
        self.moved.store(true, Ordering::Release);
        Arc::new(Hold {
            ledger: Arc::clone(self),
            offset,
        })
    }

    fn release(&self, offset: u64) {
        {
            let mut ledger = self.state.lock_recover();
            if let Some(count) = ledger.pending.get_mut(&offset) {
                *count -= 1;
                if *count == 0 {
                    ledger.pending.remove(&offset);
                }
            }
        }
        self.moved.store(true, Ordering::Release);
    }

    /// A Cell of this instance was restored: its in-memory progress went back
    /// to its durable records, and the controller re-forwards what that
    /// discarded. A lost frame the restore repaired no longer freezes it.
    pub(crate) fn restored(&self) {
        thaw(&self.instance_id);
        let mut ledger = self.state.lock_recover();
        ledger.catch_up_above = ledger.highest;
        self.moved.store(true, Ordering::Release);
    }

    /// The prefix to report now: moved since the last report, past the last
    /// one reported, and neither frozen nor waiting for a re-forward.
    fn candidate(&self) -> Option<u64> {
        if !self.moved.swap(false, Ordering::AcqRel) || frozen(&self.instance_id) {
            return None;
        }
        let ledger = self.state.lock_recover();
        if ledger.catch_up_above.is_some() {
            return None;
        }
        let prefix = match ledger.pending.keys().next() {
            Some(&low) => low.checked_sub(1)?,
            None => ledger.highest?,
        };
        (ledger.reported < Some(prefix)).then_some(prefix)
    }

    /// Send the marker when the prefix moved. A marker the wire refuses is
    /// dropped and the next pass reports again.
    pub(crate) fn report(&self) {
        let Some(prefix) = self.candidate() else {
            return;
        };
        let identity = crate::flusher::encode_cell_identity(&self.instance_id, "", "");
        let frame = crate::flusher::build_cell_frame(&identity, prefix, &[0u8; 16], &[]);
        if crate::state_sink::send_unheld(PROCESSED_PREFIX_PAYLOAD_ID, frame) {
            let mut ledger = self.state.lock_recover();
            ledger.reported = ledger.reported.max(Some(prefix));
        } else {
            self.moved.store(true, Ordering::Release);
        }
    }
}

/// Instances a record of which the reconnect ring evicted: that input's
/// record is lost, so no marker may cover it until a restore re-seats them.
static FROZEN: Mutex<Option<HashSet<String>>> = Mutex::new(None);
static ANY_FROZEN: AtomicBool = AtomicBool::new(false);

/// The ring evicted `frame`, a Cell record of some instance.
pub(crate) fn frame_evicted(frame: &[u8]) {
    let Some((instance_id, _, _)) = frame_instance(frame) else {
        return;
    };
    FROZEN
        .lock_recover()
        .get_or_insert_with(HashSet::new)
        .insert(instance_id);
    ANY_FROZEN.store(true, Ordering::Release);
}

fn frozen(instance_id: &str) -> bool {
    ANY_FROZEN.load(Ordering::Acquire)
        && FROZEN
            .lock_recover()
            .as_ref()
            .is_some_and(|set| set.contains(instance_id))
}

fn thaw(instance_id: &str) {
    if !ANY_FROZEN.load(Ordering::Acquire) {
        return;
    }
    let mut frozen = FROZEN.lock_recover();
    if let Some(set) = frozen.as_mut() {
        set.remove(instance_id);
        ANY_FROZEN.store(!set.is_empty(), Ordering::Release);
    }
}

/// The Cell identity a `build_cell_frame` frame opens with.
fn frame_instance(frame: &[u8]) -> Option<(String, String, String)> {
    let len = u16::from_be_bytes([*frame.first()?, *frame.get(1)?]) as usize;
    crate::flusher::decode_cell_identity(frame.get(2..2 + len)?)
}

#[cfg(test)]
#[path = "prefix_tests.rs"]
mod tests;
