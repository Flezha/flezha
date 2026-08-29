//! Inbound `CompactEntity`: controller → SDK ask-for-snapshot.
//!
//! Locates the live `EntitySlot` addressed by `entity_id`, drains its
//! pending changes, serializes its `Self::State` with **postcard** (the codec
//! `entity/restore.rs` decodes — NOT the JSON migration serializer), and
//! ships it as an `entity_state_snapshot` data frame stamped with the seq the
//! snapshot already contains. It then answers `CompactEntityResponse` with
//! that seq and the body's byte size, so the controller can write the snapshot
//! file and truncate the WAL up to it. An `entity_id` matching no live Entity
//! is reported as an error rather than acked as a completed compaction.
//!
//! The snapshot's state and seq are captured under the same locks the dispatch
//! path holds (`slot.state` then `slot.flush_buffer`, in that order), so a
//! concurrent invocation can neither apply a change the snapshot misses nor
//! assign a seq the snapshot claims to cover.

use std::sync::Arc;

use prost::Message;

use super::{CommandAction, Reply};
use crate::entity::EntitySlot;
use crate::pb::{CompactEntity, CompactEntityResponse};
use crate::runner::RunnerNode;
use crate::state_sink::{send, ENTITY_STATE_SNAPSHOT_PAYLOAD_ID};
use crate::util::LockRecover;

/// Snapshot one Entity under its handout exclusivity (no activation
/// interleaves) and ship the frame AFTER the drained change frames — both
/// ride the ordered reliable sink, so the controller sees every record the
/// snapshot folds before the snapshot that truncates them. `(seq, size)`, or
/// `None` with the failure recorded.
fn compact_slot(slot: &Arc<EntitySlot>, errors: &mut Vec<String>) -> Option<(u64, u64)> {
    crate::poll_core::global().with_entity_exclusive(slot, || match snapshot_slot(slot) {
        Ok((frame, seq, size)) => {
            send(ENTITY_STATE_SNAPSHOT_PAYLOAD_ID, frame);
            Some((seq, size as u64))
        }
        Err(e) => {
            errors.push(format!("compact_entity: {e}"));
            None
        }
    })
}

/// The snapshot frame, seq and size of one Entity. MUST run under the entity's
/// handout exclusivity: a host-run Entity crosses a SnapshotRequest activation;
/// a typed one encodes in-process holding the slot locks in dispatch order
/// (state, then flush_buffer, then timers).
fn snapshot_slot(slot: &Arc<EntitySlot>) -> Result<(Vec<u8>, u64, usize), String> {
    if slot.dispatcher.host_facts().is_some() {
        return crate::host::snapshot_frame_crossing(slot).map_err(|e| e.to_string());
    }
    let mut state_guard = match slot.state.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    let mut buffer_guard = slot.flush_buffer.lock_recover();
    let timer_guard = slot.timers.lock_recover();
    slot.dispatcher
        .snapshot_frame(&mut state_guard, &mut buffer_guard, &timer_guard)
}

impl CommandAction for CompactEntity {
    fn handle(&self, runner: &mut RunnerNode, _message: &str, errors: &mut Vec<String>) -> Reply {
        let mut snapshot_seq = 0u64;
        let mut snapshot_bytes_size = 0u64;

        // A stream Entity lives in the stream registry; a workflow Entity in
        // its instance's stage slots — the same two places a restore looks.
        let slot =
            crate::command::restore::find_entity_slot(runner, &self.entity_id).or_else(|| {
                runner
                    .find_workflow_entity(&self.entity_id)
                    .map(|(_, slot)| slot)
            });

        match slot {
            Some(slot) => {
                if let Some((seq, size)) = compact_slot(&slot, errors) {
                    snapshot_seq = seq;
                    snapshot_bytes_size = size;
                }
            }
            None => errors
                .push("compact_entity: no live entity matches the addressed entity_id".to_string()),
        }

        let response = CompactEntityResponse {
            entity_id: self.entity_id.clone(),
            snapshot_seq,
            snapshot_bytes_size,
        };

        Reply {
            payload_id: "compact_entity",
            bytes: response.encode_to_vec(),
        }
    }
}
