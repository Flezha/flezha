//! Inbound `CompactCell`: controller → SDK ask-for-snapshot.
//!
//! Locates the live `CellSlot` addressed by `cell_id`, drains its
//! pending changes, encodes its state (a typed Cell's with **postcard**, the
//! codec its restore decodes; a host-run Cell's by its host), and
//! ships it as a `cell_state_snapshot` data frame stamped with the seq the
//! snapshot already contains. It then answers `CompactCellResponse` with
//! that seq and the body's byte size, so the controller can write the snapshot
//! file and truncate the WAL up to it. A `cell_id` matching no live Cell
//! is reported as an error rather than acked as a completed compaction.
//!
//! The snapshot's state and seq are captured under the same locks the dispatch
//! path holds (`slot.state` then `slot.flush_buffer`, in that order), so a
//! concurrent invocation can neither apply a change the snapshot misses nor
//! assign a seq the snapshot claims to cover.

use std::sync::Arc;

use prost::Message;

use super::{CommandAction, Reply};
use crate::cell::CellSlot;
use crate::pb::{CompactCell, CompactCellResponse};
use crate::runner::RunnerNode;
use crate::state_sink::{send, CELL_STATE_SNAPSHOT_PAYLOAD_ID};

/// Snapshot one Cell under its handout exclusivity (no activation
/// interleaves) and ship the frame AFTER the drained change frames — both
/// ride the ordered reliable sink, so the controller sees every record the
/// snapshot folds before the snapshot that truncates them. `(seq, size)`, or
/// `None` with the failure recorded.
fn compact_slot(slot: &Arc<CellSlot>, errors: &mut Vec<crate::Error>) -> Option<(u64, u64)> {
    crate::poll_core::global().with_cell_exclusive(slot, || {
        match slot.dispatcher.snapshot_frame(slot) {
            Ok((frame, seq, size)) => {
                send(CELL_STATE_SNAPSHOT_PAYLOAD_ID, frame);
                Some((seq, size as u64))
            }
            Err(e) => {
                errors.push(e);
                None
            }
        }
    })
}

impl CommandAction for CompactCell {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        errors: &mut Vec<crate::Error>,
    ) -> Reply {
        let mut snapshot_seq = 0u64;
        let mut snapshot_bytes_size = 0u64;

        // The same lookup a restore takes, for a Cell of either kind.
        let slot = runner.find_cell(&self.cell_id).map(|(_, slot)| slot);

        match slot {
            Some(slot) => {
                if let Some((seq, size)) = compact_slot(&slot, errors) {
                    snapshot_seq = seq;
                    snapshot_bytes_size = size;
                }
            }
            None => errors.push(crate::Error::State(
                crate::Code::CellNotLive
                    .with([("cell_id", crate::util::hex(&self.cell_id).into())]),
            )),
        }

        let response = CompactCellResponse {
            cell_id: self.cell_id.clone(),
            snapshot_seq,
            snapshot_bytes_size,
        };

        Reply {
            payload_id: "compact_cell",
            bytes: response.encode_to_vec(),
        }
    }
}
