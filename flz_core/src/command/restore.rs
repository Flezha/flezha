//! `RestoreInitState`: rebuild a live Cell's state from the shipped
//! snapshot and change log, then re-arm its `change_seq` to `terminal_seq +
//! 1` so the next frame is contiguous with the persisted history. With no
//! live slot the log is validated and its terminal seq acked.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use prost::Message;

use super::{CommandAction, Reply};
use crate::cell::CellSlot;
use crate::pb::{RestoreInitState, RestoreInitStateResponse};
use crate::restore::{iter_change_log_records, RestoreOutcome};
use crate::runner::RunnerNode;
use crate::util::LockRecover;

impl CommandAction for RestoreInitState {
    fn handle(&self, runner: &mut RunnerNode, _message: &str, errors: &mut Vec<String>) -> Reply {
        let instance = runner.instance_of_cell(&self.cell_id);
        let slot = instance
            .as_ref()
            .and_then(|instance| instance.slot_by_identity(&self.cell_id));
        let outcome = match &slot {
            Some(slot) => Some(restore_into_slot(slot, self, errors)),
            None => self.ack_without_slot(errors),
        };
        // A bounded operator's gate counts every frame its bind shipped — applied,
        // refused by the cap, or naming a tasklet the class dropped — so it
        // never stalls on one, and takes the saga markers the controller
        // persisted: an undo in flight finishes (intent), a Cell already
        // undone is skipped (done).
        if let Some(instance) = &instance {
            crate::bounded::restore_landed(
                instance,
                &self.cell_id,
                self.has_compensation_intent,
                self.has_compensation_done,
            );
        }
        let Some(outcome) = outcome else {
            return Reply {
                payload_id: "restore_init_state",
                bytes: Vec::new(),
            };
        };

        let response = RestoreInitStateResponse {
            cell_id: self.cell_id.clone(),
            terminal_seq: outcome.terminal_seq,
            migrated: outcome.migrated,
        };

        Reply {
            payload_id: "restore_init_state",
            bytes: response.encode_to_vec(),
        }
    }
}

impl RestoreInitState {
    /// No live slot: validate the log and ack its terminal seq.
    fn ack_without_slot(&self, errors: &mut Vec<String>) -> Option<RestoreOutcome> {
        let mut terminal = 0u64;
        for record in iter_change_log_records(&self.change_log_bytes) {
            match record {
                Ok(r) => terminal = terminal.max(r.seq),
                Err(e) => {
                    errors.push(format!("change_log decode error: {e}"));
                    return None;
                }
            }
        }
        Some(RestoreOutcome {
            terminal_seq: terminal.max(self.terminal_seq),
            migrated: false,
        })
    }
}

/// Rebuild one Cell's state and re-arm its `change_seq`, through the host
/// crossing or the typed fold; a zeroed outcome on failure.
fn restore_into_slot(
    slot: &Arc<CellSlot>,
    cmd: &RestoreInitState,
    errors: &mut Vec<String>,
) -> RestoreOutcome {
    // The fold runs under the cell's handout exclusivity: no input
    // activation is handed out before it completes.
    crate::poll_core::global().with_cell_exclusive(slot, || {
        let folded = if slot.dispatcher.host_facts().is_some() {
            crate::host::restore_fold_exclusive(
                slot,
                &cmd.snapshot_header,
                &cmd.snapshot_bytes,
                &cmd.change_log_bytes,
                errors,
            )
        } else {
            restore_typed_exclusive(slot, cmd, errors)
        };
        match folded {
            Some(outcome) => {
                // The change log alone undercounts a snapshot-only restore:
                // the controller's terminal covers the snapshot too.
                let t = outcome.terminal_seq.max(cmd.terminal_seq);
                reseat_restored(slot, t, &cmd.change_log_bytes);
                RestoreOutcome {
                    terminal_seq: t,
                    migrated: outcome.migrated,
                }
            }
            None => RestoreOutcome::default(),
        }
    })
}

fn restore_typed_exclusive(
    slot: &Arc<CellSlot>,
    cmd: &RestoreInitState,
    errors: &mut Vec<String>,
) -> Option<RestoreOutcome> {
    let mut guard = match slot.state.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    let mut timers = slot.timers.lock_recover();
    match slot.dispatcher.restore_state(
        &mut guard,
        &mut timers,
        &cmd.snapshot_header,
        &cmd.snapshot_bytes,
        &cmd.change_log_bytes,
        slot.cell_key.as_deref(),
    ) {
        Ok(outcome) => Some(outcome),
        Err(e) => {
            errors.push(format!("restore_init_state: apply failed: {e}"));
            None
        }
    }
}

fn reseat_restored(slot: &Arc<CellSlot>, terminal: u64, change_log_bytes: &[u8]) {
    slot.change_seq.store(terminal + 1, Ordering::Release);

    // A restore re-seats the Cell on a different past — the crash's,
    // or (for a correction) a deliberately rewound one. What the buffer has
    // not shipped (an open checkpoint interval included) is not in the
    // restored state and the re-forwarded inputs produce it again; a
    // half-filled replay buffer belongs to a slice that was abandoned. Both
    // go, on every class.
    slot.discard_unshipped();
    slot.discard_replay_slice();
    // The mailbox goes only where the controller re-forwards it; the
    // slot owns that rule.
    let discarded = slot.discard_queued_input_on_restore();
    if discarded > 0 {
        eprintln!(
            "[flezha] restore discarded {discarded} queued input(s) for cell {} \
             — the controller re-forwards them from the durable input log",
            slot.dispatcher.cell_label()
        );
    }

    let cursors = restore_cursors(change_log_bytes);
    // The floor goes back to the offset the restored records carry, so the
    // re-forward runs exactly what the restored state has not seen.
    slot.consumed_floor.store(
        cursors.consumed_offset.map_or(-1, |floor| floor as i64),
        Ordering::Release,
    );
    if let Some(prefix) = slot.instance.upgrade().and_then(|i| i.prefix.clone()) {
        prefix.restored();
    }
    // Correction: rebuild the event-time high-water, so a resumed
    // Cell neither mistakes its own history for a flood of late
    // inputs (high-water too low) nor misses a genuinely late one.
    slot.event_time_high_water.store(
        cursors.event_time_high_water.map_or(-1, |t| t as i64),
        Ordering::Release,
    );
    // Pending timers came back with the state; the processing-time ones
    // need an entry on the wheel again. Anything already past its
    // deadline fires on the next pass, ascending. Event-time timers
    // need nothing — the restored high-water is their trigger.
    crate::timer::rearm_restored(slot);
}

#[derive(Debug, Default, PartialEq, Eq)]
struct RestoreCursors {
    /// Highest `consumed_offset` across the effect records.
    consumed_offset: Option<u64>,
    /// Highest `event_time`; the retained records always cover the correction
    /// window, since compaction may not truncate inside it.
    event_time_high_water: Option<u64>,
}

/// The cursors a restored Cell resumes from, off its effect records.
fn restore_cursors(change_log_bytes: &[u8]) -> RestoreCursors {
    let mut cursors = RestoreCursors::default();
    for record in iter_change_log_records(change_log_bytes) {
        let Ok(record) = record else { break };
        if record.header.get(9) == Some(&crate::header::FORMAT_ID_EFFECT_RECORD) {
            if let Ok(body) =
                postcard::from_bytes::<crate::flusher::EffectRecordBody>(record.change_bytes)
            {
                cursors.consumed_offset = cursors.consumed_offset.max(body.consumed_offset);
                cursors.event_time_high_water =
                    cursors.event_time_high_water.max(Some(body.event_time));
            }
        }
    }
    cursors
}
