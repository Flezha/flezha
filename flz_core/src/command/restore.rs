//! `RestoreInitState`: rebuild a live Cell's state from the shipped
//! snapshot and change log, then re-arm its `change_seq` to `terminal_seq +
//! 1` so the next frame is contiguous with the persisted history. With no
//! live slot the log is validated and its terminal seq acked.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use prost::Message;

use super::{CommandAction, Reply};
use crate::cell::CellSlot;
use crate::operative::instance::OperativeInstance;
use crate::pb::{RestoreInitState, RestoreInitStateResponse};
use crate::restore::{iter_change_log_records, RestoreOutcome};
use crate::runner::RunnerNode;
use crate::util::LockRecover;

impl CommandAction for RestoreInitState {
    fn handle(&self, runner: &mut RunnerNode, _message: &str, errors: &mut Vec<String>) -> Reply {
        let outcome = if let Some((instance, slot)) = find_cell_slot(runner, &self.cell_id) {
            let outcome = restore_into_slot(&slot, self, errors);
            // A workflow Cell's restore counts toward the run's gate whether or
            // not it applied, so the gate never stalls on a corrupt record, and
            // carries the saga markers the controller persisted: an undo in
            // flight finishes (intent), a Cell already undone is skipped (done).
            crate::workflow::restore_landed(
                &instance,
                &self.cell_id,
                self.has_compensation_intent,
                self.has_compensation_done,
            );
            outcome
        } else {
            match self.ack_without_slot(errors) {
                Some(outcome) => outcome,
                None => {
                    return Reply {
                        payload_id: "restore_init_state",
                        bytes: Vec::new(),
                    }
                }
            }
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
    // or (for a correction) a deliberately rewound one. An open
    // checkpoint interval's changes were never acknowledged, so the
    // restored state does not contain them and the re-forwarded inputs
    // produce them again; a half-filled replay buffer belongs to a
    // slice that was abandoned. Both go, on every class.
    slot.discard_staged_interval();
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
    // Effectively-once: remember the highest input-log offset the
    // restored effect records durably consumed, so the crash-replay
    // re-forward skips already-applied inputs (`drain_cell`).
    if let Some(floor) = cursors.consumed_offset {
        slot.consumed_floor.store(floor as i64, Ordering::Release);
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

/// The live slot whose wire identity equals `cell_id`, cloned out so the
/// registry locks are released before the rebuild.
/// A live Cell of either kind by wire identity, with its instance.
pub fn find_cell_slot(
    runner: &RunnerNode,
    cell_id: &[u8],
) -> Option<(Arc<OperativeInstance>, Arc<CellSlot>)> {
    runner
        .find_cell(cell_id)
        .or_else(|| find_dynamic_cell_slot(runner, cell_id))
}

/// A dynamic Cell has no slot until its first input; a restore names one
/// the controller has state for, so it is materialized here rather than
/// cold-started on top of its own WAL. An evicted Cell never gets one.
fn find_dynamic_cell_slot(
    runner: &RunnerNode,
    cell_id: &[u8],
) -> Option<(Arc<OperativeInstance>, Arc<CellSlot>)> {
    let (instance_id, tasklet_name, cell_key) = crate::flusher::decode_cell_identity(cell_id)?;
    let instance = runner.get_operative(&instance_id)?;
    let tasklet_id = *instance
        .topology
        .tasklet_name_to_id
        .get(tasklet_name.as_str())?;

    let slot = instance
        .dynamic_registries
        .get(tasklet_id as usize)?
        .as_ref()?
        .get_or_create(&cell_key)
        .ok()?;
    Some((instance, slot))
}
