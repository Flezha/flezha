//! The command crossings of a host-run Entity: restore fold, compaction
//! snapshot, live read / migration serialize and apply-state. Each runs under
//! the entity gate and crosses one activation the host answers. The
//! invariant: the schema-version decision is the core's — a host receives
//! only bodies it can decode at its own version or migrate from a named one.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::entity::BytesBuffer;
use crate::entity::EntitySlot;
use crate::flusher::FlushableBuffer;
use crate::header::{HeaderTemplate, RestoreError, SchemaDecision};
use crate::schema::{self, TimerOp};
use crate::timer::PendingTimers;
use crate::util::LockRecover;

/// A refusal from the core is the restore's failure, reported by name — the
/// host never sees the record.
fn note_error<T>(result: Result<T, RestoreError>, errors: &mut Vec<String>) -> Option<T> {
    result
        .map_err(|e| errors.push(format!("restore_init_state: {e}")))
        .ok()
}

/// The host's sink: change bodies are collected verbatim for the crossing,
/// tagged with the version the host must migrate them from. Timers are the
/// core's own concern and fold here.
#[derive(Default)]
struct HostSink {
    changes: Vec<schema::RestoreBody>,
    timers: PendingTimers,
}

impl crate::restore::ChangeLogSink for HostSink {
    fn change(&mut self, decision: SchemaDecision, bytes: &[u8]) -> Result<(), RestoreError> {
        self.changes.push(schema::RestoreBody {
            bytes: bytes.to_vec(),
            from_version: from_version(decision),
        });
        Ok(())
    }

    fn timers(&mut self, ops: &[TimerOp]) {
        for op in ops {
            self.timers.apply(op);
        }
    }
}

/// The version tag a body crosses with: absent when the host decodes it at its
/// own version.
fn from_version(decision: SchemaDecision) -> Option<u32> {
    match decision {
        SchemaDecision::Decode => None,
        SchemaDecision::Migrate { from_version } => Some(from_version),
    }
}

/// Restore a host-run Entity: split the persisted records into what the core
/// owns (cursors, timer ops, the schema decision — resolved here) and what only
/// the host can decode (its own change bodies — crossed as a
/// [`schema::RestoreFold`] activation). Runs under the entity gate; returns the
/// terminal WAL seq and whether anything migrated, or `None` with the failure
/// recorded in `errors`.
pub fn restore_fold_exclusive(
    slot: &Arc<EntitySlot>,
    snapshot_header: &[u8],
    snapshot_bytes: &[u8],
    change_log_bytes: &[u8],
    errors: &mut Vec<String>,
) -> Option<crate::restore::RestoreOutcome> {
    let facts = slot.dispatcher.host_facts()?;
    let expected = facts.header();
    let min_readable = facts.min_readable_schema_version;

    let mut sink = HostSink::default();
    let summary = note_error(
        crate::restore::fold_change_log(change_log_bytes, &expected, min_readable, &mut sink),
        errors,
    )?;
    let (snapshot, snapshot_migrated) = resolve_snapshot(
        &expected,
        min_readable,
        snapshot_header,
        snapshot_bytes,
        errors,
    )?;
    let migrated = summary.migrated | snapshot_migrated;

    let fold = schema::RestoreFold {
        snapshot,
        changes: sink.changes,
    };
    match crate::poll_core::global().host_command(slot, schema::ActivationKind::RestoreFold(fold)) {
        Ok(schema::ActivationOutcome::Restored) => {
            *slot.timers.lock_recover() = sink.timers;
            Some(crate::restore::RestoreOutcome {
                terminal_seq: summary.terminal_seq,
                migrated,
            })
        }
        Ok(schema::ActivationOutcome::Failed(err)) => {
            errors.push(format!(
                "restore_init_state: host fold failed: {}",
                err.message
            ));
            None
        }
        Ok(_) => {
            errors.push("restore_init_state: host answered with a non-restore outcome".to_string());
            None
        }
        Err(e) => {
            errors.push(format!("restore_init_state: {e}"));
            None
        }
    }
}

/// The snapshot body the host folds first, tagged with its version decision,
/// and whether that decision migrates; `None` after a refusal is recorded.
fn resolve_snapshot(
    expected: &HeaderTemplate,
    min_readable: u32,
    snapshot_header: &[u8],
    snapshot_bytes: &[u8],
    errors: &mut Vec<String>,
) -> Option<(Option<schema::RestoreBody>, bool)> {
    if snapshot_bytes.is_empty() {
        return Some((None, false));
    }
    let decision = note_error(
        crate::header::decode_header(snapshot_header)
            .and_then(|h| crate::header::resolve_state(&h, expected, min_readable)),
        errors,
    )?;
    let body = schema::RestoreBody {
        bytes: snapshot_bytes.to_vec(),
        from_version: from_version(decision),
    };
    Some((Some(body), decision.migrating()))
}

/// Compaction snapshot for a host-run Entity: drain pending changes, read
/// the seq, re-state the pending timer set (so truncation cannot eat it),
/// then cross a [`schema::ActivationKind::SnapshotRequest`] and frame the
/// host-encoded state. Runs under the entity gate.
pub fn snapshot_frame_crossing(slot: &Arc<EntitySlot>) -> Result<(Vec<u8>, u64, usize), String> {
    let facts = slot
        .dispatcher
        .host_facts()
        .ok_or("snapshot_frame_crossing: not a host-run procedure")?;
    let header = facts.header();
    let flush = |buffer: &mut Box<dyn FlushableBuffer>| {
        crate::flusher::flush_one(
            &mut **buffer,
            &slot.entity_identity,
            &slot.change_seq,
            &header,
            slot.persistence_enabled,
        );
    };
    flush(&mut slot.flush_buffer.lock_recover());
    let seq = slot.change_seq.load(Ordering::Acquire).saturating_sub(1);
    // The snapshot body is the host's State alone; the timer ops the coming
    // truncation would delete are re-stated as a full set AFTER the seq is
    // read — landing at `snapshot_seq + 1`, which `truncate_up_to` keeps.
    let register_ops = slot.timers.lock_recover().as_register_ops();
    if !register_ops.is_empty() {
        let mut buffer = slot.flush_buffer.lock_recover();
        if let Some(bytes) = buffer.as_any_mut().downcast_mut::<BytesBuffer>() {
            bytes.timer_ops = register_ops;
        }
        flush(&mut buffer);
    }
    let state = match crate::poll_core::global()
        .host_command(slot, schema::ActivationKind::SnapshotRequest)?
    {
        schema::ActivationOutcome::Snapshot { state } => state,
        schema::ActivationOutcome::Failed(err) => {
            return Err(format!("host snapshot failed: {}", err.message))
        }
        _ => return Err("host answered with a non-snapshot outcome".to_string()),
    };
    let size = state.len();
    let frame = crate::flusher::build_entity_frame(
        &slot.entity_identity,
        seq,
        &header.stamp(&state),
        &state,
    );
    Ok((frame, seq, size))
}

/// Live-read / migration serialize for a host-run Entity. MUST run under the
/// entity gate (the caller's `with_entity_exclusive` closure).
pub fn state_json_locked(
    slot: &Arc<EntitySlot>,
    migration: bool,
) -> Result<(serde_json::Value, u64), String> {
    let kind = if migration {
        schema::ActivationKind::MigrationSerialize
    } else {
        schema::ActivationKind::LiveStateRead
    };
    let json = match crate::poll_core::global().host_command(slot, kind)? {
        schema::ActivationOutcome::StateJson { json }
        | schema::ActivationOutcome::MigrationState { json } => json,
        schema::ActivationOutcome::Failed(err) => return Err(err.message),
        _ => return Err("host answered with a non-state outcome".to_string()),
    };
    let value = serde_json::from_slice(&json)
        .map_err(|e| format!("host state JSON failed to parse: {e}"))?;
    let seq = slot.change_seq.load(Ordering::Relaxed);
    Ok((value, seq))
}

/// Apply-state (migration in / init_values) for a host-run Entity. MUST run
/// under the entity gate.
pub fn apply_state_locked(slot: &Arc<EntitySlot>, value: &serde_json::Value) -> Result<(), String> {
    let state_json = serde_json::to_vec(value).map_err(|e| e.to_string())?;
    match crate::poll_core::global()
        .host_command(slot, schema::ActivationKind::ApplyState { state_json })?
    {
        schema::ActivationOutcome::StateApplied => Ok(()),
        schema::ActivationOutcome::Failed(err) => Err(err.message),
        _ => Err("host answered with a non-apply outcome".to_string()),
    }
}
