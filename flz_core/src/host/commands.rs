//! The command crossings of a host-run Cell: restore fold, compaction
//! snapshot, live read / migration serialize and apply-state. Each runs under
//! the cell gate and crosses one activation the host answers. The
//! invariant: the schema-version decision is the core's — a host receives
//! only bodies it can decode at its own version or migrate from a named one.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use super::cell::BytesBuffer;
use crate::cell::CellSlot;
use crate::error::{Code, Error};
use crate::flusher::FlushableBuffer;
use crate::header::{HeaderTemplate, RestoreError, SchemaDecision};
use crate::schema::{self, TimerOp};
use crate::timer::PendingTimers;
use crate::util::LockRecover;

/// A refusal from the core is the restore's failure, reported by name — the
/// host never sees the record.
fn note_error<T>(result: Result<T, RestoreError>, errors: &mut Vec<Error>) -> Option<T> {
    result.map_err(|e| errors.push(restore_failed(e))).ok()
}

/// A restore that could not complete, with the refusal's own words as the
/// reason.
pub fn restore_failed(reason: impl std::fmt::Display) -> Error {
    Error::State(Code::RestoreFailed.with([("reason", reason.to_string().into())]))
}

/// A host's own failure of the `command` activation.
fn host_failed(command: &str, reason: String) -> Error {
    Error::HostCrossing(
        Code::HostCommandFailed.with([("command", command.into()), ("reason", reason.into())]),
    )
}

/// A host answer of another kind than the `command` activation asks for.
fn wrong_outcome(command: &str) -> Error {
    Error::HostCrossing(Code::HostWrongOutcome.with([("command", command.into())]))
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

/// Restore a host-run Cell: split the persisted records into what the core
/// owns (cursors, timer ops, the schema decision — resolved here) and what only
/// the host can decode (its own change bodies — crossed as a
/// [`schema::RestoreFold`] activation). Runs under the cell gate; returns the
/// terminal WAL seq and whether anything migrated, or `None` with the failure
/// recorded in `errors`.
pub fn restore_fold_exclusive(
    slot: &Arc<CellSlot>,
    snapshot_header: &[u8],
    snapshot_bytes: &[u8],
    change_log_bytes: &[u8],
    errors: &mut Vec<Error>,
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
            errors.push(host_failed("restore_fold", err.message));
            None
        }
        Ok(_) => {
            errors.push(wrong_outcome("restore_fold"));
            None
        }
        Err(e) => {
            errors.push(e);
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
    errors: &mut Vec<Error>,
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

/// Compaction snapshot for a host-run Cell: drain pending changes, read
/// the seq, re-state the pending timer set (so truncation cannot eat it),
/// then cross a [`schema::ActivationKind::SnapshotRequest`] and frame the
/// host-encoded state. Runs under the cell gate.
pub fn snapshot_frame_crossing(slot: &Arc<CellSlot>) -> Result<(Vec<u8>, u64, usize), Error> {
    let facts = slot
        .dispatcher
        .host_facts()
        .ok_or_else(|| Error::HostCrossing(Code::NotHostTasklet.with([])))?;
    let header = facts.header();
    let flush = |buffer: &mut Box<dyn FlushableBuffer>| {
        crate::flusher::flush_one(
            &mut **buffer,
            &slot.cell_identity,
            &slot.change_seq,
            &header,
            slot.persistence_enabled,
        );
    };
    flush(&mut slot.flush_buffer.lock_recover());
    let seq = slot.change_seq.load(Ordering::Acquire).saturating_sub(1);
    // The snapshot body is the host's State alone; what the coming truncation
    // would delete is re-stated AFTER the seq is read — landing at
    // `snapshot_seq + 1`, which `truncate_up_to` keeps.
    let register_ops = crate::timer::restated(slot, &slot.timers.lock_recover()).as_register_ops();
    let mut buffer = slot.flush_buffer.lock_recover();
    if let Some(bytes) = buffer.as_any_mut().downcast_mut::<BytesBuffer>() {
        if bytes.restate_above_snapshot(register_ops) {
            flush(&mut buffer);
        }
    }
    drop(buffer);
    let state = snapshot_state(
        crate::poll_core::global().host_command(slot, schema::ActivationKind::SnapshotRequest)?,
    )?;
    let size = state.len();
    let frame =
        crate::flusher::build_cell_frame(&slot.cell_identity, seq, &header.stamp(&state), &state);
    Ok((frame, seq, size))
}

/// The host's snapshot body, or the refusal its answer carries.
fn snapshot_state(outcome: schema::ActivationOutcome) -> Result<Vec<u8>, Error> {
    match outcome {
        schema::ActivationOutcome::Snapshot { state } => Ok(state),
        schema::ActivationOutcome::Failed(err) => Err(host_failed("snapshot_request", err.message)),
        _ => Err(wrong_outcome("snapshot_request")),
    }
}

/// Live-read / migration serialize for a host-run Cell, waiting `wait` for
/// the host's answer. MUST run under the cell gate (the caller's
/// `with_cell_exclusive` closure).
pub fn state_json_locked(
    slot: &Arc<CellSlot>,
    migration: bool,
    wait: Duration,
) -> Result<(serde_json::Value, u64), Error> {
    let (kind, command) = if migration {
        (
            schema::ActivationKind::MigrationSerialize,
            "migration_serialize",
        )
    } else {
        (schema::ActivationKind::LiveStateRead, "live_state_read")
    };
    let json = match crate::poll_core::global().host_command_within(slot, kind, wait)? {
        schema::ActivationOutcome::StateJson { json }
        | schema::ActivationOutcome::MigrationState { json } => json,
        schema::ActivationOutcome::Failed(err) => return Err(host_failed(command, err.message)),
        _ => return Err(wrong_outcome(command)),
    };
    let value = serde_json::from_slice(&json).map_err(|e| {
        Error::HostCrossing(Code::HostStateUnreadable.with([("reason", e.to_string().into())]))
    })?;
    let seq = slot.change_seq.load(Ordering::Relaxed);
    Ok((value, seq))
}

/// Apply-state (migration in / init_values) for a host-run Cell. MUST run
/// under the cell gate.
pub fn apply_state_locked(slot: &Arc<CellSlot>, value: &serde_json::Value) -> Result<(), Error> {
    let state_json = serde_json::to_vec(value).map_err(|e| {
        Error::HostCrossing(Code::StateUnencodable.with([("reason", e.to_string().into())]))
    })?;
    match crate::poll_core::global()
        .host_command(slot, schema::ActivationKind::ApplyState { state_json })?
    {
        schema::ActivationOutcome::StateApplied => Ok(()),
        schema::ActivationOutcome::Failed(err) => Err(host_failed("apply_state", err.message)),
        _ => Err(wrong_outcome("apply_state")),
    }
}
