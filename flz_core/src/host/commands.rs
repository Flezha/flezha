//! The command crossings of a host-run Cell: restore fold, compaction
//! snapshot, state read (live or for a migration) and apply-state. Each runs under
//! the cell gate and crosses one activation the host answers. The
//! invariant: the schema-version decision is the core's — a host receives
//! only bodies it can decode at its own version or migrate from a named one.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::cell::CellSlot;
use crate::error::{Code, Error};
use crate::flusher::FlushableBuffer;
use crate::header::{HeaderTemplate, RestoreError, SchemaDecision};
use crate::restore::{RestoreInitPayload, RestoreOutcome};
use crate::schema::{self, TimerOp};
use crate::timer::PendingTimers;
use crate::util::LockRecover;

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

/// The host's sink: the snapshot and change bodies are collected verbatim for
/// the crossing, tagged with the version the host must migrate them from.
/// Timers are the core's own concern and fold here.
#[derive(Default)]
struct HostSink {
    snapshot: Option<schema::RestoreBody>,
    changes: Vec<schema::RestoreBody>,
    timers: PendingTimers,
}

impl crate::restore::ChangeLogSink for HostSink {
    fn snapshot(&mut self, decision: SchemaDecision, bytes: &[u8]) -> Result<(), RestoreError> {
        self.snapshot = Some(restore_body(decision, bytes));
        Ok(())
    }

    fn change(&mut self, decision: SchemaDecision, bytes: &[u8]) -> Result<(), RestoreError> {
        self.changes.push(restore_body(decision, bytes));
        Ok(())
    }

    fn timers(&mut self, ops: &[TimerOp]) {
        for op in ops {
            self.timers.apply(op);
        }
    }
}

/// A body as it crosses: no version tag when the host decodes it at its own.
fn restore_body(decision: SchemaDecision, bytes: &[u8]) -> schema::RestoreBody {
    schema::RestoreBody {
        bytes: bytes.to_vec(),
        from_version: match decision {
            SchemaDecision::Decode => None,
            SchemaDecision::Migrate { from_version } => Some(from_version),
        },
    }
}

fn host_facts(slot: &CellSlot) -> Result<&crate::host::HostTaskletFacts, Error> {
    slot.dispatcher
        .host_facts()
        .ok_or_else(|| Error::HostCrossing(Code::NotHostTasklet.with([])))
}

/// Restore a host-run Cell: the core resolves what it owns (cursors, timer
/// ops, the schema decision) and crosses what only the host can decode, its
/// own bodies, as a [`schema::RestoreFold`] activation. Runs under the cell
/// gate.
pub(super) fn restore_fold_exclusive(
    slot: &Arc<CellSlot>,
    payload: &RestoreInitPayload<'_>,
) -> Result<RestoreOutcome, Error> {
    let facts = host_facts(slot)?;
    let mut sink = HostSink::default();
    let outcome = crate::restore::fold_restore(
        payload,
        &facts.header(),
        facts.min_readable_schema_version,
        &mut sink,
    )
    .map_err(restore_failed)?;
    let fold = schema::RestoreFold {
        snapshot: sink.snapshot,
        changes: sink.changes,
    };
    match crate::poll_core::global()
        .host_command(slot, schema::ActivationKind::RestoreFold(fold))?
    {
        schema::ActivationOutcome::Restored => {
            *slot.timers.lock_recover() = sink.timers;
            Ok(outcome)
        }
        schema::ActivationOutcome::Failed(err) => Err(host_failed("restore_fold", err.message)),
        _ => Err(wrong_outcome("restore_fold")),
    }
}

/// A compaction snapshot of either kind of Cell, under the cell gate: drain
/// pending changes, read the seq, re-state the pending timer set (so
/// truncation cannot eat it), then frame the body `encode_body` gives as of
/// that seq. `(frame, seq, body_len)`.
pub fn snapshot_frame(
    slot: &Arc<CellSlot>,
    header: &HeaderTemplate,
    encode_body: impl FnOnce() -> Result<Vec<u8>, Error>,
) -> Result<(Vec<u8>, u64, usize), Error> {
    let flush = |buffer: &mut Box<dyn FlushableBuffer>| {
        crate::flusher::flush_one(
            &mut **buffer,
            &slot.cell_identity,
            &slot.change_seq,
            header,
            slot.keeps_state,
        );
    };
    let register_ops = crate::timer::restated(slot, &slot.timers.lock_recover()).as_register_ops();
    // One buffer lock across the drain and the seq read, so the background
    // flusher cannot assign a seq between them.
    let mut buffer = slot.flush_buffer.lock_recover();
    flush(&mut buffer);
    let seq = slot.change_seq.load(Ordering::Acquire).saturating_sub(1);
    // What the coming truncation would delete is re-stated AFTER the seq is
    // read, landing at `snapshot_seq + 1`, which `truncate_up_to` keeps; a
    // crash between the two leaves an untruncated log holding the originals.
    if buffer.restate_above_snapshot(register_ops) {
        flush(&mut buffer);
    }
    drop(buffer);
    let body = encode_body()?;
    let frame =
        crate::flusher::build_cell_frame(&slot.cell_identity, seq, &header.stamp(&body), &body);
    Ok((frame, seq, body.len()))
}

/// A host-run Cell's compaction snapshot: its body is the host's State alone,
/// crossed as a [`schema::ActivationKind::SnapshotRequest`].
pub(super) fn snapshot_frame_crossing(
    slot: &Arc<CellSlot>,
) -> Result<(Vec<u8>, u64, usize), Error> {
    snapshot_frame(slot, &host_facts(slot)?.header(), || {
        snapshot_state(
            crate::poll_core::global()
                .host_command(slot, schema::ActivationKind::SnapshotRequest)?,
        )
    })
}

/// The host's snapshot body, or the refusal its answer carries.
fn snapshot_state(outcome: schema::ActivationOutcome) -> Result<Vec<u8>, Error> {
    match outcome {
        schema::ActivationOutcome::Snapshot { state } => Ok(state),
        schema::ActivationOutcome::Failed(err) => Err(host_failed("snapshot_request", err.message)),
        _ => Err(wrong_outcome("snapshot_request")),
    }
}

/// The state as JSON for a live read or a migration of a host-run Cell,
/// waiting `wait` for the host's answer. MUST run under the cell gate (the
/// caller's `with_cell_exclusive` closure).
pub fn state_json_locked(
    slot: &Arc<CellSlot>,
    wait: Duration,
) -> Result<(serde_json::Value, u64), Error> {
    let kind = schema::ActivationKind::LiveStateRead;
    let json = match crate::poll_core::global().host_command_within(slot, kind, wait, None)? {
        schema::ActivationOutcome::StateJson { json } => json,
        schema::ActivationOutcome::Failed(err) => {
            return Err(host_failed("live_state_read", err.message))
        }
        _ => return Err(wrong_outcome("live_state_read")),
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
