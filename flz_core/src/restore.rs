//! `RestoreInitPayload` + restoration helpers per design.md Decision 5.
//!
//! When an Entity is reconnected with persisted state, the runtime
//! invokes the user's `Procedure::init_state(entity_key, Some(payload))`,
//! then automatically applies the snapshot bytes (replacing the user's
//! freshly-constructed state) and folds the change-log records via
//! `Procedure::apply_change`. The user MAY inspect `payload.snapshot_bytes`
//! and `payload.change_log_bytes` for instrumentation but never has to
//! decode them.

use crate::util::{read_u32_be, read_u64_be};

/// Snapshot + change-log bytes handed to `Procedure::init_state` on a warm
/// restore. Borrowed (`&[u8]`) so the runtime owns the underlying buffers
/// for the duration of the call without an allocation.
pub struct RestoreInitPayload<'a> {
    /// The snapshot's stored 16-byte header, as the controller read it off
    /// disk. Empty exactly when `snapshot_bytes` is empty. Carries the
    /// `schema_version` the snapshot was written at, so the snapshot resolves
    /// through the same decision as a change record.
    pub snapshot_header: &'a [u8],
    /// Snapshot bytes from the latest snapshot file. Empty if no
    /// snapshot has been written for this Entity. The body is the
    /// postcard-encoded `S::State` exactly as the SDK shipped it via
    /// `entity_state_snapshot`. The 16-byte header travels beside it in
    /// `snapshot_header`, so this slice is the raw State payload.
    pub snapshot_bytes: &'a [u8],
    /// Change-log bytes (concatenated WAL records). Each record is
    /// framed `[seq: u64 BE][len: u32 BE][header: 16B][change_bytes][crc32: u32 BE]`
    /// where `len = 16 + change_bytes.len()` and `crc32` covers
    /// `header + change_bytes`. This matches the controller's on-disk WAL
    /// record format (`Flezha.State.WalWriter.encode_record/3`).
    pub change_log_bytes: &'a [u8],
}

pub use crate::header::RestoreError;

/// What a completed restore fold reports back over the control channel.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RestoreOutcome {
    /// Highest WAL seq applied — the SDK re-arms `change_seq` to one past it.
    pub terminal_seq: u64,
    /// At least one record was read through a migration. The controller
    /// answers with a compaction, so the migration is paid once.
    pub migrated: bool,
}

/// Where a change-log walk delivers what it decoded. The walk owns the
/// framing, the CRC, the header resolution and the effect-record unwrapping;
/// the sink owns what to DO with a body — fold it into typed state (the
/// in-process path) or collect it for a host crossing.
pub trait ChangeLogSink {
    /// One of the author's change bodies. `decision` says whether it decodes
    /// at the running schema version or migrates from an older one.
    fn change(
        &mut self,
        decision: crate::header::SchemaDecision,
        bytes: &[u8],
    ) -> Result<(), RestoreError>;

    /// Timer operations, from a timer-ops record or from inside an effect
    /// record. Timers are the runtime's own shape, so they never migrate.
    fn timers(&mut self, ops: &[crate::schema::TimerOp]);
}

/// What a completed change-log walk observed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChangeLogSummary {
    /// Highest record seq in the log.
    pub terminal_seq: u64,
    /// At least one record reached the author's migration.
    pub migrated: bool,
}

/// Walk a change log once: verify each record's framing CRC, resolve its
/// header against the running procedure, and hand its bodies to `sink` in seq
/// order.
///
/// Both restore paths come through here — the typed in-process fold and the
/// host crossing — so the CRC policy, the version decision and the rule that
/// an effect record's enclosed bodies inherit its version exist once.
pub fn fold_change_log(
    change_log_bytes: &[u8],
    expected: &crate::header::HeaderTemplate,
    min_readable: u32,
    sink: &mut dyn ChangeLogSink,
) -> Result<ChangeLogSummary, RestoreError> {
    let mut summary = ChangeLogSummary::default();

    for record in iter_change_log_records(change_log_bytes) {
        let record = record?;
        summary.terminal_seq = summary.terminal_seq.max(record.seq);

        // The trailing crc32 in the WAL covers header + change_bytes.
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(record.header);
        hasher.update(record.change_bytes);
        if hasher.finalize() != record.trailing_crc {
            return Err(RestoreError::CrcMismatch {
                entity: 0,
                seq: record.seq,
            });
        }

        let header = crate::header::decode_header(record.header)?;
        let (kind, decision) = crate::header::resolve_record(&header, expected, min_readable)?;
        summary.migrated |= decision.migrating();

        match kind {
            crate::header::RecordKind::EffectRecord => {
                // Effectively-once effect record: its change portion folds
                // exactly as plain change records do, in order. Outputs and
                // consumed-offset are controller-side concerns. The enclosing
                // record's version governs the bodies it encloses — one
                // invocation wrote them all.
                let body: crate::flusher::EffectRecordBody =
                    postcard::from_bytes(record.change_bytes)
                        .map_err(|e| RestoreError::ChangeDecode(e.to_string()))?;
                for bytes in &body.changes {
                    sink.change(decision, bytes)?;
                }
                sink.timers(&body.timers);
            }
            crate::header::RecordKind::TimerOps => {
                let ops: Vec<crate::schema::TimerOp> = postcard::from_bytes(record.change_bytes)
                    .map_err(|e| RestoreError::ChangeDecode(e.to_string()))?;
                sink.timers(&ops);
            }
            crate::header::RecordKind::Change => sink.change(decision, record.change_bytes)?,
        }
    }

    Ok(summary)
}

/// One framed WAL record as produced by the controller's
/// `Flezha.State.WalWriter.encode_record/3`. Field-borrowed against the
/// source byte buffer. `change_bytes` excludes the leading 16-byte
/// header (which is exposed separately as `header`).
#[derive(Debug)]
pub struct ChangeLogRecord<'a> {
    pub seq: u64,
    pub header: &'a [u8],
    pub change_bytes: &'a [u8],
    pub trailing_crc: u32,
}

/// Iterate concatenated WAL records using the controller on-disk framing
/// `[seq: u64 BE][len: u32 BE][header: 16B][change_bytes][crc32: u32 BE]`
/// where `len = 16 + change_bytes.len()` and `crc32` covers
/// `header + change_bytes`.
pub fn iter_change_log_records(
    bytes: &[u8],
) -> impl Iterator<Item = Result<ChangeLogRecord<'_>, RestoreError>> {
    ChangeLogIter { remaining: bytes }
}

struct ChangeLogIter<'a> {
    remaining: &'a [u8],
}

impl<'a> Iterator for ChangeLogIter<'a> {
    type Item = Result<ChangeLogRecord<'a>, RestoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining.is_empty() {
            return None;
        }
        // Minimum frame: 8 (seq) + 4 (len) + 16 (header) + 4 (crc) = 32
        if self.remaining.len() < 32 {
            return Some(Err(RestoreError::HeaderInvalid));
        }
        let seq = read_u64_be(self.remaining, 0);
        let len = read_u32_be(self.remaining, 8) as usize;
        if len < 16 {
            return Some(Err(RestoreError::HeaderInvalid));
        }
        let header_start = 12usize;
        let header_end = header_start + 16;
        let change_start = header_end;
        let change_end = header_start + len; // = 12 + 16 + change_bytes.len()
        let frame_end = change_end + 4;
        if self.remaining.len() < frame_end {
            return Some(Err(RestoreError::HeaderInvalid));
        }
        let header = &self.remaining[header_start..header_end];
        let change_bytes = &self.remaining[change_start..change_end];
        let trailing_crc = read_u32_be(self.remaining, change_end);
        let record = ChangeLogRecord {
            seq,
            header,
            change_bytes,
            trailing_crc,
        };
        self.remaining = &self.remaining[frame_end..];
        Some(Ok(record))
    }
}
