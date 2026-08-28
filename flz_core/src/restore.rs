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

/// Frame one WAL record the way the controller's
/// `Flezha.State.WalWriter.encode_record/3` does:
/// `[seq: u64 BE][len: u32 BE][header: 16B][body][crc32: u32 BE]`.
///
/// The write side of [`iter_change_log_records`], kept beside it so the two
/// halves of the framing cannot drift. Test-only in practice, hence hidden:
/// SDKs receive change logs, they never author them.
#[doc(hidden)]
pub fn frame_record(seq: u64, header: &[u8; 16], body: &[u8]) -> Vec<u8> {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(header);
    hasher.update(body);

    let mut out = Vec::with_capacity(12 + 16 + body.len() + 4);
    out.extend_from_slice(&seq.to_be_bytes());
    out.extend_from_slice(&((16 + body.len()) as u32).to_be_bytes());
    out.extend_from_slice(header);
    out.extend_from_slice(body);
    out.extend_from_slice(&hasher.finalize().to_be_bytes());
    out
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::{
        HeaderTemplate, FORMAT_ID_MSGPACK, FORMAT_ID_POSTCARD, SDK_LANG_ALLOCATED, SDK_LANG_RUBY,
        SDK_LANG_RUST,
    };

    /// Collects what the walk hands it, so a test can assert that a refused
    /// record delivered NOTHING rather than a partial state.
    #[derive(Default)]
    struct RecordingSink {
        changes: Vec<Vec<u8>>,
    }

    impl ChangeLogSink for RecordingSink {
        fn change(
            &mut self,
            _decision: crate::header::SchemaDecision,
            bytes: &[u8],
        ) -> Result<(), RestoreError> {
            self.changes.push(bytes.to_vec());
            Ok(())
        }

        fn timers(&mut self, _ops: &[crate::schema::TimerOp]) {}
    }

    fn template(sdk_lang: u8) -> HeaderTemplate {
        HeaderTemplate {
            sdk_lang,
            sdk_version: 1,
            schema_version: 1,
            format_id: FORMAT_ID_POSTCARD,
        }
    }

    /// One framed WAL record stamped by `writer`.
    fn record(seq: u64, writer: &HeaderTemplate, body: &[u8]) -> Vec<u8> {
        frame_record(seq, &writer.stamp(body), body)
    }

    /// Every ordered pair of allocated languages, refused before any body is
    /// collected.
    ///
    /// The host arm and the typed arm share this walk, so the gate they both
    /// inherit is proved once here — as a sweep over the allocation table
    /// rather than a test per pair, because a pair-per-test suite covers only
    /// the pairs someone remembered to write, grows quadratically, and would
    /// leave the next allocated language uncovered until someone noticed.
    ///
    /// Both sides declare the same format id here, so the language byte is
    /// the only thing that differs between writer and reader and therefore
    /// the only thing that can have refused the record.
    #[test]
    fn a_foreign_language_record_is_refused_before_any_body_is_collected() {
        for (writer, writer_name) in SDK_LANG_ALLOCATED {
            for (reader, reader_name) in SDK_LANG_ALLOCATED {
                if writer == reader {
                    continue;
                }
                let log = record(0, &template(*writer), b"body");
                let mut sink = RecordingSink::default();

                let err = fold_change_log(&log, &template(*reader), 1, &mut sink).unwrap_err();

                // The reason travels to the controller as a string, so it has
                // to name both SDKs and not only their bytes.
                let message = err.to_string();
                assert!(message.contains(writer_name), "{message}");
                assert!(message.contains(reader_name), "{message}");
                match err {
                    RestoreError::LangMismatch { expected, found } => {
                        assert_eq!(expected, *reader);
                        assert_eq!(found, *writer);
                    }
                    other => panic!("{writer_name} read by {reader_name}: {other:?}"),
                }
                assert!(
                    sink.changes.is_empty(),
                    "a refused record must not reach the host codec"
                );
            }
        }
    }

    /// The language is read before the format, in both directions.
    ///
    /// Read the other way round, a MessagePack body reaching a postcard
    /// reader would be reported as a malformed header, and the operator would
    /// chase a corrupt WAL instead of the SDK that wrote it.
    #[test]
    fn the_language_is_read_before_the_format() {
        let msgpack = HeaderTemplate {
            sdk_lang: SDK_LANG_RUBY,
            format_id: FORMAT_ID_MSGPACK,
            ..template(SDK_LANG_RUBY)
        };
        let postcard = template(SDK_LANG_RUST);

        for (writer, reader, expected, found) in [
            (&msgpack, &postcard, SDK_LANG_RUST, SDK_LANG_RUBY),
            (&postcard, &msgpack, SDK_LANG_RUBY, SDK_LANG_RUST),
        ] {
            let log = record(0, writer, b"body");
            let mut sink = RecordingSink::default();

            let err = fold_change_log(&log, reader, 1, &mut sink).unwrap_err();

            match err {
                RestoreError::LangMismatch {
                    expected: e,
                    found: f,
                } => {
                    assert_eq!((e, f), (expected, found));
                }
                other => panic!("expected a language mismatch, got {other:?}"),
            }
            assert!(sink.changes.is_empty());
        }
    }

    /// The gate is a language check, not a blanket refusal: a log written by
    /// the reading SDK still folds.
    #[test]
    fn a_same_language_record_folds() {
        let log = record(0, &template(SDK_LANG_RUST), b"postcard-bytes");
        let mut sink = RecordingSink::default();

        let summary = fold_change_log(&log, &template(SDK_LANG_RUST), 1, &mut sink).unwrap();

        assert_eq!(summary.terminal_seq, 0);
        assert_eq!(sink.changes, vec![b"postcard-bytes".to_vec()]);
    }
}
