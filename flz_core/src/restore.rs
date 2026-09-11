//! The change-log walk a restore folds through: framing CRC, header
//! resolution and effect-record unwrapping exist once here, and a sink owns
//! what to do with each body. The invariant: a refused record delivers
//! nothing, and an effect record's enclosed bodies inherit its version.

use crate::util::{read_u32_be, read_u64_be};

/// Snapshot + change-log bytes handed to `Procedure::init_state` on a warm
/// restore, borrowed from the frame.
pub struct RestoreInitPayload<'a> {
    /// The snapshot's stored header; empty exactly when `snapshot_bytes` is.
    pub snapshot_header: &'a [u8],
    /// The raw state body the SDK shipped as `entity_state_snapshot`.
    pub snapshot_bytes: &'a [u8],
    /// Concatenated WAL records in the controller's on-disk framing.
    pub change_log_bytes: &'a [u8],
}

pub use crate::header::RestoreError;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RestoreOutcome {
    pub terminal_seq: u64,
    /// A record was migrated; the controller answers with a compaction so
    /// the migration is paid once.
    pub migrated: bool,
}

/// Where the walk delivers what it decoded: folded into typed state, or
/// collected for a host crossing.
pub trait ChangeLogSink {
    /// One change body, with the version decision it decodes under.
    fn change(
        &mut self,
        decision: crate::header::SchemaDecision,
        bytes: &[u8],
    ) -> Result<(), RestoreError>;

    /// Timer ops; the runtime's own shape, never migrated.
    fn timers(&mut self, ops: &[crate::schema::TimerOp]);
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChangeLogSummary {
    pub terminal_seq: u64,
    pub migrated: bool,
}

/// Walk a change log once — CRC, header resolution, bodies to `sink` in seq
/// order — for the typed fold and the host crossing alike.
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
        summary.migrated |= fold_record(&record, expected, min_readable, sink)?;
    }

    Ok(summary)
}

/// One record into the sink; an effect record's changes fold as plain ones
/// do, under the enclosing version. Whether it migrated.
fn fold_record(
    record: &ChangeLogRecord<'_>,
    expected: &crate::header::HeaderTemplate,
    min_readable: u32,
    sink: &mut dyn ChangeLogSink,
) -> Result<bool, RestoreError> {
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

    match kind {
        crate::header::RecordKind::EffectRecord => {
            let body: crate::flusher::EffectRecordBody = postcard::from_bytes(record.change_bytes)
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
    Ok(decision.migrating())
}

/// Frame one WAL record as `Flezha.State.WalWriter.encode_record/3` does —
/// the write side of [`iter_change_log_records`], kept beside it.
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

/// One framed WAL record, borrowed from the buffer; `change_bytes` excludes
/// the header.
#[derive(Debug)]
pub struct ChangeLogRecord<'a> {
    pub seq: u64,
    pub header: &'a [u8],
    pub change_bytes: &'a [u8],
    pub trailing_crc: u32,
}

/// Iterate concatenated WAL records:
/// `[seq: u64 BE][len: u32 BE][header: 16B][change_bytes][crc32: u32 BE]`,
/// the crc over header + body.
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

    /// Collects what the walk hands it.
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

    fn record(seq: u64, writer: &HeaderTemplate, body: &[u8]) -> Vec<u8> {
        frame_record(seq, &writer.stamp(body), body)
    }

    /// Every ordered pair of allocated languages, refused before any body is
    /// collected — a sweep over the table, so the next allocated language is
    /// covered by construction. Both sides declare one format id, so the
    /// language byte alone refused the record.
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

    /// A log written by the reading SDK still folds.
    #[test]
    fn a_same_language_record_folds() {
        let log = record(0, &template(SDK_LANG_RUST), b"postcard-bytes");
        let mut sink = RecordingSink::default();

        let summary = fold_change_log(&log, &template(SDK_LANG_RUST), 1, &mut sink).unwrap();

        assert_eq!(summary.terminal_seq, 0);
        assert_eq!(sink.changes, vec![b"postcard-bytes".to_vec()]);
    }

    /// The crc that is checked is the frame's trailing one, over header and
    /// body together, and it is checked before the header is decoded — so a
    /// corrupted body is refused even though its header parses.
    #[test]
    fn a_corrupted_body_is_refused_before_the_header_is_decoded() {
        let mut log = record(7, &template(SDK_LANG_RUST), b"postcard-bytes");
        let last_body_byte = log.len() - 5; // the trailing crc is the last four
        log[last_body_byte] ^= 0xFF;
        let mut sink = RecordingSink::default();

        let err = fold_change_log(&log, &template(SDK_LANG_RUST), 1, &mut sink).unwrap_err();

        match err {
            RestoreError::CrcMismatch { entity, seq } => assert_eq!((entity, seq), (0, 7)),
            other => panic!("expected a crc mismatch, got {other:?}"),
        }
        assert!(sink.changes.is_empty());
    }

    mod shared_fixture {
        //! The frame both runtimes read, from the one file that holds it:
        //! `test/fixtures/durable_frame.txt`. The Elixir side folds the same
        //! rows in `test/flezha/state/durable_frame_conformance_test.exs`, so a
        //! framing change on either side fails the other side's suite.

        use super::*;
        use crate::header::{decide_schema, decode_header, sdk_lang_name, SchemaDecision};

        const FIXTURE: &str = include_str!("../../test/fixtures/durable_frame.txt");

        /// Rows of one kind, already split on `|` and trimmed.
        fn rows(kind: &str) -> Vec<Vec<String>> {
            FIXTURE
                .lines()
                .filter(|line| !line.trim_start().starts_with('#'))
                .map(|line| {
                    line.split('|')
                        .map(|f| f.trim().to_string())
                        .collect::<Vec<_>>()
                })
                .filter(|fields| fields.first().map(String::as_str) == Some(kind))
                .map(|fields| fields[1..].to_vec())
                .collect()
        }

        fn from_hex(hex: &str) -> Vec<u8> {
            if hex == "-" {
                return Vec::new();
            }
            hex.as_bytes()
                .chunks(2)
                .map(|pair| {
                    u8::from_str_radix(std::str::from_utf8(pair).expect("ascii hex"), 16)
                        .expect("hex byte")
                })
                .collect()
        }

        /// The stamp every `frame` row carries, per the fixture's own header.
        fn fixture_template() -> HeaderTemplate {
            HeaderTemplate {
                sdk_lang: SDK_LANG_RUST,
                sdk_version: 65_536,
                schema_version: 1,
                format_id: FORMAT_ID_POSTCARD,
            }
        }

        #[test]
        fn every_frame_row_folds_to_the_verdict_the_fixture_states() {
            let rows = rows("frame");
            assert!(!rows.is_empty(), "the fixture holds no frame rows");

            for row in rows {
                let (name, hex, verdict, seqs) = (&row[0], &row[1], &row[2], &row[3]);
                let log = from_hex(hex);
                let mut sink = RecordingSink::default();
                let folded = fold_change_log(&log, &fixture_template(), 1, &mut sink);

                match verdict.as_str() {
                    "ok" => {
                        folded.unwrap_or_else(|e| panic!("row {name}: expected ok, got {e:?}"));
                        let walked: Vec<u64> = iter_change_log_records(&log)
                            .map(|r| r.expect("row folds").seq)
                            .collect();
                        let expected: Vec<u64> = if seqs == "-" {
                            Vec::new()
                        } else {
                            seqs.split(',').map(|s| s.parse().expect("seq")).collect()
                        };
                        assert_eq!(walked, expected, "row {name}");
                    }
                    "crc_mismatch" => match folded {
                        Err(RestoreError::CrcMismatch { .. }) => {}
                        other => panic!("row {name}: expected a crc mismatch, got {other:?}"),
                    },
                    other => panic!("row {name}: the fixture states an unknown verdict {other}"),
                }
            }
        }

        #[test]
        fn every_header_row_decodes_to_the_fields_the_fixture_states() {
            let rows = rows("header");
            assert!(!rows.is_empty(), "the fixture holds no header rows");

            for row in rows {
                let name = &row[0];
                let decoded = decode_header(&from_hex(&row[1]))
                    .unwrap_or_else(|e| panic!("row {name}: {e:?}"));

                assert_eq!(
                    decoded.sdk_lang,
                    row[2].parse::<u8>().expect("lang"),
                    "row {name}"
                );
                assert_eq!(
                    decoded.sdk_version,
                    row[3].parse::<u32>().expect("version"),
                    "row {name}"
                );
                assert_eq!(
                    decoded.schema_version,
                    row[4].parse::<u32>().expect("schema"),
                    "row {name}"
                );
                assert_eq!(
                    decoded.format_id,
                    row[5].parse::<u8>().expect("format"),
                    "row {name}"
                );
                assert_eq!(
                    decoded.crc32,
                    u32::from_str_radix(&row[6], 16).expect("crc"),
                    "row {name}"
                );
            }
        }

        #[test]
        fn the_language_table_names_what_the_fixture_names() {
            let rows = rows("lang");
            assert!(!rows.is_empty(), "the fixture holds no language rows");

            for row in rows {
                let value: u8 = row[0].parse().expect("lang value");
                // `unspecified` is allocated in the table but is not an SDK, so
                // the kernel answers `unallocated` for it; every other row is a
                // language both runtimes name the same way.
                if value == 0 {
                    continue;
                }
                assert_eq!(sdk_lang_name(value), row[1], "lang {value}");
            }
        }

        #[test]
        fn every_schema_row_reaches_the_verdict_the_fixture_states() {
            let rows = rows("schema");
            assert!(!rows.is_empty(), "the fixture holds no schema rows");

            for row in rows {
                let stamped: u32 = row[0].parse().expect("stamped");
                let running: u32 = row[1].parse().expect("running");
                let floor: u32 = row[2].parse().expect("min_readable");
                let decided = decide_schema(stamped, running, floor);

                let (verdict, version) = row[3].split_once(':').unwrap_or((&row[3], ""));
                match verdict {
                    "decode" => assert_eq!(decided.unwrap(), SchemaDecision::Decode),
                    "from_future" => assert!(matches!(
                        decided,
                        Err(RestoreError::SchemaFromTheFuture { .. })
                    )),
                    "too_old" => {
                        let reason = decided.unwrap_err().to_string();
                        assert!(
                            reason.contains(&format!("is below the readable floor {version} ")),
                            "row {row:?}: {reason}"
                        );
                    }
                    "migrate" => assert_eq!(
                        decided.unwrap(),
                        SchemaDecision::Migrate {
                            from_version: version.parse().expect("from version")
                        }
                    ),
                    other => panic!("unknown verdict {other}"),
                }
            }
        }
    }
}
