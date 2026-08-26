//! 16-byte version header for durable per-Entity records.
//!
//! Layout:
//!  - 0:1   `sdk_lang: u8` — the writing SDK, per the allocation table below
//!  - 1:5   `sdk_version: u32 BE` = (major << 16) | (minor << 8) | patch
//!  - 5:9   `schema_version: u32 BE` = the class's declared schema version
//!  - 9:10  `format_id: u8` = 1 (postcard) in v1
//!  - 10:12 reserved (zero)
//!  - 12:16 `crc32: u32 BE` = `crc32fast::hash(&change_bytes)`

use crate::util::read_u32_be;

/// Restoration failures surfaced to the control channel. Plain data — the
/// host's typed layer wraps and reports them.
#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    #[error("snapshot decode failed: {0}")]
    SnapshotDecode(String),
    #[error("change-log decode failed: {0}")]
    ChangeDecode(String),
    #[error("malformed restoration header (truncated or invalid)")]
    HeaderInvalid,
    #[error(
        "sdk_lang mismatch: this SDK is {} ({expected}), the record was written \
         by {} ({found}); durable bodies are per-SDK and are never decoded \
         across languages",
        sdk_lang_name(*expected),
        sdk_lang_name(*found)
    )]
    LangMismatch { expected: u8, found: u8 },
    #[error(
        "record stamped schema_version {found} is below the readable floor \
         {min_readable} the running procedure declares"
    )]
    SchemaTooOld { found: u32, min_readable: u32 },
    #[error(
        "record stamped schema_version {found} is above the running procedure's \
         {running}: older code cannot read newer state"
    )]
    SchemaFromTheFuture { found: u32, running: u32 },
    #[error("no migration from schema_version {from_version}: {reason}")]
    MigrationFailed { from_version: u32, reason: String },
    #[error("crc32 mismatch on change record (entity={entity}, seq={seq})")]
    CrcMismatch { entity: u32, seq: u64 },
}

/// The `sdk_lang` allocation table: 0 unspecified, 1 Rust, 2 Java, 3 Elixir,
/// 4 Node, `5..=254` free in landing order, 255 reserved.
///
/// Durable `State`/`Change` bodies are per-SDK below the shared envelope, so
/// this byte is what keeps one language's records from reaching another's
/// codec. One value per SDK, assigned when that SDK lands — a proposal takes
/// the next free one at implementation time rather than asserting one. An
/// unknown value is refused, never interpreted.
pub const SDK_LANG_UNSPECIFIED: u8 = 0;
/// Rust SDK. Also the value the typed in-process path stamps.
pub const SDK_LANG_RUST: u8 = 1;
/// Java SDK (JNI over this core).
pub const SDK_LANG_JAVA: u8 = 2;
/// Elixir SDK. Persists no Entity state in v1, but advertises the byte so its
/// factories are taggable at the placement seam.
pub const SDK_LANG_ELIXIR: u8 = 3;
/// Node SDK (N-API over this core).
pub const SDK_LANG_NODE: u8 = 4;
/// Lowest value not yet allocated to an SDK.
pub const SDK_LANG_NEXT_FREE: u8 = 5;
/// Reserved; never allocated to an SDK.
pub const SDK_LANG_RESERVED: u8 = 255;

/// Every allocated value, in allocation order. The single list the uniqueness
/// test and the allocation-check read.
pub const SDK_LANG_ALLOCATED: &[(u8, &str)] = &[
    (SDK_LANG_RUST, "rust"),
    (SDK_LANG_JAVA, "java"),
    (SDK_LANG_ELIXIR, "elixir"),
    (SDK_LANG_NODE, "node"),
];

/// Whether `lang` is a value the allocation table hands to an SDK. `0` and
/// `255` are reserved, and an unallocated value is refused rather than guessed.
pub fn sdk_lang_allocated(lang: u8) -> bool {
    SDK_LANG_ALLOCATED.iter().any(|(v, _)| *v == lang)
}

/// The SDK name behind an allocated value, for error and log text.
pub fn sdk_lang_name(lang: u8) -> &'static str {
    SDK_LANG_ALLOCATED
        .iter()
        .find(|(v, _)| *v == lang)
        .map(|(_, name)| *name)
        .unwrap_or("unallocated")
}

/// Postcard format id stamped at byte 9 of the persistence header.
pub const FORMAT_ID_POSTCARD: u8 = 1;

/// Effect-record format id (effectively-once tier): the body is a postcard
/// `EffectRecordBody` whose `changes` entries are themselves the host-encoded
/// bytes a format-1 record would carry. The durable marker that lets mixed
/// WALs (plain change records + effect records) stay readable.
pub const FORMAT_ID_EFFECT_RECORD: u8 = 2;

/// MessagePack format id: the body is a MessagePack-encoded `State`/`Change`.
/// Shared by every SDK whose codec is MessagePack rather than minted per SDK —
/// the byte names the encoding, and the cross-language gate is `sdk_lang`.
pub const FORMAT_ID_MSGPACK: u8 = 5;

/// Timer-ops format id: the body is a postcard `Vec<TimerOp>` mutating the
/// Entity's pending-timer set. Rides the ordinary `entity_state_change`
/// payload-id, so the controller stores and truncates it exactly as it does a
/// change record and needs to know nothing about timers.
pub const FORMAT_ID_TIMER_OPS: u8 = 3;

/// Parsed view of the 16-byte header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodedHeader {
    pub sdk_lang: u8,
    pub sdk_version: u32,
    pub schema_version: u32,
    pub format_id: u8,
    pub crc32: u32,
}

/// Header values frozen at Entity construction so the per-record stamp
/// is a memcpy + crc compute. Reserved bytes are always zero.
#[derive(Debug, Clone, Copy)]
pub struct HeaderTemplate {
    pub sdk_lang: u8,
    pub sdk_version: u32,
    pub schema_version: u32,
    pub format_id: u8,
}

impl HeaderTemplate {
    /// Construct the per-Entity header template. The typed layer's
    /// `header_template_for::<S>()` fills both values from compile-time
    /// constants.
    pub fn new(sdk_version: u32, schema_version: u32) -> Self {
        Self {
            sdk_lang: SDK_LANG_RUST,
            sdk_version,
            schema_version,
            format_id: FORMAT_ID_POSTCARD,
        }
    }

    /// Stamp the header onto fresh bytes. The CRC32 is computed over the
    /// caller-provided change_bytes; the returned 16-byte array is
    /// suitable for the `[header: 16B][change_bytes]` framing produced
    /// by the per-Entity flusher.
    pub fn stamp(&self, change_bytes: &[u8]) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[0] = self.sdk_lang;
        out[1..5].copy_from_slice(&self.sdk_version.to_be_bytes());
        out[5..9].copy_from_slice(&self.schema_version.to_be_bytes());
        out[9] = self.format_id;
        // bytes 10..12 are reserved and remain zero.
        let crc = crc32fast::hash(change_bytes);
        out[12..16].copy_from_slice(&crc.to_be_bytes());
        out
    }
}

/// Decode a 16-byte header into its fields. Resolution against the running
/// procedure's expectations lives in the `resolve_header_for_*` helpers so the
/// caller sees a typed error.
pub fn decode_header(bytes: &[u8]) -> Result<DecodedHeader, RestoreError> {
    if bytes.len() < 16 {
        return Err(RestoreError::HeaderInvalid);
    }
    let sdk_version = read_u32_be(bytes, 1);
    let schema_version = read_u32_be(bytes, 5);
    let crc32 = read_u32_be(bytes, 12);
    Ok(DecodedHeader {
        sdk_lang: bytes[0],
        sdk_version,
        schema_version,
        format_id: bytes[9],
        crc32,
    })
}

/// What a record's stamped `schema_version` means to the procedure now
/// running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaDecision {
    /// Stamped at the running version: decode with the current codec.
    Decode,
    /// Stamped older, and at or above the declared readable floor: the bytes
    /// belong to the procedure's migration for `from_version`.
    Migrate { from_version: u32 },
}

impl SchemaDecision {
    /// Whether this record reaches the author's migration rather than the
    /// current codec — what a restore reports back so the controller compacts.
    pub fn migrating(self) -> bool {
        matches!(self, Self::Migrate { .. })
    }
}

/// The one place a stamped schema version is resolved against the procedure
/// reading it. `running` is its `SCHEMA_VERSION`, `min_readable` its
/// `MIN_READABLE_SCHEMA_VERSION`.
///
/// A floor above `running` reads nothing but the running version — the
/// fail-closed reading of a contradictory declaration.
pub fn decide_schema(
    stamped: u32,
    running: u32,
    min_readable: u32,
) -> Result<SchemaDecision, RestoreError> {
    if stamped == running {
        return Ok(SchemaDecision::Decode);
    }
    if stamped > running {
        return Err(RestoreError::SchemaFromTheFuture {
            found: stamped,
            running,
        });
    }
    if stamped < min_readable {
        return Err(RestoreError::SchemaTooOld {
            found: stamped,
            min_readable,
        });
    }
    Ok(SchemaDecision::Migrate {
        from_version: stamped,
    })
}

/// Which record a WAL entry is. The format byte says it, read against the
/// running procedure's declared change format (postcard for Rust, Kryo for
/// Java) — effect records and timer ops are the runtime's own shapes and carry
/// their own ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordKind {
    /// The author's own `Change` body.
    Change,
    /// Effectively-once effect record: changes + outputs + consumed offset.
    EffectRecord,
    /// A mutation of the pending-timer set.
    TimerOps,
}

/// Classify and resolve one WAL record's header in a single step: the format
/// byte says which record this is, the schema version says what to do with its
/// body. Both restore folds go through here, so neither re-derives the kind
/// from the byte it just matched on, and an unknown format is refused once.
pub fn resolve_record(
    header: &DecodedHeader,
    expected: &HeaderTemplate,
    min_readable: u32,
) -> Result<(RecordKind, SchemaDecision), RestoreError> {
    refuse_foreign_language(header, expected)?;
    let kind = match header.format_id {
        FORMAT_ID_EFFECT_RECORD => RecordKind::EffectRecord,
        FORMAT_ID_TIMER_OPS => RecordKind::TimerOps,
        id if id == expected.format_id => RecordKind::Change,
        _ => return Err(RestoreError::HeaderInvalid),
    };
    Ok((
        kind,
        decide_schema(header.schema_version, expected.schema_version, min_readable)?,
    ))
}

/// Resolve the header of a state snapshot. A snapshot has no kind of its own:
/// it is the running procedure's state, in the format that procedure declares.
pub fn resolve_state(
    header: &DecodedHeader,
    expected: &HeaderTemplate,
    min_readable: u32,
) -> Result<SchemaDecision, RestoreError> {
    refuse_foreign_language(header, expected)?;
    if header.format_id != expected.format_id {
        return Err(RestoreError::HeaderInvalid);
    }
    decide_schema(header.schema_version, expected.schema_version, min_readable)
}

/// Language before anything else: a record from another SDK is neither a
/// format question nor a schema one, and the refusal has to name both
/// languages to be actionable. SDKs that share a codec would otherwise be
/// told their header is malformed — the answer a differing format byte gives
/// when it is read first.
fn refuse_foreign_language(
    header: &DecodedHeader,
    expected: &HeaderTemplate,
) -> Result<(), RestoreError> {
    if header.sdk_lang != expected.sdk_lang {
        return Err(RestoreError::LangMismatch {
            expected: expected.sdk_lang,
            found: header.sdk_lang,
        });
    }
    Ok(())
}

/// Parse `CARGO_PKG_VERSION` into the `(major << 16) | (minor << 8) | patch`
/// form stamped at bytes 1..5 of the header. Components above 255 are
/// clamped (no real-world version reaches this; defensive against
/// "0.1.0-pre.42" suffixes).
pub fn parse_pkg_version() -> u32 {
    let raw = env!("CARGO_PKG_VERSION");
    let mut parts = raw
        .split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty());
    let major = parts
        .next()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);
    let minor = parts
        .next()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);
    let patch = parts
        .next()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);
    (major.min(0xFF) << 16) | (minor.min(0xFF) << 8) | patch.min(0xFF)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The allocation table is only worth having if it cannot collide: two
    /// SDKs sharing a value would make the guard that reads it meaningless.
    #[test]
    fn allocation_table_has_no_duplicates() {
        let mut seen = std::collections::HashSet::new();
        for (value, name) in SDK_LANG_ALLOCATED {
            assert!(
                seen.insert(*value),
                "sdk_lang {value} is allocated twice (second holder: {name})"
            );
        }
        assert_eq!(seen.len(), SDK_LANG_ALLOCATED.len());
    }

    #[test]
    fn reserved_values_are_never_allocated() {
        assert!(!sdk_lang_allocated(SDK_LANG_UNSPECIFIED));
        assert!(!sdk_lang_allocated(SDK_LANG_RESERVED));
        assert!(!sdk_lang_allocated(SDK_LANG_NEXT_FREE));
        for (value, _) in SDK_LANG_ALLOCATED {
            assert!(*value >= 1 && *value < SDK_LANG_NEXT_FREE);
            assert!(sdk_lang_allocated(*value));
        }
    }

    #[test]
    fn shipped_sdks_hold_their_allocated_values() {
        assert_eq!(SDK_LANG_RUST, 1);
        assert_eq!(SDK_LANG_JAVA, 2);
        assert_eq!(SDK_LANG_ELIXIR, 3);
        assert_eq!(SDK_LANG_NODE, 4);
        assert_eq!(sdk_lang_name(SDK_LANG_JAVA), "java");
        assert_eq!(sdk_lang_name(SDK_LANG_NODE), "node");
        assert_eq!(sdk_lang_name(SDK_LANG_NEXT_FREE), "unallocated");
    }

    /// A format id names the codec, not the SDK, so MessagePack has one value
    /// every MessagePack SDK shares. What must stay unique is the set: two
    /// codecs on one byte would make a mixed WAL unreadable.
    #[test]
    fn format_ids_are_distinct() {
        let ids = [
            FORMAT_ID_POSTCARD,
            FORMAT_ID_EFFECT_RECORD,
            FORMAT_ID_TIMER_OPS,
            FORMAT_ID_MSGPACK,
        ];
        let mut seen = std::collections::HashSet::new();
        for id in ids {
            assert!(seen.insert(id), "format_id {id} is allocated twice");
        }
    }

    #[test]
    fn stamp_layout_matches_umbrella_spec() {
        // Umbrella scenario: SDK at version 0.5.3, SCHEMA_VERSION = 1,
        // change_bytes [0x01, 0x02, 0x03] -> header bytes per the spec.
        let template = HeaderTemplate::new(0x000503, 1);
        let header = template.stamp(&[0x01, 0x02, 0x03]);
        assert_eq!(header[0], 0x01);
        assert_eq!(&header[1..5], &[0x00, 0x00, 0x05, 0x03]);
        assert_eq!(&header[5..9], &[0x00, 0x00, 0x00, 0x01]);
        assert_eq!(header[9], 0x01);
        assert_eq!(&header[10..12], &[0x00, 0x00]);
        let expected_crc = crc32fast::hash(&[0x01, 0x02, 0x03]).to_be_bytes();
        assert_eq!(&header[12..16], &expected_crc);
    }

    #[test]
    fn decode_round_trips_stamp() {
        let template = HeaderTemplate::new(0x010203, 7);
        let bytes = template.stamp(b"hello");
        let decoded = decode_header(&bytes).unwrap();
        assert_eq!(decoded.sdk_lang, SDK_LANG_RUST);
        assert_eq!(decoded.sdk_version, 0x010203);
        assert_eq!(decoded.schema_version, 7);
        assert_eq!(decoded.format_id, FORMAT_ID_POSTCARD);
        assert_eq!(decoded.crc32, crc32fast::hash(b"hello"));
    }

    /// The four arms of the decision table, which is the whole of what a
    /// restore is allowed to conclude from a stamped version.
    #[test]
    fn equal_versions_decode_directly() {
        assert_eq!(decide_schema(2, 2, 1).unwrap(), SchemaDecision::Decode);
    }

    #[test]
    fn an_older_version_at_or_above_the_floor_migrates() {
        assert_eq!(
            decide_schema(1, 3, 1).unwrap(),
            SchemaDecision::Migrate { from_version: 1 }
        );
        assert_eq!(
            decide_schema(2, 3, 1).unwrap(),
            SchemaDecision::Migrate { from_version: 2 }
        );
    }

    #[test]
    fn a_version_below_the_floor_is_refused() {
        let err = decide_schema(1, 4, 3).unwrap_err();
        assert!(matches!(
            err,
            RestoreError::SchemaTooOld {
                found: 1,
                min_readable: 3
            }
        ));
    }

    #[test]
    fn a_version_above_the_running_one_is_refused_and_never_migrates() {
        let err = decide_schema(3, 2, 1).unwrap_err();
        assert!(matches!(
            err,
            RestoreError::SchemaFromTheFuture {
                found: 3,
                running: 2
            }
        ));
    }

    /// A floor above the running version is a contradictory declaration; it
    /// must read the running version and nothing else, never widen.
    #[test]
    fn a_floor_above_the_running_version_reads_only_the_running_one() {
        assert_eq!(decide_schema(2, 2, 5).unwrap(), SchemaDecision::Decode);
        assert!(matches!(
            decide_schema(1, 2, 5).unwrap_err(),
            RestoreError::SchemaTooOld { .. }
        ));
    }

    /// The resolvers claim the format the running procedure declares, so a
    /// Kryo-stamped record resolves for a Kryo class and not for a postcard
    /// one.
    #[test]
    fn the_expected_format_comes_from_the_running_procedure() {
        let kryo = HeaderTemplate {
            sdk_lang: SDK_LANG_JAVA,
            sdk_version: 1,
            schema_version: 1,
            format_id: 4,
        };
        let stamped = decode_header(&kryo.stamp(b"body")).unwrap();
        assert_eq!(
            resolve_record(&stamped, &kryo, 1).unwrap(),
            (RecordKind::Change, SchemaDecision::Decode)
        );

        // Same SDK, different declared format: the refusal has to be the
        // format one, so the language is held constant here.
        let postcard = HeaderTemplate {
            format_id: FORMAT_ID_POSTCARD,
            ..kryo
        };
        assert!(matches!(
            resolve_record(&stamped, &postcard, 1).unwrap_err(),
            RestoreError::HeaderInvalid
        ));
    }

    #[test]
    fn a_foreign_language_byte_is_refused_before_the_version_is_read() {
        let java = HeaderTemplate {
            sdk_lang: 2,
            sdk_version: 1,
            schema_version: 9,
            format_id: FORMAT_ID_POSTCARD,
        };
        let stamped = decode_header(&java.stamp(b"body")).unwrap();
        let rust = HeaderTemplate::new(1, 1);
        assert!(matches!(
            resolve_record(&stamped, &rust, 1).unwrap_err(),
            RestoreError::LangMismatch {
                expected: SDK_LANG_RUST,
                found: 2
            }
        ));
    }

    #[test]
    fn an_unknown_format_byte_is_refused_at_classification() {
        let mut template = HeaderTemplate::new(1, 1);
        template.format_id = 9;
        let stamped = decode_header(&template.stamp(b"body")).unwrap();
        assert!(matches!(
            resolve_record(&stamped, &HeaderTemplate::new(1, 1), 1).unwrap_err(),
            RestoreError::HeaderInvalid
        ));
    }

    #[test]
    fn effect_and_timer_records_classify_by_their_own_ids() {
        let expected = HeaderTemplate::new(1, 1);
        for (format_id, kind) in [
            (FORMAT_ID_EFFECT_RECORD, RecordKind::EffectRecord),
            (FORMAT_ID_TIMER_OPS, RecordKind::TimerOps),
        ] {
            let template = HeaderTemplate {
                format_id,
                ..expected
            };
            let stamped = decode_header(&template.stamp(b"body")).unwrap();
            assert_eq!(
                resolve_record(&stamped, &expected, 1).unwrap(),
                (kind, SchemaDecision::Decode)
            );
        }
    }

    /// A snapshot resolves through the same table as a change record — the
    /// gap that let a bumped version restore transposed state.
    #[test]
    fn a_snapshot_header_resolves_like_a_change_record() {
        let v1 = HeaderTemplate::new(1, 1);
        let stamped = decode_header(&v1.stamp(b"state")).unwrap();
        let v2 = HeaderTemplate::new(1, 2);

        assert!(matches!(
            resolve_state(&stamped, &v2, 2).unwrap_err(),
            RestoreError::SchemaTooOld {
                found: 1,
                min_readable: 2
            }
        ));
        assert_eq!(
            resolve_state(&stamped, &v2, 1).unwrap(),
            SchemaDecision::Migrate { from_version: 1 }
        );
    }

    #[test]
    fn decode_short_buffer_is_header_invalid() {
        let err = decode_header(&[0u8; 8]).unwrap_err();
        assert!(matches!(err, RestoreError::HeaderInvalid));
    }
}
