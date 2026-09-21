//! 16-byte version header for durable per-Cell records.
//!
//! Layout:
//!  - 0:1   `sdk_lang: u8` — the writing SDK, per the allocation table below
//!  - 1:5   `sdk_version: u32 BE` = (major << 16) | (minor << 8) | patch
//!  - 5:9   `schema_version: u32 BE` = the class's declared schema version
//!  - 9:10  `format_id: u8` = 1 (postcard) in v1
//!  - 10:12 reserved (zero)
//!  - 12:16 `crc32: u32 BE` = `crc32fast::hash(&change_bytes)`

use crate::util::read_u32_be;

/// Why a restore refused.
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
         {min_readable} the running tasklet declares"
    )]
    SchemaTooOld { found: u32, min_readable: u32 },
    #[error(
        "record stamped schema_version {found} is above the running tasklet's \
         {running}: older code cannot read newer state"
    )]
    SchemaFromTheFuture { found: u32, running: u32 },
    #[error("no migration from schema_version {from_version}: {reason}")]
    MigrationFailed { from_version: u32, reason: String },
    #[error("crc32 mismatch on change record (cell={cell}, seq={seq})")]
    CrcMismatch { cell: u32, seq: u64 },
}

/// The `sdk_lang` allocation table: durable bodies are per-SDK below the
/// shared envelope, so this byte keeps one language's records from another's
/// codec. One value per SDK, taken from `SDK_LANG_NEXT_FREE` as it lands; an
/// unknown value is refused, never interpreted.
pub const SDK_LANG_UNSPECIFIED: u8 = 0;
pub const SDK_LANG_RUST: u8 = 1;
pub const SDK_LANG_JAVA: u8 = 2;
/// Its own core, not a `flz_core` host; writes `FORMAT_ID_ETF` bodies.
pub const SDK_LANG_ELIXIR: u8 = 3;
pub const SDK_LANG_NODE: u8 = 4;
pub const SDK_LANG_RUBY: u8 = 5;
pub const SDK_LANG_PHP: u8 = 6;
pub const SDK_LANG_PYTHON: u8 = 7;
pub const SDK_LANG_NEXT_FREE: u8 = 8;
pub const SDK_LANG_RESERVED: u8 = 255;

/// Every allocated value, in allocation order.
pub const SDK_LANG_ALLOCATED: &[(u8, &str)] = &[
    (SDK_LANG_RUST, "rust"),
    (SDK_LANG_JAVA, "java"),
    (SDK_LANG_ELIXIR, "elixir"),
    (SDK_LANG_NODE, "node"),
    (SDK_LANG_RUBY, "ruby"),
    (SDK_LANG_PHP, "php"),
    (SDK_LANG_PYTHON, "python"),
];

pub fn sdk_lang_allocated(lang: u8) -> bool {
    SDK_LANG_ALLOCATED.iter().any(|(v, _)| *v == lang)
}

pub fn sdk_lang_name(lang: u8) -> &'static str {
    SDK_LANG_ALLOCATED
        .iter()
        .find(|(v, _)| *v == lang)
        .map(|(_, name)| *name)
        .unwrap_or("unallocated")
}

/// Format ids, stamped at byte 9: the body's codec, not its SDK.
pub const FORMAT_ID_POSTCARD: u8 = 1;

/// A postcard `EffectRecordBody` whose `changes` are host-encoded bodies.
pub const FORMAT_ID_EFFECT_RECORD: u8 = 2;

/// Java's Kryo codec.
pub const FORMAT_ID_KRYO: u8 = 4;

/// MessagePack, shared by every SDK whose codec it is; `sdk_lang` is the
/// cross-language gate.
pub const FORMAT_ID_MSGPACK: u8 = 5;

/// PHP's JSON codec.
pub const FORMAT_ID_JSON: u8 = 6;

/// Erlang External Term Format, the Elixir SDK's codec.
pub const FORMAT_ID_ETF: u8 = 7;

/// A postcard `Vec<TimerOp>`, stored and truncated as any change record.
pub const FORMAT_ID_TIMER_OPS: u8 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodedHeader {
    pub sdk_lang: u8,
    pub sdk_version: u32,
    pub schema_version: u32,
    pub format_id: u8,
    pub crc32: u32,
}

/// Header values frozen at construction so a stamp is a memcpy plus a crc.
#[derive(Debug, Clone, Copy)]
pub struct HeaderTemplate {
    pub sdk_lang: u8,
    pub sdk_version: u32,
    pub schema_version: u32,
    pub format_id: u8,
}

impl HeaderTemplate {
    pub fn new(sdk_version: u32, schema_version: u32) -> Self {
        Self {
            sdk_lang: SDK_LANG_RUST,
            sdk_version,
            schema_version,
            format_id: FORMAT_ID_POSTCARD,
        }
    }

    /// The header for `change_bytes`, crc included.
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

/// The header's fields; resolution against the running tasklet is
/// [`resolve_record`] / [`resolve_state`].
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

/// What a stamped `schema_version` means to the tasklet now running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaDecision {
    Decode,
    /// Older but readable: the author's migration from `from_version`.
    Migrate {
        from_version: u32,
    },
}

impl SchemaDecision {
    /// Reaches the migration — what a restore reports so the controller
    /// compacts.
    pub fn migrating(self) -> bool {
        matches!(self, Self::Migrate { .. })
    }
}

/// The one place a stamped schema version is resolved against the reading
/// tasklet. A floor above `running` reads the running version only.
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

/// Which record a WAL entry is, by its format byte against the running
/// tasklet's declared change format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordKind {
    Change,
    EffectRecord,
    TimerOps,
}

/// Classify and resolve one WAL record's header in one step; both restore
/// folds come through here.
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

/// Resolve a snapshot's header: the running tasklet's state in its format.
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

/// Language before anything else, or two SDKs sharing a codec would be told
/// their header is malformed.
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

/// A package version string as `(major << 16) | (minor << 8) | patch`, each
/// clamped at 255. The caller supplies the string so `env!` expands in the
/// crate whose version is wanted, never in this one.
pub fn pack_version(raw: &str) -> u32 {
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

    /// Two SDKs sharing a value would make the guard meaningless.
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
        assert_eq!(SDK_LANG_RUBY, 5);
        assert_eq!(SDK_LANG_PHP, 6);
        assert_eq!(SDK_LANG_PYTHON, 7);
        assert_eq!(sdk_lang_name(SDK_LANG_JAVA), "java");
        assert_eq!(sdk_lang_name(SDK_LANG_NODE), "node");
        assert_eq!(sdk_lang_name(SDK_LANG_RUBY), "ruby");
        assert_eq!(sdk_lang_name(SDK_LANG_PHP), "php");
        assert_eq!(sdk_lang_name(SDK_LANG_PYTHON), "python");
        assert_eq!(sdk_lang_name(SDK_LANG_NEXT_FREE), "unallocated");
    }

    /// A format id names the codec, not the SDK; two codecs on one byte
    /// would make a mixed WAL unreadable.
    #[test]
    fn format_ids_are_distinct() {
        let ids = [
            FORMAT_ID_POSTCARD,
            FORMAT_ID_EFFECT_RECORD,
            FORMAT_ID_TIMER_OPS,
            FORMAT_ID_KRYO,
            FORMAT_ID_MSGPACK,
            FORMAT_ID_JSON,
            FORMAT_ID_ETF,
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

    /// The four arms of the decision table.
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

    /// A Kryo-stamped record resolves for a Kryo class, not a postcard one.
    #[test]
    fn the_expected_format_comes_from_the_running_tasklet() {
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

    /// Every ordered pair of allocated languages is refused by both resolvers,
    /// each SDK stamping its own format: the SDKs that share a codec are
    /// separated by the language byte alone, and every refusal names the
    /// reader as expected and the writer as found.
    #[test]
    fn every_foreign_language_pair_is_refused_by_both_resolvers() {
        let format_of = |lang: u8| match lang {
            SDK_LANG_JAVA => FORMAT_ID_KRYO,
            SDK_LANG_NODE | SDK_LANG_RUBY | SDK_LANG_PYTHON => FORMAT_ID_MSGPACK,
            SDK_LANG_PHP => FORMAT_ID_JSON,
            _ => FORMAT_ID_POSTCARD,
        };
        let template = |lang: u8| HeaderTemplate {
            sdk_lang: lang,
            sdk_version: 1,
            schema_version: 1,
            format_id: format_of(lang),
        };
        for (writer, _) in SDK_LANG_ALLOCATED {
            let stamped = decode_header(&template(*writer).stamp(b"body")).unwrap();
            assert_eq!(
                resolve_record(&stamped, &template(*writer), 1).unwrap(),
                (RecordKind::Change, SchemaDecision::Decode)
            );
            for (reader, _) in SDK_LANG_ALLOCATED {
                if reader == writer {
                    continue;
                }
                let refusals = [
                    resolve_record(&stamped, &template(*reader), 1).unwrap_err(),
                    resolve_state(&stamped, &template(*reader), 1).unwrap_err(),
                ];
                for refusal in refusals {
                    assert!(
                        matches!(
                            refusal,
                            RestoreError::LangMismatch { expected, found }
                                if expected == *reader && found == *writer
                        ),
                        "{writer} read by {reader}: {refusal:?}"
                    );
                }
            }
        }
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

    /// A snapshot resolves through the same table as a change record.
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
