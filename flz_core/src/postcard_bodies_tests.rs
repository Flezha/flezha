//! `test/fixtures/postcard_bodies.txt`, held to the real structs: every row is
//! one value below encoded by postcard, and every value below is a row. The
//! controller's `Flezha.State.Wire` decoders and the Java and PHP codecs read
//! the same rows, so a field reordered here fails every runtime that reads it.

use std::collections::BTreeMap;
use std::fmt::Debug;

use serde::de::DeserializeOwned;
use serde::Serialize;

use super::*;
use crate::correction::CorrectionRequestBody;
use crate::effect::{EdgeRecord, EffectRecordBody, OutputRecord};
use crate::flusher::DlqEntryBody;
use crate::park::ParkBody;
use crate::util::hex;

const FIXTURE: &str = include_str!("../../test/fixtures/postcard_bodies.txt");

/// The value's bytes, after proving they decode back to it.
fn pinned<T: Serialize + DeserializeOwned + PartialEq + Debug>(value: T) -> String {
    let bytes = postcard::to_allocvec(&value).unwrap();
    assert_eq!(postcard::from_bytes::<T>(&bytes).unwrap(), value);
    hex(&bytes)
}

fn work_activation() -> Activation {
    Activation {
        token: 42,
        target: CellRef {
            relay: "op-1".into(),
            tasklet: 3,
            cell_key: Some("k".into()),
        },
        kind: ActivationKind::Work(vec![
            WorkItem::Input(InputItem {
                channel: 1,
                payload: vec![0xAA, 0xBB],
                metadata: vec![("flz.offset".into(), "7".into())],
                consumed_offset: Some(7),
                replay: true,
                now: 1_000,
                form: InputForm::Prepared(vec![vec![0x0C]]),
            }),
            WorkItem::Timer(TimerItem {
                kind: TimerKind::EventTime,
                scheduled_at: 2_000,
            }),
        ]),
        change_seq: 9,
        factory: "f".into(),
        cell_identity: Some(b"id".to_vec()),
    }
}

/// A command activation: token 1, addressed to `o`/0 with no key.
fn command(kind: ActivationKind) -> Activation {
    Activation {
        token: 1,
        target: CellRef {
            relay: "o".into(),
            tasklet: 0,
            cell_key: None,
        },
        kind,
        change_seq: 0,
        factory: "f".into(),
        cell_identity: None,
    }
}

fn result(complete: Option<&[u8]>) -> ActivationResult {
    ActivationResult {
        token: 42,
        outcome: ActivationOutcome::Work(vec![ItemEffect {
            verdict: Verdict::Ok,
            changes: vec![vec![0x01]],
            outputs: vec![Output {
                target: OutputTarget::Plugin {
                    channel: "out".into(),
                },
                key: Some("k2".into()),
                event_time: None,
                metadata: vec![],
                payload: vec![0x02],
            }],
            consumed_offset: Some(7),
            event_time: 1_000,
            timers: vec![TimerOp::cancel(TimerKind::ProcessingTime, 2_000)],
            checkpoint: true,
            dlq: None,
            complete: complete.map(<[u8]>::to_vec),
        }]),
    }
}

fn dlq_entry(payload: Option<Vec<u8>>) -> DlqEntryBody {
    DlqEntryBody {
        channel: "in".into(),
        error: "boom".into(),
        attempts: 3,
        payload,
    }
}

/// Every pinned value, keyed `<type>|<case>` as the fixture spells it.
fn cases() -> BTreeMap<String, String> {
    let restore = |bytes: u8, from_version| RestoreBody {
        bytes: vec![bytes],
        from_version,
    };
    let activations = [
        work_activation(),
        command(ActivationKind::RestoreFold(RestoreFold {
            snapshot: Some(restore(0x0F, None)),
            // The second change must be migrated, so a reader handles the
            // tagged shape and not only the plain one.
            changes: vec![restore(0x01, None), restore(0x02, Some(1))],
        })),
        command(ActivationKind::SnapshotRequest),
        command(ActivationKind::LiveStateRead),
        command(ActivationKind::ApplyState {
            state_json: b"{}".to_vec(),
        }),
        command(ActivationKind::KeyRequest(KeyRequest {
            channel: 2,
            payloads: vec![b"p".to_vec()],
            want_keys: true,
            want_event_time: false,
        })),
        command(ActivationKind::CompensateRun {
            outputs: vec![vec![0x0A]],
        }),
        command(ActivationKind::Leave { whole_relay: true }),
    ];
    let mut cases = BTreeMap::new();
    let mut pin = |key: &str, hex: String| assert!(cases.insert(key.to_string(), hex).is_none());

    // An activation's case is its kind's own name.
    for activation in activations {
        pin(
            &format!("activation|{}", activation.kind.as_str()),
            pinned(activation),
        );
    }
    pin("result|work", pinned(result(None)));
    pin("result|completing", pinned(result(Some(b"done"))));
    pin(
        "result|prepared",
        pinned(ActivationResult {
            token: 42,
            outcome: ActivationOutcome::Prepared(vec![
                Ok(PrepareAnswer {
                    values: vec![PreparedValue {
                        consumer: 1,
                        keys: vec!["a".into()],
                        bytes: vec![0x0B],
                    }],
                    event_time: Some(5),
                }),
                Err(WorkError {
                    message: "no".into(),
                    non_retryable: true,
                }),
            ]),
        }),
    );
    pin(
        "effect|outputs_edges_timer_register",
        pinned(EffectRecordBody {
            changes: vec![vec![0xAA]],
            outputs: vec![OutputRecord {
                channel: "out".into(),
                key: "k".into(),
                metadata: vec![("a".into(), "b".into())],
                payload: vec![1, 2],
            }],
            consumed_offset: Some(4711),
            dlq: None,
            event_time: 1_700_000_000_123,
            timers: vec![TimerOp::register(TimerKind::ProcessingTime, 31_000)],
            edges: vec![EdgeRecord {
                channel: 2,
                consumer: 1,
                origin: Some(4711),
                payload: vec![9],
            }],
        }),
    );
    pin(
        "effect|dead_letter_timer_cancel",
        pinned(EffectRecordBody {
            changes: vec![],
            outputs: vec![],
            consumed_offset: Some(9),
            dlq: Some(dlq_entry(None)),
            event_time: 5,
            timers: vec![TimerOp::cancel(TimerKind::EventTime, 2_000)],
            edges: vec![],
        }),
    );
    pin(
        "park|parked",
        pinned(ParkBody {
            park_id: "s#0".into(),
            origin_offset: Some(7),
            entry: dlq_entry(Some(vec![1, 2])),
            reply_to: Some("w:t".into()),
        }),
    );
    pin(
        "correction|request",
        pinned(CorrectionRequestBody {
            target_event_time: 1_700_000_000_123,
        }),
    );
    cases
}

#[test]
fn every_fixture_row_is_the_struct_encoded() {
    let rows: BTreeMap<String, String> = FIXTURE
        .lines()
        .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
        .map(|line| {
            let (key, hex) = line.rsplit_once('|').expect("a row is <type>|<case>|<hex>");
            (key.to_string(), hex.to_string())
        })
        .collect();
    let expected = cases();
    let missing: Vec<String> = expected
        .iter()
        .filter(|(key, hex)| rows.get(*key) != Some(*hex))
        .map(|(key, hex)| format!("{key}|{hex}"))
        .collect();
    let stale: Vec<&String> = rows
        .keys()
        .filter(|key| !expected.contains_key(*key))
        .collect();

    assert!(
        missing.is_empty() && stale.is_empty(),
        "test/fixtures/postcard_bodies.txt disagrees with the encoders.\n\
         Rows the encoders produce that the file lacks or spells differently:\n{}\n\
         Rows the file has that no case here produces: {stale:?}",
        missing.join("\n")
    );
}
