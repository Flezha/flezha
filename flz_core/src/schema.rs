//! The versioned Activation/ActivationResult boundary contract.
//!
//! Everything here is postcard-encoded — field order IS the wire contract, so
//! reordering a field is a schema break and must bump [`SCHEMA_VERSION`].
//! Shapes shared with the controller wire (`TimerOp`, `DlqEntry`) are
//! field-identical to the layouts `Flezha.State.Wire` already decodes.
//!
//! Payloads at this boundary are always bytes. The in-process engine may carry
//! richer host values (a same-process Rust host passes `Arc` payloads without
//! serialization); those never cross this schema, which is what the C ABI and
//! every fixture pin.

use serde::{Deserialize, Serialize};

/// Bumped on any incompatible change to the types in this module. A core and
/// a host shim negotiate at context creation and refuse to start on mismatch.
pub const SCHEMA_VERSION: u32 = 2;

pub use crate::channel::{ChannelId, ProcedureId};

/// One unit of work handed to the host. Targets exactly one entity (or, for
/// [`ActivationKind::KeyRequest`], one not-yet-keyed edge) and is exclusive:
/// the core never hands out a second activation for the same entity until
/// this one's [`ActivationResult`] arrives.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Activation {
    /// Correlation handle minted by the core; the completion echoes it.
    pub token: u64,
    pub target: EntityRef,
    pub kind: ActivationKind,
    /// The entity's durable change seq at handout (0 for pre-keying work).
    /// One input to the host's `ctx.stable_id()`; the host combines it with
    /// the entity identity and a per-invocation ordinal.
    pub change_seq: u64,
    /// The declared class this instance was built from. An out-of-process
    /// host resolves the activation to its own typed class through this —
    /// the in-process Rust host reads it off the slot's topology instead.
    pub factory: String,
}

/// The entity an activation is addressed to. `entity_key` is `None` only for
/// pre-keying work (`KeyRequest`), where the key is what's being computed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityRef {
    /// Operative instance id.
    pub operative: String,
    pub procedure: ProcedureId,
    pub entity_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ActivationKind {
    /// An ordered per-entity batch of inputs and timer firings.
    Work(Vec<WorkItem>),
    /// Fold a persisted change log into a live state; input activations for
    /// the entity are gated until this completes.
    RestoreFold(RestoreFold),
    /// Encode the current state for compaction. The core drained pending
    /// changes before handing this out and stamps `change_seq - 1` itself.
    SnapshotRequest,
    /// Serialize the current state to JSON for a live read; the core stamps
    /// the `flz.change_seq` frame metadata.
    LiveStateRead,
    /// Materialize a migrated-in state from its JSON form.
    ApplyState { state_json: Vec<u8> },
    /// Serialize the current state to JSON for a migration handover.
    MigrationSerialize,
    /// The batched prepare crossing: entry payloads whose consumers need a
    /// key and/or an extracted event time. The host answers one
    /// [`PrepareAnswer`] per payload, in order, before the core enqueues
    /// anything to an entity mailbox.
    KeyRequest(KeyRequest),
    /// A bounded run-sugar body: the seed args cross once; the host runs the
    /// imperative body (emit/recv/cancelled ride their own ABI calls) and
    /// completes with [`ActivationOutcome::BoundedDone`].
    BoundedRun { args: Vec<u8> },
    /// Saga compensation for one bounded entity: the recorded forward
    /// outputs cross; the host runs `compensate` and answers a work outcome
    /// whose single effect carries any undo outputs.
    CompensateRun { outputs: Vec<Vec<u8>> },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WorkItem {
    Input(InputItem),
    Timer(TimerItem),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InputItem {
    pub channel: ChannelId,
    pub payload: Vec<u8>,
    pub metadata: Vec<(String, String)>,
    /// Input-log offset consumed by this item (effectively-once tier).
    pub consumed_offset: Option<u64>,
    /// True when the controller is re-forwarding a journaled slice.
    pub replay: bool,
    /// The core-evaluated value of the entity's declared clock for this item.
    /// The host serves `ctx.now()` from this and never reads ambient time.
    pub now: u64,
}

/// A validated timer firing. Its `now` IS `scheduled_at` — a chained
/// `timer_after` anchors on the firing that woke it, replay-stable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerItem {
    pub kind: TimerKind,
    pub scheduled_at: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RestoreFold {
    /// Host-encoded state snapshot baseline, when one exists.
    pub snapshot: Option<Vec<u8>>,
    /// Host-encoded change records in seq order, to fold on top.
    pub changes: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyRequest {
    pub channel: ChannelId,
    pub payloads: Vec<Vec<u8>>,
    /// The host answers routing keys (a dynamic consumer needs exactly one;
    /// a static fan-out any number, empty = every declared entity).
    pub want_keys: bool,
    /// The host answers the payload's extracted event time.
    pub want_event_time: bool,
}

/// One payload's prepare answers, in `KeyRequest.payloads` order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrepareAnswer {
    /// Routing keys (`want_keys`): one for a dynamic consumer, any number
    /// for a static fan-out (empty = every declared entity).
    pub keys: Vec<String>,
    /// The extracted event time (`want_event_time`).
    pub event_time: Option<u64>,
}

/// The host's answer to an [`Activation`], addressed by token.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActivationResult {
    pub token: u64,
    pub outcome: ActivationOutcome,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ActivationOutcome {
    /// One effect per processed [`WorkItem`], in item order. May be shorter
    /// than the batch only when an item's verdict is an error — items after
    /// the failed one are re-handed by the core.
    Work(Vec<ItemEffect>),
    /// The fold completed; the host holds the materialized state.
    Restored,
    Snapshot {
        state: Vec<u8>,
    },
    StateJson {
        json: Vec<u8>,
    },
    StateApplied,
    MigrationState {
        json: Vec<u8>,
    },
    /// One outcome per `KeyRequest` payload, in order. A prepare failure
    /// enters the input channel's retry→park ladder.
    Prepared(Vec<Result<PrepareAnswer, WorkError>>),
    /// The whole activation failed before per-item processing (host-side
    /// panic equivalent). The core treats every item per the error's class.
    Failed(WorkError),
    /// A bounded run-sugar body returned. `completed` mirrors
    /// `ctx.complete(...)` having been called; `result` is its payload.
    BoundedDone { completed: bool, result: Vec<u8> },
}

/// The effect record of one processed item — the same shape the flusher
/// drains today (`EffectRecordBody`), plus the verdict.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ItemEffect {
    pub verdict: Verdict,
    /// Host-encoded change bytes, core-opaque.
    pub changes: Vec<Vec<u8>>,
    pub outputs: Vec<Output>,
    pub consumed_offset: Option<u64>,
    /// The item's declared-clock value (what `ctx.now()` served).
    pub event_time: u64,
    pub timers: Vec<TimerOp>,
    /// True when the host called `ctx.checkpoint()` — closes the open
    /// checkpoint interval (checkpoint ack-granularity classes).
    pub checkpoint: bool,
    /// Normally `None`; the core mints the DLQ entry when its ladder parks.
    pub dlq: Option<DlqEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict {
    Ok,
    RetryableError { message: String },
    NonRetryableError { message: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkError {
    pub message: String,
    pub non_retryable: bool,
}

/// One value produced by an item, already transformed and keyed for its
/// destination (producer-side evaluation — the core routes on `(edge, key)`
/// and never evaluates a host closure).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Output {
    pub target: OutputTarget,
    pub key: Option<String>,
    /// For an edge value into an event-time-clocked consumer: the consumer's
    /// extracted event time, evaluated producer-side (the host holds the
    /// typed value and the consumer's extractor; the core holds neither).
    pub event_time: Option<u64>,
    pub metadata: Vec<(String, String)>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutputTarget {
    /// A consumer edge: the value goes to `consumer`'s entity mailbox.
    Edge {
        channel: ChannelId,
        consumer: ProcedureId,
    },
    /// A plugin-bound channel: the value ships on the wire under `channel`.
    Plugin { channel: String },
}

/// One durable mutation of an entity's pending-timer set. Field order matches
/// the controller wire (`Flezha.State.Wire`) exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerOp {
    pub kind: TimerKind,
    pub scheduled_at: u64,
    /// `true` adds the timer to the pending set, `false` removes it.
    pub register: bool,
}

impl TimerOp {
    pub fn register(kind: TimerKind, scheduled_at: u64) -> Self {
        Self {
            kind,
            scheduled_at,
            register: true,
        }
    }

    pub fn cancel(kind: TimerKind, scheduled_at: u64) -> Self {
        Self {
            kind,
            scheduled_at,
            register: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TimerKind {
    EventTime,
    ProcessingTime,
}

impl TimerKind {
    pub fn as_str(self) -> &'static str {
        match self {
            TimerKind::EventTime => "event_time",
            TimerKind::ProcessingTime => "processing_time",
        }
    }
}

/// Dead-letter entry carried inside an effect record. Field order matches the
/// controller wire exactly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DlqEntry {
    pub channel: String,
    pub error: String,
    pub attempts: u32,
    pub payload: Option<Vec<u8>>,
}

pub fn encode_activation(a: &Activation) -> Vec<u8> {
    postcard::to_stdvec(a).expect("activation encoding is infallible")
}

pub fn decode_activation(bytes: &[u8]) -> Result<Activation, postcard::Error> {
    postcard::from_bytes(bytes)
}

pub fn encode_result(r: &ActivationResult) -> Vec<u8> {
    postcard::to_stdvec(r).expect("activation-result encoding is infallible")
}

pub fn decode_result(bytes: &[u8]) -> Result<ActivationResult, postcard::Error> {
    postcard::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activation_round_trips() {
        let a = Activation {
            token: 7,
            target: EntityRef {
                operative: "inst".into(),
                procedure: 2,
                entity_key: Some("k1".into()),
            },
            kind: ActivationKind::Work(vec![
                WorkItem::Input(InputItem {
                    channel: 1,
                    payload: b"p".to_vec(),
                    metadata: vec![("flz.key".into(), "k1".into())],
                    consumed_offset: Some(41),
                    replay: false,
                    now: 1000,
                }),
                WorkItem::Timer(TimerItem {
                    kind: TimerKind::ProcessingTime,
                    scheduled_at: 2000,
                }),
            ]),
            change_seq: 3,
            factory: "f".into(),
        };
        let decoded = decode_activation(&encode_activation(&a)).unwrap();
        assert_eq!(a, decoded);
    }

    #[test]
    fn result_round_trips() {
        let r = ActivationResult {
            token: 7,
            outcome: ActivationOutcome::Work(vec![ItemEffect {
                verdict: Verdict::Ok,
                changes: vec![b"c".to_vec()],
                outputs: vec![Output {
                    target: OutputTarget::Edge {
                        channel: 3,
                        consumer: 4,
                    },
                    key: Some("k2".into()),
                    event_time: None,
                    metadata: vec![],
                    payload: b"v".to_vec(),
                }],
                consumed_offset: Some(41),
                event_time: 1000,
                timers: vec![TimerOp::register(TimerKind::EventTime, 5000)],
                checkpoint: false,
                dlq: None,
            }]),
        };
        let decoded = decode_result(&encode_result(&r)).unwrap();
        assert_eq!(r, decoded);
    }
}

#[cfg(test)]
mod fixture_tests {
    use super::*;

    /// Print-and-pin helper: the canonical work activation every shim's
    /// fixture suite encodes byte-identically.
    fn work_activation() -> Activation {
        Activation {
            token: 42,
            target: EntityRef {
                operative: "op-1".into(),
                procedure: 3,
                entity_key: Some("k".into()),
            },
            kind: ActivationKind::Work(vec![
                WorkItem::Input(InputItem {
                    channel: 1,
                    payload: vec![0xAA, 0xBB],
                    metadata: vec![("flz.offset".into(), "7".into())],
                    consumed_offset: Some(7),
                    replay: true,
                    now: 1_000,
                }),
                WorkItem::Timer(TimerItem {
                    kind: TimerKind::EventTime,
                    scheduled_at: 2_000,
                }),
            ]),
            change_seq: 9,
            factory: "f".into(),
        }
    }

    fn result_fixture() -> ActivationResult {
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
            }]),
        }
    }

    /// The cross-language byte pin for schema version 1: every shim's fixture
    /// suite encodes these exact values and must produce these exact bytes.
    /// A mismatch here means the schema changed — bump [`SCHEMA_VERSION`].
    #[test]
    fn work_activation_bytes_are_pinned() {
        assert_eq!(
            hex(&encode_activation(&work_activation())),
            "2a046f702d310301016b0002000102aabb010a666c7a2e6f66667365740137010701e8070100d00f090166"
        );
        assert_eq!(
            hex(&encode_result(&result_fixture())),
            "2a0001000101010101036f757401026b32000001020107e8070101d00f000100"
        );
        // And they decode back to the same values (shims decode activations,
        // the core decodes results).
        let act = work_activation();
        assert_eq!(decode_activation(&encode_activation(&act)).unwrap(), act);
        let res = result_fixture();
        assert_eq!(decode_result(&encode_result(&res)).unwrap(), res);
    }

    /// Every command activation kind round-trips; their encodings are pinned
    /// so a shim can hard-code the same vectors.
    #[test]
    fn command_kind_bytes_are_pinned() {
        let kinds: Vec<(ActivationKind, &str)> = vec![
            (
                ActivationKind::RestoreFold(RestoreFold {
                    snapshot: Some(vec![0x0F]),
                    changes: vec![vec![0x01], vec![0x02]],
                }),
                "0101010f0201010102000166",
            ),
            (ActivationKind::SnapshotRequest, "02000166"),
            (ActivationKind::LiveStateRead, "03000166"),
            (
                ActivationKind::ApplyState {
                    state_json: b"{}".to_vec(),
                },
                "04027b7d000166",
            ),
            (ActivationKind::MigrationSerialize, "05000166"),
            (
                ActivationKind::KeyRequest(KeyRequest {
                    channel: 2,
                    payloads: vec![b"p".to_vec()],
                    want_keys: true,
                    want_event_time: false,
                }),
                "06020101700100000166",
            ),
            (
                ActivationKind::BoundedRun {
                    args: b"go".to_vec(),
                },
                "0702676f000166",
            ),
            (
                ActivationKind::CompensateRun {
                    outputs: vec![vec![0x0A]],
                },
                "0801010a000166",
            ),
        ];
        for (kind, expected) in kinds {
            let act = Activation {
                token: 1,
                target: EntityRef {
                    operative: "o".into(),
                    procedure: 0,
                    entity_key: None,
                },
                kind,
                change_seq: 0,
                factory: "f".into(),
            };
            let bytes = encode_activation(&act);
            // The envelope prefix (token 1, target o/0/None) is 5 bytes:
            // 01 01 6f 00 00.
            assert_eq!(hex(&bytes[..5]), "01016f0000");
            assert_eq!(hex(&bytes[5..]), expected);
            assert_eq!(decode_activation(&bytes).unwrap(), act);
        }
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}
