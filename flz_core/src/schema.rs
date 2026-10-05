//! The versioned Activation / ActivationResult boundary contract, postcard
//! encoded. The invariant: field order IS the wire — reordering a field bumps
//! [`SCHEMA_VERSION`] — and every payload at this boundary is bytes.

use serde::{Deserialize, Serialize};

/// Negotiated at context creation; a mismatch refuses to start.
pub const SCHEMA_VERSION: u32 = 3;

pub use crate::channel::{ChannelId, TaskletId};

/// One unit of work handed to the host, exclusive on its cell until its
/// [`ActivationResult`] arrives.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Activation {
    pub token: u64,
    pub target: CellRef,
    pub kind: ActivationKind,
    /// The cell's change seq at handout, an input to `ctx.stable_id()`.
    pub change_seq: u64,
    /// The declared class, by which a host resolves its own typed class.
    pub factory: String,
}

/// The cell an activation is addressed to; `cell_key` is `None` only for
/// pre-keying work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CellRef {
    pub relay: String,
    pub tasklet: TaskletId,
    pub cell_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ActivationKind {
    Work(Vec<WorkItem>),
    /// Fold a persisted change log into a live state.
    RestoreFold(RestoreFold),
    /// Encode the state for compaction; the core drained and stamps the seq.
    SnapshotRequest,
    /// The state as JSON for a live read.
    LiveStateRead,
    ApplyState {
        state_json: Vec<u8>,
    },
    MigrationSerialize,
    /// The prepare crossing: one [`PrepareAnswer`] per payload, in order,
    /// before anything is enqueued.
    KeyRequest(KeyRequest),
    /// `compensate` over the recorded forward outputs; the answer's single
    /// effect carries any undo outputs.
    CompensateRun {
        outputs: Vec<Vec<u8>>,
    },
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
    pub consumed_offset: Option<u64>,
    pub replay: bool,
    /// The declared clock's value; the host never reads ambient time.
    pub now: u64,
}

/// A validated firing; its `now` IS `scheduled_at`, so a chained
/// `timer_after` is replay-stable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerItem {
    pub kind: TimerKind,
    pub scheduled_at: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RestoreFold {
    pub snapshot: Option<RestoreBody>,
    pub changes: Vec<RestoreBody>,
}

/// One host-encoded body for a restore, with the version it was written at
/// when that is not the running one; the host never decides readability.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RestoreBody {
    pub bytes: Vec<u8>,
    /// `Some(v)`: written at `v`, migrate from it.
    #[serde(default)]
    pub from_version: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyRequest {
    pub channel: ChannelId,
    pub payloads: Vec<Vec<u8>>,
    /// The host answers routing keys.
    pub want_keys: bool,
    pub want_event_time: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrepareAnswer {
    /// One for a dynamic consumer, any number for a static fan-out (empty =
    /// every declared cell).
    pub keys: Vec<String>,
    pub event_time: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActivationResult {
    pub token: u64,
    pub outcome: ActivationOutcome,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ActivationOutcome {
    /// One effect per processed item; shorter than the batch only after an
    /// error, the rest re-handed by the core.
    Work(Vec<ItemEffect>),
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
    /// One outcome per `KeyRequest` payload, in order.
    Prepared(Vec<Result<PrepareAnswer, WorkError>>),
    /// The whole activation failed before per-item processing.
    Failed(WorkError),
}

/// One processed item's effect record plus its verdict.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ItemEffect {
    pub verdict: Verdict,
    pub changes: Vec<Vec<u8>>,
    pub outputs: Vec<Output>,
    pub consumed_offset: Option<u64>,
    pub event_time: u64,
    pub timers: Vec<TimerOp>,
    /// The host called `ctx.checkpoint()`.
    pub checkpoint: bool,
    pub dlq: Option<DlqEntry>,
    /// The host called `ctx.complete(result)`: inside a steps relay the instance
    /// ends after the iteration commits; a stream refuses it.
    pub complete: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict {
    Ok,
    RetryableError {
        message: String,
    },
    NonRetryableError {
        message: String,
    },
    /// The host's entry decoder rejected the payload: only an entry channel's
    /// decoder answers this. A flow relay parks the item as a non-retryable error;
    /// a steps relay parks the message once the stage's ladder exhausts and goes
    /// on waiting instead of ending.
    Unreadable {
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkError {
    pub message: String,
    pub non_retryable: bool,
}

/// One output, already transformed and keyed producer-side; the core routes
/// on `(edge, key)` and evaluates no host closure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Output {
    pub target: OutputTarget,
    pub key: Option<String>,
    /// The consumer's extracted event time, evaluated producer-side.
    pub event_time: Option<u64>,
    pub metadata: Vec<(String, String)>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutputTarget {
    Edge {
        channel: ChannelId,
        consumer: TaskletId,
    },
    Plugin {
        channel: String,
    },
}

/// One mutation of the pending-timer set; field order is the controller
/// wire's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerOp {
    pub kind: TimerKind,
    pub scheduled_at: u64,
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

/// A dead-letter entry; field order is the controller wire's.
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

/// One [`Output`] on its own — what local mode hands a host for a value its
/// graph released on a sink, in the shape the host already decodes off a
/// completion.
pub fn encode_output(o: &Output) -> Vec<u8> {
    postcard::to_stdvec(o).expect("output encoding is infallible")
}

pub fn decode_output(bytes: &[u8]) -> Result<Output, postcard::Error> {
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
            target: CellRef {
                relay: "inst".into(),
                tasklet: 2,
                cell_key: Some("k1".into()),
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
                complete: Some(b"done".to_vec()),
            }]),
        };
        let decoded = decode_result(&encode_result(&r)).unwrap();
        assert_eq!(r, decoded);
    }
}

#[cfg(test)]
mod fixture_tests {
    use super::*;
    use crate::util::hex;

    /// The canonical work activation every shim's fixture suite pins.
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
                complete: None,
            }]),
        }
    }

    /// The cross-language byte pin; a mismatch means the schema changed.
    #[test]
    fn work_activation_bytes_are_pinned() {
        assert_eq!(
            hex(&encode_activation(&work_activation())),
            "2a046f702d310301016b0002000102aabb010a666c7a2e6f66667365740137010701e8070100d00f090166"
        );
        assert_eq!(
            hex(&encode_result(&result_fixture())),
            "2a0001000101010101036f757401026b32000001020107e8070101d00f00010000"
        );
        // And they decode back to the same values (shims decode activations,
        // the core decodes results).
        let act = work_activation();
        assert_eq!(decode_activation(&encode_activation(&act)).unwrap(), act);
        let res = result_fixture();
        assert_eq!(decode_result(&encode_result(&res)).unwrap(), res);
    }

    /// Every command activation kind round-trips at pinned bytes.
    #[test]
    fn command_kind_bytes_are_pinned() {
        let kinds: Vec<(ActivationKind, &str)> = vec![
            (
                ActivationKind::RestoreFold(RestoreFold {
                    snapshot: Some(RestoreBody {
                        bytes: vec![0x0F],
                        from_version: None,
                    }),
                    changes: vec![
                        RestoreBody {
                            bytes: vec![0x01],
                            from_version: None,
                        },
                        // A body the host must migrate: pinned so a shim
                        // reproduces the tagged shape, not only the plain one.
                        RestoreBody {
                            bytes: vec![0x02],
                            from_version: Some(1),
                        },
                    ],
                }),
                "0101010f000201010001020101000166",
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
                ActivationKind::CompensateRun {
                    outputs: vec![vec![0x0A]],
                },
                "0701010a000166",
            ),
        ];
        for (kind, expected) in kinds {
            let act = Activation {
                token: 1,
                target: CellRef {
                    relay: "o".into(),
                    tasklet: 0,
                    cell_key: None,
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
}
