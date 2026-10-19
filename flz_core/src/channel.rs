//! Channel runtime kernel: interned ids, descriptors, consumer edges and
//! their erased closures, backpressure and retry policies. The typed
//! authoring surface (`Channel<T>`, the builder DSL) lives in the SDK.

use std::any::Any;
use std::sync::Arc;

use flz_transport::errors::FlezhaError;

use crate::error::Code;

pub type ChannelId = u32;
pub type TaskletId = u32;

/// What an edge's transform yields for one value: nothing, one value, or a
/// fan-out. Only the fan-out pays a `Vec`.
pub enum EdgeYield {
    Drop,
    One(Arc<dyn Any + Send + Sync>),
    Many(Vec<Arc<dyn Any + Send + Sync>>),
}

impl IntoIterator for EdgeYield {
    type Item = Arc<dyn Any + Send + Sync>;
    type IntoIter = EdgeYieldIter;

    fn into_iter(self) -> EdgeYieldIter {
        EdgeYieldIter(match self {
            EdgeYield::Drop => YieldState::Done,
            EdgeYield::One(v) => YieldState::One(v),
            EdgeYield::Many(vs) => YieldState::Many(vs.into_iter()),
        })
    }
}

pub struct EdgeYieldIter(YieldState);

enum YieldState {
    Done,
    One(Arc<dyn Any + Send + Sync>),
    Many(std::vec::IntoIter<Arc<dyn Any + Send + Sync>>),
}

impl Iterator for EdgeYieldIter {
    type Item = Arc<dyn Any + Send + Sync>;

    fn next(&mut self) -> Option<Self::Item> {
        match std::mem::replace(&mut self.0, YieldState::Done) {
            YieldState::Done => None,
            YieldState::One(v) => Some(v),
            YieldState::Many(mut rest) => {
                let next = rest.next();
                self.0 = YieldState::Many(rest);
                next
            }
        }
    }
}

/// One edge's transforms and merge mapping, composed into one erased closure.
/// MUST be pure: it runs on live delivery and again on replay. Takes the
/// `Arc` so a surviving `filter` costs a refcount bump, not a clone.
pub type EdgeTransformFn =
    Arc<dyn Fn(&Arc<dyn Any + Send + Sync>) -> EdgeYield + Send + Sync + 'static>;

/// One edge's `.key_by(f)`, evaluated on the post-transform value. Total:
/// it never declines. Pure, as [`EdgeTransformFn`] is.
pub type EdgeKeyFn = Arc<dyn Fn(&dyn Any) -> String + Send + Sync + 'static>;

/// One consumer subscription on a channel; a plain `.into::<Task>()` carries
/// neither closure.
#[derive(Clone)]
pub struct ConsumerEdge {
    pub tasklet: TaskletId,
    pub transform: Option<EdgeTransformFn>,
    pub key: Option<EdgeKeyFn>,
}

impl ConsumerEdge {
    /// Apply the transform, or `Ok(None)` when there is none. Contained: an
    /// unwind in author code becomes an error, never a dead routing thread.
    pub fn transform_value(
        &self,
        value: &Arc<dyn Any + Send + Sync>,
    ) -> Result<Option<EdgeYield>, FlezhaError> {
        let Some(f) = self.transform.as_ref() else {
            return Ok(None);
        };
        contain("transform", || f(value)).map(Some)
    }

    /// The key of the post-transform value, or `Ok(None)` when the edge
    /// declares none. Contained like the transform.
    pub fn key_of(&self, value: &dyn Any) -> Result<Option<String>, FlezhaError> {
        let Some(f) = self.key.as_ref() else {
            return Ok(None);
        };
        contain("key_by", || f(value)).map(Some)
    }
}

/// Run one edge closure, turning an unwind into an error.
fn contain<R>(what: &str, f: impl FnOnce() -> R) -> Result<R, FlezhaError> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).map_err(|panic| {
        Code::EdgeClosurePanicked.with([
            ("closure", what.into()),
            ("reason", crate::util::format_panic(panic).into()),
        ])
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelKind {
    Entry,
    InterTasklet,
    PluginBound,
}

impl ChannelKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ChannelKind::Entry => "entry",
            ChannelKind::InterTasklet => "inter_tasklet",
            ChannelKind::PluginBound => "plugin_bound",
        }
    }
}

/// What a channel discards when the consumer's mailbox is full. The default
/// is `DropNothing` for every kind; a plugin-bound channel ignores it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverflowPolicy {
    /// Discards nothing; ordering is preserved.
    #[default]
    DropNothing,
    /// Discards the arriving item.
    DropNewest,
    /// Discards the queued head; best-effort under contention.
    DropOldest,
}

impl OverflowPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            OverflowPolicy::DropNothing => "drop_nothing",
            OverflowPolicy::DropNewest => "drop_newest",
            OverflowPolicy::DropOldest => "drop_oldest",
        }
    }
}

/// Retry-then-park policy for a failing invocation: after `max_attempts`, or
/// at once on a non-retryable error, the payload parks and the cell
/// advances.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts, first run included.
    pub max_attempts: u32,
    /// Attempt n backs off `base * 2^(n-1)` ±50 %, capped at
    /// [`RETRY_BACKOFF_MAX_MS`].
    pub backoff_base_ms: u64,
}

pub const RETRY_BACKOFF_MAX_MS: u64 = 5_000;

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff_base_ms: 50,
        }
    }
}

/// The channel's plugin egress (`sink(...)`): with consumers an emit reaches
/// both; without, the channel is the plugin-bound terminal. It carries no
/// name of its own — a sink names the channel it terminates, so the one name
/// is [`ChannelDescriptor::name`] and there is no second copy to disagree.
pub struct PluginSink {
    pub serializer: PluginSerializeFn,
}

/// A `call(...)` end: where each request goes, how long its caller waits, and
/// the entry channel the outcome comes back on. A typed host also keeps here
/// the chain's transform and key, applied to a value before it becomes a
/// request as an edge applies its own.
#[derive(Clone)]
pub struct CallEnd {
    /// `id:<instance or group>` or `factory:<factory id>`.
    pub target: Arc<str>,
    pub create: bool,
    pub deadline_ms: u64,
    /// The callee's entry channel the request enters on.
    pub channel: Option<Arc<str>>,
    /// The callee's channel whose output answers; its first output when absent.
    pub answer_channel: Option<Arc<str>>,
    /// The caller's entry channel each outcome enters on.
    pub response: Arc<str>,
    pub transform: Option<EdgeTransformFn>,
    pub key: Option<EdgeKeyFn>,
}

impl CallEnd {
    /// The requests one emitted value makes, each with its key: the chain's
    /// transform applied as an edge applies it, then its key. Contained like
    /// an edge's closures. A host that ships bytes has applied both itself.
    pub fn requests(&self, value: &Arc<dyn Any + Send + Sync>) -> Result<Requests, FlezhaError> {
        let edge = ConsumerEdge {
            tasklet: 0,
            transform: self.transform.clone(),
            key: self.key.clone(),
        };
        let values: Vec<_> = match edge.transform_value(value)? {
            Some(yielded) => yielded.into_iter().collect(),
            None => vec![Arc::clone(value)],
        };
        values
            .into_iter()
            .map(|value| {
                let key = edge.key_of(value.as_ref())?;
                Ok((value, key))
            })
            .collect()
    }

    /// The metadata every request of this end carries: the target, whether
    /// it may create its callee (only when it may not), the Response channel,
    /// the wall-clock millisecond its call expires (`now_ms` plus the
    /// deadline), the callee's entry and answer channels when named, and the
    /// request's key when it has one.
    pub fn stamps(&self, key: Option<&str>, now_ms: u64) -> Vec<(String, String)> {
        let mut stamps = vec![
            (CALL_TARGET_KEY.to_string(), self.target.to_string()),
            (CALL_RESPONSE_KEY.to_string(), self.response.to_string()),
            (
                DEADLINE_AT_KEY.to_string(),
                now_ms.saturating_add(self.deadline_ms).to_string(),
            ),
        ];
        if !self.create {
            stamps.push((CALL_CREATE_KEY.to_string(), "false".to_string()));
        }
        if let Some(channel) = &self.channel {
            stamps.push((CALL_CHANNEL_KEY.to_string(), channel.to_string()));
        }
        if let Some(channel) = &self.answer_channel {
            stamps.push((CALL_ANSWER_CHANNEL_KEY.to_string(), channel.to_string()));
        }
        if let Some(key) = key.filter(|key| !key.is_empty()) {
            stamps.push(("flz.key".to_string(), key.to_string()));
        }
        stamps
    }
}

/// The requests one emitted value makes, each with the key it was given.
pub type Requests = Vec<(Arc<dyn Any + Send + Sync>, Option<String>)>;

/// The metadata keys a call end stamps; the controller reads them to deliver
/// the request and to answer it.
pub const CALL_TARGET_KEY: &str = "flz.call";
pub const CALL_CREATE_KEY: &str = "flz.call_create";
pub const CALL_RESPONSE_KEY: &str = "flz.call_response";
pub const CALL_CHANNEL_KEY: &str = "flz.call_channel";
pub const CALL_ANSWER_CHANNEL_KEY: &str = "flz.call_answer_channel";
pub const DEADLINE_AT_KEY: &str = "flz.deadline_at";

pub struct ChannelDescriptor {
    pub name: Arc<str>,
    pub kind: ChannelKind,
    pub producer: Option<TaskletId>,
    pub consumers: Vec<ConsumerEdge>,
    /// Wire bytes into the typed payload; `Some` on entry channels only.
    pub wrap_input: Option<EntryWrapFn>,
    /// The entry's `wrap_input` is a real parser: a failing decode rides the
    /// ladder rather than the passthrough drop-and-log path.
    pub entry_parsed: bool,
    /// The plugin egress; hybrid or terminal is read off `consumers`.
    pub plugin_sink: Option<PluginSink>,
    /// The channel's plugin end is a sink: the one end a part previews and
    /// whose outputs answer the call their input came from.
    pub sink: bool,
    /// The channel's plugin end is a `call(...)`: every output is a request
    /// to another relay, stamped with the end's facts, and never reaches a
    /// plugin or a listener.
    pub call: Option<CallEnd>,
    /// `.keyed()`: plugin emits require a key and carry `flz.*` stamps.
    pub keyed: bool,
    pub overflow_policy: OverflowPolicy,
    pub retry: RetryPolicy,
    /// Byte codec for inter-tasklet payloads, derived by the runtime and
    /// declared by no SDK. `Some` exactly when `kind` is
    /// [`ChannelKind::InterTasklet`], because that is the channel the journal
    /// reads back: a durable structured steps relay re-seeds a resumed tasklet
    /// through it, and nothing re-seeds from an entry or a plugin-bound
    /// terminal.
    pub codec: Option<ChannelCodec>,
}

impl ChannelDescriptor {
    pub fn consumer_ids(&self) -> impl Iterator<Item = TaskletId> + '_ {
        self.consumers.iter().map(|e| e.tasklet)
    }
}

/// The byte codec of an inter-tasklet channel's payload.
#[derive(Clone)]
pub struct ChannelCodec {
    pub encode: CodecEncodeFn,
    pub decode: EntryWrapFn,
}

pub type CodecEncodeFn = Arc<dyn Fn(&dyn Any) -> Result<Vec<u8>, String> + Send + Sync + 'static>;

pub type EntryWrapFn =
    Arc<dyn Fn(Vec<u8>) -> Result<Arc<dyn Any + Send + Sync>, String> + Send + Sync + 'static>;

pub type PluginSerializeFn = Arc<dyn Fn(&dyn Any) -> Vec<u8> + Send + Sync + 'static>;

pub type ErasedFn<R> = Arc<dyn Fn(&dyn Any) -> R + Send + Sync + 'static>;

pub type CellsForFn = ErasedFn<Vec<&'static str>>;

pub type SerializeStateFn =
    Arc<dyn Fn(&Box<dyn Any + Send>) -> Result<serde_json::Value, String> + Send + Sync + 'static>;

pub type DeserializeStateFn =
    Arc<dyn Fn(&serde_json::Value) -> Result<Box<dyn Any + Send>, String> + Send + Sync + 'static>;

pub type CellBuilder =
    Box<dyn Fn(BuildCellArgs<'_>) -> Arc<crate::cell::CellSlot> + Send + Sync + 'static>;

pub struct BuildCellArgs<'a> {
    pub topology: Arc<crate::relay::topology::RelayTopology>,
    pub instance: std::sync::Weak<crate::relay::instance::RelayInstance>,
    /// The relay INSTANCE id, which keys the WAL.
    pub instance_id: Arc<str>,
    pub tasklet_id: TaskletId,
    pub cell_key: Option<&'a str>,
    pub cell_index: usize,
    pub keeps_state: bool,
    /// The instance's processed prefix, `None` for a steps relay.
    pub prefix: Option<Arc<crate::prefix::PrefixLedger>>,
    /// The instance's stop flag ([`crate::relay::instance::RelayInstance::removed`]).
    pub relay_stopped: Arc<std::sync::atomic::AtomicBool>,
}
