//! Channel runtime kernel: interned ids, descriptors, consumer edges and
//! their erased closures, backpressure and retry policies. The typed
//! authoring surface (`Channel<T>`, the builder DSL) lives in the SDK.

use std::any::Any;
use std::sync::Arc;

pub type ChannelId = u32;
pub type ProcedureId = u32;

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

/// One consumer subscription on a channel; a plain `.into::<Proc>()` carries
/// neither closure.
#[derive(Clone)]
pub struct ConsumerEdge {
    pub procedure: ProcedureId,
    pub transform: Option<EdgeTransformFn>,
    pub key: Option<EdgeKeyFn>,
}

impl ConsumerEdge {
    /// Apply the transform, or `Ok(None)` when there is none. Contained: an
    /// unwind in author code becomes an error, never a dead routing thread.
    pub fn transform_value(
        &self,
        value: &Arc<dyn Any + Send + Sync>,
    ) -> Result<Option<EdgeYield>, String> {
        let Some(f) = self.transform.as_ref() else {
            return Ok(None);
        };
        contain("transform", || f(value)).map(Some)
    }

    /// The key of the post-transform value, or `Ok(None)` when the edge
    /// declares none. Contained like the transform.
    pub fn key_of(&self, value: &dyn Any) -> Result<Option<String>, String> {
        let Some(f) = self.key.as_ref() else {
            return Ok(None);
        };
        contain("key_by", || f(value)).map(Some)
    }
}

/// Run one edge closure, turning an unwind into an error.
fn contain<R>(what: &str, f: impl FnOnce() -> R) -> Result<R, String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
        .map_err(|panic| format!("edge {what} panicked: {}", crate::util::format_panic(panic)))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelKind {
    Entry,
    InterProcedure,
    PluginBound,
}

/// What a channel discards when the consumer's mailbox is full. The default
/// is `DropNothing` for every kind; a plugin-bound channel ignores it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OverflowPolicy {
    /// Discards nothing; ordering is preserved.
    #[default]
    DropNothing,
    /// Discards the arriving item.
    DropNewest,
    /// Discards the queued head; best-effort under contention.
    DropOldest,
}

/// Retry-then-park policy for a failing invocation: after `max_attempts`, or
/// at once on a non-retryable error, the payload parks and the entity
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
/// both; without, the channel is the plugin-bound terminal.
pub struct PluginSink {
    pub name: Arc<str>,
    pub serializer: PluginSerializeFn,
}

pub struct ChannelDescriptor {
    pub name: Arc<str>,
    pub kind: ChannelKind,
    pub producer: Option<ProcedureId>,
    pub consumers: Vec<ConsumerEdge>,
    /// Wire bytes into the typed payload; `Some` on entry channels only.
    pub wrap_input: Option<EntryWrapFn>,
    /// The entry's `wrap_input` is a real parser: a failing decode rides the
    /// ladder rather than the passthrough drop-and-log path.
    pub entry_parsed: bool,
    /// The plugin egress; hybrid or terminal is read off `consumers`.
    pub plugin_sink: Option<PluginSink>,
    /// `.keyed()`: plugin emits require a key and carry `flz.*` stamps.
    pub keyed: bool,
    pub overflow_policy: OverflowPolicy,
    pub retry: RetryPolicy,
    /// Byte codec for inter-procedure payloads; required on every inter-proc
    /// channel of a durable structured workflow, whose handoffs are journaled.
    pub codec: Option<ChannelCodec>,
}

impl ChannelDescriptor {
    pub fn consumer_ids(&self) -> impl Iterator<Item = ProcedureId> + '_ {
        self.consumers.iter().map(|e| e.procedure)
    }
}

/// The byte codec of an inter-procedure channel's payload.
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

pub type EntitiesForFn = ErasedFn<Vec<&'static str>>;

pub type SerializeStateFn =
    Arc<dyn Fn(&Box<dyn Any + Send>) -> Result<serde_json::Value, String> + Send + Sync + 'static>;

pub type DeserializeStateFn =
    Arc<dyn Fn(&serde_json::Value) -> Result<Box<dyn Any + Send>, String> + Send + Sync + 'static>;

pub type EntityBuilder =
    Box<dyn Fn(BuildEntityArgs<'_>) -> Arc<crate::entity::EntitySlot> + Send + Sync + 'static>;

pub struct BuildEntityArgs<'a> {
    pub topology: Arc<crate::operative::topology::OperativeTopology>,
    pub instance: std::sync::Weak<crate::operative::instance::StreamInstance>,
    /// The operative INSTANCE id, which keys the WAL.
    pub instance_id: Arc<str>,
    pub procedure_id: ProcedureId,
    pub entity_key: Option<&'a str>,
    pub entity_index: usize,
    pub persistence_enabled: bool,
}
