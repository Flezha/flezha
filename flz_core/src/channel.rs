//! Channel runtime kernel: interned ids, descriptors, consumer edges and
//! their erased closures, backpressure and retry policies. The typed
//! authoring surface (`Channel<T>`, the builder DSL) lives in the SDK.

use std::any::Any;
use std::sync::Arc;

pub type ChannelId = u32;
pub type ProcedureId = u32;

/// What one consumer edge's transform yields for one input value: nothing (a
/// `filter` rejection), the common single reshaped value, or a fan-out
/// (`flat_map`). `Drop` and `One` allocate nothing beyond the value itself;
/// only the fan-out pays a `Vec`.
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

/// The values one [`EdgeYield`] delivers. Allocation-free for `Drop` and
/// `One` — only the fan-out already owns a `Vec`.
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

/// One consumer edge's value transform: every `map` / `filter` / `flat_map`
/// and every merge mapping declared before the closing `.into::<Proc>()`,
/// composed into a single closure. Erased from the typed closures the author
/// wrote; the caller guarantees the value's type because the chain was
/// declared against that channel's payload type.
///
/// The transform MUST be pure — it runs on live delivery and again on WAL
/// replay, and the two must agree — the same contract entry parsers and
/// output serializers carry.
/// Takes the value behind its `Arc` rather than a bare `&dyn Any` so a
/// surviving `filter` yields `One(Arc::clone(..))` — a refcount bump, not a
/// payload clone.
pub type EdgeTransformFn =
    Arc<dyn Fn(&Arc<dyn Any + Send + Sync>) -> EdgeYield + Send + Sync + 'static>;

/// One consumer edge's Entity key function, declared with `.key_by(f)` and
/// evaluated on the **post-transform** value. Total by signature: it names an
/// Entity, it never declines — discarding an input is `filter`'s job. Carries
/// the same purity contract as [`EdgeTransformFn`].
pub type EdgeKeyFn = Arc<dyn Fn(&dyn Any) -> String + Send + Sync + 'static>;

/// One consumer subscription on a channel. A plain `.into::<Proc>()` carries
/// neither closure (the channel's payload IS the consumer's `Input`, and the
/// consumer's Entity mode is its own); a chain of transforms, a `.key_by(f)`,
/// or a `merge::<I>().from(stream, map)` subscription stores them here.
#[derive(Clone)]
pub struct ConsumerEdge {
    pub procedure: ProcedureId,
    pub transform: Option<EdgeTransformFn>,
    pub key: Option<EdgeKeyFn>,
}

impl ConsumerEdge {
    /// Apply this edge's transform, or `Ok(None)` when the payload already IS
    /// the consumer's input type (the transform-free case pays nothing).
    ///
    /// Contained: the closure is author code evaluated on the routing thread
    /// (and, on a workflow chain, on the driver thread), so an unwind becomes an
    /// error the caller's ladder handles rather than a dead thread.
    pub fn transform_value(
        &self,
        value: &Arc<dyn Any + Send + Sync>,
    ) -> Result<Option<EdgeYield>, String> {
        let Some(f) = self.transform.as_ref() else {
            return Ok(None);
        };
        contain("transform", || f(value)).map(Some)
    }

    /// Evaluate this edge's key function on the post-transform value, or
    /// `Ok(None)` when the edge declares none. Contained like the transform.
    pub fn key_of(&self, value: &dyn Any) -> Result<Option<String>, String> {
        let Some(f) = self.key.as_ref() else {
            return Ok(None);
        };
        contain("key_by", || f(value)).map(Some)
    }
}

/// Run one edge closure, turning an unwind into the error its caller's
/// retry→DLQ ladder (stream) or abort-and-compensate path (workflow) reads.
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

/// What a channel discards when the consumer's bounded mailbox is full,
/// applied at the producer-side push site. Declared via `.on_full(...)` on
/// the channel builder; the default is `DropNothing` for every kind, so
/// nothing is discarded unless the operative declared it. Plugin-bound
/// channels have no SDK-side mailbox and ignore this value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OverflowPolicy {
    /// Discards nothing: the producer's rate is coupled to the consumer's
    /// until room appears. Ordering is preserved.
    #[default]
    DropNothing,
    /// Discards the arriving item; consumer's `drops_newest_total` increments.
    DropNewest,
    /// Discards the queued head: the consumer's mailbox atomically pops its
    /// oldest item and accepts the new one; consumer's `drops_oldest_total`
    /// increments. Best-effort under MPMC contention.
    DropOldest,
}

/// Per-channel retry policy applied by the consumer-side dispatcher when an
/// procedure invocation fails (returned error or panic). After
/// `max_attempts` total attempts — or immediately on an error the author
/// classified `ProcedureError::NonRetryable` — the payload is parked to the
/// controller's dead-letter queue and the entity advances. Declared via
/// `.retry_max(...)` / `.retry_backoff_ms(...)` on the channel builder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts (first run included). Default 3.
    pub max_attempts: u32,
    /// Exponential backoff base in milliseconds: attempt n sleeps
    /// `base * 2^(n-1)` (±50% jitter), capped at [`RETRY_BACKOFF_MAX_MS`].
    pub backoff_base_ms: u64,
}

/// Cap on the per-attempt retry backoff, jitter included.
pub const RETRY_BACKOFF_MAX_MS: u64 = 5_000;

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff_base_ms: 50,
        }
    }
}

/// The runtime form of `sink(...)`: the channel's plugin egress. On a
/// channel that also has consumers a single emit reaches both the consumer
/// mailboxes (typed) and the named plugin (serialized to bytes); on one with
/// no consumers the channel is the plugin-bound terminal.
pub struct PluginSink {
    pub name: Arc<str>,
    pub serializer: PluginSerializeFn,
}

pub struct ChannelDescriptor {
    pub name: Arc<str>,
    pub kind: ChannelKind,
    pub producer: Option<ProcedureId>,
    /// Consumer subscriptions, each carrying its optional transform and key.
    pub consumers: Vec<ConsumerEdge>,
    /// For entry channels: convert wire bytes into the typed payload
    /// (`Arc<S::Input>`). For inter-procedure channels: `None` — payloads
    /// arrive already typed via `ctx.emit`. For plugin-bound channels:
    /// `None` — handled by the producer-side serializer.
    pub wrap_input: Option<EntryWrapFn>,
    /// Entry channels whose `wrap_input` is a real parser (typed JSON or a
    /// custom `input_with` parser): a failing decode rides this channel's
    /// retry→DLQ ladder instead of the passthrough drop-and-log path.
    pub entry_parsed: bool,
    /// The channel's plugin egress, declared with `sink(...)`. The one
    /// representation for both the hybrid and terminal shapes — which of the
    /// two a channel is, is read off `consumers`.
    pub plugin_sink: Option<PluginSink>,
    /// Output-identity opt-in (`.keyed()`): plugin emits require a natural
    /// key and carry `flz.*` identity stamps. Always paired with a plugin
    /// wire (validated at build).
    pub keyed: bool,
    /// Backpressure policy applied at the push site when the consumer's
    /// mailbox is full. Set at declaration time per channel kind, optionally
    /// overridden by `.on_full(...)`. Plugin-bound channels carry a
    /// placeholder value that is never consulted.
    pub overflow_policy: OverflowPolicy,
    /// Retry-then-park policy applied by this channel's consumer dispatchers.
    /// Plugin-bound channels have no SDK-side consumer and ignore this value.
    pub retry: RetryPolicy,
    /// Symmetric byte codec for inter-procedure payloads, declared via
    /// `.codec()` / `.codec_with(...)`. Required on every inter-proc channel of
    /// a durable structured workflow class so the stage handoff batch can be
    /// journaled at the proc-commit barrier and re-seeded on crash-resume
    /// (`workflow-proc-journal`). `None` elsewhere.
    pub codec: Option<ChannelCodec>,
    /// `Entry` channels: the interned cell recording when this channel last
    /// received a frame, resolved once here so the delivery path never looks
    /// a name up. `None` on every other kind — an inter-procedure or
    /// plugin-bound channel is app-visible at both ends and needs no observer.
    pub entry_observation: Option<Arc<crate::entry_input::EntryObservation>>,
}

impl ChannelDescriptor {
    /// The consuming procedures, edge closures stripped — what the graph-shape
    /// walks and the workflow DAG builder need.
    pub fn consumer_ids(&self) -> impl Iterator<Item = ProcedureId> + '_ {
        self.consumers.iter().map(|e| e.procedure)
    }

    /// Note that a frame reached this channel. A no-op off an entry channel.
    #[inline]
    pub fn observe_delivery(&self) {
        if let Some(observation) = &self.entry_observation {
            observation.stamp();
        }
    }
}

/// The observation cell an `Entry` channel named `name` stamps, `None` for
/// every other kind. The one place a descriptor's cell is resolved, so both
/// construction sites (the Rust builder and the C-ABI host declaration) agree.
pub fn entry_observation_for(
    kind: ChannelKind,
    name: &str,
) -> Option<Arc<crate::entry_input::EntryObservation>> {
    (kind == ChannelKind::Entry).then(|| crate::entry_input::observation_for(name))
}

/// Typed→bytes / bytes→typed codec pair for an inter-procedure channel's
/// payload. `encode` receives the type-erased collected value; `decode` is
/// the inter-proc sibling of the entry channel's `wrap_input`.
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
    /// The operative INSTANCE id (controller-assigned operative_id). The on-disk WAL
    /// is keyed by this, not the factory id, so two operatives of the same factory
    /// keep independent per-Entity state instead of colliding on one WAL.
    pub instance_id: Arc<str>,
    pub procedure_id: ProcedureId,
    pub entity_key: Option<&'a str>,
    /// Zero-based entity index within the Procedure. `0` for single-mode
    /// procedures; `0..entity_keys().len()` for static-mode procedures.
    pub entity_index: usize,
    /// Frozen-at-instance-start persistence flag, propagated from the
    /// `StreamInstance`. The `EntitySlot` carries this on its
    /// `persistence_enabled` field; the per-process flusher inspects it
    /// at drain time to decide whether to call `send_frame`.
    pub persistence_enabled: bool,
}
