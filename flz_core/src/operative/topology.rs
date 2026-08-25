//! `OperativeTopology`: shared, immutable topology produced by
//! `define_stream::<R>()`. Section 2 implements `build_topology`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::channel::{
    ChannelDescriptor, ChannelId, DeserializeStateFn, EntitiesForFn, EntityBuilder, ProcedureId,
    SerializeStateFn,
};

/// How a Procedure maps inputs to Entities. Resolved once at registration from
/// the Procedure's own `entity_keys()` and from the WIRING — whether any
/// consumer edge into it carries a `.key_by(...)`: non-empty keyset →
/// `Static`, keyed edge → `Dynamic`, neither → `Single`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityMode {
    /// One Entity per Procedure; no keying of any kind.
    Single,
    /// Fixed keyset declared by `entity_keys()`; routed by `entities_for`.
    Static,
    /// Stream keyset; Entities created lazily on first sight of the key the
    /// consumer edge's `.key_by(...)` returns.
    Dynamic,
}

/// One Procedure's slot in the topology. Pre-resolved channel ids so the
/// hot path needs zero name lookups.
pub struct ProcedureDescriptor {
    pub name: Arc<str>,
    pub entity_keys: Vec<&'static str>,
    pub entities_for_overridden: bool,
    /// `Procedure::COMPENSATE_OVERRIDDEN`, captured at registration. The workflow
    /// saga driver fires compensation only for procedures with this set; a
    /// stream operative never reads it.
    pub compensate_overridden: bool,
    /// Entity mode resolved at registration. Drives whether the
    /// dispatch path routes by static key, single slot, or the dynamic
    /// registry.
    pub mode: EntityMode,
    /// Dynamic-mode idle-TTL eviction window (`Procedure::entity_idle_ttl`).
    /// `None` (the default) means no eviction; meaningful only when
    /// `mode == Dynamic`.
    pub idle_ttl: Option<Duration>,
    /// Dynamic-mode soft cap on live Entities (`Procedure::max_entities`).
    pub max_entities: usize,
    /// Channels this procedure consumes. Several = fan-in (the node receives
    /// the accumulated contributions of all its producers); empty only for an
    /// procedure never wired as a consumer.
    pub input_channel_ids: Vec<ChannelId>,
    pub output_channel_id: Option<ChannelId>,
    pub input_type_id: std::any::TypeId,
    pub output_type_id: std::any::TypeId,
    /// Final per-Entity mailbox capacity for every Entity this
    /// Procedure owns, after applying the override / env-var / default
    /// precedence in `operative::builder::resolve_mailbox_capacity`.
    pub mailbox_capacity: usize,
    /// `Procedure::MAILBOX_CAPACITY_OVERRIDDEN` captured at registration time.
    /// Carried for diagnostics so a future tooling change can show which
    /// Procedures opted into an explicit capacity.
    pub mailbox_capacity_overridden: bool,
    /// `Procedure::compact_after_bytes()` captured at registration time —
    /// the per-Entity WAL size threshold the controller resolves for
    /// compaction, announced in the `AddOperative` response metadata.
    pub compact_after_bytes: usize,
    /// `Some` for host-run (C-ABI) procedures: the declared facts the
    /// prepare crossing and the completion path read before any entity
    /// exists. Typed procedures carry `None`.
    pub host_facts: Option<std::sync::Arc<crate::host::HostProcedureFacts>>,
    pub build_entity: EntityBuilder,
    pub entities_for_erased: EntitiesForFn,
    /// JSON-serialize an Entity's typed state via the per-Procedure fn
    /// pointer captured at registration time. Used by `EntityState`
    /// and `MigrationSerialization` to produce the Decision-10 wire blob.
    pub serialize_state: SerializeStateFn,
    /// Deserialize a per-Procedure JSON value into the type-erased state
    /// payload that `EntitySlot::state` holds. Used by
    /// `apply_init_values` (and `MigrationDeserialization`) to seed
    /// Entities from the controller-supplied JSON blob.
    pub deserialize_state: DeserializeStateFn,
}

pub struct OperativeTopology {
    /// The SDK language behind this class, from the allocation table in
    /// [`crate::header`]. Stamped at byte 0 of every durable record the class
    /// writes and advertised on the factory, so the controller places an
    /// operative only on a runner that can read its state.
    pub sdk_lang: u8,
    pub channels: Vec<ChannelDescriptor>,
    pub name_to_id: HashMap<&'static str, ChannelId>,
    pub procedures: Vec<ProcedureDescriptor>,
    pub procedure_name_to_id: HashMap<&'static str, ProcedureId>,
    pub factory_id: &'static str,
    /// Captured `StreamOperative::PERSISTENCE_ENABLED` const at registration.
    /// `StreamInstance::new` resolves the final per-instance flag using
    /// this const + the runner override from operative-start metadata.
    pub persistence_enabled_const: bool,
    /// Effectively-once delivery tier, declared via
    /// `ChannelBuilder::effectively_once()`. Frozen at registration;
    /// announced to the controller in the `AddOperative` response metadata
    /// and drives the tier emit path (emits ride the reliable state sink
    /// inside the atomic effect record instead of the primary stream).
    pub effectively_once: bool,

    /// `ChannelBuilder::correction_window()`. `None` (the default) means the
    /// operative never corrects: a late input is just an input. Declared, it
    /// announces the window to the controller in the `AddOperative` response
    /// metadata and arms lateness detection on the dispatch path.
    pub correction: Option<CorrectionPolicy>,

    /// Where the effectively-once tier acknowledges its inputs, declared via
    /// `ChannelBuilder::checkpoint_ack()`. Frozen at registration; announced
    /// to the controller in the `AddOperative` response metadata.
    pub ack_granularity: AckGranularity,
}

/// Where a tier-enabled operative's durable consumed-offset advances.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AckGranularity {
    /// One atomic record per effectful invocation, each acknowledging its own
    /// input. The default, and the only granularity for an operative whose
    /// state is kernel-visible after every input.
    #[default]
    PerInput,
    /// The consumed-offset advances only when the interval closes. Changes
    /// applied in between are held in the Entity's flush buffer and ship —
    /// with the offset of the input that closed the interval — as one fsynced
    /// record, so the state a restore sees and the offset it resumes from can
    /// never tear. The interval's inputs are re-forwarded from the input log
    /// on recovery and its outputs re-emitted; both are absorbed by the
    /// keyed-sink dedup the tier already requires.
    Checkpoint(CheckpointPolicy),
}

impl AckGranularity {
    /// The checkpoint policy, or `None` under the per-input default.
    pub fn checkpoint(self) -> Option<CheckpointPolicy> {
        match self {
            AckGranularity::PerInput => None,
            AckGranularity::Checkpoint(policy) => Some(policy),
        }
    }

    /// Whether the consumed-offset waits for an interval to close.
    pub fn is_checkpoint(self) -> bool {
        self.checkpoint().is_some()
    }
}

/// How a checkpoint-ack operative closes its interval, and what the open
/// interval does with the outputs its invocations produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CheckpointPolicy {
    /// Close the interval once this many inputs have folded into it. `None`
    /// — the default — leaves `ctx.checkpoint()` its only closer.
    pub every: Option<u32>,
    /// What the open interval does with its outputs.
    pub outputs: CheckpointOutputs,
    /// Close the interval this long after the input that opened it, so a
    /// quiet Entity never holds an output indefinitely. Always set under
    /// [`CheckpointOutputs::AtCheckpoint`] — the surface that selects it
    /// takes the delay, so an unbounded hold cannot be declared.
    pub max_delay_ms: Option<u64>,
}

/// What an open checkpoint interval does with the outputs its invocations
/// produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CheckpointOutputs {
    /// Flushed as they are produced, each in a record carrying no
    /// consumed-offset: checkpointing moves the acknowledgement, not the
    /// output. The default, and byte-identical to the mode before the option
    /// existed.
    #[default]
    AsProduced,
    /// Held with the interval's changes and shipped inside its checkpoint
    /// record, which the controller flushes as one batch after that record's
    /// fsync — one WAL append per interval instead of one per invocation, at
    /// the cost of up to `max_delay_ms` of output latency.
    AtCheckpoint,
}

/// What an operative does about an input that arrives behind its Entity's
/// event-time high-water. Frozen at registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CorrectionPolicy {
    /// How far behind the high-water an input may be and still be repaired by
    /// a rewind-restore-replay. Bounds the WAL rewind, the input-log slice,
    /// and hence the retention the controller must guarantee.
    pub window_ms: u64,
    /// What happens to an input that is later than `window_ms` — the past is
    /// too old to rebuild.
    pub beyond_window: BeyondWindowPolicy,
}

/// Fate of a late input the correction window cannot reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BeyondWindowPolicy {
    /// Process it as if it had just happened — the engine's behaviour with no
    /// correction declared at all, so declaring a window never silently starts
    /// discarding data that used to be processed. The default for that reason.
    #[default]
    ProcessAsArrival,
    /// Discard it. Opt in only when a stale event is worse than a missing one.
    Drop,
}

impl OperativeTopology {
    pub fn lookup_channel_id(&self, name: &str) -> Option<ChannelId> {
        self.name_to_id.get(name).copied()
    }

    pub fn channel(&self, id: ChannelId) -> Option<&ChannelDescriptor> {
        self.channels.get(id as usize)
    }

    pub fn procedure(&self, id: ProcedureId) -> Option<&ProcedureDescriptor> {
        self.procedures.get(id as usize)
    }
}
