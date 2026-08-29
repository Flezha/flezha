//! `OperativeTopology`: the immutable, registration-time facts of a class —
//! its channels, procedures and tier — shared by every instance.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::channel::{
    ChannelDescriptor, ChannelId, DeserializeStateFn, EntitiesForFn, EntityBuilder, ProcedureId,
    SerializeStateFn,
};

/// How a Procedure maps inputs to Entities: a declared keyset is `Static`, a
/// keyed edge into it `Dynamic`, neither `Single`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityMode {
    Single,
    Static,
    /// Entities minted on first sight of a key.
    Dynamic,
}

/// One Procedure's registration-time facts; channel ids pre-resolved so the
/// hot path looks no name up.
pub struct ProcedureDescriptor {
    pub name: Arc<str>,
    pub entity_keys: Vec<&'static str>,
    pub entities_for_overridden: bool,
    /// The workflow driver compensates only a procedure with this set.
    pub compensate_overridden: bool,
    pub mode: EntityMode,
    /// Dynamic-mode idle eviction window; `None` never evicts.
    pub idle_ttl: Option<Duration>,
    /// Dynamic-mode cap on live Entities.
    pub max_entities: usize,
    /// Channels this procedure consumes; several is a fan-in.
    pub input_channel_ids: Vec<ChannelId>,
    pub output_channel_id: Option<ChannelId>,
    pub input_type_id: std::any::TypeId,
    pub output_type_id: std::any::TypeId,
    /// Per-Entity mailbox capacity after override / env / default precedence.
    pub mailbox_capacity: usize,
    /// Whether the class declared its capacity explicitly.
    pub mailbox_capacity_overridden: bool,
    /// The per-Entity WAL size the controller compacts at, announced in the
    /// `AddOperative` response.
    pub compact_after_bytes: usize,
    /// `Some` for a host-run procedure: the declared facts read before any
    /// entity exists.
    pub host_facts: Option<std::sync::Arc<crate::host::HostProcedureFacts>>,
    pub build_entity: EntityBuilder,
    pub entities_for_erased: EntitiesForFn,
    /// An Entity's state as the live-read / migration JSON.
    pub serialize_state: SerializeStateFn,
    /// The JSON back into the erased state `EntitySlot::state` holds.
    pub deserialize_state: DeserializeStateFn,
}

/// The compaction threshold default, in lockstep with the controller's
/// `:default_compact_after_bytes`; the `AddOperative` metadata omits it when
/// equal.
pub const DEFAULT_COMPACT_AFTER_BYTES: usize = 8 * 1024 * 1024;

pub struct OperativeTopology {
    /// The SDK language ([`crate::header`]), stamped at byte 0 of every
    /// durable record and advertised so placement stays within one language.
    pub sdk_lang: u8,
    pub channels: Vec<ChannelDescriptor>,
    pub name_to_id: HashMap<&'static str, ChannelId>,
    pub procedures: Vec<ProcedureDescriptor>,
    pub procedure_name_to_id: HashMap<&'static str, ProcedureId>,
    pub factory_id: &'static str,
    /// The class's declared persistence; the instance resolves the runner
    /// override against it.
    pub persistence_enabled_const: bool,
    /// The effectively-once tier: emits ride the effect record on the
    /// reliable sink instead of the primary stream.
    pub effectively_once: bool,

    /// The late-input correction window; `None` never corrects.
    pub correction: Option<CorrectionPolicy>,

    /// Where the effectively-once tier acknowledges its inputs.
    pub ack_granularity: AckGranularity,
}

/// Where a tier-enabled operative's durable consumed-offset advances.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AckGranularity {
    /// One atomic record per invocation, each acknowledging its own input.
    #[default]
    PerInput,
    /// The offset advances only when the interval closes: its changes ship
    /// as one record with the closing input's offset, so state and offset
    /// never tear; the interval's inputs are re-forwarded on recovery.
    Checkpoint(CheckpointPolicy),
}

impl AckGranularity {
    pub fn checkpoint(self) -> Option<CheckpointPolicy> {
        match self {
            AckGranularity::PerInput => None,
            AckGranularity::Checkpoint(policy) => Some(policy),
        }
    }

    pub fn is_checkpoint(self) -> bool {
        self.checkpoint().is_some()
    }
}

/// How a checkpoint-ack operative closes its interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CheckpointPolicy {
    /// Close after this many inputs; `None` leaves `ctx.checkpoint()` the
    /// only closer.
    pub every: Option<u32>,
    pub outputs: CheckpointOutputs,
    /// Close this long after the opening input; always set under
    /// [`CheckpointOutputs::AtCheckpoint`], so an unbounded hold cannot be
    /// declared.
    pub max_delay_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CheckpointOutputs {
    /// Flushed as produced, each in a record carrying no consumed offset.
    #[default]
    AsProduced,
    /// Held and shipped inside the checkpoint record: one append per
    /// interval, at up to `max_delay_ms` of output latency.
    AtCheckpoint,
}

/// What an operative does about an input behind its event-time high-water.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CorrectionPolicy {
    /// How far behind an input may be and still be repaired; bounds the
    /// rewind, the slice, and the retention the controller guarantees.
    pub window_ms: u64,
    pub beyond_window: BeyondWindowPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BeyondWindowPolicy {
    /// Process it as if it had just happened — the default, so declaring a
    /// window never silently starts discarding.
    #[default]
    ProcessAsArrival,
    /// Discard it: for when a stale event is worse than a missing one.
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
