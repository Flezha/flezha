//! `RelayTopology`: the immutable, registration-time facts of a class —
//! its channels, tasklets and tier — shared by every instance.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::channel::{
    CellBuilder, CellsForFn, ChannelDescriptor, ChannelId, ChannelKind, DeserializeStateFn,
    SerializeStateFn, TaskletId,
};

/// How a Tasklet maps inputs to Cells: a declared keyset is `Static`, a
/// keyed edge into it `Dynamic`, neither `Single`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellMode {
    Single,
    Static,
    /// Cells minted on first sight of a key.
    Dynamic,
}

impl CellMode {
    pub fn as_str(self) -> &'static str {
        match self {
            CellMode::Single => "single",
            CellMode::Static => "static",
            CellMode::Dynamic => "dynamic",
        }
    }
}

/// One Tasklet's registration-time facts; channel ids pre-resolved so the
/// hot path looks no name up.
pub struct TaskletDescriptor {
    pub name: Arc<str>,
    pub cell_keys: Vec<&'static str>,
    pub cells_for_overridden: bool,
    /// A steps run compensates only a tasklet with this set.
    pub compensate_overridden: bool,
    pub mode: CellMode,
    /// Dynamic-mode idle eviction window; `None` never evicts.
    pub idle_ttl: Option<Duration>,
    /// Dynamic-mode cap on live Cells.
    pub max_cells: usize,
    /// Channels this tasklet consumes; several is a fan-in.
    pub input_channel_ids: Vec<ChannelId>,
    pub output_channel_id: Option<ChannelId>,
    pub input_type_id: std::any::TypeId,
    pub output_type_id: std::any::TypeId,
    /// Per-Cell mailbox capacity after override / env / default precedence.
    pub mailbox_capacity: usize,
    /// Whether the class declared its capacity explicitly.
    pub mailbox_capacity_overridden: bool,
    /// The per-Cell WAL size the controller compacts at, announced in the
    /// `AddRelay` response.
    pub compact_after_bytes: usize,
    /// `Some` for a host-run tasklet: the declared facts read before any
    /// cell exists.
    pub host_facts: Option<std::sync::Arc<crate::host::HostTaskletFacts>>,
    pub build_cell: CellBuilder,
    pub cells_for_erased: CellsForFn,
    /// A Cell's state as the live-read / migration JSON.
    pub serialize_state: SerializeStateFn,
    /// The JSON back into the erased state `CellSlot::state` holds.
    pub deserialize_state: DeserializeStateFn,
}

/// The compaction threshold default, in lockstep with the controller's
/// `:default_compact_after_bytes`; the `AddRelay` metadata omits it when
/// equal.
pub const DEFAULT_COMPACT_AFTER_BYTES: usize = 8 * 1024 * 1024;

pub struct RelayTopology {
    /// The SDK language ([`crate::header`]), stamped at byte 0 of every
    /// durable record and advertised so placement stays within one language.
    pub sdk_lang: u8,
    pub channels: Vec<ChannelDescriptor>,
    pub name_to_id: HashMap<&'static str, ChannelId>,
    pub tasklets: Vec<TaskletDescriptor>,
    pub tasklet_name_to_id: HashMap<&'static str, TaskletId>,
    pub factory_id: &'static str,
    /// What the class is for, announced with its factory id.
    pub description: &'static str,
    /// Where the class keeps its work, and how long an idle instance waits
    /// before it sleeps.
    pub durable: crate::relay::settings::Durable,
    /// A wall-clock stop, for either run.
    pub max_duration: Option<std::time::Duration>,
    /// The effectively-once tier: emits ride the effect record on the
    /// reliable sink instead of the primary stream.
    pub effectively_once: bool,

    /// The late-input correction window; `None` never corrects.
    pub correction: Option<CorrectionPolicy>,

    /// Where the effectively-once tier acknowledges its inputs.
    pub ack_granularity: AckGranularity,

    /// `Some` for a class that runs in steps: its DAG and lifecycle facts.
    /// How a class runs is this field and nothing else.
    pub steps: Option<crate::steps::spec::StepsSpec>,

    /// What each channel announces in the relay's API, by channel id; a
    /// channel past its end announces the defaults. Read only when the
    /// runner announces its factories.
    pub api: Vec<ChannelApi>,
}

/// A channel's API facts as its host declared them: an entry's input, or the
/// value an output sends.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChannelApi {
    /// `json`, `text`, `bytes`, `msgpack` or `custom`; empty is `json`.
    pub codec: &'static str,
    /// The JSON Schema, serialised; empty for none.
    pub schema_json: String,
    pub description: String,
}

impl RelayTopology {
    /// How long an idle instance waits before it sleeps; `None` never.
    pub fn sleep_after(&self) -> Option<std::time::Duration> {
        self.durable.sleep_after(self.steps.is_some())
    }
}

// Manual: channels and tasklets hold closures. A summary is enough for test
// assertions (`expect`/`expect_err`) and diagnostics.
impl std::fmt::Debug for RelayTopology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelayTopology")
            .field("factory_id", &self.factory_id)
            .field("channels", &self.channels.len())
            .field("tasklets", &self.tasklets.len())
            .field("steps", &self.steps)
            .finish()
    }
}

/// Where a tier-enabled relay's durable consumed-offset advances.
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

/// How a checkpoint-ack relay closes its interval.
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

/// What a relay does about an input behind its event-time high-water.
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

/// What a channel name resolved to, for a caller that wants a channel of one
/// kind. Names are one namespace across every kind, so a lookup that only
/// asked "is this a declared name" would take a sink for an entry channel.
pub enum ChannelLookup<'a> {
    Found(ChannelId, &'a ChannelDescriptor),
    /// Declared, but of another kind — an entry name that is really a sink.
    WrongKind(ChannelKind),
    /// No channel of that name at all.
    Undeclared,
}

impl RelayTopology {
    /// Whether a Cell of this class settles every invocation into one effect
    /// record: under the effectively-once tier, and for a durable steps relay,
    /// whose stage commit names the records it stands for.
    pub fn records_effects(&self, keeps_state: bool) -> bool {
        self.effectively_once
            || (self.steps.is_some() && keeps_state && crate::runner::coordinator_installed())
    }

    pub fn lookup_channel_id(&self, name: &str) -> Option<ChannelId> {
        self.name_to_id.get(name).copied()
    }

    pub fn channel(&self, id: ChannelId) -> Option<&ChannelDescriptor> {
        self.channels.get(id as usize)
    }

    pub fn tasklet(&self, id: TaskletId) -> Option<&TaskletDescriptor> {
        self.tasklets.get(id as usize)
    }

    /// The declared channel of `kind` under `name`. The one resolution every
    /// caller submits to: a frame off the wire and a local-mode feed ask the
    /// same question and get the same answer.
    pub fn channel_of_kind(&self, name: &str, kind: ChannelKind) -> ChannelLookup<'_> {
        match self
            .lookup_channel_id(name)
            .and_then(|id| self.channel(id).map(|channel| (id, channel)))
        {
            Some((id, declared)) if declared.kind == kind => ChannelLookup::Found(id, declared),
            Some((_, declared)) => ChannelLookup::WrongKind(declared.kind),
            None => ChannelLookup::Undeclared,
        }
    }

    /// The first declared channel of `kind`, in declaration order — where a
    /// frame naming no channel goes.
    pub fn first_of_kind(&self, kind: ChannelKind) -> Option<(ChannelId, &ChannelDescriptor)> {
        self.channels
            .iter()
            .position(|c| c.kind == kind)
            .and_then(|idx| {
                self.channel(idx as ChannelId)
                    .map(|c| (idx as ChannelId, c))
            })
    }

    /// Each entry channel with the tasklets it feeds, in declaration order:
    /// the roots of a steps relay's chains.
    pub fn entry_consumers(&self) -> Vec<(ChannelId, Vec<TaskletId>)> {
        self.channels
            .iter()
            .enumerate()
            .filter(|(_, c)| c.kind == ChannelKind::Entry)
            .map(|(id, c)| {
                (
                    id as ChannelId,
                    c.consumers.iter().map(|e| e.tasklet).collect(),
                )
            })
            .collect()
    }

    /// The declared channel names of one kind, sorted, so every refusal and
    /// every park names the same set in the same order — and so the Elixir
    /// SDK, whose channels live in a map, can name it identically.
    pub fn declared_names(&self, kind: ChannelKind) -> Vec<&str> {
        let mut names: Vec<&str> = self
            .channels
            .iter()
            .filter(|c| c.kind == kind)
            .map(|c| &*c.name)
            .collect();
        names.sort_unstable();
        names
    }
}
