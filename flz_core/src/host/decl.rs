//! The host class declaration as the C ABI's JSON deserializes it: the serde
//! types and their defaults. The key set is a cross-SDK contract pinned by
//! `test/fixtures/host_class_decl_keys.txt`; `deny_unknown_fields` makes a
//! key the core never learned a refusal rather than a silent degrade.

use crate::channel::RetryPolicy;

/// The declared clock; the core stamps `now` from it on every item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostClock {
    Wall,
    Logical,
    EventTime,
}

impl HostClock {
    /// The name refusals and timer errors print for this clock.
    pub fn name(self) -> &'static str {
        match self {
            HostClock::Wall => "wall",
            HostClock::Logical => "logical",
            HostClock::EventTime => "event_time",
        }
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostChannelDecl {
    pub name: String,
    pub kind: HostChannelKind,
    #[serde(default)]
    pub producer: Option<u32>,
    /// The authoring record: per handle, the ordered links the builder was
    /// given. Consumers, the channel kind and the plugin-sink name are
    /// DERIVED from these by [`super::register::check`], never declared.
    #[serde(default)]
    pub chains: Vec<Vec<HostLink>>,
    #[serde(default = "default_retry_max")]
    pub retry_max: u32,
    #[serde(default = "channel_backoff_ms")]
    pub retry_backoff_ms: u64,
    #[serde(default)]
    pub overflow: HostOverflow,
    #[serde(default)]
    pub keyed: bool,
}

/// One link of a fluent chain declared off a channel handle, in the order the
/// builder was called. A chain closed by `Into` or `Merge` wires an edge;
/// `Sink` terminates the channel on a plugin, `Response` on the caller's
/// answer; `Branch` records the sugar's arity and closes the chain it stands
/// in.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(tag = "link", rename_all = "snake_case")]
pub enum HostLink {
    Transform,
    Key,
    /// Overrides the derived channel name for every terminator that follows.
    Named {
        name: String,
    },
    /// Terminates the channel on the plugin layer, and names it: the string
    /// an author writes here IS the channel's name. Absent for the
    /// no-argument `sink()`, which leaves the name derived as it would be
    /// without a sink at all.
    Sink {
        #[serde(default)]
        name: Option<String>,
    },
    /// Terminates the channel on the answer to the outside call its input
    /// carried. Takes no name: nothing outside the operator addresses it.
    Response,
    /// Terminates the chain on a call to another operator: every value that
    /// reaches it, after the chain's transforms and key, is one request. The
    /// outcome enters the operator on the entry channel `response` names.
    Call(HostCall),
    /// Closes the chain onto a consumer edge into a tasklet index.
    Into {
        tasklet: u32,
    },
    /// Closes the chain as one source of a merge; `group` ties the sources of
    /// one merge together across the channels they are declared on.
    Merge {
        tasklet: u32,
        group: u32,
    },
    /// The `branch(...)` sugar's declared arity.
    Branch {
        arms: usize,
    },
}

/// A `call(...)` end as the builder recorded it: exactly one of `target` (an
/// instance or a partition group) and `factory`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct HostCall {
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub factory: Option<String>,
    pub deadline_ms: u64,
    /// A factory call starts the instance its key names when none holds it.
    #[serde(default = "default_create")]
    pub create: bool,
    /// The callee's entry channel the request enters on; its first when absent.
    #[serde(default)]
    pub channel: Option<String>,
    /// The caller's entry channel each outcome arrives on.
    pub response: String,
}

fn default_create() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HostChannelKind {
    Entry,
    InterTask,
    Plugin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostOverflow {
    #[default]
    DropNothing,
    DropNewest,
    DropOldest,
}

fn default_retry_max() -> u32 {
    RetryPolicy::default().max_attempts
}
fn channel_backoff_ms() -> u64 {
    RetryPolicy::default().backoff_base_ms
}
fn default_mailbox_capacity() -> usize {
    1024
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostTaskletDecl {
    pub name: String,
    #[serde(default)]
    pub cell_mode: HostCellModeDecl,
    #[serde(default)]
    pub cell_keys: Vec<String>,
    #[serde(default)]
    pub idle_ttl_ms: Option<u64>,
    #[serde(default)]
    pub max_cells: Option<usize>,
    /// The host overrides cells-for: entry payloads cross the prepare
    /// crossing so it can answer which keys an input fans out to.
    #[serde(default)]
    pub cells_for_overridden: bool,
    /// The host declares a `compensate` hook on this tasklet.
    #[serde(default)]
    pub compensate_overridden: bool,
    #[serde(default = "default_clock")]
    pub clock: HostClock,
    #[serde(default = "default_mailbox_capacity")]
    pub mailbox_capacity: usize,
    /// This tasklet's Change type is the empty one: it journals nothing, so
    /// a logical clock over it never advances. An erased host with no
    /// per-tasklet change type writes the class's own `!persistent`.
    #[serde(default)]
    pub change_is_nochange: bool,
    /// Stamped into every durable record this tasklet writes; per
    /// tasklet, never class-wide.
    #[serde(default = "default_decl_schema_version")]
    pub schema_version: u32,
    /// The oldest `schema_version` this tasklet still reads; defaults to
    /// its own.
    #[serde(default)]
    pub min_readable_schema_version: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostCellModeDecl {
    #[default]
    Single,
    Static,
    Dynamic,
}

fn default_clock() -> HostClock {
    HostClock::Wall
}

/// A host class declaration; what the core cannot honour is refused naming
/// the field, never degraded.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostClassDecl {
    pub factory_id: String,
    /// Change bytes in completions reach the per-Cell WAL.
    #[serde(default)]
    pub persistent: bool,
    /// An idle instance sleeps; the SDK resolves the kind's default.
    #[serde(default)]
    pub sleep_when_idle: bool,
    /// The language byte ([`crate::header`]); mandatory, since an untagged
    /// veneer's state could be handed to another language.
    pub sdk_lang: u8,
    /// The writing SDK's own released version, as that SDK's package metadata
    /// spells it (`"1.4.2"`). The core packs it, so every SDK's header carries
    /// one packing; a value that packs to `0` is refused.
    #[serde(default)]
    pub sdk_version: String,
    /// The format byte of the host's change bodies.
    #[serde(default = "default_change_format")]
    pub change_format_id: u8,
    #[serde(default)]
    pub effectively_once: bool,
    /// The author's explicit `checkpoint_ack()`. The granularity itself is
    /// DERIVED: any checkpoint key implies it.
    #[serde(default)]
    pub checkpoint_ack: bool,
    #[serde(default)]
    pub checkpoint_every: Option<u32>,
    #[serde(default)]
    pub checkpoint_outputs: HostCheckpointOutputs,
    #[serde(default)]
    pub checkpoint_max_delay_ms: Option<u64>,
    #[serde(default)]
    pub correction_window_ms: Option<u64>,
    #[serde(default)]
    pub beyond_window_policy: HostBeyondWindow,
    #[serde(default)]
    pub compact_after_bytes: Option<usize>,
    /// The controller ships each commit barrier to the durability backend
    /// before acking it; bounded classes only.
    #[serde(default)]
    pub sync_durability: bool,
    pub channels: Vec<HostChannelDecl>,
    pub tasklets: Vec<HostTaskletDecl>,
    /// Present for a bounded class: its loop and rollback facts over the
    /// tasklets declared above.
    #[serde(default)]
    pub bounded: Option<HostBoundedDecl>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostBoundedDecl {
    #[serde(default)]
    pub loop_mode: HostLoopMode,
    #[serde(default)]
    pub rollback_mode: HostRollbackMode,
    #[serde(default)]
    pub max_duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostLoopMode {
    #[default]
    Once,
    PerInput,
    Chained,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostRollbackMode {
    #[default]
    AllOrNothing,
    KeepProgress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HostAckGranularity {
    #[default]
    PerInput,
    Checkpoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostCheckpointOutputs {
    #[default]
    AsProduced,
    AtCheckpoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostBeyondWindow {
    #[default]
    ProcessAsArrival,
    Drop,
}

fn default_decl_schema_version() -> u32 {
    1
}
fn default_change_format() -> u8 {
    crate::header::FORMAT_ID_POSTCARD
}
