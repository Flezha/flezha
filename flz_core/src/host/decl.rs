//! The host class declaration as the C ABI's JSON deserializes it: the serde
//! types and their defaults. The key set is a cross-SDK contract pinned by
//! `test/fixtures/host_class_decl_keys.txt`; `deny_unknown_fields` makes a
//! key the core never learned a refusal rather than a silent degrade.

use crate::channel::RetryPolicy;

/// The declared clock of a host procedure — the core evaluates it and
/// stamps `now` on every handed-out item; the host serves `ctx.now()` from
/// that and never reads ambient time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostClock {
    Wall,
    Logical,
    EventTime,
}

/// A host channel as declared through the C ABI.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostChannelDecl {
    pub name: String,
    /// `entry` | `inter_proc` | `plugin`.
    pub kind: HostChannelKind,
    /// Producing procedure index (inter-proc and plugin channels).
    #[serde(default)]
    pub producer: Option<u32>,
    /// Consuming procedure indices (entry and inter-proc channels).
    #[serde(default)]
    pub consumers: Vec<u32>,
    #[serde(default = "default_retry_max")]
    pub retry_max: u32,
    #[serde(default = "channel_backoff_ms")]
    pub retry_backoff_ms: u64,
    #[serde(default)]
    pub overflow: HostOverflow,
    /// Plugin wire keying: emitted frames carry identity stamps + `flz.key`.
    #[serde(default)]
    pub keyed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostChannelKind {
    Entry,
    InterProc,
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

/// One procedure of a host class.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostProcedureDecl {
    pub name: String,
    /// `single` (default) | `static` (with `entity_keys`) | `dynamic`.
    #[serde(default)]
    pub entity_mode: HostEntityModeDecl,
    /// Static mode's declared keys.
    #[serde(default)]
    pub entity_keys: Vec<String>,
    /// Dynamic mode: idle eviction TTL (absent = eviction off).
    #[serde(default)]
    pub idle_ttl_ms: Option<u64>,
    /// Dynamic mode: lazy-creation cap (absent = stream).
    #[serde(default)]
    pub max_entities: Option<usize>,
    /// Whether the host overrides entities-for on this (static) procedure —
    /// when true, entry payloads cross the prepare crossing so the host can
    /// answer which declared keys an input fans out to.
    #[serde(default)]
    pub entities_for_overridden: bool,
    /// Whether the host declares a `compensate` hook on this procedure
    /// (workflow classes: drives the saga plan and the controller's
    /// `compensable_procs` announcement).
    #[serde(default)]
    pub compensate_overridden: bool,
    #[serde(default = "default_clock")]
    pub clock: HostClock,
    #[serde(default = "default_mailbox_capacity")]
    pub mailbox_capacity: usize,
    /// Stamped into every durable record this procedure writes (header byte
    /// 5..9). Per procedure: a class-wide value re-stamps a procedure's
    /// records with a version it never declared.
    #[serde(default = "default_decl_schema_version")]
    pub schema_version: u32,
    /// The oldest `schema_version` this procedure can still read. Defaults to
    /// its own `schema_version` — "reads only what it wrote". Below it, the
    /// host declares migrations for every version in between.
    #[serde(default)]
    pub min_readable_schema_version: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostEntityModeDecl {
    #[default]
    Single,
    Static,
    Dynamic,
}

fn default_clock() -> HostClock {
    HostClock::Wall
}

/// A host class declaration — the C ABI's JSON deserializes into this. A
/// declaration the core cannot honor is refused at declaration time with an
/// error naming the field — never accepted and degraded silently
/// (`deny_unknown_fields` covers the fields the core never learned).
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostClassDecl {
    pub factory_id: String,
    /// Whether change bytes in completions are flushed to the controller's
    /// per-Entity WAL. When false they are dropped (stateless class).
    #[serde(default)]
    pub persistent: bool,
    /// Header lang byte for durable records, from the allocation table in
    /// [`crate::header`]. Mandatory: a veneer that reaches placement untagged
    /// is a veneer whose state another language can be handed, so the core
    /// refuses the declaration rather than substituting a default.
    pub sdk_lang: u8,
    /// Header format byte the host's plain change bodies claim
    /// (postcard = 1, Kryo = 4, MessagePack = 5).
    #[serde(default = "default_change_format")]
    pub change_format_id: u8,
    /// Effectively-once delivery tier.
    #[serde(default)]
    pub effectively_once: bool,
    /// `per_input` (default) | `checkpoint`.
    #[serde(default)]
    pub ack_granularity: HostAckGranularity,
    /// Close a checkpoint interval once this many inputs have folded into it
    /// (absent = only an explicit checkpoint closes it).
    #[serde(default)]
    pub checkpoint_every: Option<u32>,
    /// `as_produced` (default) | `at_checkpoint`.
    #[serde(default)]
    pub checkpoint_outputs: HostCheckpointOutputs,
    /// Close a checkpoint interval this long after the input that opened it.
    #[serde(default)]
    pub checkpoint_max_delay_ms: Option<u64>,
    /// Late-input correction window (event-time classes).
    #[serde(default)]
    pub correction_window_ms: Option<u64>,
    /// `process_as_arrival` (default) | `drop`.
    #[serde(default)]
    pub beyond_window_policy: HostBeyondWindow,
    /// Compaction threshold declaration (absent = platform default).
    #[serde(default)]
    pub compact_after_bytes: Option<usize>,
    /// Synchronous remote durability (`remote-durability-tier`): the
    /// controller ships each commit barrier to the durability backend before
    /// acking it. Workflow classes only — a stream class commits at input
    /// rates, where a remote round trip per ack is not a guarantee anyone can
    /// keep, so declaring it there is refused rather than silently ignored.
    #[serde(default)]
    pub sync_durability: bool,
    pub channels: Vec<HostChannelDecl>,
    pub procedures: Vec<HostProcedureDecl>,
    /// Present for a workflow class. With empty `procedures` the class is
    /// run-sugar (one imperative body); with procedures it is a structured
    /// chain over the same channels graph.
    #[serde(default)]
    pub workflow: Option<HostWorkflowDecl>,
}

/// Workflow lifecycle facts of a host class.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostWorkflowDecl {
    #[serde(default)]
    pub loop_mode: HostLoopMode,
    #[serde(default)]
    pub rollback_mode: HostRollbackMode,
    /// Total attempts of the per-entity stage retry ladder (0 collapses to
    /// fail-fast).
    #[serde(default = "default_workflow_retry_attempts")]
    pub retry_attempts: u32,
    #[serde(default = "workflow_backoff_ms")]
    pub retry_backoff_ms: u64,
    /// Sends of one commit barrier before an unacked commit fails the run.
    #[serde(default = "default_barrier_retry_attempts")]
    pub barrier_retry_attempts: u32,
    #[serde(default)]
    pub max_duration_ms: Option<u64>,
    /// Durable wake deadline armed when a `per_input` loop parks.
    #[serde(default)]
    pub wake_after_ms: Option<u64>,
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

fn default_workflow_retry_attempts() -> u32 {
    3
}
fn workflow_backoff_ms() -> u64 {
    500
}
fn default_barrier_retry_attempts() -> u32 {
    3
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
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
