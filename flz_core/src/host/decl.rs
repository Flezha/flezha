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

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostChannelDecl {
    pub name: String,
    pub kind: HostChannelKind,
    #[serde(default)]
    pub producer: Option<u32>,
    #[serde(default)]
    pub consumers: Vec<u32>,
    #[serde(default = "default_retry_max")]
    pub retry_max: u32,
    #[serde(default = "channel_backoff_ms")]
    pub retry_backoff_ms: u64,
    #[serde(default)]
    pub overflow: HostOverflow,
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

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostProcedureDecl {
    pub name: String,
    #[serde(default)]
    pub entity_mode: HostEntityModeDecl,
    #[serde(default)]
    pub entity_keys: Vec<String>,
    #[serde(default)]
    pub idle_ttl_ms: Option<u64>,
    #[serde(default)]
    pub max_entities: Option<usize>,
    /// The host overrides entities-for: entry payloads cross the prepare
    /// crossing so it can answer which keys an input fans out to.
    #[serde(default)]
    pub entities_for_overridden: bool,
    /// The host declares a `compensate` hook on this procedure.
    #[serde(default)]
    pub compensate_overridden: bool,
    #[serde(default = "default_clock")]
    pub clock: HostClock,
    #[serde(default = "default_mailbox_capacity")]
    pub mailbox_capacity: usize,
    /// Stamped into every durable record this procedure writes; per
    /// procedure, never class-wide.
    #[serde(default = "default_decl_schema_version")]
    pub schema_version: u32,
    /// The oldest `schema_version` this procedure still reads; defaults to
    /// its own.
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

/// A host class declaration; what the core cannot honour is refused naming
/// the field, never degraded.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostClassDecl {
    pub factory_id: String,
    /// Change bytes in completions reach the per-Entity WAL.
    #[serde(default)]
    pub persistent: bool,
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
    #[serde(default)]
    pub ack_granularity: HostAckGranularity,
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
    /// before acking it; workflow classes only.
    #[serde(default)]
    pub sync_durability: bool,
    pub channels: Vec<HostChannelDecl>,
    pub procedures: Vec<HostProcedureDecl>,
    /// Present for a workflow class: run-sugar with no procedures, a
    /// structured chain with them.
    #[serde(default)]
    pub workflow: Option<HostWorkflowDecl>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostWorkflowDecl {
    #[serde(default)]
    pub loop_mode: HostLoopMode,
    #[serde(default)]
    pub rollback_mode: HostRollbackMode,
    /// Attempts of the per-entity ladder; 0 is fail-fast.
    #[serde(default = "default_workflow_retry_attempts")]
    pub retry_attempts: u32,
    #[serde(default = "workflow_backoff_ms")]
    pub retry_backoff_ms: u64,
    #[serde(default = "default_barrier_retry_attempts")]
    pub barrier_retry_attempts: u32,
    #[serde(default)]
    pub max_duration_ms: Option<u64>,
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
