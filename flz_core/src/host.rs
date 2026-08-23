//! The erased host surface: what a non-Rust SDK declares and drives through
//! the C ABI (`flz_core_ffi`).
//!
//! A host class is a topology declared as data — no generics, no host
//! closures beyond the bytes-passthrough entry wrapper. Its entities carry a
//! [`BytesBuffer`] (host-encoded change bytes, core-framed) and a
//! [`HostDispatcher`] that answers the engine's metadata questions;
//! invocations never run in-process — the host polls
//! [`crate::poll_core::PollCore`], runs its own user code, and completes with
//! a [`crate::schema::ActivationResult`] this module applies.
//!
//! Full host capability: entity modes (single/static/dynamic with idle-TTL
//! eviction), every clock kind, the effectively-once and checkpoint
//! ack-granularity tiers, late-input correction, per-entity timers,
//! inter-procedure edges (producer-side evaluation, routed on `(edge, key)`),
//! the five command activation kinds (restore fold, snapshot, live read,
//! apply-state, migration serialize), the prepare crossing for keys/event
//! times, and workflow lifecycles (run-sugar sessions + structured chains
//! whose stage and compensation bodies cross as activations).

use std::any::{Any, TypeId};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::channel::{
    BuildEntityArgs, ChannelDescriptor, ChannelKind, OverflowPolicy, RetryPolicy,
};
use crate::entity::{EntityItem, EntitySlot, ItemDisposition, ProcedureDispatcher};
use crate::flusher::{EntityFlusher, FlushFrame, FlushableBuffer};
use crate::header::HeaderTemplate;
use crate::operative::instance::StreamInstance;
use crate::operative::topology::{
    AckGranularity, CheckpointOutputs, CorrectionPolicy, EntityMode, OperativeTopology,
    ProcedureDescriptor,
};
use crate::poll_core::{EngineActivation, EnginePayload, PollCore, WorkItem};
use crate::schema::{self, TimerOp};
use crate::timer::PendingTimers;
use crate::util::LockRecover;
use crate::workflow::{LoopMode, RollbackMode};

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
    #[serde(default = "default_retry_backoff_ms")]
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
    Block,
    DropNewest,
    DropOldest,
}

fn default_retry_max() -> u32 {
    RetryPolicy::default().max_attempts
}
fn default_retry_backoff_ms() -> u64 {
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
    /// Stamped into every durable record header (byte 5..9).
    #[serde(default = "default_decl_schema_version")]
    pub schema_version: u32,
    /// Whether change bytes in completions are flushed to the controller's
    /// per-Entity WAL. When false they are dropped (stateless class).
    #[serde(default)]
    pub persistent: bool,
    /// Header lang byte for durable records (Rust = 1, Java = 2).
    #[serde(default = "default_sdk_lang")]
    pub sdk_lang: u8,
    /// Header format byte the host's plain change bodies claim
    /// (postcard = 1, Kryo = 4).
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
    #[serde(default = "default_workflow_retry_backoff_ms")]
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
fn default_workflow_retry_backoff_ms() -> u64 {
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
fn default_sdk_lang() -> u8 {
    crate::header::SDK_LANG_RUST
}
fn default_change_format() -> u8 {
    crate::header::FORMAT_ID_POSTCARD
}

/// Parse and register a host class from its JSON declaration — the body of
/// `flz_core_declare_class`.
pub fn register_host_class_json(bytes: &[u8]) -> Result<(), String> {
    let decl: HostClassDecl = serde_json::from_slice(bytes)
        .map_err(|e| format!("class declaration is not valid JSON for the schema: {e}"))?;
    register_host_class(decl)
}

/// One entry payload as the host layer carries it between the wire, the
/// prepare crossing and the mailbox: the raw bytes plus everything the core
/// resolved about them. This is the `Arc<dyn Any>` value inside a host
/// class's `EntityPayload`.
pub struct HostPayload {
    pub bytes: Vec<u8>,
    /// The channel the value arrived on (entry) or was produced onto
    /// (inter-proc edge).
    pub channel: crate::channel::ChannelId,
    /// Routing keys: exactly one for a dynamic consumer, any number for a
    /// static fan-out (empty = every declared entity), unused for single.
    pub keys: Vec<String>,
    /// The extracted event time (event-time classes; from the prepare
    /// crossing for entry payloads, from the producing completion for
    /// inter-proc values).
    pub event_time: Option<u64>,
    /// Wire metadata worth carrying to the host, if any.
    pub metadata: Vec<(String, String)>,
}

/// Validate the declaration and build the class topology. The controller's
/// `AddOperative` then instantiates it exactly like a typed class.
pub fn register_host_class(decl: HostClassDecl) -> Result<(), String> {
    if decl.procedures.is_empty() && decl.workflow.is_none() {
        return Err("a host class declares at least one procedure".to_string());
    }
    if let Some(workflow) = &decl.workflow {
        if decl.procedures.is_empty() && !decl.channels.is_empty() {
            return Err(
                "a run-sugar workflow class (no procedures) declares no channels".to_string(),
            );
        }
        if decl.effectively_once {
            return Err("a workflow class cannot declare effectively_once".to_string());
        }
        if decl.correction_window_ms.is_some() {
            return Err("a workflow class cannot declare correction_window_ms".to_string());
        }
        if decl.sync_durability && !decl.persistent {
            return Err(
                "sync_durability requires persistent: there is no commit barrier to ship \
                 for a class that journals nothing"
                    .to_string(),
            );
        }
        if workflow.retry_attempts > 1 && workflow.retry_backoff_ms == 0 {
            return Err(format!(
                "workflow retry ladder ({} attempts) declares a zero backoff base; retries \
                 would re-run the failing entity in a hot loop",
                workflow.retry_attempts
            ));
        }
        for proc_decl in &decl.procedures {
            if proc_decl.entity_mode == HostEntityModeDecl::Dynamic {
                return Err(format!(
                    "workflow procedure `{}` declares dynamic entity mode, which workflow \
                     operatives do not support",
                    proc_decl.name
                ));
            }
        }
        if decl.procedures.is_empty() {
            // Run-sugar: one imperative body, no topology.
            let factory_id: &'static str = Box::leak(decl.factory_id.clone().into_boxed_str());
            crate::workflow::registry::register_class(
                factory_id,
                crate::workflow::spec::WorkflowSpec {
                    run: host_run_unreachable,
                    topology: None,
                    dag: Default::default(),
                    max_duration: workflow
                        .max_duration_ms
                        .map(std::time::Duration::from_millis),
                    rollback_mode: rollback_of(workflow.rollback_mode),
                    loop_mode: LoopMode::Once,
                    wake_after: workflow.wake_after_ms.map(std::time::Duration::from_millis),
                    retry: RetryPolicy {
                        max_attempts: workflow.retry_attempts,
                        backoff_base_ms: workflow.retry_backoff_ms,
                    },
                    barrier_retry_attempts: workflow.barrier_retry_attempts,
                    persistence_enabled: decl.persistent,
                    sync_durability: decl.sync_durability,
                    host: true,
                },
            );
            return Ok(());
        }
        if workflow.loop_mode != HostLoopMode::Once || workflow.wake_after_ms.is_some() {
            // Loops iterate a structured chain; validated below with the DAG.
        }
    }
    if decl.sync_durability && decl.workflow.is_none() {
        return Err(format!(
            "class `{}` declares sync_durability, which only a workflow class may: a stream \
             class acknowledges at input rates, and a remote round trip per acknowledgement is \
             not a guarantee it can keep",
            decl.factory_id
        ));
    }
    if decl.effectively_once && !decl.persistent {
        return Err("effectively_once requires persistent: the tier IS durable state".to_string());
    }
    if decl.ack_granularity == HostAckGranularity::Checkpoint && !decl.effectively_once {
        return Err("ack_granularity checkpoint requires effectively_once".to_string());
    }
    if decl.ack_granularity != HostAckGranularity::Checkpoint
        && (decl.checkpoint_every.is_some()
            || decl.checkpoint_outputs == HostCheckpointOutputs::AtCheckpoint
            || decl.checkpoint_max_delay_ms.is_some())
    {
        return Err(
            "checkpoint_every / checkpoint_outputs / checkpoint_max_delay_ms require \
             ack_granularity checkpoint"
                .to_string(),
        );
    }
    if decl.checkpoint_outputs == HostCheckpointOutputs::AtCheckpoint
        && decl.checkpoint_max_delay_ms.is_none()
    {
        return Err(
            "checkpoint_outputs at_checkpoint requires checkpoint_max_delay_ms: an interval that \
             never closes would hold its outputs forever on a quiet Entity"
                .to_string(),
        );
    }
    if decl.checkpoint_every == Some(0) || decl.checkpoint_max_delay_ms == Some(0) {
        return Err(
            "checkpoint_every / checkpoint_max_delay_ms must be greater than zero".to_string(),
        );
    }
    if decl.correction_window_ms.is_some()
        && !decl
            .procedures
            .iter()
            .any(|p| p.clock == HostClock::EventTime)
    {
        return Err(
            "correction_window_ms requires at least one event_time-clocked procedure".to_string(),
        );
    }
    let mut seen = std::collections::HashSet::new();
    for channel in &decl.channels {
        if !seen.insert(channel.name.as_str()) {
            return Err(format!("duplicate channel name `{}`", channel.name));
        }
        let arity = |idx: u32| (idx as usize) < decl.procedures.len();
        if let Some(p) = channel.producer {
            if !arity(p) {
                return Err(format!(
                    "channel `{}` names producer {} but the class declares {} procedure(s)",
                    channel.name,
                    p,
                    decl.procedures.len()
                ));
            }
        }
        if let Some(&c) = channel.consumers.iter().find(|&&c| !arity(c)) {
            return Err(format!(
                "channel `{}` names consumer {} but the class declares {} procedure(s)",
                channel.name,
                c,
                decl.procedures.len()
            ));
        }
        match channel.kind {
            HostChannelKind::Entry => {
                if channel.producer.is_some() {
                    return Err(format!(
                        "entry channel `{}` cannot name a producer",
                        channel.name
                    ));
                }
                if channel.consumers.is_empty() {
                    return Err(format!(
                        "entry channel `{}` names no consumer",
                        channel.name
                    ));
                }
            }
            HostChannelKind::InterProc => {
                if channel.producer.is_none() {
                    return Err(format!(
                        "inter_proc channel `{}` names no producer",
                        channel.name
                    ));
                }
                if channel.consumers.is_empty() {
                    return Err(format!(
                        "inter_proc channel `{}` names no consumer",
                        channel.name
                    ));
                }
            }
            HostChannelKind::Plugin => {
                if !channel.consumers.is_empty() {
                    return Err(format!(
                        "plugin channel `{}` cannot name consumers",
                        channel.name
                    ));
                }
            }
        }
    }
    let mut seen_procs = std::collections::HashSet::new();
    for (idx, proc_decl) in decl.procedures.iter().enumerate() {
        if !seen_procs.insert(proc_decl.name.as_str()) {
            return Err(format!("duplicate procedure name `{}`", proc_decl.name));
        }
        match proc_decl.entity_mode {
            HostEntityModeDecl::Static if proc_decl.entity_keys.is_empty() => {
                return Err(format!(
                    "static procedure `{}` declares no entity_keys",
                    proc_decl.name
                ));
            }
            HostEntityModeDecl::Single | HostEntityModeDecl::Dynamic
                if !proc_decl.entity_keys.is_empty() =>
            {
                return Err(format!(
                    "procedure `{}` declares entity_keys without static mode",
                    proc_decl.name
                ));
            }
            HostEntityModeDecl::Dynamic if proc_decl.max_entities == Some(0) => {
                return Err(format!(
                    "dynamic procedure `{}` declares max_entities 0",
                    proc_decl.name
                ));
            }
            _ => {}
        }
        let has_input = decl
            .channels
            .iter()
            .any(|c| c.kind != HostChannelKind::Plugin && c.consumers.contains(&(idx as u32)));
        if !has_input {
            return Err(format!(
                "procedure `{}` has no input channel",
                proc_decl.name
            ));
        }
    }

    let factory_id: &'static str = Box::leak(decl.factory_id.clone().into_boxed_str());
    let schema_version = decl.schema_version;
    let persistent = decl.persistent;
    let sync_durability = decl.sync_durability;
    let correction = decl.correction_window_ms.map(|window_ms| CorrectionPolicy {
        window_ms,
        beyond_window: match decl.beyond_window_policy {
            HostBeyondWindow::ProcessAsArrival => {
                crate::operative::topology::BeyondWindowPolicy::ProcessAsArrival
            }
            HostBeyondWindow::Drop => crate::operative::topology::BeyondWindowPolicy::Drop,
        },
    });
    // Resolved once: the per-procedure facts and the shared topology must
    // agree on the interval's rules, so they read the same value.
    let ack_granularity = match decl.ack_granularity {
        HostAckGranularity::PerInput => AckGranularity::PerInput,
        HostAckGranularity::Checkpoint => {
            AckGranularity::Checkpoint(crate::operative::topology::CheckpointPolicy {
                every: decl.checkpoint_every,
                outputs: match decl.checkpoint_outputs {
                    HostCheckpointOutputs::AsProduced => CheckpointOutputs::AsProduced,
                    HostCheckpointOutputs::AtCheckpoint => CheckpointOutputs::AtCheckpoint,
                },
                max_delay_ms: decl.checkpoint_max_delay_ms,
            })
        }
    };
    let checkpoint_policy = ack_granularity.checkpoint();
    let compact_after_bytes = decl
        .compact_after_bytes
        .unwrap_or(crate::persistence::DEFAULT_COMPACT_AFTER_BYTES);

    let mut channels: Vec<ChannelDescriptor> = Vec::with_capacity(decl.channels.len());
    let mut name_to_id: HashMap<&'static str, u32> = HashMap::new();
    for (channel_id, channel_decl) in decl.channels.iter().enumerate() {
        let channel_id = channel_id as u32;
        let name: &'static str = Box::leak(channel_decl.name.clone().into_boxed_str());
        let retry = RetryPolicy {
            max_attempts: channel_decl.retry_max,
            backoff_base_ms: channel_decl.retry_backoff_ms,
        };
        let overflow_policy = match channel_decl.overflow {
            HostOverflow::Block => OverflowPolicy::Block,
            HostOverflow::DropNewest => OverflowPolicy::DropNewest,
            HostOverflow::DropOldest => OverflowPolicy::DropOldest,
        };
        let kind = match channel_decl.kind {
            HostChannelKind::Entry => ChannelKind::Entry,
            HostChannelKind::InterProc => ChannelKind::InterProcedure,
            HostChannelKind::Plugin => ChannelKind::PluginBound,
        };
        // Entry payloads wrap into a HostPayload naming their channel; the
        // prepare crossing fills keys/event time before delivery when a
        // consumer needs them.
        let wrap_input = (channel_decl.kind == HostChannelKind::Entry).then(|| {
            Arc::new(move |bytes: Vec<u8>| {
                Ok(Arc::new(HostPayload {
                    bytes,
                    channel: channel_id,
                    keys: Vec::new(),
                    event_time: None,
                    metadata: Vec::new(),
                }) as Arc<dyn Any + Send + Sync>)
            }) as crate::channel::EntryWrapFn
        });
        channels.push(ChannelDescriptor {
            name: Arc::from(name),
            kind,
            producer: channel_decl.producer,
            consumers: channel_decl
                .consumers
                .iter()
                .map(|&procedure| crate::channel::ConsumerEdge {
                    procedure,
                    transform: None,
                    key: None,
                })
                .collect(),
            wrap_input,
            entry_parsed: false,
            // A host class serializes on its own side and ships bytes, so the
            // descriptor carries no serializer — the kind is the whole record
            // of its plugin egress.
            plugin_sink: None,
            keyed: channel_decl.keyed,
            overflow_policy,
            retry,
            // Host inter-proc values are bytes already; the journal codec is
            // the lossless HostPayload wrapper, so a durable chained loop
            // can re-seed exactly what the live handoff carried.
            codec: (channel_decl.kind == HostChannelKind::InterProc)
                .then(|| host_payload_codec(channel_id)),
            entry_observation: crate::channel::entry_observation_for(kind, name),
        });
        name_to_id.insert(name, channel_id);
    }

    let mut procedures: Vec<ProcedureDescriptor> = Vec::with_capacity(decl.procedures.len());
    let mut procedure_name_to_id: HashMap<&'static str, u32> = HashMap::new();
    for (proc_id, proc_decl) in decl.procedures.iter().enumerate() {
        let proc_id = proc_id as u32;
        let input_channel_ids: Vec<u32> = decl
            .channels
            .iter()
            .enumerate()
            .filter(|(_, c)| c.consumers.contains(&proc_id))
            .map(|(id, _)| id as u32)
            .collect();
        let output_channel_id = decl
            .channels
            .iter()
            .position(|c| c.producer == Some(proc_id))
            .map(|id| id as u32);
        let retry = input_channel_ids
            .first()
            .and_then(|&id| channels.get(id as usize))
            .map(|c| c.retry)
            .unwrap_or_default();
        let mode = match proc_decl.entity_mode {
            HostEntityModeDecl::Single => EntityMode::Single,
            HostEntityModeDecl::Static => EntityMode::Static,
            HostEntityModeDecl::Dynamic => EntityMode::Dynamic,
        };
        let entity_keys: Vec<&'static str> = proc_decl
            .entity_keys
            .iter()
            .map(|k| Box::leak(k.clone().into_boxed_str()) as &'static str)
            .collect();
        let proc_name: &'static str = Box::leak(proc_decl.name.clone().into_boxed_str());
        let facts = Arc::new(HostProcedureFacts {
            name: proc_name,
            factory: factory_id,
            retry,
            schema_version,
            persistent,
            effectively_once: decl.effectively_once,
            checkpoint: checkpoint_policy,
            clock: proc_decl.clock,
            sdk_lang: decl.sdk_lang,
            change_format_id: decl.change_format_id,
            correction: if proc_decl.clock == HostClock::EventTime {
                correction
            } else {
                None
            },
        });
        procedures.push(ProcedureDescriptor {
            name: Arc::from(proc_name),
            entity_keys,
            entities_for_overridden: proc_decl.entities_for_overridden,
            compensate_overridden: proc_decl.compensate_overridden,
            mode,
            idle_ttl: proc_decl.idle_ttl_ms.map(std::time::Duration::from_millis),
            max_entities: proc_decl.max_entities.unwrap_or(usize::MAX),
            input_channel_ids,
            output_channel_id,
            input_type_id: TypeId::of::<HostPayload>(),
            output_type_id: TypeId::of::<HostPayload>(),
            mailbox_capacity: proc_decl.mailbox_capacity,
            mailbox_capacity_overridden: false,
            compact_after_bytes,
            host_facts: Some(Arc::clone(&facts)),
            build_entity: Box::new(move |args: BuildEntityArgs<'_>| {
                build_host_entity_slot(args, Arc::clone(&facts))
            }),
            entities_for_erased: Arc::new(|_| Vec::new()),
            serialize_state: Arc::new(|_| {
                Err("host-held state: a live read reaches the host through its \
                     LiveStateRead activation, not an in-process serializer"
                    .to_string())
            }),
            deserialize_state: Arc::new(|_| {
                Err("host-held state: apply-state reaches the host through its \
                     ApplyState activation"
                    .to_string())
            }),
        });
        procedure_name_to_id.insert(proc_name, proc_id);
    }

    let topology = Arc::new(OperativeTopology {
        channels,
        name_to_id,
        procedures,
        procedure_name_to_id,
        factory_id,
        persistence_enabled_const: persistent,
        effectively_once: decl.effectively_once,
        correction,
        ack_granularity,
    });
    match &decl.workflow {
        Some(workflow) => {
            let dag = build_host_dag(&topology)?;
            if workflow.loop_mode == HostLoopMode::Chained && dag.single_leaf().is_none() {
                return Err(format!(
                    "a handoff-chained loop needs exactly one terminal procedure; the graph \
                     has {}",
                    dag.leaves().count()
                ));
            }
            crate::workflow::registry::register_class(
                factory_id,
                crate::workflow::spec::WorkflowSpec {
                    run: host_run_unreachable,
                    topology: Some(topology),
                    dag,
                    max_duration: workflow
                        .max_duration_ms
                        .map(std::time::Duration::from_millis),
                    rollback_mode: rollback_of(workflow.rollback_mode),
                    loop_mode: match workflow.loop_mode {
                        HostLoopMode::Once => LoopMode::Once,
                        HostLoopMode::PerInput => LoopMode::PerInput,
                        HostLoopMode::Chained => LoopMode::Chained,
                    },
                    wake_after: workflow.wake_after_ms.map(std::time::Duration::from_millis),
                    retry: RetryPolicy {
                        max_attempts: workflow.retry_attempts,
                        backoff_base_ms: workflow.retry_backoff_ms,
                    },
                    barrier_retry_attempts: workflow.barrier_retry_attempts,
                    persistence_enabled: persistent,
                    sync_durability,
                    host: true,
                },
            );
        }
        None => crate::operative::registry::register_class(factory_id, topology),
    }
    Ok(())
}

fn rollback_of(mode: HostRollbackMode) -> RollbackMode {
    match mode {
        HostRollbackMode::AllOrNothing => RollbackMode::AllOrNothing,
        HostRollbackMode::KeepProgress => RollbackMode::KeepProgress,
    }
}

/// `WorkflowSpec.run` placeholder for host classes — the driver branches on
/// `spec.host` before ever calling it.
fn host_run_unreachable(
    _ctx: &mut crate::workflow::WorkflowCtx,
    _args: &str,
) -> Result<(), crate::workflow::WorkflowError> {
    Err(crate::workflow::WorkflowError::from(
        "host workflow classes run through the poll crossing",
    ))
}

/// Kahn toposort of a structured host chain from its entry root, refusing
/// what the typed builder refuses: several entry consumers, a procedure
/// producing more than one channel, cycles, disconnected procedures.
fn build_host_dag(topology: &OperativeTopology) -> Result<crate::workflow::spec::ProcDag, String> {
    let entry = topology
        .channels
        .iter()
        .find(|c| c.kind == ChannelKind::Entry)
        .ok_or("a structured workflow class declares one entry channel")?;
    if entry.consumers.len() != 1 {
        return Err(format!(
            "a bounded topology has a single root; entry channel `{}` feeds {} procedures",
            entry.name,
            entry.consumers.len()
        ));
    }
    let n = topology.procedures.len();
    for (proc_id, _) in topology.procedures.iter().enumerate() {
        let produced = topology
            .channels
            .iter()
            .filter(|c| c.producer == Some(proc_id as u32))
            .count();
        if produced > 1 {
            return Err(format!(
                "procedure `{}` produces more than one channel; a procedure emits on exactly \
                 one output channel",
                topology.procedures[proc_id].name
            ));
        }
    }
    // parents/children over inter-proc channels, indexed by procedure id.
    let mut parents: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut children: Vec<Vec<usize>> = vec![Vec::new(); n];
    for channel in &topology.channels {
        if channel.kind != ChannelKind::InterProcedure {
            continue;
        }
        let Some(producer) = channel.producer else {
            continue;
        };
        for edge in &channel.consumers {
            parents[edge.procedure as usize].push(producer as usize);
            children[producer as usize].push(edge.procedure as usize);
        }
    }
    let root = entry.consumers[0].procedure as usize;
    let mut in_degree: Vec<usize> = parents.iter().map(Vec::len).collect();
    let mut order: Vec<u32> = Vec::with_capacity(n);
    let mut ready: std::collections::VecDeque<usize> = std::collections::VecDeque::new();
    ready.push_back(root);
    let mut seen = vec![false; n];
    seen[root] = true;
    while let Some(node) = ready.pop_front() {
        order.push(node as u32);
        for &child in &children[node] {
            in_degree[child] = in_degree[child].saturating_sub(1);
            if in_degree[child] == 0 && !seen[child] {
                seen[child] = true;
                ready.push_back(child);
            }
        }
    }
    if order.len() != n {
        let missing = topology
            .procedures
            .iter()
            .enumerate()
            .find(|(i, _)| !seen[*i])
            .map(|(_, p)| p.name.to_string())
            .unwrap_or_default();
        return Err(format!(
            "procedure `{missing}` is on a cycle or not reachable from the entry channel"
        ));
    }
    // Re-index parents/children positionally by order (the driver's shape).
    let index_of: std::collections::HashMap<usize, usize> = order
        .iter()
        .enumerate()
        .map(|(pos, &p)| (p as usize, pos))
        .collect();
    let dag_parents: Vec<Vec<usize>> = order
        .iter()
        .map(|&p| parents[p as usize].iter().map(|pp| index_of[pp]).collect())
        .collect();
    let dag_children: Vec<Vec<usize>> = order
        .iter()
        .map(|&p| children[p as usize].iter().map(|cc| index_of[cc]).collect())
        .collect();
    Ok(crate::workflow::spec::ProcDag {
        order,
        parents: dag_parents,
        children: dag_children,
    })
}

/// The journal codec for a host inter-proc channel: a lossless postcard
/// wrapper over the payload's routing facts, so a durable re-seed rebuilds
/// the exact `HostPayload` the live handoff carried.
fn host_payload_codec(channel_id: u32) -> crate::channel::ChannelCodec {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct JournalForm {
        bytes: Vec<u8>,
        keys: Vec<String>,
        event_time: Option<u64>,
    }
    crate::channel::ChannelCodec {
        encode: Arc::new(|value: &dyn Any| {
            let payload = value
                .downcast_ref::<HostPayload>()
                .ok_or("host codec: value is not a host payload")?;
            postcard::to_stdvec(&JournalForm {
                bytes: payload.bytes.clone(),
                keys: payload.keys.clone(),
                event_time: payload.event_time,
            })
            .map_err(|e| e.to_string())
        }),
        decode: Arc::new(move |bytes: Vec<u8>| {
            let form: JournalForm = postcard::from_bytes(&bytes).map_err(|e| e.to_string())?;
            Ok(Arc::new(HostPayload {
                bytes: form.bytes,
                channel: channel_id,
                keys: form.keys,
                event_time: form.event_time,
                metadata: Vec::new(),
            }) as Arc<dyn Any + Send + Sync>)
        }),
    }
}

/// The per-procedure facts a [`HostDispatcher`] answers with. `Some` from
/// [`crate::entity::ProcedureDispatcher::host_facts`] is what marks a slot as
/// host-run: its command interactions cross as queued activations instead of
/// in-process calls.
pub struct HostProcedureFacts {
    pub name: &'static str,
    /// The declaring class — stamped on every activation for this procedure.
    pub factory: &'static str,
    pub retry: RetryPolicy,
    pub schema_version: u32,
    pub persistent: bool,
    /// Effectively-once tier: completions produce effect records (changes +
    /// outputs + consumed offset in ONE durable record) instead of plain
    /// change frames, and outputs ride the transactional outbox.
    pub effectively_once: bool,
    /// Checkpoint ack-granularity: the open interval's changes stage until a
    /// completion carries `checkpoint` (or a park closes the interval).
    pub checkpoint: Option<crate::operative::topology::CheckpointPolicy>,
    /// The declared clock; drives `now` stamping and event-time admission.
    pub clock: HostClock,
    /// Header lang byte the host's durable records claim (Rust 1, Java 2).
    pub sdk_lang: u8,
    /// Header format byte for plain change bodies (postcard 1, Kryo 4).
    pub change_format_id: u8,
    /// Late-input correction, present only on event-time-clocked procedures.
    pub correction: Option<CorrectionPolicy>,
}

impl HostProcedureFacts {
    /// The durable-record header for this procedure's frames — the host's
    /// declared lang/format instead of the typed constructor's Rust ones.
    pub fn header(&self) -> HeaderTemplate {
        HeaderTemplate {
            sdk_lang: self.sdk_lang,
            sdk_version: crate::header::parse_pkg_version(),
            schema_version: self.schema_version,
            format_id: self.change_format_id,
        }
    }
}

/// Mirror of the typed `build_entity_slot` for a host-run procedure: the
/// shared kernel assembly (`entity::assemble_slot`), a [`BytesBuffer`]
/// instead of a `TypedBuffer<S>`, a [`HostDispatcher`] instead of the typed
/// impl.
fn build_host_entity_slot(
    args: BuildEntityArgs<'_>,
    facts: Arc<HostProcedureFacts>,
) -> Arc<EntitySlot> {
    let procedure_desc = args
        .topology
        .procedure(args.procedure_id)
        .expect("build_host_entity_slot: procedure id missing in topology");
    let mailbox_capacity = procedure_desc.mailbox_capacity;
    let procedure_name = procedure_desc.name.to_string();
    let parts =
        crate::entity::slot_parts(args.instance_id.as_ref(), &procedure_name, args.entity_key);
    let flush_buffer: Arc<Mutex<Box<dyn FlushableBuffer>>> = Arc::new(Mutex::new(
        Box::<BytesBuffer>::default() as Box<dyn FlushableBuffer>,
    ));
    let header = facts.header();
    let flusher_entry_id = facts.persistent.then(|| {
        EntityFlusher::instance().register(
            Arc::clone(&parts.entity_identity),
            Arc::clone(&flush_buffer),
            args.persistence_enabled,
            Arc::clone(&parts.change_seq),
            header,
        )
    });
    let dispatcher = Arc::new(HostDispatcher {
        procedure_id: args.procedure_id,
        entity_label: Arc::clone(&parts.entity_label),
        facts,
    }) as Arc<dyn ProcedureDispatcher>;
    crate::entity::assemble_slot(
        args,
        parts,
        Box::new(()) as Box<dyn Any + Send>,
        flush_buffer,
        dispatcher,
        flusher_entry_id,
        mailbox_capacity,
    )
}

/// The metadata half of the dispatcher trait for a host-run procedure.
/// `dispatch_item` is unreachable in a host process — no in-process poll
/// worker exists to call it; work leaves through the C ABI's poll.
struct HostDispatcher {
    procedure_id: u32,
    entity_label: Arc<str>,
    facts: Arc<HostProcedureFacts>,
}

/// Why the in-process workflow seams are unreachable for a host class: its work
/// leaves as a step activation the host polls, and the answer folds through
/// [`fold_workflow_outcome`].
fn host_workflow_unreachable(what: &str) -> crate::workflow::WorkflowError {
    crate::workflow::WorkflowError::non_retryable(format!(
        "host-run procedure: {what} reaches the host as a workflow step activation, \
         never this in-process seam"
    ))
}

/// Map one workflow step's work outcome into the driver's collect sink: changes
/// into that entity's flush buffer, edge outputs collected for the next stage,
/// plugin outputs staged/forwarded by the sink. Runs on the thread that
/// completed the step, with its driver parked at the barrier the fold's result
/// answers.
pub fn fold_workflow_outcome(
    flush_buffer: &Mutex<Box<dyn FlushableBuffer>>,
    outcome: schema::ActivationOutcome,
    sink: &mut dyn crate::workflow::WorkflowCollectSink,
) -> Result<(), crate::workflow::WorkflowError> {
    let effects = match outcome {
        schema::ActivationOutcome::Work(effects) => effects,
        schema::ActivationOutcome::Failed(err) => {
            return Err(crate::workflow::WorkflowError::new(
                err.message,
                err.non_retryable,
            ));
        }
        _ => {
            return Err(crate::workflow::WorkflowError::from(
                "workflow step answered with a non-work outcome".to_string(),
            ));
        }
    };
    for effect in effects {
        match effect.verdict {
            schema::Verdict::Ok => {}
            schema::Verdict::RetryableError { message } => {
                return Err(crate::workflow::WorkflowError::new(message, false));
            }
            schema::Verdict::NonRetryableError { message } => {
                return Err(crate::workflow::WorkflowError::new(message, true));
            }
        }
        if !effect.changes.is_empty() {
            let mut buffer = flush_buffer.lock_recover();
            if let Some(bytes) = buffer.as_any_mut().downcast_mut::<BytesBuffer>() {
                bytes.changes.extend(effect.changes);
            }
        }
        for output in effect.outputs {
            match output.target {
                schema::OutputTarget::Edge { channel, .. } => {
                    sink.collect(Arc::new(HostPayload {
                        bytes: output.payload,
                        channel,
                        keys: output.key.into_iter().collect(),
                        event_time: output.event_time,
                        metadata: output.metadata,
                    }) as Arc<dyn Any + Send + Sync>);
                }
                schema::OutputTarget::Plugin { channel } => {
                    sink.emit_plugin_bytes(&channel, output.payload);
                }
            }
        }
    }
    Ok(())
}

impl ProcedureDispatcher for HostDispatcher {
    fn dispatch_item(
        &self,
        _work: WorkItem,
        _state: &mut Box<dyn Any + Send>,
        _flush_buffer: &mut Box<dyn FlushableBuffer>,
        _timers: &mut PendingTimers,
        _instance: &Weak<crate::operative::instance::StreamInstance>,
    ) -> ItemDisposition {
        tracing::error!(
            target: "flezha::host",
            entity = %self.entity_label,
            "in-process dispatch reached a host-run procedure; item dropped"
        );
        ItemDisposition::Done { ops: Vec::new() }
    }

    fn host_facts(&self) -> Option<&HostProcedureFacts> {
        Some(self.facts.as_ref())
    }

    fn close_interval(&self, flush_buffer: &mut Box<dyn crate::flusher::FlushableBuffer>) -> bool {
        // Only a checkpoint class has an interval to close.
        if self.facts.checkpoint.is_none() {
            return false;
        }
        let Some(bytes) = flush_buffer.as_any_mut().downcast_mut::<BytesBuffer>() else {
            return false;
        };
        let Some(record) = bytes.staged.close_on_deadline() else {
            return false;
        };
        bytes
            .effects
            .push(crate::flusher::EffectRecordBody::from_shipped(record, None));
        true
    }

    fn interval_deadline(
        &self,
        flush_buffer: &mut Box<dyn crate::flusher::FlushableBuffer>,
    ) -> Option<u64> {
        let policy = self.facts.checkpoint?;
        let bytes = flush_buffer.as_any_mut().downcast_mut::<BytesBuffer>()?;
        bytes.staged.deadline_ms(policy)
    }

    fn procedure_id(&self) -> u32 {
        self.procedure_id
    }

    fn procedure_name(&self) -> &'static str {
        self.facts.name
    }

    fn retry_policy(&self) -> RetryPolicy {
        self.facts.retry
    }

    fn is_event_time(&self) -> bool {
        self.facts.clock == HostClock::EventTime
    }

    fn correction_policy(&self) -> Option<CorrectionPolicy> {
        self.facts.correction
    }

    fn entity_label(&self) -> &str {
        &self.entity_label
    }

    fn event_time_of(&self, payload: &crate::entity::EntityPayload) -> Option<u64> {
        payload
            .value
            .downcast_ref::<HostPayload>()
            .and_then(|p| p.event_time)
    }

    fn run_collecting(
        &self,
        _input: Arc<dyn Any + Send + Sync>,
        _state: &mut Box<dyn Any + Send>,
        _topology: &OperativeTopology,
        _sink: &mut dyn crate::workflow::WorkflowCollectSink,
    ) -> Result<(), crate::workflow::WorkflowError> {
        Err(host_workflow_unreachable("a stage batch"))
    }

    fn compensate_collecting(
        &self,
        _outputs: Vec<Arc<dyn Any + Send + Sync>>,
        _state: &mut Box<dyn Any + Send>,
        _topology: &OperativeTopology,
        _sink: &mut dyn crate::workflow::WorkflowCollectSink,
    ) -> Result<(), crate::workflow::WorkflowError> {
        Err(host_workflow_unreachable("a saga undo"))
    }

    fn restore_state(
        &self,
        _state: &mut Box<dyn Any + Send>,
        _timers: &mut PendingTimers,
        _snapshot_bytes: &[u8],
        _change_log_bytes: &[u8],
        _entity_key: Option<&str>,
    ) -> Result<u64, crate::header::RestoreError> {
        Err(crate::header::RestoreError::SnapshotDecode(
            "host-held state: restore reaches the host through its RestoreFold \
             activation (`restore_fold_exclusive`), never this in-process seam"
                .to_string(),
        ))
    }

    fn snapshot_frame(
        &self,
        _state: &mut Box<dyn Any + Send>,
        _flush_buffer: &mut Box<dyn FlushableBuffer>,
        _timers: &PendingTimers,
    ) -> Result<(Vec<u8>, u64, usize), String> {
        Err("host-held state: compaction reaches the host through its \
             SnapshotRequest activation (`snapshot_frame_crossing`), never \
             this in-process seam"
            .to_string())
    }
}

/// Host-encoded per-Entity flush buffer: change bodies arrive pre-encoded in
/// completions; the core frames and ships them exactly as it does a typed
/// drain. Mirrors `TypedBuffer<S>` field-for-field at the bytes level:
/// `changes`/`timer_ops` are the tier-off home, `effects` the
/// effectively-once one, `staged` holds a checkpoint interval's
/// unacknowledged accumulation, and `pending_attempt` holds a
/// failed-but-retryable attempt's changes out of the flusher's reach until
/// the item resolves.
#[derive(Default)]
pub struct BytesBuffer {
    pub changes: Vec<Vec<u8>>,
    pub timer_ops: Vec<TimerOp>,
    pub effects: Vec<crate::flusher::EffectRecordBody>,
    pub staged: crate::flusher::StagedInterval<Vec<u8>>,
    pub pending_attempt: Vec<Vec<u8>>,
}

impl FlushableBuffer for BytesBuffer {
    fn drain_to_frames(
        &mut self,
        entity_identity: &[u8],
        change_seq: &AtomicU64,
        header: &HeaderTemplate,
    ) -> (usize, Vec<FlushFrame>) {
        let mut frames = crate::flusher::frame_change_bodies(
            std::mem::take(&mut self.changes),
            std::mem::take(&mut self.timer_ops),
            entity_identity,
            change_seq,
            header,
        );
        // Effect records are never coalesced or merged: a coalesced-away
        // record would drop its outputs and consumed-offset.
        for body in std::mem::take(&mut self.effects) {
            let seq = change_seq.fetch_add(1, Ordering::Relaxed);
            frames.push(FlushFrame {
                payload_id: crate::state_sink::ENTITY_EFFECT_RECORD_PAYLOAD_ID,
                bytes: crate::flusher::build_effect_record_frame(
                    entity_identity,
                    seq,
                    header,
                    &body,
                ),
            });
        }
        (frames.len(), frames)
    }

    fn is_empty(&self) -> bool {
        self.changes.is_empty() && self.timer_ops.is_empty() && self.effects.is_empty()
    }

    fn discard_staged(&mut self) {
        self.staged.clear();
        // A mid-ladder attempt's changes belong to the same superseded past.
        self.pending_attempt.clear();
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// The instance id off an `instance/procedure/key` entity label.
fn operative_of(label: &str) -> String {
    label.split('/').next().unwrap_or("").to_string()
}

/// The schema address of a slot — the instance id off the entity label, the
/// numeric procedure id, the key as declared.
pub fn entity_ref_of(slot: &EntitySlot) -> schema::EntityRef {
    schema::EntityRef {
        operative: operative_of(slot.dispatcher.entity_label()),
        procedure: slot.procedure_id,
        entity_key: slot.entity_key.as_deref().map(str::to_string),
    }
}

/// Translate a handed-out engine activation into the schema form the C ABI
/// ships. Wall-clock `now` is stamped here — the host never reads ambient
/// time for `ctx.now()`.
pub fn to_schema(act: &EngineActivation) -> schema::Activation {
    match &act.payload {
        EnginePayload::Command {
            target,
            change_seq,
            factory,
            kind,
            ..
        } => schema::Activation {
            token: act.token,
            target: target.clone(),
            kind: kind.clone(),
            change_seq: *change_seq,
            factory: factory.clone(),
        },
        EnginePayload::Prepare(crossing) => schema::Activation {
            token: act.token,
            target: schema::EntityRef {
                operative: crossing.instance.instance_id.to_string(),
                procedure: crossing
                    .instance
                    .topology
                    .channel(crossing.channel)
                    .and_then(|c| c.consumers.first())
                    .map(|e| e.procedure)
                    .unwrap_or(0),
                entity_key: None,
            },
            kind: schema::ActivationKind::KeyRequest(schema::KeyRequest {
                channel: crossing.channel,
                payloads: crossing.entries.iter().map(|e| e.bytes.clone()).collect(),
                want_keys: crossing.want_keys,
                want_event_time: crossing.want_event_time,
            }),
            change_seq: 0,
            factory: crossing.instance.topology.factory_id.to_string(),
        },
        EnginePayload::WorkflowStep(step) => schema::Activation {
            token: act.token,
            target: entity_ref_of(&step.slot),
            kind: workflow_step_kind(step),
            change_seq: step.slot.change_seq.load(Ordering::Acquire),
            factory: step.slot.topology.factory_id.to_string(),
        },
        EnginePayload::Work { slot, items } => {
            let now_fallback = host_clock_now(slot);
            let schema_items = items
                .iter()
                // A close is consumed by the batch assembler; it never becomes
                // host work, and filtering keeps the effect-to-item zip aligned
                // if that invariant ever loosened.
                .filter(|work| !matches!(work.item, EntityItem::CloseInterval))
                .map(|work| match &work.item {
                    EntityItem::Input(payload) => {
                        let (channel, bytes, metadata) =
                            host_input_parts(payload.value.downcast_ref::<HostPayload>(), slot);
                        schema::WorkItem::Input(schema::InputItem {
                            channel,
                            payload: bytes,
                            metadata,
                            consumed_offset: payload.consumed_offset,
                            replay: payload.replay,
                            now: work.now.unwrap_or(now_fallback),
                        })
                    }
                    EntityItem::Timer(fire) => schema::WorkItem::Timer(schema::TimerItem {
                        kind: fire.kind,
                        scheduled_at: fire.scheduled_at,
                    }),
                    // Filtered out above; a firing item is the harmless shape.
                    EntityItem::CloseInterval => schema::WorkItem::Timer(schema::TimerItem {
                        kind: schema::TimerKind::ProcessingTime,
                        scheduled_at: 0,
                    }),
                })
                .collect();
            schema::Activation {
                token: act.token,
                target: entity_ref_of(slot),
                kind: schema::ActivationKind::Work(schema_items),
                change_seq: slot.change_seq.load(Ordering::Acquire),
                factory: slot.topology.factory_id.to_string(),
            }
        }
    }
}

/// One workflow step as the host receives it: a stage batch is an ordinary
/// `Work` activation (the driver's ladder and barriers stay driver-side, so the
/// host runs exactly one attempt), an undo is a `CompensateRun`.
fn workflow_step_kind(step: &crate::workflow::step::WorkflowStep) -> schema::ActivationKind {
    use crate::workflow::step::StepWork;
    match &step.work {
        StepWork::Items(items) => {
            let now = host_clock_now(&step.slot);
            schema::ActivationKind::Work(
                items
                    .iter()
                    .map(|value| {
                        let (channel, bytes, metadata) =
                            host_input_parts(value.downcast_ref::<HostPayload>(), &step.slot);
                        schema::WorkItem::Input(schema::InputItem {
                            channel,
                            payload: bytes,
                            metadata,
                            consumed_offset: None,
                            replay: false,
                            now,
                        })
                    })
                    .collect(),
            )
        }
        StepWork::Compensate(outputs) => schema::ActivationKind::CompensateRun {
            outputs: outputs
                .iter()
                .filter_map(|value| value.downcast_ref::<HostPayload>())
                .map(|payload| payload.bytes.clone())
                .collect(),
        },
    }
}

/// The declared clock's value for one handout. The engine stamps `now` for
/// event-time items; wall and logical clocks evaluate here, once per handout —
/// the host never reads ambient time (or its own counters) for `ctx.now()`.
fn host_clock_now(slot: &EntitySlot) -> u64 {
    let clock = slot
        .dispatcher
        .host_facts()
        .map(|f| f.clock)
        .unwrap_or(HostClock::Wall);
    match clock {
        HostClock::Logical => slot.change_seq.load(Ordering::Acquire),
        _ => crate::util::wall_now_ms(),
    }
}

/// The channel, payload and metadata one item carries across. A value that is
/// not a `HostPayload` cannot reach a host class — its codecs mint them — so
/// the fallback is defensive: an empty payload on the entity's input channel,
/// which the host's own decoder rejects as a work failure.
fn host_input_parts(
    payload: Option<&HostPayload>,
    slot: &EntitySlot,
) -> (u32, Vec<u8>, Vec<(String, String)>) {
    match payload {
        Some(p) => (p.channel, p.bytes.clone(), p.metadata.clone()),
        None => (input_channel_of(slot), Vec::new(), Vec::new()),
    }
}

fn input_channel_of(slot: &EntitySlot) -> u32 {
    slot.topology
        .procedure(slot.procedure_id)
        .and_then(|p| p.input_channel_ids.first().copied())
        .unwrap_or(0)
}

/// Apply a host completion to its outstanding activation: buffer the change
/// bytes, fold and arm timer ops, ship plugin outputs, then drive the
/// engine's completion protocol (release, ladder, or park).
pub fn apply_result(
    core: &PollCore,
    act: EngineActivation,
    result: schema::ActivationResult,
) -> Result<(), String> {
    let EngineActivation { token, payload } = act;
    let (slot, items) = match payload {
        EnginePayload::Command { reply, .. } => {
            // A command interaction: the blocked command thread owns the
            // entity gate and validates the outcome; this side only
            // rendezvous-routes.
            let _ = reply.send(result.outcome);
            return Ok(());
        }
        EnginePayload::WorkflowStep(step) => {
            // A workflow stage step: its driver is parked at the barrier this
            // answer feeds, so the fold happens here rather than on a thread
            // waiting between the two.
            crate::workflow::step::complete_workflow_step(step, result.outcome);
            return Ok(());
        }
        EnginePayload::Prepare(crossing) => {
            return apply_prepare_outcome(crossing, result.outcome);
        }
        EnginePayload::Work { slot, items } => (slot, items),
    };
    let effects = match result.outcome {
        schema::ActivationOutcome::Work(effects) => effects,
        schema::ActivationOutcome::Failed(err) => {
            // Whole-activation failure: ladder the head item with the rest
            // behind it, or park it when non-retryable/final. The host
            // reported no per-item effect, so the park resolves with an
            // empty one (any pending-attempt changes still flush with it).
            let head_final = items.first().map(|w| w.is_final).unwrap_or(false);
            if err.non_retryable || head_final {
                if let Some(head) = items.first() {
                    resolve_item(
                        &slot,
                        head,
                        empty_effect(),
                        Some((err.message.clone(), head.attempt)),
                    );
                }
                // The head parked; the tail never ran and is owed a fresh
                // round, not a retry.
                let rest: Vec<WorkItem> = items.into_iter().skip(1).collect();
                core.rehand_unprocessed(token, rest);
            } else {
                core.retry_after_failure(token, items, err.message);
            }
            return Ok(());
        }
        _ => return Err("work activation answered with a non-work outcome".to_string()),
    };

    let mut queue: std::collections::VecDeque<WorkItem> = items.into();
    for effect in effects {
        let Some(work) = queue.pop_front() else {
            return Err("completion carries more effects than items".to_string());
        };
        match effect.verdict.clone() {
            schema::Verdict::Ok => {
                resolve_item(&slot, &work, effect, None);
                crate::entity::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
            }
            schema::Verdict::RetryableError { message } => {
                if work.is_final {
                    resolve_item(&slot, &work, effect, Some((message, work.attempt)));
                    crate::entity::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                // Keep the failed attempt's changes out of the flusher's
                // reach until the item resolves — the resolving effect
                // re-takes them, so durable state cannot diverge from the
                // host's memory.
                stash_pending_attempt(&slot, effect.changes);
                let mut rest = Vec::with_capacity(queue.len() + 1);
                rest.push(work);
                rest.extend(queue);
                core.retry_after_failure(token, rest, message);
                return Ok(());
            }
            schema::Verdict::NonRetryableError { message } => {
                resolve_item(&slot, &work, effect, Some((message, work.attempt)));
                crate::entity::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    // A parked head stops the host mid-batch ("items after the failed one
    // are re-handed by the core"); anything still queued is owed a fresh
    // round — dropping it here is how a healthy payload behind a poison one
    // would vanish.
    let rest: Vec<WorkItem> = queue.into_iter().collect();
    if rest.is_empty() {
        core.complete(token);
    } else {
        core.rehand_unprocessed(token, rest);
    }
    Ok(())
}

/// The effect a whole-activation failure resolves its head item with: the
/// host reported nothing per-item.
fn empty_effect() -> schema::ItemEffect {
    schema::ItemEffect {
        verdict: schema::Verdict::Ok,
        changes: Vec::new(),
        outputs: Vec::new(),
        consumed_offset: None,
        event_time: 0,
        timers: Vec::new(),
        checkpoint: false,
        dlq: None,
    }
}

/// Hold a failed-but-retryable attempt's changes until its item resolves.
fn stash_pending_attempt(slot: &Arc<EntitySlot>, changes: Vec<Vec<u8>>) {
    if changes.is_empty() {
        return;
    }
    let mut buffer = slot.flush_buffer.lock_recover();
    if let Some(bytes) = buffer.as_any_mut().downcast_mut::<BytesBuffer>() {
        bytes.pending_attempt.extend(changes);
    }
}

/// Resolve one item — the host boundary's mirror of the typed layer's
/// per-invocation effect block: fold the item's changes and timer ops into
/// the entity per its tier, route its outputs, and (when `park` is set) mint
/// the DLQ entry exactly where the typed layer does — inside the effect
/// record for the effectively-once tier (atomic with the offset advance),
/// through the blocking tier-off park otherwise.
fn resolve_item(
    slot: &Arc<EntitySlot>,
    work: &WorkItem,
    effect: schema::ItemEffect,
    park: Option<(String, u32)>,
) {
    let Some(facts) = slot.dispatcher.host_facts() else {
        return;
    };
    let stream = match &work.item {
        EntityItem::Input(p) => Arc::clone(&p.stream),
        EntityItem::Timer(f) => Arc::clone(&f.stream),
        // Never resolved as host work (the assembler consumes it); without a
        // connection of its own it takes the live secondary, as a chained
        // deadline does.
        EntityItem::CloseInterval => match crate::state_sink::secondary_stream() {
            Some(stream) => stream,
            None => return,
        },
    };
    let parked = park.is_some();

    // A failed invocation commits no timer registrations — except a poison
    // firing, which still records its removal so it parks once instead of
    // firing forever.
    let timers: Vec<TimerOp> = if parked {
        match &work.item {
            EntityItem::Timer(fire) => vec![TimerOp::cancel(fire.kind, fire.scheduled_at)],
            EntityItem::Input(_) | EntityItem::CloseInterval => Vec::new(),
        }
    } else {
        effect.timers
    };
    // A failed attempt's captured outputs must not ship — the invocation did
    // not complete.
    let outputs = if parked { Vec::new() } else { effect.outputs };
    // Edge values deliver in-process to their named consumer (producer-side
    // evaluation: the host already ran transforms and key); wire outputs
    // ride the tier's path below.
    let (edge_outputs, wire_outputs): (Vec<schema::Output>, Vec<schema::Output>) = outputs
        .into_iter()
        .partition(|o| matches!(o.target, schema::OutputTarget::Edge { .. }));
    // The offset is core-authoritative: the item the engine handed out knows
    // what it consumed; a firing consumes nothing.
    let consumed_offset = match &work.item {
        EntityItem::Input(p) => p.consumed_offset,
        EntityItem::Timer(_) | EntityItem::CloseInterval => None,
    };

    let dlq = park.map(|(error, attempts)| {
        crate::park::PARKED_TOTAL.fetch_add(1, Ordering::Relaxed);
        eprintln!(
            "[flezha] procedure {} parking poison payload to DLQ after {} attempt(s): {}",
            slot.dispatcher.procedure_name(),
            attempts,
            error
        );
        crate::flusher::DlqEntryBody {
            channel: park_channel_of(slot),
            error,
            attempts,
            payload: match &work.item {
                EntityItem::Input(p) => p
                    .value
                    .downcast_ref::<HostPayload>()
                    .map(|hp| hp.bytes.clone()),
                EntityItem::Timer(_) | EntityItem::CloseInterval => None,
            },
        }
    });

    let mut tier_off_dlq = None;
    let mut plugin_outputs: Vec<schema::Output> = Vec::new();
    {
        let mut buffer = slot.flush_buffer.lock_recover();
        if let Some(bytes) = buffer.as_any_mut().downcast_mut::<BytesBuffer>() {
            // The item resolves (success or park): re-take any changes a
            // failed-but-retryable attempt left behind.
            let mut changes = std::mem::take(&mut bytes.pending_attempt);
            changes.extend(effect.changes);
            if facts.effectively_once {
                // Tier mode: this item's changes + outputs + consumed offset
                // become ONE effect record. An effect-free item contributes
                // nothing durable — unless it parked, in which case the
                // record MUST ship so the dead-letter park and the
                // consumed-offset advance stay atomic.
                let output_records: Vec<crate::flusher::OutputRecord> = wire_outputs
                    .into_iter()
                    .filter_map(|o| match o.target {
                        schema::OutputTarget::Plugin { channel } => {
                            Some(crate::flusher::OutputRecord {
                                channel,
                                key: o.key.unwrap_or_default(),
                                metadata: o.metadata,
                                payload: o.payload,
                            })
                        }
                        schema::OutputTarget::Edge { .. } => None,
                    })
                    .collect();
                let shipped = bytes.staged.fold(
                    facts.checkpoint,
                    crate::flusher::InvocationEffects {
                        changes,
                        timers: timers.clone(),
                        outputs: output_records,
                        offset: consumed_offset,
                        event_time: effect.event_time,
                        checkpointed: effect.checkpoint,
                        parked: dlq.is_some(),
                    },
                    crate::util::wall_now_ms(),
                );
                if let Some(record) = shipped {
                    bytes
                        .effects
                        .push(crate::flusher::EffectRecordBody::from_shipped(record, dlq));
                }
            } else {
                if slot.flusher_entry_id.is_some() {
                    bytes.changes.extend(changes);
                    bytes.timer_ops.extend(timers.iter().copied());
                }
                // Tier-off outputs ship on the wire directly, below; the
                // park ships as its own blocking frame after the buffer ops.
                plugin_outputs = wire_outputs;
                tier_off_dlq = dlq;
            }
        }
    }

    if !timers.is_empty() {
        {
            let mut pending = slot.timers.lock_recover();
            for op in &timers {
                pending.apply(op);
            }
        }
        crate::timer::apply_ops(slot, &timers, &stream);
    }

    crate::timer::arm_interval_deadline(slot);

    // Edge values into consumer mailboxes — the offset propagates exactly as
    // the typed emit path propagates it.
    if !edge_outputs.is_empty() {
        if let Some(instance) = slot.instance.upgrade() {
            for output in edge_outputs {
                let schema::OutputTarget::Edge { channel, consumer } = output.target else {
                    continue;
                };
                let value: Arc<dyn Any + Send + Sync> = Arc::new(HostPayload {
                    bytes: output.payload,
                    channel,
                    keys: output.key.into_iter().collect(),
                    event_time: output.event_time,
                    metadata: output.metadata,
                });
                if let Err(e) = crate::routing::deliver_host_output(
                    &instance,
                    channel,
                    consumer,
                    &value,
                    &stream,
                    consumed_offset,
                ) {
                    tracing::warn!(
                        target: "flezha::host",
                        entity = slot.dispatcher.entity_label(),
                        "edge output dropped: {e}"
                    );
                }
            }
        }
    }

    // `plugin_outputs` is empty on a park and under the tier (where outputs
    // ride the effect record's transactional outbox instead). The frame's
    // payload id is the OPERATIVE ID — that is what the controller routes
    // `:on_output` by; the entity label would name nothing.
    let operative_id = entity_ref_of(slot).operative;
    for output in plugin_outputs {
        match output.target {
            schema::OutputTarget::Plugin { channel } => {
                let mut metadata: HashMap<String, String> = output.metadata.into_iter().collect();
                metadata.insert("channel".to_string(), channel);
                crate::routing::send_plugin_frame(
                    &stream,
                    &operative_id,
                    &output.payload,
                    &metadata,
                );
            }
            schema::OutputTarget::Edge { .. } => unreachable!("edge outputs partitioned above"),
        }
    }

    if let Some(entry) = tier_off_dlq {
        // Tier-off park: ship the park frame and hold this entity until the
        // controller acks the DLQ append as durable — park-then-advance.
        crate::park::park_tier_off(
            &slot.entity_identity,
            slot.dispatcher.entity_label(),
            consumed_offset,
            entry,
        );
    }
}

fn park_channel_of(slot: &Arc<EntitySlot>) -> String {
    slot.topology
        .procedure(slot.procedure_id)
        .and_then(|d| d.input_channel_ids.first())
        .and_then(|&id| slot.topology.channel(id))
        .map(|c| c.name.to_string())
        .unwrap_or_else(|| "<unwired>".to_string())
}

/// Live run-sugar bodies keyed by instance id — what the ABI's workflow
/// session calls (`emit` / `recv` / `cancelled`) resolve against while the
/// host runs the imperative body.
static WORKFLOW_SESSIONS: std::sync::LazyLock<dashmap::DashMap<String, Arc<HostWorkflowSession>>> =
    std::sync::LazyLock::new(dashmap::DashMap::new);

pub struct HostWorkflowSession {
    cancel: Arc<std::sync::atomic::AtomicBool>,
    inbox: Mutex<crossbeam::channel::Receiver<crate::workflow::instance::InboxItem>>,
}

/// The ABI's `flz_core_workflow_recv`: block up to `timeout` for the next
/// inbox item of a live run-sugar body. `Ok(None)` = timeout; `Err` = no
/// such session / inbox closed.
pub fn workflow_session_recv(
    instance_id: &str,
    timeout: std::time::Duration,
) -> Result<Option<Vec<u8>>, String> {
    let session = WORKFLOW_SESSIONS
        .get(instance_id)
        .map(|s| Arc::clone(s.value()))
        .ok_or_else(|| format!("no live workflow run body for instance `{instance_id}`"))?;
    let inbox = { session.inbox.lock_recover() };
    match inbox.recv_timeout(timeout) {
        Ok(item) => Ok(Some(item.payload)),
        Err(crossbeam::channel::RecvTimeoutError::Timeout) => Ok(None),
        Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
            Err("workflow inbox closed".to_string())
        }
    }
}

/// The ABI's `flz_core_workflow_cancelled`.
pub fn workflow_session_cancelled(instance_id: &str) -> bool {
    WORKFLOW_SESSIONS
        .get(instance_id)
        .map(|s| s.cancel.load(Ordering::Acquire))
        .unwrap_or(true)
}

/// Drive one host run-sugar body: register the session, cross the seed as a
/// [`schema::ActivationKind::WorkflowRun`] activation, and block for the
/// terminal completion — exactly as long as a typed `run` body would run.
/// Returns `(body outcome, result payload)` for the driver's `classify`.
#[allow(clippy::type_complexity)]
pub fn run_workflow_sugar(
    instance_id: &Arc<str>,
    factory_id: &str,
    args: &str,
    cancel: &Arc<std::sync::atomic::AtomicBool>,
    inbox: crossbeam::channel::Receiver<crate::workflow::instance::InboxItem>,
) -> (
    std::thread::Result<Result<(), crate::workflow::WorkflowError>>,
    Vec<u8>,
) {
    WORKFLOW_SESSIONS.insert(
        instance_id.to_string(),
        Arc::new(HostWorkflowSession {
            cancel: Arc::clone(cancel),
            inbox: Mutex::new(inbox),
        }),
    );
    let (reply, rx) = crossbeam::channel::bounded(1);
    crate::poll_core::global().submit_direct(EnginePayload::Command {
        target: schema::EntityRef {
            operative: instance_id.to_string(),
            procedure: 0,
            entity_key: None,
        },
        change_seq: 0,
        factory: factory_id.to_string(),
        kind: schema::ActivationKind::WorkflowRun {
            args: args.as_bytes().to_vec(),
        },
        reply,
    });
    let outcome = rx.recv();
    WORKFLOW_SESSIONS.remove(instance_id.as_ref());
    match outcome {
        Ok(schema::ActivationOutcome::WorkflowDone { result, .. }) => (Ok(Ok(())), result),
        Ok(schema::ActivationOutcome::Failed(err)) => (
            Ok(Err(crate::workflow::WorkflowError::new(
                err.message,
                err.non_retryable,
            ))),
            Vec::new(),
        ),
        Ok(_) => (
            Ok(Err(crate::workflow::WorkflowError::from(
                "workflow run answered with a non-terminal outcome",
            ))),
            Vec::new(),
        ),
        Err(_) => (
            Ok(Err(crate::workflow::WorkflowError::from(
                "host context dropped the workflow run activation",
            ))),
            Vec::new(),
        ),
    }
}

/// Per-instance staging for the prepare crossing, plus the deferred
/// end-of-slice broadcast (a broadcast must not overtake slice members still
/// waiting on their keys/event times).
#[derive(Default)]
pub struct HostPrepareState {
    queues: HashMap<u32, HostPrepareQueue>,
    /// Replay-slice members currently staged or in a crossing; the
    /// end-of-slice broadcast defers until this reaches zero.
    replay_staged: usize,
    pending_replay_end: bool,
}

#[derive(Default)]
struct HostPrepareQueue {
    queued: VecDeque<StagedEntry>,
    inflight: bool,
}

/// One entry payload staged for the prepare crossing.
#[derive(Clone)]
pub struct StagedEntry {
    pub bytes: Vec<u8>,
    pub consumed_offset: Option<u64>,
    pub replay: bool,
    pub stream: Arc<flz_transport::transport::FlezhaStream>,
}

/// A batched prepare crossing for one entry channel, handed to the host as a
/// [`schema::ActivationKind::KeyRequest`].
pub struct PrepareCrossing {
    pub instance: Arc<StreamInstance>,
    pub channel: crate::channel::ChannelId,
    pub want_keys: bool,
    pub want_event_time: bool,
    pub entries: Vec<StagedEntry>,
    /// A ladder's single-payload blocking re-crossing awaits here instead of
    /// the inline application path.
    pub reply:
        Option<crossbeam::channel::Sender<Vec<Result<schema::PrepareAnswer, schema::WorkError>>>>,
}

/// Cap per crossing, so one flood does not become one giant activation.
const PREPARE_BATCH_MAX: usize = 256;

/// What an entry channel's consumers need resolved before enqueue:
/// `(want_keys, want_event_time)`. `(false, false)` = no crossing.
pub fn prepare_wants(topology: &OperativeTopology, channel: &ChannelDescriptor) -> (bool, bool) {
    let mut want_keys = false;
    let mut want_event_time = false;
    for edge in &channel.consumers {
        let Some(desc) = topology.procedure(edge.procedure) else {
            continue;
        };
        let Some(facts) = desc.host_facts.as_ref() else {
            continue;
        };
        match desc.mode {
            EntityMode::Dynamic => want_keys = true,
            EntityMode::Static if desc.entities_for_overridden => want_keys = true,
            _ => {}
        }
        if facts.clock == HostClock::EventTime {
            want_event_time = true;
        }
    }
    (want_keys, want_event_time)
}

/// Wire-dispatch interception: stage an entry payload for the prepare
/// crossing when its consumers need keys/event times the core cannot derive
/// from bytes. Returns the payload back when no crossing is needed.
pub fn maybe_stage_prepare(
    instance: &Arc<StreamInstance>,
    channel_id: crate::channel::ChannelId,
    channel: &ChannelDescriptor,
    payload: Vec<u8>,
    consumed_offset: Option<u64>,
    replay: bool,
    stream: &Arc<flz_transport::transport::FlezhaStream>,
) -> Option<Vec<u8>> {
    if channel.kind != ChannelKind::Entry {
        return Some(payload);
    }
    let (want_keys, want_event_time) = prepare_wants(&instance.topology, channel);
    if !want_keys && !want_event_time {
        return Some(payload);
    }
    {
        let mut state = instance.host_prepare.lock_recover();
        if replay {
            state.replay_staged += 1;
        }
        state
            .queues
            .entry(channel_id)
            .or_default()
            .queued
            .push_back(StagedEntry {
                bytes: payload,
                consumed_offset,
                replay,
                stream: Arc::clone(stream),
            });
    }
    submit_next_crossing(instance, channel_id, want_keys, want_event_time);
    None
}

/// End-of-slice broadcast, deferred behind any slice members still waiting
/// in the prepare crossing — otherwise the close would overtake the very
/// inputs being re-ordered. Instances with nothing staged broadcast at once.
pub fn deferred_end_replay_slice(instance: &Arc<StreamInstance>) {
    {
        let mut state = instance.host_prepare.lock_recover();
        if state.replay_staged > 0 {
            state.pending_replay_end = true;
            return;
        }
    }
    crate::routing::end_replay_slice(instance);
}

/// Assemble and submit the next crossing for a channel unless one is
/// already in flight. Called at staging and at crossing completion.
fn submit_next_crossing(
    instance: &Arc<StreamInstance>,
    channel_id: u32,
    want_keys: bool,
    want_event_time: bool,
) {
    let entries = {
        let mut state = instance.host_prepare.lock_recover();
        let Some(queue) = state.queues.get_mut(&channel_id) else {
            return;
        };
        if queue.inflight || queue.queued.is_empty() {
            return;
        }
        queue.inflight = true;
        let take = queue.queued.len().min(PREPARE_BATCH_MAX);
        queue.queued.drain(..take).collect::<Vec<_>>()
    };
    crate::poll_core::global().submit_direct(EnginePayload::Prepare(PrepareCrossing {
        instance: Arc::clone(instance),
        channel: channel_id,
        want_keys,
        want_event_time,
        entries,
        reply: None,
    }));
}

/// Route a completed crossing: deliver answered payloads, ladder failed
/// ones, then re-arm the channel's next batch (or fire a deferred
/// end-of-slice broadcast once no slice member is waiting).
fn apply_prepare_outcome(
    crossing: PrepareCrossing,
    outcome: schema::ActivationOutcome,
) -> Result<(), String> {
    let mut answers = match outcome {
        schema::ActivationOutcome::Prepared(answers) => answers,
        schema::ActivationOutcome::Failed(err) => {
            vec![Err(err); crossing.entries.len()]
        }
        _ => {
            vec![
                Err(schema::WorkError {
                    message: "prepare crossing answered with a non-prepare outcome".to_string(),
                    non_retryable: false,
                });
                crossing.entries.len()
            ]
        }
    };
    if answers.len() < crossing.entries.len() {
        answers.resize(
            crossing.entries.len(),
            Err(schema::WorkError {
                message: "prepare crossing answered fewer payloads than asked".to_string(),
                non_retryable: false,
            }),
        );
    }
    let PrepareCrossing {
        instance,
        channel,
        want_keys,
        want_event_time,
        entries,
        reply,
    } = crossing;
    if let Some(reply) = reply {
        let _ = reply.send(answers);
        return Ok(());
    }
    let Some(channel_desc) = instance.topology.channel(channel) else {
        return Err("prepare crossing names an unknown channel".to_string());
    };
    let mut replay_resolved = 0usize;
    for (entry, answer) in entries.into_iter().zip(answers) {
        if entry.replay {
            replay_resolved += 1;
        }
        match answer {
            Ok(ans) => deliver_prepared(&instance, channel, channel_desc, entry, ans),
            Err(e) => spawn_prepare_ladder(
                Arc::clone(&instance),
                channel,
                want_keys,
                want_event_time,
                entry,
                e,
            ),
        }
    }
    finish_crossing(
        &instance,
        channel,
        want_keys,
        want_event_time,
        replay_resolved,
    );
    Ok(())
}

/// Deliver one prepared payload to the channel's consumers through the one
/// routing seam live delivery and replay slices share.
fn deliver_prepared(
    instance: &Arc<StreamInstance>,
    channel_id: u32,
    channel: &ChannelDescriptor,
    entry: StagedEntry,
    answer: schema::PrepareAnswer,
) {
    let value: Arc<dyn Any + Send + Sync> = Arc::new(HostPayload {
        bytes: entry.bytes,
        channel: channel_id,
        keys: answer.keys,
        event_time: answer.event_time,
        metadata: Vec::new(),
    });
    crate::routing::route_to_consumers(
        instance,
        channel,
        &value,
        &entry.stream,
        entry.consumed_offset,
        entry.replay,
    );
}

/// A failed prepare climbs the entry channel's retry→park ladder — each
/// re-attempt is a single-payload blocking crossing, and exhaustion parks
/// under the channel's identity exactly like a poison entry parse.
fn spawn_prepare_ladder(
    instance: Arc<StreamInstance>,
    channel_id: u32,
    want_keys: bool,
    want_event_time: bool,
    entry: StagedEntry,
    first_error: schema::WorkError,
) {
    crate::routing::spawn_park_thread("flezha-prepare-park", move || {
        let channel = instance
            .topology
            .channel(channel_id)
            .expect("prepare channel present in topology");
        crate::routing::run_retry_park_ladder(
            &format!("prepare for entry channel `{}`", channel.name),
            channel,
            &instance.instance_id,
            "_entry",
            entry.consumed_offset,
            Some(entry.bytes.clone()),
            first_error.message,
            || {
                let (tx, rx) = crossbeam::channel::bounded(1);
                crate::poll_core::global().submit_direct(EnginePayload::Prepare(PrepareCrossing {
                    instance: Arc::clone(&instance),
                    channel: channel_id,
                    want_keys,
                    want_event_time,
                    entries: vec![entry.clone()],
                    reply: Some(tx),
                }));
                let mut answers = rx
                    .recv_timeout(std::time::Duration::from_secs(30))
                    .map_err(|_| "prepare re-attempt not answered".to_string())?;
                match answers.pop() {
                    Some(Ok(ans)) => {
                        deliver_prepared(&instance, channel_id, channel, entry.clone(), ans);
                        Ok(())
                    }
                    Some(Err(e)) => Err(e.message),
                    None => Err("prepare re-attempt answered nothing".to_string()),
                }
            },
        );
    });
}

/// Crossing epilogue: release the channel's inflight gate, re-arm the next
/// batch, and fire a deferred end-of-slice broadcast once the last waiting
/// slice member resolved.
fn finish_crossing(
    instance: &Arc<StreamInstance>,
    channel_id: u32,
    want_keys: bool,
    want_event_time: bool,
    replay_resolved: usize,
) {
    let fire_broadcast = {
        let mut state = instance.host_prepare.lock_recover();
        state.replay_staged = state.replay_staged.saturating_sub(replay_resolved);
        if let Some(queue) = state.queues.get_mut(&channel_id) {
            queue.inflight = false;
        }
        state.replay_staged == 0
            && state.pending_replay_end
            && std::mem::take(&mut state.pending_replay_end)
    };
    submit_next_crossing(instance, channel_id, want_keys, want_event_time);
    if fire_broadcast {
        crate::routing::end_replay_slice(instance);
    }
}

/// Restore a host-run Entity: split the persisted records into what the core
/// owns (cursors, timer ops — folded here) and what only the host can decode
/// (its own change bodies — crossed as a [`schema::RestoreFold`] activation).
/// Runs under the entity gate; returns the terminal WAL seq, or `None` with
/// the failure recorded in `errors`.
pub fn restore_fold_exclusive(
    slot: &Arc<EntitySlot>,
    snapshot_bytes: &[u8],
    change_log_bytes: &[u8],
    errors: &mut Vec<String>,
) -> Option<u64> {
    let mut changes: Vec<Vec<u8>> = Vec::new();
    let mut pending = PendingTimers::default();
    let mut terminal = 0u64;
    for record in crate::restore::iter_change_log_records(change_log_bytes) {
        let record = match record {
            Ok(r) => r,
            Err(e) => {
                errors.push(format!("restore_init_state: change_log decode error: {e}"));
                return None;
            }
        };
        terminal = terminal.max(record.seq);
        match record.header.get(9) {
            Some(&crate::header::FORMAT_ID_EFFECT_RECORD) => {
                match postcard::from_bytes::<crate::flusher::EffectRecordBody>(record.change_bytes)
                {
                    Ok(body) => {
                        changes.extend(body.changes);
                        for op in &body.timers {
                            pending.apply(op);
                        }
                    }
                    Err(e) => {
                        errors.push(format!(
                            "restore_init_state: effect record decode error: {e}"
                        ));
                        return None;
                    }
                }
            }
            Some(&crate::header::FORMAT_ID_TIMER_OPS) => {
                match postcard::from_bytes::<Vec<TimerOp>>(record.change_bytes) {
                    Ok(ops) => {
                        for op in &ops {
                            pending.apply(op);
                        }
                    }
                    Err(e) => {
                        errors.push(format!("restore_init_state: timer ops decode error: {e}"));
                        return None;
                    }
                }
            }
            _ => changes.push(record.change_bytes.to_vec()),
        }
    }
    let fold = schema::RestoreFold {
        snapshot: (!snapshot_bytes.is_empty()).then(|| snapshot_bytes.to_vec()),
        changes,
    };
    match crate::poll_core::global().host_command(slot, schema::ActivationKind::RestoreFold(fold)) {
        Ok(schema::ActivationOutcome::Restored) => {
            *slot.timers.lock_recover() = pending;
            Some(terminal)
        }
        Ok(schema::ActivationOutcome::Failed(err)) => {
            errors.push(format!(
                "restore_init_state: host fold failed: {}",
                err.message
            ));
            None
        }
        Ok(_) => {
            errors.push("restore_init_state: host answered with a non-restore outcome".to_string());
            None
        }
        Err(e) => {
            errors.push(format!("restore_init_state: {e}"));
            None
        }
    }
}

/// Compaction snapshot for a host-run Entity: drain pending changes, read
/// the seq, re-state the pending timer set (so truncation cannot eat it),
/// then cross a [`schema::ActivationKind::SnapshotRequest`] and frame the
/// host-encoded state. Runs under the entity gate.
pub fn snapshot_frame_crossing(slot: &Arc<EntitySlot>) -> Result<(Vec<u8>, u64, usize), String> {
    let facts = slot
        .dispatcher
        .host_facts()
        .ok_or("snapshot_frame_crossing: not a host-run procedure")?;
    let header = facts.header();
    let flush = |buffer: &mut Box<dyn FlushableBuffer>| {
        crate::flusher::flush_one(
            &mut **buffer,
            &slot.entity_identity,
            &slot.change_seq,
            &header,
            slot.persistence_enabled,
        );
    };
    flush(&mut slot.flush_buffer.lock_recover());
    let seq = slot.change_seq.load(Ordering::Acquire).saturating_sub(1);
    // The snapshot body is the host's State alone; the timer ops the coming
    // truncation would delete are re-stated as a full set AFTER the seq is
    // read — landing at `snapshot_seq + 1`, which `truncate_up_to` keeps.
    let register_ops = slot.timers.lock_recover().as_register_ops();
    if !register_ops.is_empty() {
        let mut buffer = slot.flush_buffer.lock_recover();
        if let Some(bytes) = buffer.as_any_mut().downcast_mut::<BytesBuffer>() {
            bytes.timer_ops = register_ops;
        }
        flush(&mut buffer);
    }
    let state = match crate::poll_core::global()
        .host_command(slot, schema::ActivationKind::SnapshotRequest)?
    {
        schema::ActivationOutcome::Snapshot { state } => state,
        schema::ActivationOutcome::Failed(err) => {
            return Err(format!("host snapshot failed: {}", err.message))
        }
        _ => return Err("host answered with a non-snapshot outcome".to_string()),
    };
    let size = state.len();
    let frame = crate::flusher::build_entity_frame(
        &slot.entity_identity,
        seq,
        &header.stamp(&state),
        &state,
    );
    Ok((frame, seq, size))
}

/// Live-read / migration serialize for a host-run Entity. MUST run under the
/// entity gate (the caller's `with_entity_exclusive` closure).
pub fn state_json_locked(
    slot: &Arc<EntitySlot>,
    migration: bool,
) -> Result<(serde_json::Value, u64), String> {
    let kind = if migration {
        schema::ActivationKind::MigrationSerialize
    } else {
        schema::ActivationKind::LiveStateRead
    };
    let json = match crate::poll_core::global().host_command(slot, kind)? {
        schema::ActivationOutcome::StateJson { json }
        | schema::ActivationOutcome::MigrationState { json } => json,
        schema::ActivationOutcome::Failed(err) => return Err(err.message),
        _ => return Err("host answered with a non-state outcome".to_string()),
    };
    let value = serde_json::from_slice(&json)
        .map_err(|e| format!("host state JSON failed to parse: {e}"))?;
    let seq = slot.change_seq.load(Ordering::Relaxed);
    Ok((value, seq))
}

/// Apply-state (migration in / init_values) for a host-run Entity. MUST run
/// under the entity gate.
pub fn apply_state_locked(slot: &Arc<EntitySlot>, value: &serde_json::Value) -> Result<(), String> {
    let state_json = serde_json::to_vec(value).map_err(|e| e.to_string())?;
    match crate::poll_core::global()
        .host_command(slot, schema::ActivationKind::ApplyState { state_json })?
    {
        schema::ActivationOutcome::StateApplied => Ok(()),
        schema::ActivationOutcome::Failed(err) => Err(err.message),
        _ => Err("host answered with a non-apply outcome".to_string()),
    }
}

#[cfg(test)]
mod entry_observation_decl_tests {
    //! A host-language class reaches the wire through the same `dispatch` the
    //! typed Rust classes do, so its entry channels must carry the same
    //! observation cell — that is what makes the input-liveness signal one
    //! implementation for both SDKs rather than two.

    use crate::channel::ChannelKind;

    #[test]
    fn a_host_declared_entry_channel_carries_the_observation_its_peers_read() {
        let id = format!("host_entry_observation_{}", std::process::id());
        super::register_host_class_json(
            format!(
                r#"{{"factory_id":"{id}","persistent":false,
                     "channels":[{{"name":"host_trades_in","kind":"entry","consumers":[0]}},
                                 {{"name":"host_out","kind":"plugin","producer":0}}],
                     "procedures":[{{"name":"p0"}}]}}"#
            )
            .as_bytes(),
        )
        .expect("the class registers");

        let topology = crate::operative::registry::get_class(&id).expect("class registered");
        let entry = topology
            .channels
            .iter()
            .find(|c| c.kind == ChannelKind::Entry)
            .expect("entry channel");
        let cell = entry
            .entry_observation
            .as_ref()
            .expect("an entry channel is observed");
        assert!(
            crate::entry_input::observation_of("host_trades_in")
                .is_some_and(|shared| std::sync::Arc::ptr_eq(&shared, cell)),
            "the descriptor caches the cell a reader reaches by name"
        );

        assert!(
            topology
                .channels
                .iter()
                .filter(|c| c.kind != ChannelKind::Entry)
                .all(|c| c.entry_observation.is_none()),
            "only entry channels are observed"
        );

        entry.observe_delivery();
        assert!(crate::entry_input::last_delivery("host_trades_in").is_some());
    }
}

#[cfg(test)]
mod checkpoint_interval_decl_tests {
    //! The checkpoint interval's declaration crosses the C ABI as JSON, so its
    //! refusals live here rather than in a typed SDK — a declaration can arrive
    //! from any host language, and `deny_unknown_fields` makes a field the core
    //! never learned a refusal rather than a silent degrade.

    use crate::operative::topology::CheckpointOutputs;

    fn decl(json: &str) -> Result<(), String> {
        super::register_host_class_json(json.as_bytes())
    }

    fn factory(name: &str) -> String {
        format!("{name}_{}", std::process::id())
    }

    fn tier_class(id: &str, extra: &str) -> String {
        format!(
            r#"{{"factory_id":"{id}","persistent":true,"effectively_once":true,{extra}
                 "channels":[{{"name":"in","kind":"entry","consumers":[0]}},
                             {{"name":"out","kind":"plugin","producer":0,"keyed":true}}],
                 "procedures":[{{"name":"p0"}}]}}"#
        )
    }

    #[test]
    fn a_declared_interval_reaches_the_topology() {
        let id = factory("ckpt_full");
        decl(&tier_class(
            &id,
            r#""ack_granularity":"checkpoint","checkpoint_every":500,
               "checkpoint_outputs":"at_checkpoint","checkpoint_max_delay_ms":50,"#,
        ))
        .expect("a fully declared checkpoint interval registers");

        let policy = crate::operative::registry::get_class(&id)
            .expect("class registered")
            .ack_granularity
            .checkpoint()
            .expect("the class declared the checkpoint granularity");

        assert_eq!(policy.every, Some(500));
        assert_eq!(policy.outputs, CheckpointOutputs::AtCheckpoint);
        assert_eq!(policy.max_delay_ms, Some(50));
    }

    #[test]
    fn undeclared_options_are_the_as_produced_default() {
        let id = factory("ckpt_bare");
        decl(&tier_class(&id, r#""ack_granularity":"checkpoint","#))
            .expect("a bare checkpoint declaration registers");

        let policy = crate::operative::registry::get_class(&id)
            .expect("class registered")
            .ack_granularity
            .checkpoint()
            .expect("the class declared the checkpoint granularity");

        assert_eq!(policy.every, None);
        assert_eq!(policy.outputs, CheckpointOutputs::AsProduced);
        assert_eq!(policy.max_delay_ms, None);
    }

    #[test]
    fn holding_outputs_without_a_delay_is_refused_by_name() {
        let id = factory("ckpt_no_delay");
        let err = decl(&tier_class(
            &id,
            r#""ack_granularity":"checkpoint","checkpoint_outputs":"at_checkpoint","#,
        ))
        .expect_err("an unbounded output hold must not be declarable");

        assert!(
            err.contains("checkpoint_max_delay_ms"),
            "the refusal names the missing field: {err}"
        );
    }

    #[test]
    fn interval_options_without_the_checkpoint_granularity_are_refused() {
        let id = factory("ckpt_orphan");
        let err = decl(&tier_class(&id, r#""checkpoint_every":500,"#))
            .expect_err("an interval option means nothing without an interval");

        assert!(err.contains("ack_granularity checkpoint"), "{err}");
    }

    #[test]
    fn a_zero_interval_is_refused() {
        let id = factory("ckpt_zero");
        let err = decl(&tier_class(
            &id,
            r#""ack_granularity":"checkpoint","checkpoint_every":0,"#,
        ))
        .expect_err("an interval that closes every zero inputs is not an interval");

        assert!(err.contains("greater than zero"), "{err}");
    }
}

#[cfg(test)]
mod sync_durability_decl_tests {
    //! Synchronous remote durability is a workflow-only declaration
    //! (`remote-durability-tier`). The refusal lives at the host-declaration
    //! boundary rather than in a typed SDK, because that is where a
    //! declaration can arrive from any host language.

    fn decl(json: &str) -> Result<(), String> {
        super::register_host_class_json(json.as_bytes())
    }

    fn factory(name: &str) -> String {
        format!("{name}_{}", std::process::id())
    }

    #[test]
    fn a_stream_class_declaring_sync_durability_is_refused_by_name() {
        let id = factory("sync_stream");
        let err = decl(&format!(
            r#"{{"factory_id":"{id}","persistent":true,"sync_durability":true,
                 "channels":[{{"name":"in","kind":"entry","consumers":[0]}}],
                 "procedures":[{{"name":"p0"}}]}}"#
        ))
        .expect_err("a stream class must not be able to declare sync durability");

        assert!(err.contains(&id), "the refusal names the class: {err}");
        assert!(
            err.contains("sync_durability"),
            "the refusal names the reason: {err}"
        );
    }

    #[test]
    fn a_workflow_class_declaring_sync_durability_without_persistence_is_refused() {
        let id = factory("sync_wf_transient");
        let err = decl(&format!(
            r#"{{"factory_id":"{id}","persistent":false,"sync_durability":true,
                 "channels":[],"procedures":[],"workflow":{{}}}}"#
        ))
        .expect_err("there is no barrier to ship for a class that journals nothing");

        assert!(err.contains("sync_durability requires persistent"), "{err}");
    }

    #[test]
    fn a_durable_workflow_class_may_declare_it() {
        let id = factory("sync_wf_ok");
        decl(&format!(
            r#"{{"factory_id":"{id}","persistent":true,"sync_durability":true,
                 "channels":[],"procedures":[],"workflow":{{}}}}"#
        ))
        .expect("a durable workflow class declares sync durability");

        let spec = crate::workflow::registry::get_class(&id).expect("class registered");
        assert!(spec.sync_durability);
    }

    #[test]
    fn undeclared_is_the_default() {
        let id = factory("sync_wf_default");
        decl(&format!(
            r#"{{"factory_id":"{id}","persistent":true,
                 "channels":[],"procedures":[],"workflow":{{}}}}"#
        ))
        .expect("a workflow class declaring nothing registers");

        let spec = crate::workflow::registry::get_class(&id).expect("class registered");
        assert!(!spec.sync_durability);
    }
}

#[cfg(test)]
mod declaration_key_tests {
    //! The declaration key set is a cross-SDK contract, so it is pinned in a
    //! file both sides read: `test/fixtures/host_class_decl_keys.txt`. Here the
    //! core proves the file matches what it accepts; `HostClassDeclarationTest`
    //! proves the Java builder emits every key in it. A key added to one side
    //! alone fails one of the two.

    const FIXTURE: &str = include_str!("../../test/fixtures/host_class_decl_keys.txt");
    const SOURCE: &str = include_str!("host.rs");

    fn pinned(section: &str) -> Vec<String> {
        FIXTURE
            .lines()
            .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
            .filter_map(|line| line.strip_prefix(&format!("{section}.")))
            .map(str::to_string)
            .collect()
    }

    /// The `pub <name>:` fields of one declaration struct, in declaration order.
    fn fields_of(struct_name: &str) -> Vec<String> {
        let body = SOURCE
            .split_once(&format!("pub struct {struct_name} {{"))
            .unwrap_or_else(|| panic!("{struct_name} is declared in host.rs"))
            .1;
        let body = body.split_once("\n}").expect("the struct body ends").0;

        body.lines()
            .filter_map(|line| line.trim().strip_prefix("pub "))
            .filter_map(|rest| rest.split_once(':'))
            .map(|(name, _)| name.to_string())
            .collect()
    }

    #[test]
    fn every_declaration_field_is_pinned_by_the_shared_fixture() {
        for (section, struct_name) in [
            ("class", "HostClassDecl"),
            ("channel", "HostChannelDecl"),
            ("procedure", "HostProcedureDecl"),
            ("workflow", "HostWorkflowDecl"),
        ] {
            assert_eq!(
                fields_of(struct_name),
                pinned(section),
                "{struct_name} and the `{section}.` lines of \
                 test/fixtures/host_class_decl_keys.txt disagree — teach the fixture, \
                 the Rust builder and the Java builder in the same change"
            );
        }
    }

    #[test]
    fn a_declaration_carrying_every_pinned_key_is_accepted() {
        // `deny_unknown_fields` makes this exact: a key the fixture spells
        // differently from the core is a parse error naming it.
        let value = |section: &str, key: &str| -> &'static str {
            match (section, key) {
                (_, "factory_id") | (_, "name") => r#""pinned_decl""#,
                ("class", "schema_version") => "1",
                ("class", "sdk_lang") | ("class", "change_format_id") => "1",
                ("class", "ack_granularity") => r#""checkpoint""#,
                ("class", "checkpoint_outputs") => r#""at_checkpoint""#,
                ("class", "beyond_window_policy") => r#""drop""#,
                ("class", "channels") => r#"[{"name":"in","kind":"entry","consumers":[0]}]"#,
                ("class", "procedures") => r#"[{"name":"p0"}]"#,
                ("class", "workflow") => "null",
                ("channel", "kind") => r#""entry""#,
                ("channel", "producer") => "null",
                ("channel", "consumers") => "[0]",
                ("channel", "overflow") => r#""block""#,
                ("procedure", "entity_mode") => r#""single""#,
                ("procedure", "entity_keys") => "[]",
                ("procedure", "clock") => r#""wall""#,
                ("workflow", "loop_mode") => r#""once""#,
                ("workflow", "rollback_mode") => r#""all_or_nothing""#,
                (_, k) if k.ends_with("_ms") || k.ends_with("_bytes") => "1000",
                (_, k) if k.ends_with("_every") || k.ends_with("_attempts") => "1",
                (_, k) if k.ends_with("_max") || k.ends_with("_capacity") => "1",
                (_, k) if k.ends_with("_entities") => "1",
                _ => "true",
            }
        };

        let object = |section: &str| -> String {
            pinned(section)
                .iter()
                .map(|key| format!(r#""{key}":{}"#, value(section, key)))
                .collect::<Vec<_>>()
                .join(",")
        };

        let id = format!("pinned_decl_{}", std::process::id());
        let json = format!(
            "{{{},{},{},{}}}",
            object("class").replace(
                r#""factory_id":"pinned_decl""#,
                &format!(r#""factory_id":"{id}""#)
            ),
            format_args!(r#""channels":[{{{}}}]"#, object("channel")),
            format_args!(r#""procedures":[{{{}}}]"#, object("procedure")),
            format_args!(r#""workflow":{{{}}}"#, object("workflow")),
        );

        // The class-level `channels` / `procedures` / `workflow` placeholders
        // above are superseded by the fully populated ones appended after them,
        // so strip the placeholders before parsing.
        let json = json
            .replace(
                r#""channels":[{"name":"in","kind":"entry","consumers":[0]}],"#,
                "",
            )
            .replace(r#""procedures":[{"name":"p0"}],"#, "")
            .replace(r#""workflow":null,"#, "");

        let decl: super::HostClassDecl = serde_json::from_slice(json.as_bytes())
            .unwrap_or_else(|e| panic!("a fully pinned declaration must parse: {e}\n{json}"));

        assert_eq!(decl.factory_id, id);
        assert_eq!(decl.channels.len(), 1);
        assert_eq!(decl.procedures.len(), 1);
        assert!(decl.workflow.is_some());
    }
}
