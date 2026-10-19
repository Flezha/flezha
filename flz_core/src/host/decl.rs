//! The host class declaration as the C ABI's JSON deserializes it: the serde
//! types and their defaults. The key set is a cross-SDK contract pinned by
//! `test/fixtures/host_class_decl_keys.txt`; `deny_unknown_fields` makes a
//! key the core never learned a refusal rather than a silent degrade.

use crate::channel::{OverflowPolicy, RetryPolicy};

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
    pub overflow: OverflowPolicy,
    #[serde(default)]
    pub keyed: bool,
    /// The payload format the channel declares, announced in its relay's
    /// API: an entry's input, an output's answer. Absent is `json`.
    #[serde(default)]
    pub codec: Option<HostCodec>,
    /// The JSON Schema of an entry's input or an output's value, declared or
    /// derived by the host; only a `json` channel carries one.
    #[serde(default)]
    pub schema: Option<serde_json::Value>,
    /// What an entry channel is for, announced in its relay's API.
    #[serde(default)]
    pub description: String,
}

/// A channel's payload format, as a relay's API names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostCodec {
    Json,
    Text,
    Bytes,
    Msgpack,
    Custom,
}

impl HostCodec {
    pub fn name(self) -> &'static str {
        match self {
            HostCodec::Json => "json",
            HostCodec::Text => "text",
            HostCodec::Bytes => "bytes",
            HostCodec::Msgpack => "msgpack",
            HostCodec::Custom => "custom",
        }
    }
}

/// One link of a fluent chain declared off a channel handle, in the order the
/// builder was called. A chain closed by `Into` or `Merge` wires an edge;
/// `Sink` terminates the channel on a plugin; `Branch` records the sugar's
/// arity and closes the chain it stands in.
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
    /// Terminates the chain on a call to another relay: every value that
    /// reaches it, after the chain's transforms and key, is one request. The
    /// outcome enters the relay on the entry channel `response` names.
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
    /// The callee's channel whose output answers; its first output when absent.
    #[serde(default)]
    pub answer_channel: Option<String>,
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

fn default_retry_max() -> u32 {
    RetryPolicy::default().max_attempts
}
fn channel_backoff_ms() -> u64 {
    RetryPolicy::default().backoff_base_ms
}
fn default_mailbox_capacity() -> usize {
    flz_transport::env::env_parse_positive("FLEZHA_MAILBOX_CAPACITY", 1024)
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostTaskletDecl {
    pub name: String,
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
    /// The tasklet supplies a migration, so a floor below its own version
    /// can be read up to it.
    #[serde(default)]
    pub migrates: bool,
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
    /// What the class is for, announced with its factory id.
    #[serde(default)]
    pub description: String,
    /// A wall-clock stop, for either run.
    #[serde(default)]
    pub max_duration_ms: Option<u64>,
    /// How the class runs, as the settings tree writes it; absent is `flow`.
    /// Read by [`crate::relay::settings::parse_run`], which names a
    /// misplaced setting with its parent.
    #[serde(default)]
    pub run: Option<serde_json::Value>,
    /// Where its work is kept; absent is `local`, sleeping as its run does.
    #[serde(default)]
    pub durable: Option<serde_json::Value>,
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
    pub compact_after_bytes: Option<usize>,
    pub channels: Vec<HostChannelDecl>,
    pub tasklets: Vec<HostTaskletDecl>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HostAckGranularity {
    #[default]
    PerInput,
    Checkpoint,
}

fn default_decl_schema_version() -> u32 {
    1
}
fn default_change_format() -> u8 {
    crate::header::FORMAT_ID_POSTCARD
}
