//! The reference vocabulary of registration-time refusals: one name and one
//! sentence per shape a declared topology can take that the runtime cannot
//! honour. Every SDK submits its record to
//! [`super::register::check`] and renders these names; the veneer suites
//! assert substrings of these sentences, so a text is a contract.
//!
//! A variant here is paired with a row of `test/fixtures/reference_topologies.txt`
//! by `test/sdk_conformance/topology_check_test.exs`: a variant with no row and
//! a row with no variant both fail that test by name.

/// Reasons a host class declaration fails at registration time.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TopologyIssue {
    #[error("channel `{0}` declared more than once on this operative")]
    DuplicateChannel(String),

    #[error(
        "node identity `{name}` is declared by two distinct nodes; a node name must be unique \
         per operative and distinct from every procedure name()"
    )]
    NodeIdentityCollision { name: String },

    #[error(
        "channel `{channel}` has a {len}-byte name; the name rides every frame this channel \
         produces, under a one-byte length prefix, so it may be at most {max} bytes"
    )]
    ChannelNameTooLong {
        channel: String,
        len: usize,
        max: usize,
    },

    #[error("branch(...) requires at least two arms; {arms} given")]
    BranchTooFewArms { arms: usize },

    #[error(
        "procedure `{procedure}` is on a cycle; an operative topology is a DAG — a node's \
         output may never reach its own input"
    )]
    Cycle { procedure: String },

    #[error("procedure `{procedure}` is not reachable from any entry channel")]
    UnreachableProcedure { procedure: String },

    #[error(
        "procedure `{name}` declares non-empty entity_keys() but does not override entities_for \
         (set ENTITIES_FOR_OVERRIDDEN = true)"
    )]
    EntitiesForMissing { name: String },

    #[error(
        "channel `{channel}` declares .key_by(...) into procedure `{procedure}`, which declares a \
         non-empty entity_keys() (static mode); static keying routes by entities_for and may name \
         several Entities, edge keying names exactly one — declare one OR the other, not both"
    )]
    KeyByOnUnkeyableTarget { channel: String, procedure: String },

    #[error("channel `{channel}` declares .key_by(...) twice before one `.into(...)`")]
    DuplicateKeyBy { channel: String },

    #[error(
        "channel `{channel}` declares a transform or .key_by(...) after a plugin sink; the sink \
         serializes the channel's own value, so nothing downstream of it can be reshaped"
    )]
    TransformAfterTerminal { channel: String },

    #[error(
        "channel `{channel}` declares a transform after .key_by(...); the key is evaluated on the \
         final post-transform value, so it must be the last link before `.into(...)`"
    )]
    TransformAfterKeyBy { channel: String },

    #[error(
        "channel `{channel}` declares a transform or .key_by(...) that is never closed by \
         `.into(...)`; a transform chain is stored on the consumer edge it terminates in, so a \
         dangling one would silently do nothing"
    )]
    DanglingTransformChain { channel: String },

    #[error(
        "channel `{channel}` feeds procedure `{procedure}` without a .key_by(...), but another \
         edge keys it into dynamic mode; every edge into a dynamic-mode procedure must name the \
         Entity its inputs reach"
    )]
    UnkeyedEdgeIntoDynamicProcedure { channel: String, procedure: String },

    #[error(
        "procedure `{procedure}` declared mailbox_capacity = 0; capacity must be in 1..=1_048_576"
    )]
    MailboxCapacityZero { procedure: String },

    #[error(
        "procedure `{procedure}` declared mailbox_capacity = {value}; capacity must be in 1..=1_048_576"
    )]
    MailboxCapacityTooLarge { procedure: String, value: usize },

    #[error(
        "channel `{channel}` was declared with `sink(...)` more than once; \
         only one plugin sink per channel is allowed"
    )]
    DuplicatePluginSink { channel: String },

    #[error(
        "channel `{channel}` is named twice: .named(`{named}`) and .sink(`{sink}`) both name it. \
         A sink names the channel it terminates, so write the name once — .sink(`{named}`) to \
         keep the channel's name, or .named(`{named}`) with the no-argument .sink()"
    )]
    ChannelNamedTwice {
        channel: String,
        named: String,
        sink: String,
    },

    #[error(
        "channel `{channel}` is named by two plugin sinks; a sink names the channel it \
         terminates, so two channels cannot name one sink. Give each its own name — a plugin \
         still reaches both by attaching to the operative with no channel filter"
    )]
    DuplicateSinkName { channel: String },

    #[error(
        "channel `{channel}` is declared .keyed() but has no plugin wire; output identity \
         stamps only exist on plugin-bound frames (add .sink(...))"
    )]
    KeyedChannelWithoutPluginWire { channel: String },

    #[error(
        "procedure `{procedure}` declares clock `{clock_type}` with `type Change = NoChange`; \
         the runtime cannot serve it (logical time is the durable change seq, which never \
         advances without persistent changes)"
    )]
    UnservableClock {
        procedure: String,
        clock_type: String,
    },

    #[error(
        "channel `{channel}` has a plugin wire but is not declared .keyed(); the \
         effectively-once tier deduplicates by output identity, so every plugin-bound \
         channel of an effectively_once() operative must opt in with .keyed()"
    )]
    UnkeyedChannelUnderEffectivelyOnce { channel: String },

    #[error(
        "channel `{channel}` declared retry_max = 0; at least one attempt is required \
         (use ProcedureError::non_retryable to park without retries)"
    )]
    RetryMaxZero { channel: String },

    #[error(
        "channel `{channel}` declares .on_full({policy}) on an operative that declares \
         effectively_once(); a shed input's offset is still advanced past by the tier's \
         watermark, so the loss is permanent and reported nowhere — declare one or the other, \
         not both (a correction window implies the tier, so it is refused here too)"
    )]
    DroppingPolicyUnderEffectivelyOnce { channel: String, policy: String },

    #[error(
        "operative declares correction_window() without effectively_once(); correction \
         replays the input log against a rewound baseline, and only the tier journals \
         inputs before forwarding them (an at-least-once operative has no durable input \
         to replay)"
    )]
    CorrectionWithoutTier,

    #[error(
        "operative declares correction_window() but procedure `{procedure}` declares clock \
         `{clock_type}`; correction orders a replayed slice by event time, which only \
         `EventTimeFrom` yields"
    )]
    CorrectionWithoutEventTime {
        procedure: String,
        clock_type: String,
    },

    #[error("operative declares correction_window(0); a zero window can correct nothing")]
    CorrectionWindowZero,

    #[error(
        "operative declares checkpoint_every(0); an interval that closes every zero inputs is \
         not an interval — omit it to close only on an explicit ctx.checkpoint()"
    )]
    CheckpointEveryZero,

    #[error(
        "operative declares checkpoint_ack() without effectively_once(); the checkpoint is \
         where the tier's consumed-offset advances, and an at-least-once operative has no \
         durable offset to hold back (nor a durable input log to replay the interval from)"
    )]
    CheckpointAckWithoutTier,

    #[error(
        "merge into procedure `{procedure}` declares {sources} source channel(s); a merge \
         consumes two or more — wire a single source with `.into::<Proc>()` instead"
    )]
    MergeTooFewSources { procedure: String, sources: usize },

    #[error(
        "merge subscribes channel `{channel}` twice; each merge source must be a distinct \
         channel (one mapping per channel into the merged input)"
    )]
    DuplicateMergeSource { channel: String },

    #[error(
        "channel `{channel}` already feeds procedure `{procedure}`; an edge carries exactly one \
         transform and one key or neither, so a re-declaration where either side carries one is \
         refused rather than silently keeping the first"
    )]
    EdgeAlreadyWired { channel: String, procedure: String },

    // The class facts the fixture pins. Each carries the sentence `register`
    // printed as free text before it had a name.
    #[error("a workflow class cannot declare effectively_once")]
    EffectivelyOnceOnWorkflow,

    #[error("a workflow class cannot declare correction_window_ms")]
    CorrectionWindowOnWorkflow,

    #[error(
        "class `{factory_id}` declares sync_durability, which only a workflow class may: a stream \
         class acknowledges at input rates, and a remote round trip per acknowledgement is \
         not a guarantee it can keep"
    )]
    SyncDurabilityOnStream { factory_id: String },

    #[error(
        "checkpoint_outputs at_checkpoint requires checkpoint_max_delay_ms: an interval that \
         never closes would hold its outputs forever on a quiet Entity"
    )]
    AtCheckpointWithoutMaxDelay,

    /// A refusal this vocabulary has not named yet: the residue of the text
    /// refusals `register` has always printed (the language byte, the SDK
    /// version, the workflow ladder, the DAG leaf, the index arities). It is
    /// the one variant the fixture does not pair a row with, so that a
    /// declaration is never refused with no verdict at all.
    #[error("{0}")]
    Declaration(String),
}

impl TopologyIssue {
    /// The variant's name as the fixture and every SDK's verdict spell it.
    /// Read by the FFI verdict encoder and by each SDK's rendering, so a
    /// refusal reaches an author under one name in every language.
    pub fn name(&self) -> &'static str {
        match self {
            Self::DuplicateChannel(..) => "DuplicateChannel",
            Self::NodeIdentityCollision { .. } => "NodeIdentityCollision",
            Self::ChannelNameTooLong { .. } => "ChannelNameTooLong",
            Self::BranchTooFewArms { .. } => "BranchTooFewArms",
            Self::Cycle { .. } => "Cycle",
            Self::UnreachableProcedure { .. } => "UnreachableProcedure",
            Self::EntitiesForMissing { .. } => "EntitiesForMissing",
            Self::KeyByOnUnkeyableTarget { .. } => "KeyByOnUnkeyableTarget",
            Self::DuplicateKeyBy { .. } => "DuplicateKeyBy",
            Self::TransformAfterTerminal { .. } => "TransformAfterTerminal",
            Self::TransformAfterKeyBy { .. } => "TransformAfterKeyBy",
            Self::DanglingTransformChain { .. } => "DanglingTransformChain",
            Self::UnkeyedEdgeIntoDynamicProcedure { .. } => "UnkeyedEdgeIntoDynamicProcedure",
            Self::MailboxCapacityZero { .. } => "MailboxCapacityZero",
            Self::MailboxCapacityTooLarge { .. } => "MailboxCapacityTooLarge",
            Self::DuplicatePluginSink { .. } => "DuplicatePluginSink",
            Self::ChannelNamedTwice { .. } => "ChannelNamedTwice",
            Self::DuplicateSinkName { .. } => "DuplicateSinkName",
            Self::KeyedChannelWithoutPluginWire { .. } => "KeyedChannelWithoutPluginWire",
            Self::UnservableClock { .. } => "UnservableClock",
            Self::UnkeyedChannelUnderEffectivelyOnce { .. } => "UnkeyedChannelUnderEffectivelyOnce",
            Self::RetryMaxZero { .. } => "RetryMaxZero",
            Self::DroppingPolicyUnderEffectivelyOnce { .. } => "DroppingPolicyUnderEffectivelyOnce",
            Self::CorrectionWithoutTier => "CorrectionWithoutTier",
            Self::CorrectionWithoutEventTime { .. } => "CorrectionWithoutEventTime",
            Self::CorrectionWindowZero => "CorrectionWindowZero",
            Self::CheckpointEveryZero => "CheckpointEveryZero",
            Self::CheckpointAckWithoutTier => "CheckpointAckWithoutTier",
            Self::MergeTooFewSources { .. } => "MergeTooFewSources",
            Self::DuplicateMergeSource { .. } => "DuplicateMergeSource",
            Self::EdgeAlreadyWired { .. } => "EdgeAlreadyWired",
            Self::EffectivelyOnceOnWorkflow => "EffectivelyOnceOnWorkflow",
            Self::CorrectionWindowOnWorkflow => "CorrectionWindowOnWorkflow",
            Self::SyncDurabilityOnStream { .. } => "SyncDurabilityOnStream",
            Self::AtCheckpointWithoutMaxDelay => "AtCheckpointWithoutMaxDelay",
            Self::Declaration(..) => "Declaration",
        }
    }
}

/// Render a verdict the way every surface that is not JSON renders it: the
/// names joined for the reader, then the sentences. The FFI error text and
/// the Rust builder's panic both go through here, so the substring contract
/// holds wherever a refusal is printed.
pub fn render_issues(issues: &[TopologyIssue]) -> String {
    let names: Vec<&str> = issues.iter().map(TopologyIssue::name).collect();
    let mut out = format!("topology rejected [{}]:", names.join(", "));
    for issue in issues {
        out.push_str("\n  - ");
        out.push_str(&issue.to_string());
    }
    out
}
