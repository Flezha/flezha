//! The reference vocabulary of registration-time refusals: one variant per
//! shape a declared topology can take that the runtime cannot honour, each
//! answering its `topology.*` code. Every SDK submits its record to
//! [`super::register::check`]; a refusal's sentence is the catalogue's
//! (`flz_transport/errors.json`), filled from the variant's fields, so the code
//! is the contract and the text is free to change.
//!
//! A variant here is paired with a row of `test/fixtures/reference_topologies.txt`
//! by `test/sdk_conformance/topology_check_test.exs`: a variant with no row and
//! a row with no variant both fail that test by name.

use flz_transport::errors::{ErrorModel, FlezhaError};
use serde_json::{Map, Value};

use crate::error::Code;

/// Reasons a host class declaration fails at registration time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopologyIssue {
    DuplicateChannel(String),
    NodeIdentityCollision {
        name: String,
    },
    ChannelNameTooLong {
        channel: String,
        len: usize,
        max: usize,
    },
    BranchTooFewArms {
        arms: usize,
    },
    Cycle {
        tasklet: String,
    },
    UnreachableTasklet {
        tasklet: String,
    },
    CellsForMissing {
        name: String,
    },
    KeyByOnUnkeyableTarget {
        channel: String,
        tasklet: String,
    },
    DuplicateKeyBy {
        channel: String,
    },
    TransformAfterTerminal {
        channel: String,
    },
    TransformAfterKeyBy {
        channel: String,
    },
    DanglingTransformChain {
        channel: String,
    },
    UnkeyedEdgeIntoDynamicTasklet {
        channel: String,
        tasklet: String,
    },
    MailboxCapacityZero {
        tasklet: String,
    },
    MailboxCapacityTooLarge {
        tasklet: String,
        value: usize,
    },
    DuplicatePluginSink {
        channel: String,
    },
    ChannelNamedTwice {
        channel: String,
        named: String,
        sink: String,
    },
    DuplicateSinkName {
        channel: String,
    },
    KeyedChannelWithoutPluginWire {
        channel: String,
    },
    UnservableClock {
        tasklet: String,
        clock_type: String,
    },
    UnkeyedChannelUnderEffectivelyOnce {
        channel: String,
    },
    RetryMaxZero {
        channel: String,
    },
    DroppingPolicyUnderEffectivelyOnce {
        channel: String,
        policy: String,
    },
    CorrectionWithoutTier,
    CorrectionWithoutEventTime {
        tasklet: String,
        clock_type: String,
    },
    CorrectionWindowZero,
    CheckpointEveryZero,
    CheckpointAckWithoutTier,
    MergeTooFewSources {
        tasklet: String,
        sources: usize,
    },
    DuplicateMergeSource {
        channel: String,
    },
    EdgeAlreadyWired {
        channel: String,
        tasklet: String,
    },
    EffectivelyOnceOnBounded,
    CorrectionWindowOnBounded,
    CheckpointOnBounded,
    SyncDurabilityOnUnbounded {
        factory_id: String,
    },
    SleepWithoutPersistence {
        factory_id: String,
    },
    AtCheckpointWithoutMaxDelay,
    UnknownSdkLang {
        sdk_lang: u8,
    },
    UnusableSdkVersion {
        factory_id: String,
        sdk_version: String,
    },
    NoTasklets,
    SyncDurabilityWithoutPersistence,
    EffectivelyOnceWithoutPersistence,
    CheckpointMaxDelayZero,
    ProducerOutOfRange {
        channel: String,
        producer: u32,
        tasklets: usize,
    },
    ConsumerOutOfRange {
        channel: String,
        consumer: u32,
        tasklets: usize,
    },
    EntryChannelWithProducer {
        channel: String,
    },
    EntryChannelWithoutConsumer {
        channel: String,
    },
    InterTaskChannelWithoutProducer {
        channel: String,
    },
    PluginChannelWithConsumers {
        channel: String,
    },
    StaticTaskletWithoutCellKeys {
        tasklet: String,
    },
    CellKeysWithoutStaticMode {
        tasklet: String,
    },
    MaxCellsZero {
        tasklet: String,
    },
    DeclarationUnreadable {
        reason: String,
    },
    NoEntryChannel,
    MultipleOutputChannels {
        tasklet: String,
    },
    BranchingChainedLoop {
        channel: String,
        leaves: usize,
    },
    ChainedLoopTerminalWithoutHandoff {
        tasklet: String,
    },
}

impl ErrorModel for TopologyIssue {
    /// The refusal in the model's one shape, the variant's fields as its
    /// details.
    fn to_error(&self) -> FlezhaError {
        self.kind().error(self.details())
    }
}

impl TopologyIssue {
    fn kind(&self) -> Code {
        match self {
            Self::DuplicateChannel(..) => Code::DuplicateChannel,
            Self::NodeIdentityCollision { .. } => Code::NodeIdentityCollision,
            Self::ChannelNameTooLong { .. } => Code::ChannelNameTooLong,
            Self::BranchTooFewArms { .. } => Code::BranchTooFewArms,
            Self::Cycle { .. } => Code::Cycle,
            Self::UnreachableTasklet { .. } => Code::UnreachableTasklet,
            Self::CellsForMissing { .. } => Code::CellsForMissing,
            Self::KeyByOnUnkeyableTarget { .. } => Code::KeyByOnUnkeyableTarget,
            Self::DuplicateKeyBy { .. } => Code::DuplicateKeyBy,
            Self::TransformAfterTerminal { .. } => Code::TransformAfterTerminal,
            Self::TransformAfterKeyBy { .. } => Code::TransformAfterKeyBy,
            Self::DanglingTransformChain { .. } => Code::DanglingTransformChain,
            Self::UnkeyedEdgeIntoDynamicTasklet { .. } => Code::UnkeyedEdgeIntoDynamicTasklet,
            Self::MailboxCapacityZero { .. } => Code::MailboxCapacityZero,
            Self::MailboxCapacityTooLarge { .. } => Code::MailboxCapacityTooLarge,
            Self::DuplicatePluginSink { .. } => Code::DuplicatePluginSink,
            Self::ChannelNamedTwice { .. } => Code::ChannelNamedTwice,
            Self::DuplicateSinkName { .. } => Code::DuplicateSinkName,
            Self::KeyedChannelWithoutPluginWire { .. } => Code::KeyedChannelWithoutPluginWire,
            Self::UnservableClock { .. } => Code::UnservableClock,
            Self::UnkeyedChannelUnderEffectivelyOnce { .. } => {
                Code::UnkeyedChannelUnderEffectivelyOnce
            }
            Self::RetryMaxZero { .. } => Code::RetryMaxZero,
            Self::DroppingPolicyUnderEffectivelyOnce { .. } => {
                Code::DroppingPolicyUnderEffectivelyOnce
            }
            Self::CorrectionWithoutTier => Code::CorrectionWithoutTier,
            Self::CorrectionWithoutEventTime { .. } => Code::CorrectionWithoutEventTime,
            Self::CorrectionWindowZero => Code::CorrectionWindowZero,
            Self::CheckpointEveryZero => Code::CheckpointEveryZero,
            Self::CheckpointAckWithoutTier => Code::CheckpointAckWithoutTier,
            Self::MergeTooFewSources { .. } => Code::MergeTooFewSources,
            Self::DuplicateMergeSource { .. } => Code::DuplicateMergeSource,
            Self::EdgeAlreadyWired { .. } => Code::EdgeAlreadyWired,
            Self::EffectivelyOnceOnBounded => Code::EffectivelyOnceOnBounded,
            Self::CorrectionWindowOnBounded => Code::CorrectionWindowOnBounded,
            Self::CheckpointOnBounded => Code::CheckpointOnBounded,
            Self::SyncDurabilityOnUnbounded { .. } => Code::SyncDurabilityOnUnbounded,
            Self::SleepWithoutPersistence { .. } => Code::SleepWithoutPersistence,
            Self::AtCheckpointWithoutMaxDelay => Code::AtCheckpointWithoutMaxDelay,
            Self::UnknownSdkLang { .. } => Code::UnknownSdkLang,
            Self::UnusableSdkVersion { .. } => Code::UnusableSdkVersion,
            Self::NoTasklets => Code::NoTasklets,
            Self::SyncDurabilityWithoutPersistence => Code::SyncDurabilityWithoutPersistence,
            Self::EffectivelyOnceWithoutPersistence => Code::EffectivelyOnceWithoutPersistence,
            Self::CheckpointMaxDelayZero => Code::CheckpointMaxDelayZero,
            Self::ProducerOutOfRange { .. } => Code::ProducerOutOfRange,
            Self::ConsumerOutOfRange { .. } => Code::ConsumerOutOfRange,
            Self::EntryChannelWithProducer { .. } => Code::EntryChannelWithProducer,
            Self::EntryChannelWithoutConsumer { .. } => Code::EntryChannelWithoutConsumer,
            Self::InterTaskChannelWithoutProducer { .. } => Code::InterTaskChannelWithoutProducer,
            Self::PluginChannelWithConsumers { .. } => Code::PluginChannelWithConsumers,
            Self::StaticTaskletWithoutCellKeys { .. } => Code::StaticTaskletWithoutCellKeys,
            Self::CellKeysWithoutStaticMode { .. } => Code::CellKeysWithoutStaticMode,
            Self::MaxCellsZero { .. } => Code::MaxCellsZero,
            Self::DeclarationUnreadable { .. } => Code::DeclarationUnreadable,
            Self::NoEntryChannel => Code::NoEntryChannel,
            Self::MultipleOutputChannels { .. } => Code::MultipleOutputChannels,
            Self::BranchingChainedLoop { .. } => Code::BranchingChainedLoop,
            Self::ChainedLoopTerminalWithoutHandoff { .. } => {
                Code::ChainedLoopTerminalWithoutHandoff
            }
        }
    }

    fn details(&self) -> Map<String, Value> {
        let pairs: Vec<(&str, Value)> = match self {
            Self::DuplicateChannel(channel) => vec![("channel", channel.as_str().into())],
            Self::NodeIdentityCollision { name } => vec![("name", name.clone().into())],
            Self::ChannelNameTooLong { channel, len, max } => vec![
                ("channel", channel.clone().into()),
                ("len", (*len).into()),
                ("max", (*max).into()),
            ],
            Self::BranchTooFewArms { arms } => vec![("arms", (*arms).into())],
            Self::Cycle { tasklet } => vec![("tasklet", tasklet.clone().into())],
            Self::UnreachableTasklet { tasklet } => vec![("tasklet", tasklet.clone().into())],
            Self::CellsForMissing { name } => vec![("name", name.clone().into())],
            Self::KeyByOnUnkeyableTarget { channel, tasklet } => vec![
                ("channel", channel.clone().into()),
                ("tasklet", tasklet.clone().into()),
            ],
            Self::DuplicateKeyBy { channel } => vec![("channel", channel.clone().into())],
            Self::TransformAfterTerminal { channel } => vec![("channel", channel.clone().into())],
            Self::TransformAfterKeyBy { channel } => vec![("channel", channel.clone().into())],
            Self::DanglingTransformChain { channel } => vec![("channel", channel.clone().into())],
            Self::UnkeyedEdgeIntoDynamicTasklet { channel, tasklet } => vec![
                ("channel", channel.clone().into()),
                ("tasklet", tasklet.clone().into()),
            ],
            Self::MailboxCapacityZero { tasklet } => vec![("tasklet", tasklet.clone().into())],
            Self::MailboxCapacityTooLarge { tasklet, value } => vec![
                ("tasklet", tasklet.clone().into()),
                ("value", (*value).into()),
            ],
            Self::DuplicatePluginSink { channel } => vec![("channel", channel.clone().into())],
            Self::ChannelNamedTwice {
                channel,
                named,
                sink,
            } => vec![
                ("channel", channel.clone().into()),
                ("named", named.clone().into()),
                ("sink", sink.clone().into()),
            ],
            Self::DuplicateSinkName { channel } => vec![("channel", channel.clone().into())],
            Self::KeyedChannelWithoutPluginWire { channel } => {
                vec![("channel", channel.clone().into())]
            }
            Self::UnservableClock {
                tasklet,
                clock_type,
            } => vec![
                ("tasklet", tasklet.clone().into()),
                ("clock_type", clock_type.clone().into()),
            ],
            Self::UnkeyedChannelUnderEffectivelyOnce { channel } => {
                vec![("channel", channel.clone().into())]
            }
            Self::RetryMaxZero { channel } => vec![("channel", channel.clone().into())],
            Self::DroppingPolicyUnderEffectivelyOnce { channel, policy } => vec![
                ("channel", channel.clone().into()),
                ("policy", policy.clone().into()),
            ],
            Self::CorrectionWithoutEventTime {
                tasklet,
                clock_type,
            } => vec![
                ("tasklet", tasklet.clone().into()),
                ("clock_type", clock_type.clone().into()),
            ],
            Self::MergeTooFewSources { tasklet, sources } => vec![
                ("tasklet", tasklet.clone().into()),
                ("sources", (*sources).into()),
            ],
            Self::DuplicateMergeSource { channel } => vec![("channel", channel.clone().into())],
            Self::EdgeAlreadyWired { channel, tasklet } => vec![
                ("channel", channel.clone().into()),
                ("tasklet", tasklet.clone().into()),
            ],
            Self::SyncDurabilityOnUnbounded { factory_id } => {
                vec![("factory_id", factory_id.clone().into())]
            }
            Self::SleepWithoutPersistence { factory_id } => {
                vec![("factory_id", factory_id.clone().into())]
            }
            Self::UnknownSdkLang { sdk_lang } => vec![("sdk_lang", (*sdk_lang).into())],
            Self::UnusableSdkVersion {
                factory_id,
                sdk_version,
            } => vec![
                ("factory_id", factory_id.clone().into()),
                ("sdk_version", sdk_version.clone().into()),
            ],
            Self::ProducerOutOfRange {
                channel,
                producer,
                tasklets,
            } => vec![
                ("channel", channel.clone().into()),
                ("producer", (*producer).into()),
                ("tasklets", (*tasklets).into()),
            ],
            Self::ConsumerOutOfRange {
                channel,
                consumer,
                tasklets,
            } => vec![
                ("channel", channel.clone().into()),
                ("consumer", (*consumer).into()),
                ("tasklets", (*tasklets).into()),
            ],
            Self::EntryChannelWithProducer { channel } => vec![("channel", channel.clone().into())],
            Self::EntryChannelWithoutConsumer { channel } => {
                vec![("channel", channel.clone().into())]
            }
            Self::InterTaskChannelWithoutProducer { channel } => {
                vec![("channel", channel.clone().into())]
            }
            Self::PluginChannelWithConsumers { channel } => {
                vec![("channel", channel.clone().into())]
            }
            Self::StaticTaskletWithoutCellKeys { tasklet } => {
                vec![("tasklet", tasklet.clone().into())]
            }
            Self::CellKeysWithoutStaticMode { tasklet } => {
                vec![("tasklet", tasklet.clone().into())]
            }
            Self::MaxCellsZero { tasklet } => vec![("tasklet", tasklet.clone().into())],
            Self::DeclarationUnreadable { reason } => vec![("reason", reason.clone().into())],
            Self::MultipleOutputChannels { tasklet } => vec![("tasklet", tasklet.clone().into())],
            Self::BranchingChainedLoop { channel, leaves } => vec![
                ("channel", channel.clone().into()),
                ("leaves", (*leaves).into()),
            ],
            Self::ChainedLoopTerminalWithoutHandoff { tasklet } => {
                vec![("tasklet", tasklet.clone().into())]
            }
            Self::CorrectionWithoutTier
            | Self::CorrectionWindowZero
            | Self::CheckpointEveryZero
            | Self::CheckpointAckWithoutTier
            | Self::EffectivelyOnceOnBounded
            | Self::CorrectionWindowOnBounded
            | Self::CheckpointOnBounded
            | Self::AtCheckpointWithoutMaxDelay
            | Self::NoTasklets
            | Self::SyncDurabilityWithoutPersistence
            | Self::EffectivelyOnceWithoutPersistence
            | Self::CheckpointMaxDelayZero
            | Self::NoEntryChannel => Vec::new(),
        };
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect()
    }
}

impl std::fmt::Display for TopologyIssue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_error().message)
    }
}

impl std::error::Error for TopologyIssue {}

/// A verdict as one error: the first issue's code, message and details, with
/// `details.issues` listing every issue as `{code, message, details}`, so a
/// reader learns every refusal that fired without parsing prose.
pub fn render_issues(issues: &[TopologyIssue]) -> FlezhaError {
    let listed: Vec<Value> = issues
        .iter()
        .map(|issue| {
            let error = issue.to_error();
            serde_json::json!({
                "code": error.code,
                "message": error.message,
                "details": error.details,
            })
        })
        .collect();
    let mut error = match issues.first() {
        Some(first) => first.to_error(),
        // `check` refuses only with at least one issue.
        None => FlezhaError::with(
            flz_transport::errors::UNCATALOGUED,
            [("reason", "an empty topology verdict".into())],
        ),
    };
    error
        .details
        .insert("issues".to_string(), Value::Array(listed));
    error
}
