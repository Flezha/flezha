//! The crate's one refusal type, by the boundary that refused, and [`Code`],
//! every catalogue code the core constructs. A variant carries its error in
//! the model's one shape (`flz_transport::errors`), built from a `Code` and
//! its details, so `Display` is the catalogue's message and the code is what
//! the controller and every veneer branch on.

use flz_transport::errors::{Class, ErrorModel, FlezhaError};
use flz_transport::transport::config::QuicListError;
use flz_transport::utility::socket_dir::SocketDirError;
use serde_json::{Map, Value};

use crate::pb;

macro_rules! codes {
    ($($variant:ident => $code:literal,)*) => {
        /// Every code the core constructs; each is an entry of
        /// `flz_transport/errors.json`.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum Code {
            $($variant,)*
        }

        impl Code {
            pub const ALL: &'static [Code] = &[$(Code::$variant,)*];

            pub fn as_str(self) -> &'static str {
                match self {
                    $(Code::$variant => $code,)*
                }
            }
        }
    };
}

codes! {
    DuplicateChannel => "topology.duplicate_channel",
    NodeIdentityCollision => "topology.node_identity_collision",
    ChannelNameTooLong => "topology.channel_name_too_long",
    BranchTooFewArms => "topology.branch_too_few_arms",
    Cycle => "topology.cycle",
    UnreachableTasklet => "topology.unreachable_tasklet",
    CellsForMissing => "topology.cells_for_missing",
    KeyByOnUnkeyableTarget => "topology.key_by_on_unkeyable_target",
    DuplicateKeyBy => "topology.duplicate_key_by",
    TransformAfterTerminal => "topology.transform_after_terminal",
    TransformAfterKeyBy => "topology.transform_after_key_by",
    DanglingTransformChain => "topology.dangling_transform_chain",
    UnkeyedEdgeIntoDynamicTasklet => "topology.unkeyed_edge_into_dynamic_tasklet",
    MailboxCapacityZero => "topology.mailbox_capacity_zero",
    MailboxCapacityTooLarge => "topology.mailbox_capacity_too_large",
    DuplicatePluginOutput => "topology.duplicate_plugin_output",
    ChannelEndsTwice => "topology.channel_ends_twice",
    ChannelNamedTwice => "topology.channel_named_twice",
    DuplicateOutputName => "topology.duplicate_output_name",
    KeyedChannelWithoutPluginWire => "topology.keyed_channel_without_plugin_wire",
    UnservableClock => "topology.unservable_clock",
    UnkeyedChannelUnderEffectivelyOnce => "topology.unkeyed_channel_under_effectively_once",
    RetryMaxZero => "topology.retry_max_zero",
    DroppingPolicyUnderEffectivelyOnce => "topology.dropping_policy_under_effectively_once",
    CorrectionWithoutEventTime => "topology.correction_without_event_time",
    CorrectionWindowZero => "topology.correction_window_zero",
    CheckpointEveryZero => "topology.checkpoint_every_zero",
    MergeTooFewSources => "topology.merge_too_few_sources",
    DuplicateMergeSource => "topology.duplicate_merge_source",
    EdgeAlreadyWired => "topology.edge_already_wired",
    AtCheckpointWithoutMaxDelay => "topology.at_checkpoint_without_max_delay",
    UnknownSdkLang => "topology.unknown_sdk_lang",
    UnusableSdkVersion => "topology.unusable_sdk_version",
    NoTasklets => "topology.no_tasklets",
    EffectivelyOnceWithoutPersistence => "topology.effectively_once_without_persistence",
    CheckpointMaxDelayZero => "topology.checkpoint_max_delay_zero",
    ProducerOutOfRange => "topology.producer_out_of_range",
    ConsumerOutOfRange => "topology.consumer_out_of_range",
    EntryChannelWithProducer => "topology.entry_channel_with_producer",
    EntryChannelWithoutConsumer => "topology.entry_channel_without_consumer",
    InterTaskChannelWithoutProducer => "topology.inter_task_channel_without_producer",
    PluginChannelWithConsumers => "topology.plugin_channel_with_consumers",
    StaticTaskletWithoutCellKeys => "topology.static_tasklet_without_cell_keys",
    CellKeysWithoutStaticMode => "topology.cell_keys_without_static_mode",
    MaxCellsZero => "topology.max_cells_zero",
    DeclarationUnreadable => "topology.declaration_unreadable",
    UnknownField => "topology.unknown_field",
    InvalidSetting => "topology.invalid_setting",
    NoEntryChannel => "topology.no_entry_channel",
    MultipleOutputChannels => "topology.multiple_output_channels",
    BranchingChainedLoop => "topology.branching_chained_loop",
    ChainedLoopTerminalWithoutHandoff => "topology.chained_loop_terminal_without_handoff",
    PartitionsWithoutPerInputLoop => "topology.partitions_without_per_input_loop",
    PartitionsWithoutKeyedTasklet => "topology.partitions_without_keyed_tasklet",
    CallOutsidePerInputLoop => "topology.call_outside_per_input_loop",
    CallWithoutKey => "topology.call_without_key",
    CallDeadlineZero => "topology.call_deadline_zero",
    CallResponseUnread => "topology.call_response_unread",

    EventTimeTimerWithoutEventTimeClock => "runtime.event_time_timer_without_event_time_clock",
    TimerCapExceeded => "runtime.timer_cap_exceeded",
    ClassNotRegistered => "runtime.class_not_registered",
    LocalNeedsController => "runtime.local_needs_controller",
    LocalRunBusy => "runtime.local_run_busy",
    ChannelWrongKind => "runtime.channel_wrong_kind",
    ChannelUndeclared => "runtime.channel_undeclared",
    EdgeClosurePanicked => "runtime.edge_closure_panicked",
    UnknownChannelId => "runtime.unknown_channel_id",
    NotAConsumer => "runtime.not_a_consumer",
    KeyOutsidePartition => "runtime.key_outside_partition",
    RunnerShuttingDown => "runtime.runner_shutting_down",
    HostTimeout => "runtime.host_timeout",
    HostDroppedActivation => "runtime.host_dropped_activation",
    HostCommandFailed => "runtime.host_command_failed",
    HostWrongOutcome => "runtime.host_wrong_outcome",
    HostStateUnreadable => "runtime.host_state_unreadable",
    StateUnencodable => "runtime.state_unencodable",
    HostStateInProcess => "runtime.host_state_in_process",
    NotHostTasklet => "runtime.not_host_tasklet",
    CompletionTooManyEffects => "runtime.completion_too_many_effects",
    UnknownActivationToken => "runtime.unknown_activation_token",
    PrepareUnknownChannel => "runtime.prepare_unknown_channel",
    RelayNotFound => "runtime.relay_not_found",
    RelayNotLocal => "runtime.relay_not_local",
    RelayMessageUnsupported => "runtime.relay_message_unsupported",
    UnreadableReply => "runtime.unreadable_reply",
    MaxCellsReached => "runtime.max_cells_reached",
    ResultUndecodable => "runtime.result_undecodable",
    ArgumentNotUtf8 => "runtime.argument_not_utf8",
    InvalidIfRunning => "runtime.invalid_if_running",
    SpawnHandleReleased => "runtime.spawn_handle_released",
    RunnerAlreadyRunning => "runtime.runner_already_running",
    NoLocalRun => "runtime.no_local_run",
    ConnectUnconfirmed => "runtime.connect_unconfirmed",
    SchemaMismatch => "runtime.schema_mismatch",

    CellKeyWithoutTasklet => "state.cell_key_without_tasklet",
    TaskletNotFound => "state.tasklet_not_found",
    CellNotFound => "state.cell_not_found",
    CellEvicted => "state.cell_evicted",
    CellBusy => "state.cell_busy",
    ApplyNotSupported => "state.apply_not_supported",
    InitValuesInvalidJson => "state.init_values_invalid_json",
    InitValuesNotObject => "state.init_values_not_object",
    SerializeFailed => "state.serialize_failed",
    SnapshotFailed => "state.snapshot_failed",
    RestoreFailed => "state.restore_failed",
    CellNotLive => "state.cell_not_live",
    MigrationUnreadable => "state.migration_unreadable",

    UdsUnavailable => "runner.uds_unavailable",
    ControllerUnreachable => "runner.controller_unreachable",
    QuicAddressesUnresolved => "runner.quic_addresses_unresolved",
    SocketDirRefused => "runner.socket_dir_refused",
    QuicAddressesInvalid => "runner.quic_addresses_invalid",
    DataStreamsFailed => "runner.data_streams_failed",

    PartNoOutput => "part.no_output",
    PartOutsideInvocation => "part.outside_invocation",

    EffectBadName => "effect.bad_name",
    EffectNested => "effect.nested",
    EffectNotRecorded => "effect.not_recorded",
    EffectOutsideInvocation => "effect.outside_invocation",
    EffectTooLarge => "effect.too_large",

    SpawnTimeout => "spawn.timeout",
    SpawnUnavailable => "spawn.unavailable",
    OptionsConflict => "usage.options_conflict",
    ControlNoControlPlane => "control.no_control_plane",
    ControlTimeout => "control.timeout",
}

impl Code {
    /// This code's error, its templates filled from `details`.
    pub fn error(self, details: Map<String, Value>) -> FlezhaError {
        FlezhaError::new(self.as_str(), details)
    }

    /// [`error`](Self::error) with the details written inline as pairs.
    pub fn with<const N: usize>(self, pairs: [(&str, Value); N]) -> FlezhaError {
        FlezhaError::with(self.as_str(), pairs)
    }
}

#[derive(Debug)]
pub enum Error {
    /// A host class declaration the core cannot honour.
    Declaration(FlezhaError),
    /// A command activation the host did not answer as asked.
    HostCrossing(FlezhaError),
    /// A completion or abandon that does not fit its activation.
    Answer(FlezhaError),
    /// An edge value that could not reach its consumer.
    Routing(FlezhaError),
    /// A key this partition member does not own.
    Partition(FlezhaError),
    /// A state read or apply the relay cannot serve.
    State(FlezhaError),
    /// A part the invocation may not send.
    Part(FlezhaError),
    /// A control connection that could not be opened, or the controller's own
    /// refusal of it.
    Connect(FlezhaError),
    /// The transport's refusal: the socket ladder or the QUIC list.
    Transport(flz_transport::Error),
    /// The per-tasklet `max_cells` cap is reached; the new-key input is
    /// dropped (`inputs_dropped_capped_total` bumped).
    Capped,
}

impl ErrorModel for Error {
    fn to_error(&self) -> FlezhaError {
        match self {
            Error::Declaration(e)
            | Error::HostCrossing(e)
            | Error::Answer(e)
            | Error::Routing(e)
            | Error::Partition(e)
            | Error::State(e)
            | Error::Part(e)
            | Error::Connect(e) => e.clone(),
            Error::Transport(refusal) => {
                transport_code(refusal).with([("reason", refusal.to_string().into())])
            }
            Error::Capped => Code::MaxCellsReached.with([]),
        }
    }
}

fn transport_code(refusal: &flz_transport::Error) -> Code {
    match refusal {
        flz_transport::Error::SocketDir(_) => Code::SocketDirRefused,
        flz_transport::Error::QuicList(_) => Code::QuicAddressesInvalid,
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_error().message)
    }
}

impl std::error::Error for Error {}

impl From<flz_transport::Error> for Error {
    fn from(refusal: flz_transport::Error) -> Self {
        Error::Transport(refusal)
    }
}

impl From<SocketDirError> for Error {
    fn from(refusal: SocketDirError) -> Self {
        Error::Transport(refusal.into())
    }
}

impl From<QuicListError> for Error {
    fn from(refusal: QuicListError) -> Self {
        Error::Transport(refusal.into())
    }
}

/// The wire's `Error` as the model's: the one place a controller's error is
/// read. A class the catalogue does not name falls back to the code's own
/// entry, then to internal, so an unreadable class never loses the code.
impl From<pb::Error> for FlezhaError {
    fn from(wire: pb::Error) -> Self {
        let class = Class::parse(&wire.class)
            .or_else(|| flz_transport::errors::entry(&wire.code).map(|entry| entry.class))
            .unwrap_or(Class::Internal);
        let details = serde_json::from_str::<Value>(&wire.details_json)
            .ok()
            .and_then(|value| value.as_object().cloned())
            .unwrap_or_default();
        FlezhaError {
            code: wire.code,
            class,
            message: wire.message,
            details,
            hint: (!wire.hint.is_empty()).then_some(wire.hint),
        }
    }
}

/// The model's error as the wire carries it.
impl From<&FlezhaError> for pb::Error {
    fn from(error: &FlezhaError) -> Self {
        pb::Error {
            code: error.code.clone(),
            class: error.class.as_str().to_string(),
            message: error.message.clone(),
            details_json: error.details_json(),
            hint: error.hint.clone().unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_code_the_core_constructs_is_catalogued() {
        let missing: Vec<&str> = Code::ALL
            .iter()
            .map(|code| code.as_str())
            .filter(|code| flz_transport::errors::entry(code).is_none())
            .collect();
        assert!(missing.is_empty(), "uncatalogued core codes: {missing:?}");
    }

    #[test]
    fn a_variant_answers_its_code_and_renders_the_catalogue_message() {
        let state = Error::State(
            Code::CellNotFound.with([("tasklet", "candle".into()), ("cell_key", "X".into())]),
        );
        assert_eq!(state.code(), "state.cell_not_found");
        assert_eq!(
            state.to_string(),
            "tasklet 'candle' holds no Cell keyed 'X'"
        );

        let socket_dir = Error::from(SocketDirError::Ambiguous { live: Vec::new() });
        assert_eq!(socket_dir.code(), "runner.socket_dir_refused");
        assert_eq!(
            socket_dir.to_error().details["reason"],
            SocketDirError::Ambiguous { live: Vec::new() }.to_string()
        );
        assert_eq!(
            Error::from(QuicListError::NoEndpoint).code(),
            "runner.quic_addresses_invalid"
        );
        assert_eq!(Error::Capped.code(), "runtime.max_cells_reached");
    }

    #[test]
    fn an_error_survives_the_wire_both_ways() {
        let error =
            Code::TaskletNotFound.with([("relay", "op".into()), ("tasklet", "missing".into())]);
        assert_eq!(FlezhaError::from(pb::Error::from(&error)), error);

        let unknown_class = pb::Error {
            code: "quota.exceeded".into(),
            class: "sideways".into(),
            message: "m".into(),
            details_json: "not json".into(),
            hint: String::new(),
        };
        let read = FlezhaError::from(unknown_class);
        assert_eq!(
            read.class,
            Class::Conflict,
            "the code's own class is the fallback"
        );
        assert!(read.details.is_empty() && read.hint.is_none());
    }
}
