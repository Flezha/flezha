//! The erased host surface: what a non-Rust SDK declares and drives through
//! the C ABI. A host class is a topology declared as data; its cells carry
//! a [`BytesBuffer`] and a dispatcher that answers only metadata. The
//! invariant: no host code runs in-process — the host polls
//! [`crate::poll_core::PollCore`], runs its own user code, and completes
//! with a [`crate::schema::ActivationResult`] this module applies. Every
//! capability a typed class has is reachable here.

mod activation;
mod build;
mod cell;
mod chains;
mod commands;
mod decl;
mod handout;
mod issue;
mod leave;
mod prepare;
mod register;
mod stamp;

#[cfg(test)]
mod activation_tests;
#[cfg(test)]
mod api_tests;
#[cfg(test)]
mod check_tests;
#[cfg(test)]
mod decl_tests;
#[cfg(test)]
pub(crate) mod leave_tests;

#[cfg(feature = "bench-internals")]
pub(crate) use activation::empty_effect;
pub(crate) use activation::settle_steps_undo;
pub use activation::{
    abandon_activation, apply_result, park_entry, ship_plugin_output, stream_of, ShipAt,
};
pub use build::call_end;
pub use cell::{cell_ref_of, BytesBuffer, HostPayload, HostTaskletFacts};
pub use commands::{
    apply_state_locked, restore_failed, restore_fold_exclusive, snapshot_frame_crossing,
    state_json_locked,
};
pub use decl::{
    HostAckGranularity, HostCall, HostChannelDecl, HostChannelKind, HostClassDecl, HostClock,
    HostCodec, HostLink, HostTaskletDecl,
};
pub use handout::to_schema;
pub use issue::{render_issues, TopologyIssue};
pub use leave::{cell_left_locked, pending_leave, relay_left, PendingLeave};
pub use prepare::{
    deferred_end_replay_slice, prepare_wants, stage_prepare, HostPrepareState, PrepareCrossing,
    StagedEntry,
};
pub use register::{
    check, check_class_json, register_host_class, register_host_class_json, Derived, DerivedCall,
    DerivedChannel, DerivedEdge, MAX_MAILBOX_CAPACITY,
};
pub use stamp::{end_stamps, output_identity};
