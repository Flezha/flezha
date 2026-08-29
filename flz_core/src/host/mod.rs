//! The erased host surface: what a non-Rust SDK declares and drives through
//! the C ABI. A host class is a topology declared as data; its entities carry
//! a [`BytesBuffer`] and a dispatcher that answers only metadata. The
//! invariant: no host code runs in-process — the host polls
//! [`crate::poll_core::PollCore`], runs its own user code, and completes
//! with a [`crate::schema::ActivationResult`] this module applies. Every
//! capability a typed class has is reachable here.

mod activation;
mod commands;
mod decl;
mod entity;
mod prepare;
mod register;
mod workflow;

#[cfg(test)]
mod activation_tests;
#[cfg(test)]
mod decl_tests;

#[cfg(feature = "bench-internals")]
pub(crate) use activation::empty_effect;
pub use activation::{abandon_activation, apply_result, to_schema};
pub use commands::{
    apply_state_locked, restore_fold_exclusive, snapshot_frame_crossing, state_json_locked,
};
pub use decl::{
    HostAckGranularity, HostBeyondWindow, HostChannelDecl, HostChannelKind, HostCheckpointOutputs,
    HostClassDecl, HostClock, HostEntityModeDecl, HostLoopMode, HostOverflow, HostProcedureDecl,
    HostRollbackMode, HostWorkflowDecl,
};
pub use entity::{entity_ref_of, BytesBuffer, HostPayload, HostProcedureFacts};
pub use prepare::{
    deferred_end_replay_slice, maybe_stage_prepare, prepare_wants, HostPrepareState,
    PrepareCrossing, StagedEntry,
};
pub use register::{register_host_class, register_host_class_json};
pub use workflow::{
    fold_workflow_outcome, run_workflow_sugar, workflow_session_cancelled, workflow_session_recv,
    HostWorkflowSession,
};
