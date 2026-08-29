//! The erased host surface: what a non-Rust SDK declares and drives through
//! the C ABI (`flz_core_ffi`).
//!
//! A host class is a topology declared as data — no generics, no host
//! closures beyond the bytes-passthrough entry wrapper. Its entities carry a
//! [`BytesBuffer`] (host-encoded change bytes, core-framed) and a
//! `HostDispatcher` that answers the engine's metadata questions;
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
