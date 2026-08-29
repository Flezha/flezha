//! The language-agnostic SDK core.
//!
//! `flz_core` owns transport, ordering, durability, retries, timers, and
//! credit for a Flezha runner. A host language SDK drives it through exactly
//! one invocation surface: `poll() -> Activation` / `complete(ActivationResult)`.
//! The core never calls into host code and never owns a host thread.
//!
//! [`schema`] is the versioned boundary contract: what an [`schema::Activation`]
//! and [`schema::ActivationResult`] look like as bytes. Non-Rust hosts consume
//! it through the C ABI (`flz_core_ffi`); the Rust SDK consumes the same
//! engine this crate exposes as a library.

#[doc(hidden)]
pub mod admission;
#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub mod bench_support;
pub mod channel;
#[doc(hidden)]
pub mod command;
#[doc(hidden)]
pub mod control_unit;
#[doc(hidden)]
pub mod coordinator;
#[doc(hidden)]
pub mod correction;
#[doc(hidden)]
pub mod credit;
#[doc(hidden)]
pub mod dynamic;
#[doc(hidden)]
pub mod effect;
pub mod entity;
pub mod entry_input;
pub mod error;
#[doc(hidden)]
pub mod flusher;
pub mod header;
pub mod host;
#[doc(hidden)]
pub mod mailbox;
pub mod operative;
#[doc(hidden)]
pub mod park;
#[doc(hidden)]
pub mod persistent_id;
pub mod poll_core;
#[doc(hidden)]
pub mod restore;
#[doc(hidden)]
pub mod retry;
#[doc(hidden)]
pub mod routing;
#[doc(hidden)]
pub mod runner;
pub mod schema;
#[doc(hidden)]
pub mod shutdown;
#[doc(hidden)]
pub mod state_sink;
#[cfg(any(test, feature = "bench-internals"))]
mod test_support;
pub mod timer;
#[doc(hidden)]
pub mod util;
pub mod workflow;

/// The controller↔runner protobuf command surface (prost-generated).
pub mod pb {
    include!("generated/runner_commands.rs");
}

pub use error::Error;
pub use schema::SCHEMA_VERSION;
