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
/// Administering the namespace a runner's connection proved.
pub mod admin;
pub mod admission;
#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub mod bench_support;
pub mod call;
pub mod cell;
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
pub mod effect_result;
pub mod error;
#[doc(hidden)]
pub mod flusher;
pub mod header;
pub mod host;
pub mod local;
#[doc(hidden)]
pub mod mailbox;
pub mod origin;
#[doc(hidden)]
pub mod park;
pub mod part;
#[doc(hidden)]
pub mod persistent_id;
pub mod poll_core;
pub mod poll_pool;
#[doc(hidden)]
pub mod prefix;
pub mod relay;
#[doc(hidden)]
pub mod restore;
#[doc(hidden)]
pub mod retry;
#[doc(hidden)]
pub mod routing;
#[doc(hidden)]
pub mod runner;
pub mod schema;
pub mod session;
#[doc(hidden)]
pub mod shutdown;
pub mod spawn;
#[doc(hidden)]
pub mod state_sink;
pub mod steps;
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod test_support;
pub mod timer;
#[doc(hidden)]
pub mod util;

/// The controller↔runner protobuf command surface (prost-generated).
// A `ControlFrame` is decoded once per inbound frame and moved on, so its
// request stays inline rather than paying a box on every frame.
#[allow(clippy::large_enum_variant)]
pub mod pb {
    include!("generated/runner_commands.rs");
}

pub use error::{Code, Error};
pub use flz_transport::errors::{ErrorModel, FlezhaError};
pub use schema::SCHEMA_VERSION;
