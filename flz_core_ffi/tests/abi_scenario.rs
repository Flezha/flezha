//! The C-ABI scenario suite (spec `sdk-core-abi`): every capability the crate
//! API has must be reachable through `flz_core_ffi` itself.
//!
//! `lifecycle` drives the capabilities and `shutdown` ends the runner;
//! `harness` is the fake controller and the poll/complete pair they drive
//! them with; `shape` proves the mapping every binding shares, once, against
//! a recording pair; `admin` holds the administration surface to one entry;
//! `spawn` drives a tasklet's spawn and `part` a tasklet's part, each in a
//! process of its own.
//!
//! Each veneer arm hangs off that same shape and proves only what its own
//! language contributes — how it builds and reads a value — so a veneer is
//! covered by the scenario that already covers the capability rather than by
//! a suite of its own that has to be kept in step with this one. The arms are
//! feature-gated: `jni` for the Java SDK's declared native surface, `napi` for
//! the Node addon's surface, `ruby-embed` for an embedded CRuby VM, `python`
//! for an embedded CPython interpreter.

#[path = "abi/admin.rs"]
mod admin;
#[path = "abi/harness.rs"]
mod harness;
#[path = "abi/lifecycle.rs"]
mod lifecycle;
#[path = "abi/part.rs"]
mod part;
#[path = "abi/shape.rs"]
mod shape;
#[path = "abi/shutdown.rs"]
mod shutdown;
#[path = "abi/spawn.rs"]
mod spawn;
#[path = "abi/surface.rs"]
mod surface;

#[cfg(feature = "jni")]
#[path = "abi/veneer_jni.rs"]
mod veneer_jni;

#[cfg(feature = "napi")]
#[path = "abi/veneer_napi.rs"]
mod veneer_napi;

#[cfg(feature = "python")]
#[path = "abi/python_binding.rs"]
mod python_binding;
#[cfg(feature = "python")]
#[path = "abi/veneer_python.rs"]
mod veneer_python;

#[cfg(feature = "ruby-embed")]
#[path = "abi/veneer_ruby.rs"]
mod veneer_ruby;
