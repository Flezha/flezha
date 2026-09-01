//! The C-ABI scenario suite (spec `sdk-core-abi`): every capability the crate
//! API has must be reachable through `flz_core_ffi` itself.
//!
//! `lifecycle` drives the capabilities and `shutdown` ends the runner;
//! `harness` is the fake controller and the poll/complete pair they drive
//! them with; `shape` proves the mapping every binding shares, once, against
//! a recording pair.
//!
//! Each veneer arm hangs off that same shape and proves only what its own
//! language contributes — how it builds and reads a value — so a veneer is
//! covered by the scenario that already covers the capability rather than by
//! a suite of its own that has to be kept in step with this one. The arms are
//! feature-gated: `napi` for the Node addon's surface, `ruby-embed` for an
//! embedded CRuby VM, `python` for an embedded CPython interpreter.

#[path = "abi/harness.rs"]
mod harness;
#[path = "abi/lifecycle.rs"]
mod lifecycle;
#[path = "abi/shape.rs"]
mod shape;
#[path = "abi/shutdown.rs"]
mod shutdown;

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
