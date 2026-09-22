//! The operative kernel: topology, instance, registry.

pub mod instance;
pub mod registry;
pub mod sleep;
#[cfg(all(test, unix))]
mod sleep_tests;
pub mod topology;
