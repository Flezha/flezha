//! The operator kernel: topology, instance, registry.

pub mod completion;
pub mod instance;
pub mod registry;
pub mod settings;
pub mod sleep;
#[cfg(all(test, unix))]
mod sleep_tests;
pub mod topology;
