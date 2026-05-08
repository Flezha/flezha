use std::sync::Mutex;

use super::config::FlezhaConfigOwned;
use super::logging::LogState;
use super::types::FlezhaLogLevel;

/// The opaque context that holds all library state.
///
/// Created via `flezha_context_new`, destroyed via `flezha_context_destroy`.
/// All operations on a live context are thread-safe.
/// Creation and destruction must not be concurrent with other operations on the same context.
// reason: fields are read across the C-FFI boundary via cbindgen-generated header; rustc can't see those uses
#[allow(dead_code)]
pub struct FlezhaContext {
    pub(crate) config: FlezhaConfigOwned,
    pub(crate) log_state: Mutex<LogState>,
}

// reason: ctor + helpers consumed by the C-FFI surface in flezha_context_new etc.
#[allow(dead_code)]
impl FlezhaContext {
    pub(crate) fn new(config: FlezhaConfigOwned) -> Self {
        FlezhaContext {
            config,
            log_state: Mutex::new(LogState::new()),
        }
    }

    pub(crate) fn log(&self, level: FlezhaLogLevel, message: &str) {
        if let Ok(state) = self.log_state.lock() {
            state.log(level, message);
        }
    }
}
