//! The control-socket peer a runner meets when there is no controller.
//!
//! It binds the real control socket, speaks the real wire protocol through
//! [`flz_transport::service::payloader`], captures every runner→controller
//! frame and pushes commands back. Two callers drive it and neither holds a
//! copy: the crate's own ABI scenario, which starts a runner inside its own
//! process, and `flz-fake-controller`, which drives one that is a separate
//! process in another language.
//!
//! What it is not is a controller. There is no scheduler, no placement, no
//! durable store and no replica of the fifteen-capability vocabulary; it
//! answers the handshake, starts one class, feeds inputs and counts outputs.
//! An SDK repository calls that its smoke tier ("connects, gets work,
//! completes, shuts down"), and every end-to-end scenario stays in the
//! controller's own tree.

use std::path::PathBuf;
use std::time::Duration;

#[cfg(unix)]
mod uds;
#[cfg(unix)]
pub use uds::{
    effect_result, part_line, shutdown_notification, spawn_request, CapturedFrame, Controller,
};

/// What a smoke run drives a runner through, and what it expects back.
pub struct Scenario {
    /// Where the control and data sockets live. Both processes resolve it
    /// through `FLEZHA_SOCKET_FILES_PATH`.
    pub socket_dir: PathBuf,
    /// The connection's session token; it names the data sockets.
    pub runner_id: String,
    /// The class to start and the relay id to start it under.
    pub factory_id: String,
    pub relay_id: String,
    /// The entry channel inputs are fed to; `None` uses the first declared.
    pub channel: Option<String>,
    /// One input payload per element, fed in order on the primary stream.
    pub inputs: Vec<Vec<u8>>,
    /// How many output frames the run must see before it is done.
    pub expect_outputs: usize,
    /// The ceiling on every single wait.
    pub timeout: Duration,
}

/// Each step the scenario takes, reported as it is awaited so a failing run
/// says how far it got.
pub type Progress<'a> = &'a dyn Fn(&str);

/// UDS is the only transport a smoke tier can offer: a QUIC peer needs this
/// repository's test certificates. The refusal is the crate's own wording for
/// a platform without Unix domain sockets.
#[cfg(not(unix))]
impl Scenario {
    pub fn run(&self, _report: Progress<'_>) -> Result<(), String> {
        Err("UDS transport is not available on this platform".to_string())
    }
}
