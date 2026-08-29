//! Graceful shutdown: on SIGTERM, `Drop` of the runner handle or a
//! controller-ordered disconnect, each live operative's state is serialized
//! and shipped as a `RunnerShutdownNotification`, and the process exits once
//! the acks land or the bounded wait expires — never blocking its
//! container's grace period. Also the waiter registry every SDK-initiated
//! request's reply is matched through.

use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(not(windows))]
use std::sync::Once;
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;
use prost::Message;

use crate::pb;

/// Per-operative ack wait, run in parallel so the total is bounded by this
/// regardless of operative count (Kubernetes' default grace period is 30 s).
const ACK_TIMEOUT: Duration = Duration::from_secs(2);

/// The bidirectional `CommandRequest` envelope's payload id.
const SHUTDOWN_PAYLOAD_ID: &str = "control_command";

/// `(instance_id, serialized state)` per live stream instance.
pub type OperativeSnapshotFn = Box<dyn Fn() -> Vec<(String, Vec<u8>)> + Send + Sync>;

/// What a graceful shutdown needs of the runner, reachable from the SIGTERM
/// handler through [`active_bridge`] and from the runner handle's `Drop`.
pub struct ShutdownBridge {
    /// The live control stream, replaced on every reconnect.
    control_stream: Mutex<Option<Arc<FlezhaStream>>>,

    snapshot_fn: Mutex<Option<OperativeSnapshotFn>>,

    /// A shutdown runs at most once per bridge, whichever of SIGTERM and
    /// `Drop` comes first.
    shutdown_started: AtomicBool,
}

impl ShutdownBridge {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            control_stream: Mutex::new(None),
            snapshot_fn: Mutex::new(None),
            shutdown_started: AtomicBool::new(false),
        })
    }

    pub fn set_control_stream(&self, stream: Option<Arc<FlezhaStream>>) {
        *self.control_stream.lock().unwrap() = stream;
    }

    pub fn set_snapshot_fn(&self, snapshot_fn: OperativeSnapshotFn) {
        *self.snapshot_fn.lock().unwrap() = Some(snapshot_fn);
    }

    /// Run the shutdown sequence once; a second caller returns at once.
    pub fn run_graceful_shutdown(self: &Arc<Self>) {
        if self.shutdown_started.swap(true, Ordering::SeqCst) {
            return;
        }

        let stream = match self.control_stream.lock().unwrap().clone() {
            Some(s) => s,
            None => {
                // No control channel attached: e.g. the SDK was dropped
                // before it ever connected, or the connection is in the
                // middle of a reconnect cycle. Nothing to ship.
                return;
            }
        };

        let operatives = match self.snapshot_fn.lock().unwrap().as_ref() {
            Some(f) => f(),
            None => return,
        };

        if operatives.is_empty() {
            return;
        }

        let wait_handles: Vec<_> = operatives
            .into_iter()
            .filter_map(|(operative_id, serialized_state)| {
                ship_notification(&stream, operative_id, serialized_state)
            })
            .collect();

        for handle in wait_handles {
            let _ = handle.join();
        }
    }
}

/// Write one operative's notification and spawn the wait for its ack;
/// `None` when the write failed. A timed-out wait drops its registration.
fn ship_notification(
    stream: &Arc<FlezhaStream>,
    operative_id: String,
    serialized_state: Vec<u8>,
) -> Option<thread::JoinHandle<()>> {
    let reference_id = format!("shutdown-{}-{}", operative_id, crate::util::nonce_suffix());

    let request = pb::CommandRequest {
        reference_id: reference_id.clone(),
        command: Some(pb::command_request::Command::RunnerShutdownNotification(
            pb::RunnerShutdownNotification {
                operative_id: operative_id.clone(),
                serialized_state,
            },
        )),
    };

    let encoded = request.encode_to_vec();
    let payload = payloader::build_from_binary(SHUTDOWN_PAYLOAD_ID, &encoded, HashMap::new());

    let rx = register_pending_ack(reference_id.clone());

    if let Err(e) = (&**stream).write_all(&payload) {
        eprintln!(
            "[flezha] Failed to write RunnerShutdownNotification for {}: {}",
            operative_id, e
        );
        drop_pending_ack(&reference_id);
        return None;
    }

    Some(thread::spawn(move || match rx.recv_timeout(ACK_TIMEOUT) {
        Ok(_) => {}
        Err(_) => {
            eprintln!("[flezha] RunnerShutdownAck timeout for {}", operative_id);
            drop_pending_ack(&reference_id);
        }
    }))
}

// ---------------------------------------------------------------------------
// Process-global registries.
//
// `PENDING_ACKS` lets the controller->SDK `RunnerShutdownAck` command handler
// (in `command::runner_shutdown_ack`) signal a per-operative wait
// channel without needing access to the originating bridge — handlers
// only see `&mut RunnerNode`, so a global keyed by reference_id is
// the simplest plumbing.
//
// `ACTIVE_BRIDGE` gives the SIGTERM handler thread the runner's bridge. The
// singleton guard allows one runner at a time; a restarted runner replaces
// the previous (already-shut-down) runner's bridge.
// ---------------------------------------------------------------------------

/// A controller reply to an SDK-initiated request: what it reports beyond
/// "it worked" (empty for a bare ack), or the failure it names.
pub type AckResult = Result<String, String>;

fn pending_acks_map() -> &'static Mutex<HashMap<String, mpsc::SyncSender<AckResult>>> {
    static PENDING_ACKS: OnceLock<Mutex<HashMap<String, mpsc::SyncSender<AckResult>>>> =
        OnceLock::new();
    PENDING_ACKS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register a waiter for the reply to `reference_id`.
pub fn register_pending_ack(reference_id: String) -> mpsc::Receiver<AckResult> {
    let (tx, rx) = mpsc::sync_channel(1);
    pending_acks_map().lock().unwrap().insert(reference_id, tx);
    rx
}

pub fn drop_pending_ack(reference_id: &str) {
    pending_acks_map().lock().unwrap().remove(reference_id);
}

/// Wake the waiter under `reference_id`; a missing entry already timed out.
pub fn signal_result(reference_id: &str, result: AckResult) {
    let entry = pending_acks_map().lock().unwrap().remove(reference_id);
    match entry {
        Some(tx) => {
            let _ = tx.send(result);
        }
        None => {
            eprintln!(
                "[flezha] controller reply for unknown reference_id {} — \
                 either timed out already or never registered.",
                reference_id
            );
        }
    }
}

fn active_bridge() -> &'static Mutex<Option<Arc<ShutdownBridge>>> {
    static ACTIVE_BRIDGE: OnceLock<Mutex<Option<Arc<ShutdownBridge>>>> = OnceLock::new();
    ACTIVE_BRIDGE.get_or_init(|| Mutex::new(None))
}

pub fn register_bridge(bridge: Arc<ShutdownBridge>) {
    *active_bridge().lock().unwrap() = Some(bridge);
    install_sigterm_handler();
}

/// Record a terminal disconnect the controller ordered; the exit waits for
/// the disconnect response to be written.
pub fn request_terminal_exit(reason: String) {
    *terminal_exit().lock().unwrap() = Some(reason);
}

pub fn take_terminal_exit() -> Option<String> {
    terminal_exit().lock().unwrap().take()
}

fn terminal_exit() -> &'static Mutex<Option<String>> {
    static TERMINAL_EXIT: OnceLock<Mutex<Option<String>>> = OnceLock::new();
    TERMINAL_EXIT.get_or_init(|| Mutex::new(None))
}

/// Exit code of a terminally-disconnected runner: non-zero so the
/// orchestrator restarts it, distinct so the logs tell it from a crash.
pub const TERMINAL_EXIT_CODE: i32 = 70;

/// Run the graceful shutdown and exit NON-ZERO at the controller's order —
/// the convergence ladder's last rung, since a wedged process that stays
/// placeable wedges again and only the orchestrator restarts a failure.
/// Called after the disconnect response is on the wire.
pub fn exit_terminally(reason: &str) -> ! {
    eprintln!(
        "[flezha] controller ordered a terminal disconnect (reason={}); exiting {} so the \
         orchestrator restarts this runner",
        reason, TERMINAL_EXIT_CODE
    );
    // A no-op when the caller already shipped state — the bridge latches.
    run_active_graceful_shutdown();
    crate::poll_core::global().shutdown();
    std::process::exit(TERMINAL_EXIT_CODE);
}

/// Run the registered bridge's shutdown, if any; the bridge's latch makes a
/// second caller a no-op.
pub fn run_active_graceful_shutdown() {
    let bridge = active_bridge().lock().unwrap().clone();
    if let Some(bridge) = bridge {
        bridge.run_graceful_shutdown();
    }
}

/// Install the SIGTERM handler once: it runs the active bridge's shutdown,
/// then exits.
#[cfg(not(windows))]
fn install_sigterm_handler() {
    static INSTALLED: Once = Once::new();
    INSTALLED.call_once(|| {
        match signal_hook::iterator::Signals::new([signal_hook::consts::SIGTERM]) {
            Ok(mut signals) => {
                thread::spawn(move || {
                    if signals.forever().next().is_some() {
                        eprintln!("[flezha] SIGTERM received; running graceful shutdown");
                        run_active_graceful_shutdown();
                        std::process::exit(0);
                    }
                });
            }
            Err(e) => {
                eprintln!(
                    "[flezha] Failed to install SIGTERM handler ({}); graceful \
                     shutdown will rely on Drop only.",
                    e
                );
            }
        }
    });
}

/// Windows has no SIGTERM; shutdown rides `Drop` and the disconnect.
#[cfg(windows)]
fn install_sigterm_handler() {}

#[cfg(test)]
mod tests {
    //! `run_graceful_shutdown` against a `UnixStream::pair()` standing in for
    //! the control channel: the notification's framing and the bounded wait.
    use std::io::Read;
    use std::os::unix::net::UnixStream;
    use std::time::Instant;

    use prost::Message;

    use super::*;
    use crate::pb;
    use crate::test_support::stream_pair;

    fn build_bridge_with_operative(
        operative_id: &str,
        snapshot: Vec<u8>,
    ) -> (Arc<ShutdownBridge>, UnixStream) {
        let (stream, controller_side) = stream_pair();

        let bridge = ShutdownBridge::new();
        bridge.set_control_stream(Some(stream));

        let pid = operative_id.to_string();
        let snap = snapshot.clone();
        bridge.set_snapshot_fn(Box::new(move || vec![(pid.clone(), snap.clone())]));

        (bridge, controller_side)
    }

    #[test]
    fn graceful_shutdown_emits_framed_runner_shutdown_notification() {
        let snapshot = b"final-state-bytes".to_vec();
        let (bridge, mut controller_side) = build_bridge_with_operative("p1", snapshot.clone());

        let bridge_for_run = Arc::clone(&bridge);
        // run_graceful_shutdown blocks waiting for an ack we never send;
        // off-thread it so we can drive the controller side and bound the wait.
        let run_handle = std::thread::spawn(move || bridge_for_run.run_graceful_shutdown());

        // Read the frame the SDK emitted. UnixStream is blocking — the
        // SDK writes the notification before sleeping on the ack channel,
        // so a single read() pulls the full frame.
        let mut buf = vec![0u8; 4096];
        let n = controller_side.read(&mut buf).expect("read shutdown frame");
        buf.truncate(n);

        let (payload_id, message, _meta) =
            payloader::extract_payload(&buf[..]).expect("decode shutdown frame");
        assert_eq!(payload_id, "control_command");

        let request = pb::CommandRequest::decode(message.as_slice())
            .expect("decode CommandRequest from shutdown frame");

        let notif = match request.command.expect("oneof command set") {
            pb::command_request::Command::RunnerShutdownNotification(n) => n,
            other => panic!("expected RunnerShutdownNotification, got {:?}", other),
        };

        assert_eq!(notif.operative_id, "p1");
        assert_eq!(notif.serialized_state, snapshot);
        assert!(!request.reference_id.is_empty());

        // Now signal the ack so the wait thread releases. Closing the
        // socket on the controller side without acking would also work but
        // we exercise the happy path explicitly.
        signal_result(&request.reference_id, Ok(String::new()));
        run_handle.join().expect("shutdown thread joins");
    }

    #[test]
    fn graceful_shutdown_returns_within_2s_when_ack_never_arrives() {
        let (bridge, _controller_side) =
            build_bridge_with_operative("p_timeout", b"unused".to_vec());

        let started = Instant::now();
        bridge.run_graceful_shutdown();
        let elapsed = started.elapsed();

        // Timeout is 2 s per operative; allow a small slack for thread
        // spawn + join. The Kubernetes default grace period is 30 s, so
        // anything significantly above 2.5 s here would mean we have a
        // bug that could blow past the grace period.
        assert!(
            elapsed < Duration::from_millis(2500),
            "graceful shutdown should bound wait at 2s; took {:?}",
            elapsed
        );
        assert!(
            elapsed >= Duration::from_millis(1900),
            "graceful shutdown should wait the full 2s before timing out; took {:?}",
            elapsed
        );
    }

    #[test]
    fn signal_result_for_unknown_reference_does_not_panic() {
        // Matches the post-timeout cleanup race: the wait thread already
        // dropped its registration; a late ack arrives and finds nothing.
        // Must not panic.
        signal_result("never-registered-ref", Ok(String::new()));
    }

    #[test]
    fn run_graceful_shutdown_is_idempotent() {
        let (bridge, _controller_side) = build_bridge_with_operative("p_idem", b"x".to_vec());

        // First run — exercises the timeout path. Second run sees
        // shutdown_started == true and returns immediately.
        bridge.run_graceful_shutdown();
        let started = Instant::now();
        bridge.run_graceful_shutdown();
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "second run must be a no-op"
        );
    }
}
