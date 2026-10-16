//! Graceful shutdown: on SIGTERM, `Drop` of the runner handle or a
//! controller-ordered disconnect, each live relay's state is serialized
//! and shipped as a `RunnerShutdownNotification`, and the process exits once
//! the acks land or the bounded wait expires — never blocking its
//! container's grace period. Also the waiter registry every SDK-initiated
//! request's reply is matched through.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex, Once, OnceLock};
use std::thread;
use std::time::Duration;

use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;
use prost::Message;

use crate::pb;

/// Per-relay ack wait, run in parallel so the total is bounded by this
/// regardless of relay count (Kubernetes' default grace period is 30 s).
/// Public so a controller can wait exactly as long as the runner does for the
/// notification it is about to answer, rather than pick its own number.
pub const ACK_TIMEOUT: Duration = Duration::from_secs(2);

/// The bidirectional `CommandRequest` envelope's payload id.
const SHUTDOWN_PAYLOAD_ID: &str = "control_command";

/// `(instance_id, serialized state)` per live flow instance.
pub type RelaySnapshotFn = Box<dyn Fn() -> Vec<(String, Vec<u8>)> + Send + Sync>;

/// What a graceful shutdown needs of the runner, reachable from the SIGTERM
/// handler through [`active_bridge`] and from the runner handle's `Drop`.
pub struct ShutdownBridge {
    /// The live control stream, replaced on every reconnect.
    control_stream: Mutex<Option<Arc<FlezhaStream>>>,

    snapshot_fn: Mutex<Option<RelaySnapshotFn>>,

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

    pub fn control_stream(&self) -> Option<Arc<FlezhaStream>> {
        self.control_stream.lock().unwrap().clone()
    }

    pub fn set_snapshot_fn(&self, snapshot_fn: RelaySnapshotFn) {
        *self.snapshot_fn.lock().unwrap() = Some(snapshot_fn);
    }

    /// Run the shutdown sequence once; a second caller returns at once.
    pub fn run_graceful_shutdown(self: &Arc<Self>) {
        if self.shutdown_started.swap(true, Ordering::SeqCst) {
            return;
        }

        let stream = match self.control_stream() {
            Some(s) => s,
            None => {
                // No control channel attached: e.g. the SDK was dropped
                // before it ever connected, or the connection is in the
                // middle of a reconnect cycle. Nothing to ship.
                return;
            }
        };

        let relays = match self.snapshot_fn.lock().unwrap().as_ref() {
            Some(f) => f(),
            None => return,
        };

        if relays.is_empty() {
            return;
        }

        let wait_handles: Vec<_> = relays
            .into_iter()
            .filter_map(|(relay_id, serialized_state)| {
                ship_notification(&stream, relay_id, serialized_state)
            })
            .collect();

        for handle in wait_handles {
            let _ = handle.join();
        }
    }
}

/// Write one relay's notification and spawn the wait for its ack;
/// `None` when the write failed. A timed-out wait drops its registration.
fn ship_notification(
    stream: &Arc<FlezhaStream>,
    relay_id: String,
    serialized_state: Vec<u8>,
) -> Option<thread::JoinHandle<()>> {
    let reference_id = format!("shutdown-{}-{}", relay_id, crate::util::nonce_suffix());

    let request = pb::CommandRequest {
        reference_id: reference_id.clone(),
        command: Some(pb::command_request::Command::RunnerShutdownNotification(
            pb::RunnerShutdownNotification {
                relay_id: relay_id.clone(),
                serialized_state,
            },
        )),
    };

    let encoded = request.encode_to_vec();
    let payload = payloader::build_without_metadata(SHUTDOWN_PAYLOAD_ID, &encoded);

    let rx = pending_ack_channel(reference_id.clone());

    // The relay's final state is what makes a reconnect-resume loss-free,
    // so a refused write is held for the next bound stream rather than printed
    // and lost. Only what reached the wire is waited on: an ack cannot answer
    // a held frame.
    if !matches!(
        crate::state_sink::write(crate::state_sink::Class::Durable, Some(stream), payload),
        crate::state_sink::Verdict::Written
    ) {
        drop_pending_ack(&reference_id);
        return None;
    }

    Some(thread::spawn(move || match rx.recv_timeout(ACK_TIMEOUT) {
        Ok(_) => {}
        Err(_) => {
            say(&format!(
                "[flezha] RunnerShutdownAck timeout for {relay_id}"
            ));
            drop_pending_ack(&reference_id);
        }
    }))
}

// ---------------------------------------------------------------------------
// Process-global registries.
//
// `PENDING_ACKS` lets the controller->SDK `RunnerShutdownAck` command handler
// (in `command::runner_shutdown_ack`) signal a per-relay wait
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
pub type AckResult = Result<String, flz_transport::errors::FlezhaError>;

/// What a reply to one request does, run once on the thread that brings it.
pub type OnReply = Box<dyn FnOnce(AckResult) + Send>;

fn pending_acks_map() -> &'static Mutex<HashMap<String, OnReply>> {
    static PENDING_ACKS: OnceLock<Mutex<HashMap<String, OnReply>>> = OnceLock::new();
    PENDING_ACKS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register what the reply to `reference_id` does when it arrives.
pub fn register_pending_ack(reference_id: String, on_reply: OnReply) {
    pending_acks_map()
        .lock()
        .unwrap()
        .insert(reference_id, on_reply);
}

/// Register a waiter for the reply to `reference_id`: the channel form, for
/// a caller that blocks on it.
pub fn pending_ack_channel(reference_id: String) -> mpsc::Receiver<AckResult> {
    let (tx, rx) = mpsc::sync_channel(1);
    register_pending_ack(
        reference_id,
        Box::new(move |result| {
            let _ = tx.send(result);
        }),
    );
    rx
}

pub fn drop_pending_ack(reference_id: &str) {
    pending_acks_map().lock().unwrap().remove(reference_id);
}

#[cfg(test)]
pub(crate) fn has_pending_ack(reference_id: &str) -> bool {
    pending_acks_map()
        .lock()
        .unwrap()
        .contains_key(reference_id)
}

/// How long a caller waits for the controller's reply to a request [`ask`]
/// shipped. Generous: the controller may have to start a relay, which
/// waits for a runner advertising the factory.
pub const REPLY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Ship an SDK-initiated control request and hand back the receiver its reply
/// arrives on.
///
/// The one place the ordering rule lives: the waiter is registered BEFORE the
/// send, so a reply can never arrive before the waiter exists. `Err` means
/// there was no control plane to ship it on, and nothing is left registered.
pub fn ask(
    reference_id: String,
    command: crate::pb::command_request::Command,
) -> Result<mpsc::Receiver<AckResult>, NoControlPlane> {
    let rx = pending_ack_channel(reference_id.clone());

    let sent = crate::runner::coordinator_try_send(crate::runner::RunnerMessage::control_request(
        reference_id.clone(),
        command,
    ));

    if sent {
        Ok(rx)
    } else {
        drop_pending_ack(&reference_id);
        Err(NoControlPlane)
    }
}

/// There was no control plane to ship a request on: a transport-less test or
/// bench, or a runner that has closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoControlPlane;

/// Answer the waiter under `reference_id`, outside the registry's lock; a
/// missing entry already timed out.
pub fn signal_result(reference_id: &str, result: AckResult) {
    let entry = pending_acks_map().lock().unwrap().remove(reference_id);
    match entry {
        Some(on_reply) => on_reply(result),
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
    say(&format!(
        "[flezha] controller ordered a terminal disconnect (reason={reason}); exiting \
         {TERMINAL_EXIT_CODE} so the orchestrator restarts this runner"
    ));
    // A no-op when the caller already shipped state — the bridge latches.
    run_active_graceful_shutdown();
    crate::poll_core::global().shutdown();
    end_process(TERMINAL_EXIT_CODE)
}

/// Run the registered bridge's shutdown, if any; the bridge's latch makes a
/// second caller a no-op.
pub fn run_active_graceful_shutdown() {
    let bridge = active_bridge().lock().unwrap().clone();
    if let Some(bridge) = bridge {
        bridge.run_graceful_shutdown();
    }
}

/// What a healthy runner has to drain in once SIGTERM has been observed. Small
/// beside a container's grace period, and the same budget a closed standard
/// input gives, because both end the same way.
const SIGTERM_GRACE_MS: u32 = 5_000;

/// Poll interval for a standard input the host runtime has made non-blocking,
/// and for the drain the exit waits on.
const STDIN_POLL_INTERVAL: Duration = Duration::from_millis(50);
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// End the process when standard input reaches end of file, on a thread of the
/// core's own.
///
/// Native, and that is the point. A conformance harness stops a runner by
/// closing its pipes rather than by signalling it, and the obvious place to
/// observe that is the host's event loop — which is exactly the thread a
/// compute-bound handler holds. A runner whose only loop is wedged would then
/// be unstoppable without a signal, so the watch runs beside
/// [`install_sigterm_handler`] and reaches the exit the same way: through
/// [`run_active_graceful_shutdown`], on a thread no host's virtual machine
/// owns.
///
/// A read ERROR is not end of file. Standard input is non-blocking whenever the
/// runtime has taken an interest in it — which it has the moment a worker
/// thread exists — so a read with nothing buffered answers `WouldBlock`.
/// Conflating that with end of file shuts the runner down a few hundred
/// milliseconds after it connects, every time. Only a genuine end of file, or a
/// descriptor that has gone away, ends the watch.
///
/// A healthy runner has `grace_ms` to drain and exit on its own; one that has
/// not exited by then is ended, because nothing is left that could.
pub fn watch_stdin_eof(grace_ms: u32) {
    static WATCHING: Once = Once::new();
    WATCHING.call_once(|| {
        if let Err(e) = thread::Builder::new()
            .name("flz-stdin-eof".into())
            .spawn(move || {
                wait_for_stdin_eof();
                end_gracefully("standard input reached end of file", grace_ms);
            })
        {
            say(&format!(
                "[flezha] Failed to start the standard-input watcher ({e}); a closed \
                 standard input will not end this runner."
            ));
        }
    });
}

/// A line to standard error that cannot take the caller down with it.
///
/// A conformance harness stops a runner by closing its pipes, so the very
/// thread that then has to end the process is writing to a pipe with no reader.
/// `eprintln!` panics on that EPIPE, and a panicked watcher leaves the runner
/// alive for ever — which is the outcome the watch exists to prevent.
pub(crate) fn say(line: &str) {
    use std::io::Write;
    let _ = writeln!(std::io::stderr(), "{line}");
}

/// The one exit both facts lead to: ship every relay's final state, tell
/// the host's pollers to stop so its own workers drain, give a healthy runner
/// `grace_ms` to finish, then end. A host needs no thread and no callback of
/// its own for either, so a runner cannot drain differently for one than for
/// the other.
fn end_gracefully(what: &str, grace_ms: u32) -> ! {
    say(&format!("[flezha] {what}; draining"));
    run_active_graceful_shutdown();
    let core = crate::poll_core::global();
    core.shutdown();
    // A healthy runner finishes what it is holding and its pollers leave on
    // the core's word; one that has not by the deadline is ended, because
    // nothing is left that could.
    let deadline = std::time::Instant::now() + Duration::from_millis(u64::from(grace_ms));
    while core.outstanding_count() > 0 && std::time::Instant::now() < deadline {
        thread::sleep(DRAIN_POLL_INTERVAL);
    }
    end_process(0)
}

/// End the process at once, without running the exit handlers.
///
/// Both callers run on a thread of the core's own while the host's runtime is
/// still alive, and often while it is tearing itself down: exit handlers run
/// from here destroy state the host's main thread is still using, and a Node
/// main thread leaving its event loop then locks a destroyed mutex and aborts.
/// Every relay's state has shipped by now, so the handlers have nothing
/// left to do.
fn end_process(code: i32) -> ! {
    use std::io::Write;
    let _ = std::io::stdout().flush();

    #[cfg(unix)]
    // SAFETY: `_exit` takes no pointer and does not return.
    unsafe {
        libc::_exit(code)
    }

    #[cfg(not(unix))]
    std::process::exit(code)
}

fn wait_for_stdin_eof() {
    use std::io::{ErrorKind, Read};
    let mut byte = [0u8; 1];
    loop {
        match std::io::stdin().read(&mut byte) {
            // End of file: the far end closed. The only read outcome that
            // means shutdown.
            Ok(0) => return,
            Ok(_) => continue,
            Err(e) if e.kind() == ErrorKind::WouldBlock => thread::sleep(STDIN_POLL_INTERVAL),
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            // Anything else means the descriptor is gone, which is end of file
            // by another name.
            Err(_) => return,
        }
    }
}

/// Install the termination-signal handler once: SIGTERM and SIGINT both run the
/// active bridge's shutdown and then exit, on a thread the core owns rather
/// than on the host's own — which is what lets every host stop installing a
/// trap of its own beside this one. The handler installed before it still runs
/// after it, so a host runtime whose own handler ends the process must hand the
/// signal over before the core starts.
#[cfg(not(windows))]
fn install_sigterm_handler() {
    static INSTALLED: Once = Once::new();
    INSTALLED.call_once(|| {
        match signal_hook::iterator::Signals::new([
            signal_hook::consts::SIGTERM,
            signal_hook::consts::SIGINT,
        ]) {
            Ok(mut signals) => {
                thread::spawn(move || {
                    if signals.forever().next().is_some() {
                        end_gracefully("termination signal received", SIGTERM_GRACE_MS);
                    }
                });
            }
            Err(e) => {
                eprintln!(
                    "[flezha] Failed to install the termination-signal handler ({}); \
                     graceful shutdown will rely on Drop only.",
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

    fn build_bridge_with_relay(
        relay_id: &str,
        snapshot: Vec<u8>,
    ) -> (Arc<ShutdownBridge>, UnixStream) {
        let (stream, controller_side) = stream_pair();

        let bridge = ShutdownBridge::new();
        bridge.set_control_stream(Some(stream));

        let pid = relay_id.to_string();
        let snap = snapshot.clone();
        bridge.set_snapshot_fn(Box::new(move || vec![(pid.clone(), snap.clone())]));

        (bridge, controller_side)
    }

    #[test]
    fn graceful_shutdown_emits_framed_runner_shutdown_notification() {
        let snapshot = b"final-state-bytes".to_vec();
        let (bridge, mut controller_side) = build_bridge_with_relay("p1", snapshot.clone());

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

        assert_eq!(notif.relay_id, "p1");
        assert_eq!(notif.serialized_state, snapshot);
        assert!(!request.reference_id.is_empty());

        // Now signal the ack so the wait thread releases. Closing the
        // socket on the controller side without acking would also work but
        // we exercise the happy path explicitly.
        signal_result(&request.reference_id, Ok(String::new()));
        run_handle.join().expect("shutdown thread joins");
    }

    /// How many ack-timeout lines for `relay_id` the test `name` of this module
    /// printed in a process of its own: `None` inside that process.
    fn ack_timeouts_alone(name: &str, relay_id: &str) -> Option<usize> {
        let stderr = crate::test_support::passes_alone(&format!("shutdown::tests::{name}"))?;
        Some(
            stderr
                .matches(&format!(
                    "[flezha] RunnerShutdownAck timeout for {relay_id}\n"
                ))
                .count(),
        )
    }

    #[test]
    fn graceful_shutdown_returns_within_2s_when_ack_never_arrives() {
        if let Some(lines) = ack_timeouts_alone(
            "graceful_shutdown_returns_within_2s_when_ack_never_arrives",
            "p_timeout",
        ) {
            assert_eq!(lines, 1, "one line for the timed-out ack");
            return;
        }
        let (bridge, _controller_side) = build_bridge_with_relay("p_timeout", b"unused".to_vec());

        let started = Instant::now();
        bridge.run_graceful_shutdown();
        let elapsed = started.elapsed();

        // Timeout is 2 s per relay; allow a small slack for thread
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
    fn end_process_runs_no_exit_handler() {
        // The child: a handler that aborts stands in for a host runtime whose
        // teardown an exit handler would race.
        let Some(child) =
            crate::test_support::run_alone("shutdown::tests::end_process_runs_no_exit_handler")
        else {
            extern "C" fn abort_on_exit() {
                std::process::abort();
            }
            // SAFETY: registers a plain function with no captured state.
            unsafe { libc::atexit(abort_on_exit) };
            end_process(0);
        };

        assert_eq!(
            child.status.code(),
            Some(0),
            "the exit ran an exit handler: {}",
            child.status
        );
    }

    #[test]
    fn run_graceful_shutdown_is_idempotent() {
        if let Some(lines) = ack_timeouts_alone("run_graceful_shutdown_is_idempotent", "p_idem") {
            assert_eq!(lines, 1, "the second run ships nothing to time out");
            return;
        }
        let (bridge, _controller_side) = build_bridge_with_relay("p_idem", b"x".to_vec());

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
