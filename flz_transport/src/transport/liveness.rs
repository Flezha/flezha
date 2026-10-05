//! Control-channel liveness, one implementation for every SDK: a first-frame
//! ack deadline (`FLEZHA_CONTROL_ACK_TIMEOUT_MS`, so a controller that
//! accepts but never registers cannot hold the reader forever) and a
//! steady-state idle watchdog (`FLEZHA_CONTROL_IDLE_TIMEOUT_MS`, re-armed by
//! every inbound frame) that the runner's jittered keepalive ping keeps a
//! healthy channel inside. Both are read timeouts on `FlezhaStream`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::env::env_ms;
use crate::utility::noise::noise;

/// Payload id for the steady-state keepalive round-trip. Carried on its own
/// payload (a bare `Ping`/`PingResponse`), distinct from `"control_command"`,
/// so it never enters the command dispatcher on either side — the controller
/// echoes it and both SDKs drop it after the read-level liveness re-arm.
pub const CONTROL_PING_PAYLOAD_ID: &str = "control_ping";

/// Payload id for the data-channel heartbeat. The runner writes a bare frame
/// on every secondary stream so an idle-but-healthy relay keeps the controller's
/// per-channel liveness clock fresh without producing real output.
pub const DATA_HEARTBEAT_PAYLOAD_ID: &str = "data_heartbeat";

const DEFAULT_ACK_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_IDLE_TIMEOUT_MS: u64 = 45_000;
const DEFAULT_KEEPALIVE_MS: u64 = 15_000;
const DEFAULT_DATA_HEARTBEAT_MS: u64 = 10_000;

/// First-frame ack-deadline (the assign-runner-id ack). Default 10 s.
pub fn control_ack_timeout() -> Duration {
    env_ms("FLEZHA_CONTROL_ACK_TIMEOUT_MS", DEFAULT_ACK_TIMEOUT_MS)
}

/// Steady-state idle window after the handshake. Default 45 s — comfortably
/// above the keepalive interval so a live channel always re-arms in time.
pub fn control_idle_timeout() -> Duration {
    env_ms("FLEZHA_CONTROL_IDLE_TIMEOUT_MS", DEFAULT_IDLE_TIMEOUT_MS)
}

/// How often the runner sends a control-channel keepalive ping. Default 15 s.
pub fn control_keepalive_interval() -> Duration {
    env_ms("FLEZHA_CONTROL_KEEPALIVE_MS", DEFAULT_KEEPALIVE_MS)
}

/// How often the runner heartbeats the data channel. Default 10 s — below the
/// controller's stall threshold so a couple of missed beats stay inside it.
pub fn data_heartbeat_interval() -> Duration {
    env_ms("FLEZHA_DATA_HEARTBEAT_MS", DEFAULT_DATA_HEARTBEAT_MS)
}

/// Full-jitter a base duration into `[base/2, base]` so a fleet that all
/// reconnect / keepalive at once spreads its traffic instead of thundering.
pub fn jittered(base: Duration) -> Duration {
    let ms = base.as_millis() as u64;
    if ms < 2 {
        return base;
    }
    let half = ms / 2;
    Duration::from_millis(half + (noise() % (half + 1)))
}

/// What the control reader should do after a read error, given the deadline
/// state machine.
#[derive(Debug, PartialEq, Eq)]
pub enum ReadAction {
    /// Transient (`EINTR`) — retry the read.
    Retry,
    /// The control channel is dead — ack window missed, idle wedge, or a real
    /// disconnect. Drive reconnect.
    Reconnect,
}

/// The runner's own answer to its session's `ConnectSocket`: raised by the
/// thread that executes the command, read by the control reader. One per
/// session, so a reconnect never inherits the last session's answer.
#[derive(Clone, Default)]
pub struct ConnectAnswered(Arc<AtomicBool>);

impl ConnectAnswered {
    /// The pair set came up; the answer is about to be written.
    pub fn note(&self) {
        self.0.store(true, Ordering::Release);
    }

    fn is_set(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Tracks the control-channel ack/idle deadline across reads, and whether the
/// session's handshake completed. Construct once per control connection; arm
/// `current_deadline` before each read and call `note_frame` after each
/// successful frame.
///
/// Two facts, never one: the deadline asks whether any frame has arrived, the
/// reconnect ladder asks whether the controller admitted the runner, and a
/// controller can answer the first frame and still fail the handshake at the
/// second.
pub struct ControlLiveness {
    acked: bool,
    completed: bool,
    answered: ConnectAnswered,
    ack_timeout: Duration,
    idle_timeout: Duration,
}

impl Default for ControlLiveness {
    fn default() -> Self {
        Self::new()
    }
}

impl ControlLiveness {
    pub fn new() -> Self {
        ControlLiveness {
            acked: false,
            completed: false,
            answered: ConnectAnswered::default(),
            ack_timeout: control_ack_timeout(),
            idle_timeout: control_idle_timeout(),
        }
    }

    /// The read deadline to arm before the next read: the ack window until the
    /// first frame arrives, then the steady-state idle window.
    pub fn current_deadline(&self) -> Duration {
        if self.acked {
            self.idle_timeout
        } else {
            self.ack_timeout
        }
    }

    /// Record a successful frame. Returns `true` exactly once — when this is
    /// the first frame and the channel leaves the ack window.
    ///
    /// A frame read after the runner answered `ConnectSocket` completes the
    /// handshake. The runner cannot see the controller accept its answer, only
    /// the controller carrying on with the session; a refusal closes the
    /// connection instead.
    pub fn note_frame(&mut self) -> bool {
        self.completed |= self.answered.is_set();
        if self.acked {
            false
        } else {
            self.acked = true;
            true
        }
    }

    pub fn acked(&self) -> bool {
        self.acked
    }

    /// Whether this session was admitted, which is what resets the reconnect
    /// ladder.
    pub fn handshake_completed(&self) -> bool {
        self.completed
    }

    /// The handle the `ConnectSocket` executor raises for this session.
    pub fn connect_answered(&self) -> ConnectAnswered {
        self.answered.clone()
    }

    /// Classify a read error. A timeout (`WouldBlock`/`TimedOut`) is fatal in
    /// both states — before ack it means the controller never registered us,
    /// after ack it means the channel wedged past the idle window. Only
    /// `Interrupted` is retried; every other error is a disconnect.
    pub fn classify_error(&self, e: &std::io::Error) -> ReadAction {
        match e.kind() {
            std::io::ErrorKind::Interrupted => ReadAction::Retry,
            _ => ReadAction::Reconnect,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    #[test]
    fn deadline_uses_ack_window_until_first_frame_then_idle_window() {
        std::env::set_var("FLEZHA_CONTROL_ACK_TIMEOUT_MS", "700");
        std::env::set_var("FLEZHA_CONTROL_IDLE_TIMEOUT_MS", "9000");

        let mut live = ControlLiveness::new();
        assert!(!live.acked());
        assert_eq!(live.current_deadline(), Duration::from_millis(700));

        // First frame transitions out of the ack window exactly once, and
        // admits nothing.
        assert!(live.note_frame());
        assert!(live.acked());
        assert_eq!(live.current_deadline(), Duration::from_millis(9000));
        assert!(!live.note_frame());
        assert!(!live.handshake_completed());

        std::env::remove_var("FLEZHA_CONTROL_ACK_TIMEOUT_MS");
        std::env::remove_var("FLEZHA_CONTROL_IDLE_TIMEOUT_MS");
    }

    #[test]
    fn only_a_frame_after_the_connect_answer_completes_the_handshake() {
        let mut live = ControlLiveness::new();
        let answered = live.connect_answered();

        // `assign_runner_id`, then `ConnectSocket`.
        live.note_frame();
        live.note_frame();
        answered.note();
        assert!(
            !live.handshake_completed(),
            "an answer the controller has not carried on from is not admission"
        );

        live.note_frame();
        assert!(live.handshake_completed());
    }

    #[test]
    fn a_new_session_does_not_inherit_the_last_answer() {
        ControlLiveness::new().connect_answered().note();

        let mut next = ControlLiveness::new();
        next.note_frame();
        assert!(!next.handshake_completed());
    }

    #[test]
    fn timeout_is_fatal_in_both_states_interrupted_retries() {
        let live = ControlLiveness::new();
        let wb = io::Error::new(io::ErrorKind::WouldBlock, "x");
        let to = io::Error::new(io::ErrorKind::TimedOut, "x");
        let intr = io::Error::new(io::ErrorKind::Interrupted, "x");
        let eof = io::Error::new(io::ErrorKind::BrokenPipe, "x");

        assert_eq!(live.classify_error(&wb), ReadAction::Reconnect);
        assert_eq!(live.classify_error(&to), ReadAction::Reconnect);
        assert_eq!(live.classify_error(&intr), ReadAction::Retry);
        assert_eq!(live.classify_error(&eof), ReadAction::Reconnect);
    }

    #[test]
    fn jitter_stays_within_half_to_base() {
        let base = Duration::from_millis(1000);
        for _ in 0..50 {
            let j = jittered(base);
            assert!(j >= Duration::from_millis(500), "jitter {:?} below half", j);
            assert!(j <= base, "jitter {:?} above base", j);
        }
    }
}
