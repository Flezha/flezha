//! Shared control-channel liveness, used by BOTH SDKs — the Rust SDK's
//! `sock_conn` reader and the Java SDK via `flz_jni`'s control reader. One
//! implementation, one set of env knobs, so Rust/Java parity holds by
//! construction.
//!
//! Two guarantees:
//!   1. First-frame ack-deadline — a freshly opened control channel must see
//!      the controller's first frame (the assign-executor-id ack) within
//!      `FLEZHA_CONTROL_ACK_TIMEOUT_MS`. A controller mid-shutdown accepts the
//!      socket but never registers us (the warm-restart hang); without this
//!      bound the reader blocks forever and the executor never reaches a live
//!      controller.
//!   2. Steady-state liveness — after the ack, an idle watchdog re-arms on
//!      every inbound frame; if the channel sees nothing for
//!      `FLEZHA_CONTROL_IDLE_TIMEOUT_MS` (a half-open / wedged controller the
//!      OS never surfaces as a read error) the reader drives reconnect. The
//!      executor's jittered keepalive ping keeps a healthy-but-quiet channel
//!      inside the window, so the watchdog never fires on a live controller.
//!
//! Both deadlines are enforced with `FlezhaStream::set_read_timeout`, honored
//! on UDS and — since this change — on QUIC (see `quic::ChannelReader`).

use std::time::Duration;

/// Payload id for the steady-state keepalive round-trip. Carried on its own
/// payload (a bare `Ping`/`PingResponse`), distinct from `"control_command"`,
/// so it never enters the command dispatcher on either side — the controller
/// echoes it and both SDKs drop it after the read-level liveness re-arm.
pub const CONTROL_PING_PAYLOAD_ID: &str = "control_ping";

const DEFAULT_ACK_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_IDLE_TIMEOUT_MS: u64 = 45_000;
const DEFAULT_KEEPALIVE_MS: u64 = 15_000;

fn env_ms(key: &str, default_ms: u64) -> Duration {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_millis(default_ms))
}

/// First-frame ack-deadline (the assign-executor-id ack). Default 10 s.
pub fn control_ack_timeout() -> Duration {
    env_ms("FLEZHA_CONTROL_ACK_TIMEOUT_MS", DEFAULT_ACK_TIMEOUT_MS)
}

/// Steady-state idle window after the handshake. Default 45 s — comfortably
/// above the keepalive interval so a live channel always re-arms in time.
pub fn control_idle_timeout() -> Duration {
    env_ms("FLEZHA_CONTROL_IDLE_TIMEOUT_MS", DEFAULT_IDLE_TIMEOUT_MS)
}

/// How often the executor sends a control-channel keepalive ping. Default 15 s.
pub fn control_keepalive_interval() -> Duration {
    env_ms("FLEZHA_CONTROL_KEEPALIVE_MS", DEFAULT_KEEPALIVE_MS)
}

/// Full-jitter a base duration into `[base/2, base]` so a fleet that all
/// reconnect / keepalive at once spreads its traffic instead of thundering.
pub fn jittered(base: Duration) -> Duration {
    let ms = base.as_millis() as u64;
    if ms < 2 {
        return base;
    }
    let half = ms / 2;
    // Dependency-free jitter: the monotonic clock's sub-millisecond noise is
    // plenty to decorrelate a fleet; we don't need cryptographic randomness.
    let noise = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    Duration::from_millis(half + (noise % (half + 1)))
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

/// Tracks the control-channel ack/idle deadline across reads. Construct once
/// per control connection; arm `current_deadline` before each read and call
/// `note_frame` after each successful frame.
pub struct ControlLiveness {
    acked: bool,
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
    pub fn note_frame(&mut self) -> bool {
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

        // First frame transitions out of the ack window exactly once.
        assert!(live.note_frame());
        assert!(live.acked());
        assert_eq!(live.current_deadline(), Duration::from_millis(9000));
        assert!(!live.note_frame());

        std::env::remove_var("FLEZHA_CONTROL_ACK_TIMEOUT_MS");
        std::env::remove_var("FLEZHA_CONTROL_IDLE_TIMEOUT_MS");
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
