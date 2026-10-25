//! Sender-side credit window for the runner → controller state stream: the
//! controller offers bytes (`CreditWindowInit`), every record [`acquire`]s
//! its size before the sink write, and `CreditGrant` [`grant`]s bytes back
//! as the controller fsyncs. The invariant: an exhausted window parks the
//! sending thread and never drops — a bounded wait re-checked every
//! [`PARK_INTERVAL_VAR`] milliseconds (50 by default), counted on
//! [`CREDIT_PARKED`] so a parked runner reads as parked, not wedged — while
//! `arm`/`grant` run where control frames are read and never block. A frame
//! larger than the whole window is sent once it is fully replenished.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// How long a parked sender waits before re-checking its window, in
/// milliseconds. Not a pacing dial: `arm` and `grant` both `notify_all`, so
/// the wait ends on the event and this bounds only a wake that never came.
/// Lowering it buys no latency in the normal case and costs wakeups; raising
/// it only lengthens recovery from a lost wake.
pub const PARK_INTERVAL_VAR: &str = "FLEZHA_CREDIT_PARK_INTERVAL_MS";

const DEFAULT_PARK_INTERVAL_MS: usize = 50;
const WARN_AFTER: Duration = Duration::from_secs(5);

/// The lost-wake fallback, read from the process environment inside this crate
/// so a runner in any SDK satisfies it from its own environment — threading it
/// through a builder would owe every host SDK a declaration key and a setter
/// for a diagnostic. Read once: it is per-park, and a knob that can change
/// under a parked sender is a knob two senders can disagree about.
fn park_interval() -> Duration {
    static CACHE: OnceLock<Duration> = OnceLock::new();
    *CACHE.get_or_init(|| {
        Duration::from_millis(crate::util::env_parse_positive(
            PARK_INTERVAL_VAR,
            DEFAULT_PARK_INTERVAL_MS,
        ) as u64)
    })
}

/// Credit-park wait iterations, process-wide; on the heartbeat, where
/// advancing beside a frozen `dispatches` means parked, not wedged.
pub static CREDIT_PARKED: AtomicU64 = AtomicU64::new(0);

pub fn credit_parked() -> u64 {
    CREDIT_PARKED.load(Ordering::Relaxed)
}

#[derive(Default)]
struct WindowState {
    /// The adopted credit session; `None` means no credit accounting.
    session: Option<String>,
    window_bytes: u64,
    available: i64,
}

pub struct CreditWindow {
    state: Mutex<WindowState>,
    cond: Condvar,
}

impl Default for CreditWindow {
    fn default() -> Self {
        Self::new()
    }
}

impl CreditWindow {
    pub fn new() -> Self {
        CreditWindow {
            state: Mutex::new(WindowState::default()),
            cond: Condvar::new(),
        }
    }

    /// Adopt an offered window, replacing any session. Never blocks.
    pub fn arm(&self, session: String, window_bytes: u64) {
        let mut st = self.state.lock().expect("credit window mutex poisoned");
        tracing::info!(
            target: "flezha::credit",
            "credit window armed: session={} window_bytes={}",
            session,
            window_bytes,
        );
        st.session = Some(session);
        st.window_bytes = window_bytes;
        st.available = window_bytes as i64;
        drop(st);
        self.cond.notify_all();
    }

    /// Drop the session; parked senders proceed un-metered until the next
    /// `arm`.
    pub fn disarm(&self) {
        let mut st = self.state.lock().expect("credit window mutex poisoned");
        if st.session.is_some() {
            tracing::info!(target: "flezha::credit", "credit window disarmed (reconnect)");
        }
        *st = WindowState::default();
        drop(st);
        self.cond.notify_all();
    }

    /// Apply a `CreditGrant`; one for another session is stale and dropped.
    /// Never blocks.
    pub fn grant(&self, session: &str, bytes: u64) {
        let mut st = self.state.lock().expect("credit window mutex poisoned");
        match st.session.as_deref() {
            Some(current) if current == session => {
                st.available = st
                    .available
                    .saturating_add(bytes as i64)
                    .min(st.window_bytes as i64);
                drop(st);
                self.cond.notify_all();
            }
            _ => {
                tracing::debug!(
                    target: "flezha::credit",
                    "discarding stale credit grant for session {}",
                    session,
                );
            }
        }
    }

    /// Debit `frame_len`, parking until enough credit is available. Never
    /// from the thread that reads control frames: it applies the grants.
    pub fn acquire(&self, frame_len: usize) {
        let need = frame_len as i64;
        let mut st = self.state.lock().expect("credit window mutex poisoned");
        let mut parked_since: Option<Instant> = None;
        let mut warned = false;
        loop {
            match st.session {
                None => return,
                Some(_) => {
                    // Full-window overshoot: a frame larger than the whole
                    // window sends when nothing is outstanding.
                    if st.available >= need || st.available >= st.window_bytes as i64 {
                        st.available -= need;
                        if parked_since.is_some() {
                            tracing::debug!(
                                target: "flezha::credit",
                                "credit park resumed after {:?}",
                                parked_since.map(|t| t.elapsed()).unwrap_or_default(),
                            );
                        }
                        return;
                    }

                    CREDIT_PARKED.fetch_add(1, Ordering::Relaxed);
                    let since = *parked_since.get_or_insert_with(Instant::now);
                    if !warned && since.elapsed() >= WARN_AFTER {
                        warned = true;
                        flz_transport::notice!(
                            Warn,
                            "sender parked on credit for {:?} (need {need} bytes, {} available) — \
                             controller fsync stalled or grant lost",
                            since.elapsed(),
                            st.available
                        );
                    }
                    st = self
                        .cond
                        .wait_timeout(st, park_interval())
                        .expect("credit window mutex poisoned")
                        .0;
                }
            }
        }
    }

    /// Wait at most `wait` for the window to have room, or for there to be no
    /// window; whether it has. A cut asks this before it drains under the
    /// Cell's gate, so a stalled window refuses the cut rather than parking it.
    pub fn await_room(&self, wait: Duration) -> bool {
        let deadline = Instant::now() + wait;
        let mut st = self.state.lock().expect("credit window mutex poisoned");
        while st.session.is_some() && st.available <= 0 {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return false;
            }
            st = self
                .cond
                .wait_timeout(st, left.min(park_interval()))
                .expect("credit window mutex poisoned")
                .0;
        }
        true
    }

    #[cfg(test)]
    pub fn current_session(&self) -> Option<String> {
        self.state
            .lock()
            .expect("credit window mutex poisoned")
            .session
            .clone()
    }

    #[cfg(test)]
    pub fn available_bytes(&self) -> i64 {
        self.state
            .lock()
            .expect("credit window mutex poisoned")
            .available
    }
}

// ---- process-global window (production send path) --------------------------

pub fn global() -> &'static CreditWindow {
    static WINDOW: OnceLock<CreditWindow> = OnceLock::new();
    WINDOW.get_or_init(CreditWindow::new)
}

#[cfg(test)]
mod tests {
    use std::sync::{mpsc, Arc};
    use std::thread;
    use std::time::Duration;

    use super::*;

    /// `acquire(frame_len)` on a thread of its own; the receiver hears once
    /// it returns.
    fn acquire_on_thread(
        w: &Arc<CreditWindow>,
        frame_len: usize,
    ) -> (thread::JoinHandle<()>, mpsc::Receiver<()>) {
        let (tx, rx) = mpsc::channel();
        let w = Arc::clone(w);
        let sender = thread::spawn(move || {
            w.acquire(frame_len);
            tx.send(()).unwrap();
        });
        (sender, rx)
    }

    // Each test uses its OWN CreditWindow instance — the process-global
    // window stays disarmed, so concurrently-running flusher/sink tests
    // whose send path passes through `credit::acquire` are never metered
    // by these tests' tiny windows.

    #[test]
    fn legacy_session_never_debits_or_parks() {
        let w = CreditWindow::new();
        w.acquire(1_000_000);
        assert_eq!(w.current_session(), None);
    }

    #[test]
    fn armed_window_debits_and_exhaustion_parks_until_grant() {
        let w = Arc::new(CreditWindow::new());
        w.arm("s1".to_string(), 100);
        w.acquire(60);
        assert_eq!(w.available_bytes(), 40);

        let parked_before = credit_parked();
        let (sender, rx) = acquire_on_thread(&w, 60); // only 40 available -> parks

        assert!(
            rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "sender must park while credit is insufficient"
        );
        assert!(credit_parked() > parked_before, "park counter must advance");

        w.grant("s1", 60);
        rx.recv_timeout(Duration::from_secs(2))
            .expect("grant must unpark the sender");
        sender.join().unwrap();
        assert_eq!(w.available_bytes(), 40 - 60 + 60);
    }

    // fails when: a cut under a Cell's gate parks on an exhausted window with
    // no bound, holding the gate after the controller gave up on the reply.
    #[test]
    fn a_room_wait_is_bounded_and_ends_on_a_grant() {
        let w = Arc::new(CreditWindow::new());
        assert!(w.await_room(Duration::ZERO), "no window, always room");
        w.arm("s1".to_string(), 100);
        w.acquire(100);
        assert!(
            !w.await_room(Duration::from_millis(20)),
            "an exhausted window answers no within the bound"
        );

        let waiter = {
            let w = Arc::clone(&w);
            thread::spawn(move || w.await_room(Duration::from_secs(5)))
        };
        w.grant("s1", 10);
        assert!(waiter.join().unwrap(), "a grant gives room");
    }

    #[test]
    fn stale_grant_is_discarded_and_reconnect_resets() {
        let w = CreditWindow::new();
        w.arm("s1".to_string(), 100);
        w.acquire(100);
        w.grant("s-old", 100);
        assert_eq!(w.available_bytes(), 0, "stale-session grant must not apply");

        // Re-attach: a fresh handshake resets to a full window.
        w.arm("s2".to_string(), 100);
        assert_eq!(w.available_bytes(), 100);
        w.grant("s1", 50);
        assert_eq!(
            w.available_bytes(),
            100,
            "grant for the dead session is void"
        );
    }

    #[test]
    fn oversized_frame_sends_on_full_window() {
        let w = Arc::new(CreditWindow::new());
        w.arm("s1".to_string(), 100);
        w.acquire(250); // larger than the window: allowed at full window
        assert_eq!(w.available_bytes(), -150);

        // Next frame parks until the window is fully replenished.
        let (sender, rx) = acquire_on_thread(&w, 10);
        assert!(rx.recv_timeout(Duration::from_millis(200)).is_err());
        w.grant("s1", 250); // caps at window_bytes
        rx.recv_timeout(Duration::from_secs(2))
            .expect("replenished window must unpark");
        sender.join().unwrap();
        assert_eq!(w.available_bytes(), 90);
    }

    #[test]
    fn disarm_wakes_parked_senders() {
        let w = Arc::new(CreditWindow::new());
        w.arm("s1".to_string(), 10);
        w.acquire(10);

        let (sender, rx) = acquire_on_thread(&w, 10);
        assert!(rx.recv_timeout(Duration::from_millis(200)).is_err());
        w.disarm();
        rx.recv_timeout(Duration::from_secs(2))
            .expect("disarm must release parked senders to the legacy path");
        sender.join().unwrap();
    }

    /// The credit hold and the transport's send gate must never nest: the
    /// window's mutex is held across `acquire`'s whole body, and `arm`/`grant`
    /// are documented as never blocking, so a wait for a send buffer taken
    /// inside that body would stall the control frame reader.
    ///
    /// The lost-wake fallback is read from the environment, so the test reads
    /// the same environment: run bare it proves the default, run with
    /// `FLEZHA_CREDIT_PARK_INTERVAL_MS` set it proves the knob is honoured,
    /// and it never mutates the variable — a `set_var` here would race every
    /// other test in this binary.
    #[test]
    fn the_park_interval_comes_from_the_environment_or_its_default() {
        let expected = crate::util::env_parse_positive(PARK_INTERVAL_VAR, DEFAULT_PARK_INTERVAL_MS);

        assert_eq!(park_interval(), Duration::from_millis(expected as u64));
    }

    /// It cannot be a comment, and it cannot be a runtime assertion either —
    /// what makes nesting impossible is that `acquire` takes a byte count and
    /// returns nothing, so no caller can run anything inside its hold, and
    /// this module reaches neither the gate nor the sink. This reads the
    /// source rather than trusting either statement.
    #[test]
    fn nothing_in_the_credit_hold_can_reach_a_send() {
        let src = include_str!("credit.rs");
        let body = &src[..src.find("#[cfg(test)]").expect("the test module")];

        for forbidden in ["send_gate", "SendGate", "state_sink", "FlezhaStream"] {
            assert!(
                !body.contains(forbidden),
                "the credit window must not reach `{}`: a send taken inside \
                 `acquire`'s mutex hold blocks `arm` and `grant`",
                forbidden
            );
        }
        assert!(
            body.contains("pub fn acquire(&self, frame_len: usize) {"),
            "acquire takes a length and no callback, which is what makes a \
             nested wait unwritable rather than merely absent"
        );
    }
}
