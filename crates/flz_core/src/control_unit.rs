use std::net::SocketAddr;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use std::{io, thread};

use crossbeam::channel::Sender;
use flz_transport::env::env_ms;
use flz_transport::errors::FlezhaError;
use flz_transport::service::payloader;
use flz_transport::transport::config::{
    self as transport_config, get_transport_type, TransportType,
};
use flz_transport::transport::liveness::{self, ConnectAnswered, ControlLiveness, ReadAction};
use flz_transport::transport::quic::QuicClient;
use flz_transport::transport::FlezhaStream;
use flz_transport::utility::last_good::LastGood;
// `socket_dir` is reached only from `try_open_uds`, which is `#[cfg(unix)]`.
// Ungated, this import is dead on a Windows target and `-D warnings` in CI
// makes that an error.
#[cfg(unix)]
use flz_transport::utility::socket_dir;
use flz_transport::utility::socket_probe;
use prost::Message;
use serde::Serialize;

use crate::error::{Code, Error};
use crate::pb::command_request::Command;
use crate::pb::Ping;
use crate::runner::{ClientResponseMessage, Owed, RunnerMessage};
use crate::util::nonce_suffix;

/// The QUIC address this process last connected through.
static LAST_GOOD_QUIC: LastGood<SocketAddr> = LastGood::new();

/// Control sessions that failed since the last one the controller admitted.
///
/// Held here rather than in [`open_with_retry`] because the rung belongs to
/// the channel: a counter local to that function restarts at zero on every
/// reconnect. And it counts a REFUSED session, not only a failed `connect(2)`
/// — [`try_open`] answers `Ok` as soon as the socket connects and the
/// handshake is written later, by the thread [`sock_conn`] spawns, so a
/// refusal reaches the `Err` arm never. Uncounted, a refused runner reconnects
/// as fast as the kernel allows: 2 600 connects a second against one
/// controller.
static FAILED_SESSIONS: AtomicU32 = AtomicU32::new(0);

/// Bumped when a host stops waiting for a start: a starting connect that
/// began under an older value stops retrying.
static STARTS: AtomicU32 = AtomicU32::new(0);

/// End every start still retrying its first connect, because the host that
/// asked for it stopped waiting.
pub fn abandon_starts() {
    STARTS.fetch_add(1, Ordering::AcqRel);
}

type ControlConn = (ConnectAnswered, Arc<FlezhaStream>, Option<Arc<QuicClient>>);

#[derive(Serialize)]
struct ControlInitMessage {
    namespace: String,
    factories: Vec<crate::pb::FactoryDecl>,
    /// The runner's stable identity, on every connect; the controller's
    /// binding key.
    stable_id: String,
    /// This process's incarnation, the same on every connect it makes: a new
    /// one under a known `stable_id` tells the controller the runner restarted
    /// and runs none of the relays it was bound to.
    incarnation: String,
    /// The credential the controller checks the claimed namespace against.
    /// Absent when this runner holds none, so a deployment authenticating with a
    /// SPIFFE certificate — or one with enforcement off — sends what it always
    /// did.
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace_secret: Option<String>,
}

pub fn open(
    namespace: String,
    declared: Arc<RwLock<Vec<String>>>,
    itx: Sender<ClientResponseMessage>,
    otx: Arc<Mutex<Sender<RunnerMessage>>>,
) -> Option<ControlConn> {
    // A starting runner is owed no wait for the refusals of the one before it.
    // The count belongs to the runner; without this a host refused on one
    // namespace waits 12.8 s or more on its next start, past the 10 s the C ABI
    // allows the handshake.
    FAILED_SESSIONS.store(0, Ordering::Relaxed);
    let started_as = STARTS.load(Ordering::Acquire);
    let abandoned = move || STARTS.load(Ordering::Acquire) != started_as;
    open_with_retry(namespace, declared, Some(itx), otx, &abandoned)
}

/// A reconnect sends no startup confirmation, and gives up only once the core
/// is shut down: a closing runner has nothing to reconnect for.
pub fn open_reconnect(
    namespace: String,
    declared: Arc<RwLock<Vec<String>>>,
    otx: Arc<Mutex<Sender<RunnerMessage>>>,
) -> Option<ControlConn> {
    open_with_retry(namespace, declared, None, otx, &|| {
        crate::poll_core::global().is_shut_down()
    })
}

fn open_with_retry(
    namespace: String,
    declared: Arc<RwLock<Vec<String>>>,
    itx: Option<Sender<ClientResponseMessage>>,
    otx: Arc<Mutex<Sender<RunnerMessage>>>,
    abandoned: &dyn Fn() -> bool,
) -> Option<ControlConn> {
    // The rung this channel is already on. A first connect, and a reconnect
    // after a session the controller admitted, both read zero and wait for
    // nothing.
    let mut attempt = FAILED_SESSIONS.load(Ordering::Relaxed);
    let mut diagnosis = OnceOnly::pending();

    loop {
        if attempt > 0 {
            let backoff = reconnect_backoff(attempt);
            flz_transport::notice!(
                Info,
                "Control channel attempt {} in {backoff:?}...",
                attempt + 1
            );
            thread::sleep(backoff);
        }
        if abandoned() {
            return None;
        }

        match try_open(
            namespace.clone(),
            Arc::clone(&declared),
            itx.clone(),
            otx.clone(),
        ) {
            // A socket that connects is not yet a runner the controller
            // admitted: the read loop reports that once the handshake is whole.
            Ok(result) => return Some(result),
            Err(e) => {
                attempt += 1;
                FAILED_SESSIONS.store(attempt, Ordering::Relaxed);
                // Once, on entry to the retry loop: which of the four
                // situations this is and what fixes it. Repeating it every
                // backoff teaches the user to skip the line that matters.
                if let Some(text) = diagnosis.first(connect_diagnosis) {
                    flz_transport::notice!(Warn, "{text}");
                }
                flz_transport::notice!(Warn, "Connection attempt {attempt} failed: {e}");
            }
        }
    }
}

const DEFAULT_RECONNECT_CAP_MS: u64 = 30_000;

fn reconnect_backoff(attempt: u32) -> Duration {
    flz_transport::backoff::Ladder {
        base: Duration::from_millis(100),
        factor_cap: 8,
        cap: env_ms("FLEZHA_RECONNECT_MAX_BACKOFF_MS", DEFAULT_RECONNECT_CAP_MS),
    }
    .delay(attempt)
}

/// End a control session: record how it ended, then post the reconnect — the
/// coordinator opens the next one off this count.
///
/// Only a completed handshake resets the rung, so a controller that restarts
/// under a healthy fleet is reconnected to at once. The first inbound frame is
/// not admission: a controller that writes `assign_runner_id` and then fails
/// the connect, or severs the data streams before the session is whole, would
/// otherwise be retried as fast as the kernel allows.
fn end_session(
    otx: &Mutex<Sender<RunnerMessage>>,
    stream: &Arc<FlezhaStream>,
    completed: bool,
    reason: &'static str,
) {
    if completed {
        FAILED_SESSIONS.store(0, Ordering::Relaxed);
    } else {
        FAILED_SESSIONS.fetch_add(1, Ordering::Relaxed);
    }
    let _ = otx.lock().unwrap().send(RunnerMessage::ControlLost {
        stream: Arc::clone(stream),
        reason,
    });
}

/// The diagnosis is printed once per retry loop; the per-attempt line is
/// what repeats.
struct OnceOnly(bool);

impl OnceOnly {
    fn pending() -> Self {
        OnceOnly(false)
    }

    fn first<F: FnOnce() -> String>(&mut self, produce: F) -> Option<String> {
        if self.0 {
            return None;
        }
        self.0 = true;
        Some(produce())
    }
}

fn try_open(
    namespace: String,
    declared: Arc<RwLock<Vec<String>>>,
    itx: Option<Sender<ClientResponseMessage>>,
    otx: Arc<Mutex<Sender<RunnerMessage>>>,
) -> Result<ControlConn, Error> {
    match get_transport_type() {
        #[cfg(unix)]
        TransportType::Uds => {
            let (answered, stream) = try_open_uds(namespace, declared, itx, otx)?;
            Ok((answered, stream, None))
        }
        #[cfg(not(unix))]
        TransportType::Uds => Err(Error::Connect(Code::UdsUnavailable.with([]))),
        TransportType::Quic => try_open_quic(namespace, declared, itx, otx),
    }
}

#[cfg(unix)]
fn try_open_uds(
    namespace: String,
    declared: Arc<RwLock<Vec<String>>>,
    itx: Option<Sender<ClientResponseMessage>>,
    otx: Arc<Mutex<Sender<RunnerMessage>>>,
) -> Result<(ConnectAnswered, Arc<FlezhaStream>), Error> {
    let socket_path = socket_dir::control_socket_path()?;

    let stream = UnixStream::connect(socket_path.clone()).map_err(|e| {
        Error::Connect(Code::ControllerUnreachable.with([(
            "reason",
            format!("control socket {socket_path}: {e}").into(),
        )]))
    })?;

    // The directory that answered is where this runner's data sockets are
    // opened, and where a reconnect returns to.
    socket_dir::note_connected(&socket_path);

    let stream_box = Arc::new(FlezhaStream::unix(stream));

    Ok(sock_conn(namespace, declared, stream_box, itx, otx))
}

fn try_open_quic(
    namespace: String,
    declared: Arc<RwLock<Vec<String>>>,
    itx: Option<Sender<ClientResponseMessage>>,
    otx: Arc<Mutex<Sender<RunnerMessage>>>,
) -> Result<ControlConn, Error> {
    let ca_cert_path = transport_config::ca_cert_path();
    let (candidates, unresolved) = quic_candidates()?;

    let mut failures = unresolved;
    for addr in candidates {
        // A control stream this candidate cannot open is this candidate
        // failing, not the attempt failing — the next one still gets its turn,
        // and only a candidate that carried a control stream is remembered.
        let dialled = QuicClient::connect(addr, &ca_cert_path)
            .map(Arc::new)
            .and_then(|client| client.open_bidi_stream().map(|stream| (client, stream)));

        match dialled {
            Ok((client, control_stream)) => {
                LAST_GOOD_QUIC.note(addr);

                let stream_box = Arc::new(FlezhaStream::Quic(control_stream));
                let (answered, stream) = sock_conn(namespace, declared, stream_box, itx, otx);

                return Ok((answered, stream, Some(client)));
            }
            // Every failure names the address it was dialled against — a
            // failure that names no address cannot be acted on.
            Err(e) => failures.push(format!("{}: {}", addr, e)),
        }
    }

    Err(Error::Connect(
        Code::ControllerUnreachable.with([("reason", failures.join("; ").into())]),
    ))
}

/// Every address the list resolves to on THIS attempt, last-good first, plus
/// the entries that failed to resolve.
fn quic_candidates() -> Result<(Vec<SocketAddr>, Vec<String>), Error> {
    let entries = transport_config::quic_addresses()?;
    let (resolved, unresolved) = transport_config::resolve_entries(&entries);
    if resolved.is_empty() {
        return Err(Error::Connect(
            Code::QuicAddressesUnresolved.with([("reason", unresolved.join("; ").into())]),
        ));
    }
    Ok((LAST_GOOD_QUIC.prefer(resolved), unresolved))
}

/// The diagnosis every SDK and `flz runner check` render.
fn connect_diagnosis() -> String {
    socket_probe::connect_diagnosis(get_transport_type(), None, None)
}

pub(crate) fn sock_conn(
    namespace: String,
    declared: Arc<RwLock<Vec<String>>>,
    stream_box: Arc<FlezhaStream>,
    itx: Option<Sender<ClientResponseMessage>>,
    otx: Arc<Mutex<Sender<RunnerMessage>>>,
) -> (ConnectAnswered, Arc<FlezhaStream>) {
    let stream_clone = Arc::clone(&stream_box);

    // Shared (Rust + Java) control-channel liveness: an ack-deadline until
    // the controller's first frame, then a steady-state idle watchdog that
    // re-arms on every frame. Catches both the warm-restart "accept but
    // never ack" hang and a controller that wedges after the handshake.
    let mut live = ControlLiveness::new();
    let answered = live.connect_answered();

    // Present the self-owned stable identity on every connect (initial and
    // reconnect): the controller resolves a reconnect from the always-present
    // stable id + the existing bindings (a binding swap), so there is no
    // separate reconnect frame and no `itx`-gated behaviour. Written before the
    // stream is handed back: the controller reads only a connection's first
    // frame as the handshake, and the caller may write (a queued keepalive)
    // the moment this returns.
    if let Err(e) = send_init_msg(namespace, &declared, stream_clone.clone()) {
        flz_transport::notice!(
            Warn,
            "control channel init send failed (peer vanished mid-handshake); reconnecting: {e}"
        );
        end_session(&otx, &stream_box, false, "write_error");
        return (answered, stream_box);
    }

    thread::spawn(move || {
        let _ = stream_clone.set_read_timeout(Some(live.current_deadline()));

        // Only an initial connect is confirmed, and only once the controller
        // has answered it, or has let the grace pass without a word.
        let start = PendingStart(Owed::new(itx));
        start.confirm_after(CONNECT_ANSWER_GRACE);

        read_loop(&stream_clone, &otx, &mut live, &start);
    });

    (answered, stream_box)
}

/// How long a starting runner waits for the controller's first frame before
/// it reports itself started anyway. A refusal is written the moment the
/// controller reads the handshake, so it lands well inside this; a controller
/// that stays silent (a test's bare socket) costs a start no more than this.
const CONNECT_ANSWER_GRACE: Duration = Duration::from_secs(1);

/// The starting caller's confirmation, owed once: by the controller's first
/// frame, its refusal, or the grace running out, whichever comes first.
#[derive(Clone)]
struct PendingStart(Owed<Sender<ClientResponseMessage>>);

impl PendingStart {
    /// Whether a starting caller was still waiting for this answer.
    fn answer(&self, message: ClientResponseMessage) -> bool {
        let taken = self.0.claim();
        if let Some(itx) = &taken {
            let _ = itx.send(message);
        }
        taken.is_some()
    }

    fn confirm_after(&self, grace: Duration) {
        if self.0.is_claimed() {
            return;
        }
        let start = self.clone();
        thread::spawn(move || {
            thread::sleep(grace);
            start.answer(ClientResponseMessage::RunnerIdConfirmation);
        });
    }
}

/// The controller's refusal of this connect, when `frame` is one.
fn connect_refusal(frame: &[u8]) -> Option<FlezhaError> {
    match crate::command::request_of(frame).ok()?.command? {
        Command::ConnectRefused(refused) => Some(refused.error.map_or_else(
            || {
                Code::UnreadableReply
                    .with([("reply", "a connect refusal without its error".into())])
            },
            FlezhaError::from,
        )),
        _ => None,
    }
}

/// Feed control frames to the coordinator until the channel fails, then ask
/// for a reconnect with the classified reason.
fn read_loop(
    stream: &Arc<FlezhaStream>,
    otx: &Arc<Mutex<Sender<RunnerMessage>>>,
    live: &mut ControlLiveness,
    start: &PendingStart,
) {
    loop {
        match payloader::extract_payload(&**stream) {
            Ok((payload_id, message, metadata)) => {
                let admitted = live.handshake_completed();
                // Once per refused attempt, and never forwarded. A refused
                // start is reported to its host, which owns the retry, so its
                // session ends here; on a reconnect the controller closes the
                // stream next, and that read error drives the next attempt.
                if let Some(refusal) = (!admitted).then(|| connect_refusal(&message)).flatten() {
                    flz_transport::notice!(
                        Error,
                        "the controller refused the connect: {}",
                        refusal.text()
                    );
                    if start.answer(ClientResponseMessage::ConnectRefused(refusal)) {
                        return;
                    }
                    continue;
                }
                start.answer(ClientResponseMessage::RunnerIdConfirmation);
                live.note_frame();
                if !admitted && live.handshake_completed() {
                    let failed = FAILED_SESSIONS.load(Ordering::Relaxed);
                    if failed > 0 {
                        flz_transport::notice!(Warn, "Connected after {failed} attempts");
                    }
                }
                let _ = stream.set_read_timeout(Some(live.current_deadline()));
                if crate::command::answer_credit(stream, &payload_id, &message) {
                    continue;
                }
                // A runner abandoned meanwhile has no one left to hand it to.
                let handed = otx.lock().unwrap_or_else(|e| e.into_inner()).send(
                    RunnerMessage::HandleCommandAction(payload_id, message, metadata),
                );
                if handed.is_err() {
                    return;
                }
            }
            Err(e) => match live.classify_error(&e) {
                ReadAction::Retry => continue,
                ReadAction::Reconnect => {
                    let reason = classify_reconnect_reason(&e, live.acked());
                    if ack_window_elapsed(&e, live.acked()) {
                        flz_transport::notice!(
                            Warn,
                            "control channel: controller never acknowledged within ack window \
                             ({reason}); reconnecting: {e}"
                        );
                    } else {
                        flz_transport::notice!(
                            Warn,
                            "control channel went silent / disconnected ({reason}); reconnecting: {e}"
                        );
                    }
                    end_session(otx, stream, live.handshake_completed(), reason);
                    return;
                }
            },
        }
    }
}

/// A bare `Ping` on `control_ping`; the controller's echo re-arms the idle
/// watchdog.
fn build_keepalive_ping() -> Vec<u8> {
    let ping = Ping {
        nonce: format!("ka-{}", nonce_suffix()),
    };
    payloader::build_without_metadata(liveness::CONTROL_PING_PAYLOAD_ID, &ping.encode_to_vec())
}

pub fn send_keepalive_ping(stream: &FlezhaStream) -> io::Result<()> {
    let frame = build_keepalive_ping();
    crate::state_sink::write(crate::state_sink::Class::Control, Some(stream), frame)
        .refused()
        .map_err(|e| {
            // Every tick during a reconnect meets the dead stream; the reader
            // already reported the loss.
            tracing::debug!(
                target: "flezha::transport",
                kind = ?e.kind(),
                "control keepalive ping write failed: {}", e
            );
            e
        })
}

/// True when the ack window really elapsed: the read waited it out and no ack
/// arrived. Any other error ended the session before a window could pass, so
/// the warning must not claim one was waited out.
fn ack_window_elapsed(e: &io::Error, acked: bool) -> bool {
    !acked
        && matches!(
            e.kind(),
            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
        )
}

/// A read error as the controller's `reason` string: a post-ack timeout is
/// the watchdog, a pre-ack one a channel never registered, anything else a
/// break.
fn classify_reconnect_reason(e: &io::Error, acked: bool) -> &'static str {
    match e.kind() {
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => {
            if acked {
                "keepalive_timeout"
            } else {
                "control_stream_broken"
            }
        }
        io::ErrorKind::UnexpectedEof
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::BrokenPipe => "control_stream_broken",
        _ => "read_error",
    }
}

/// The connect init frame, with the factory set read at write time so every
/// reconnect resyncs it.
fn send_init_msg(
    namespace: String,
    declared: &RwLock<Vec<String>>,
    stream_clone: Arc<FlezhaStream>,
) -> io::Result<()> {
    let factory_ids: Vec<String> = declared.read().unwrap().clone();
    let factories = crate::relay::registry::advertised(&factory_ids);

    // Loaded once per process (cached); the controller binds relays to it.
    let stable_id = crate::persistent_id::stable_id(&namespace, &factory_ids);

    let control_init_message = ControlInitMessage {
        namespace,
        factories,
        stable_id,
        incarnation: crate::persistent_id::incarnation(),
        namespace_secret: flz_transport::env::connect_raw("FLEZHA_NAMESPACE_SECRET"),
    };

    let control_message = serde_json::to_string(&control_init_message).unwrap();
    let payload =
        payloader::build_without_metadata("connect_control_channel", control_message.as_bytes());

    // A failed init write means the peer vanished between connect and write
    // (e.g. a controller that died mid-handshake); the class refuses it to the
    // caller so it drives a reconnect instead of panicking the reader thread.
    crate::state_sink::write(
        crate::state_sink::Class::Control,
        Some(&stream_clone),
        payload,
    )
    .refused()
}

#[cfg(test)]
#[path = "control_unit_tests.rs"]
mod tests;
