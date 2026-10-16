use std::net::SocketAddr;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use std::{io, thread};

use crossbeam::channel::Sender;
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
use crate::pb::{self, Ping};
use crate::runner::{ClientResponseMessage, RunnerMessage};
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
struct FactoryInfo {
    id: String,
    description: Option<String>,
    /// The class's declared language, from the registry; `0` for a factory
    /// with no class, which the controller refuses to place.
    sdk_lang: u8,
    /// `"flow"` or `"steps"`, from the registry; empty for a factory with no
    /// class.
    run: String,
    /// A steps class's loop shape and failure mode; empty for flow.
    #[serde(skip_serializing_if = "String::is_empty")]
    each: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    on_failure: String,
    /// The factory's API, as `FactoryDecl.api` carries it.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    api: Vec<crate::pb::ApiEntry>,
    /// The class's graph, as `FactoryDecl.graph` carries it.
    #[serde(skip_serializing_if = "Option::is_none")]
    graph: Option<crate::pb::RelayGraph>,
}

#[derive(Serialize)]
struct ControlInitMessage {
    namespace: String,
    factory_ids: Vec<String>,
    factories: Vec<FactoryInfo>,
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
    itx: Arc<Mutex<Sender<ClientResponseMessage>>>,
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
    itx: Option<Arc<Mutex<Sender<ClientResponseMessage>>>>,
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
            eprintln!(
                "[flezha] Control channel attempt {} in {:?}...",
                attempt + 1,
                backoff
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
                    eprintln!("[flezha] {}", text);
                }
                eprintln!("[flezha] Connection attempt {} failed: {}", attempt, e);
            }
        }
    }
}

/// The wait before an attempt: jitter on the exponential backoff so a
/// controller restart that drops a whole fleet does not produce a synchronized
/// reconnect storm. Every SDK over this crate shares `liveness::jittered`,
/// which spreads `[d/2, d]`.
fn reconnect_backoff(attempt: u32) -> Duration {
    liveness::jittered(std::cmp::min(
        Duration::from_millis(100 * 2u64.pow(attempt.min(8))),
        Duration::from_secs(30),
    ))
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
    itx: Option<Arc<Mutex<Sender<ClientResponseMessage>>>>,
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
    itx: Option<Arc<Mutex<Sender<ClientResponseMessage>>>>,
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
    itx: Option<Arc<Mutex<Sender<ClientResponseMessage>>>>,
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
    itx: Option<Arc<Mutex<Sender<ClientResponseMessage>>>>,
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
        tracing::warn!(
            target: "flezha::transport",
            kind = ?e.kind(),
            "control channel init send failed (peer vanished mid-handshake); reconnecting: {}",
            e
        );
        end_session(&otx, &stream_box, false, "write_error");
        return (answered, stream_box);
    }

    thread::spawn(move || {
        let _ = stream_clone.set_read_timeout(Some(live.current_deadline()));

        // Only an initial connect is confirmed, and only once the controller
        // has answered it, or has let the grace pass without a word.
        let start = PendingStart(Arc::new(Mutex::new(itx)));
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
struct PendingStart(Arc<Mutex<Option<StartConfirm>>>);

type StartConfirm = Arc<Mutex<Sender<ClientResponseMessage>>>;

impl PendingStart {
    /// Whether a starting caller was still waiting for this answer.
    fn answer(&self, message: ClientResponseMessage) -> bool {
        let taken = self.0.lock().unwrap_or_else(|p| p.into_inner()).take();
        if let Some(itx) = &taken {
            let _ = itx.lock().unwrap_or_else(|p| p.into_inner()).send(message);
        }
        taken.is_some()
    }

    fn confirm_after(&self, grace: Duration) {
        if self.0.lock().unwrap_or_else(|p| p.into_inner()).is_none() {
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
    match pb::CommandRequest::decode(frame).ok()?.command? {
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
                    eprintln!(
                        "[flezha] the controller refused the connect: {}",
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
                        eprintln!("[flezha] Connected after {} attempts", failed);
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
                        tracing::warn!(
                            target: "flezha::transport",
                            reason, idle_deadline = ?live.current_deadline(),
                            "control channel: controller never acknowledged within ack window; reconnecting: {}",
                            e
                        );
                    } else {
                        tracing::warn!(
                            target: "flezha::transport",
                            reason, kind = ?e.kind(),
                            "control channel went silent / disconnected; reconnecting: {}",
                            e
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
    let factories: Vec<FactoryInfo> = factory_ids
        .iter()
        .map(|id| {
            let decl = crate::relay::registry::advertised_for_factory(id);
            FactoryInfo {
                id: decl.id,
                description: (!decl.description.is_empty()).then_some(decl.description),
                sdk_lang: decl.sdk_lang as u8,
                run: decl.run,
                each: decl.each,
                on_failure: decl.on_failure,
                api: decl.api,
                graph: decl.graph,
            }
        })
        .collect();

    // Loaded once per process (cached); the controller binds relays to it.
    let stable_id = crate::persistent_id::stable_id(&namespace, &factory_ids);

    let control_init_message = ControlInitMessage {
        namespace,
        factory_ids,
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
mod tests {
    use std::io::{Read, Write};

    use crossbeam::channel::unbounded;

    use super::*;
    use crate::test_support::stream_pair;

    #[test]
    fn classify_reconnect_reason_maps_kinds_to_canonical_strings() {
        let acked = true;
        let pre_ack = false;

        let timeout = io::Error::new(io::ErrorKind::TimedOut, "");
        assert_eq!(
            classify_reconnect_reason(&timeout, acked),
            "keepalive_timeout"
        );
        assert_eq!(
            classify_reconnect_reason(&timeout, pre_ack),
            "control_stream_broken"
        );

        let would_block = io::Error::new(io::ErrorKind::WouldBlock, "");
        assert_eq!(
            classify_reconnect_reason(&would_block, acked),
            "keepalive_timeout"
        );

        let eof = io::Error::new(io::ErrorKind::UnexpectedEof, "");
        assert_eq!(
            classify_reconnect_reason(&eof, acked),
            "control_stream_broken"
        );

        let pipe = io::Error::new(io::ErrorKind::BrokenPipe, "");
        assert_eq!(
            classify_reconnect_reason(&pipe, acked),
            "control_stream_broken"
        );

        let reset = io::Error::new(io::ErrorKind::ConnectionReset, "");
        assert_eq!(
            classify_reconnect_reason(&reset, acked),
            "control_stream_broken"
        );

        let other = io::Error::other("x");
        assert_eq!(classify_reconnect_reason(&other, acked), "read_error");
    }

    // The liveness env knobs are process-global; tests that set them
    // serialize on the crate-wide poison-tolerant lock so a failure in one
    // never cascades into the others (and so they also serialize against
    // the `fast` tests that touch FLEZHA_RUNNER_ID_PATH).
    use crate::runner::env_test_guard;

    #[test]
    fn the_ack_window_warning_is_the_timeouts_alone() {
        let timeout = io::Error::new(io::ErrorKind::TimedOut, "");
        let eof = io::Error::new(io::ErrorKind::UnexpectedEof, "");

        // A pre-ack read that timed out really did wait the window out; a
        // controller that took the socket and closed it at once did not.
        assert!(ack_window_elapsed(&timeout, false));
        assert!(!ack_window_elapsed(&eof, false));
        assert!(!ack_window_elapsed(&timeout, true));
    }

    // A controller that accepts the socket but never acknowledges (one
    // mid-shutdown) must not hold the reader: the ack deadline drives a
    // reconnect.
    #[test]
    fn reconnects_when_controller_never_acknowledges() {
        let _guard = env_test_guard();
        std::env::set_var("FLEZHA_CONTROL_ACK_TIMEOUT_MS", "600");
        std::env::set_var("FLEZHA_CONTROL_IDLE_TIMEOUT_MS", "60000");

        let (stream, ctrl_end) = stream_pair();

        // Fake non-servicing controller: drain the init frame, then hold the
        // connection open and stay silent — never acknowledges the runner —
        // until the test lets it go.
        let (release, held) = std::sync::mpsc::channel::<()>();
        let ctrl = thread::spawn(move || {
            let mut ctrl_end = ctrl_end;
            let mut buf = [0u8; 1024];
            let _ = ctrl_end.read(&mut buf);
            let _ = held.recv();
        });

        let (itx, _irx) = unbounded();
        let (otx, orx) = unbounded();

        let _ = sock_conn(
            "test_ns".to_string(),
            Arc::new(RwLock::new(vec!["probe".to_string()])),
            stream,
            Some(Arc::new(Mutex::new(itx))),
            Arc::new(Mutex::new(otx)),
        );

        let reconnected = matches!(
            orx.recv_timeout(Duration::from_secs(2)),
            Ok(RunnerMessage::ControlLost { .. })
        );
        assert!(
            reconnected,
            "SDK must post Reconnect when the controller accepts the socket but never \
             acknowledges within the ack window"
        );

        std::env::remove_var("FLEZHA_CONTROL_ACK_TIMEOUT_MS");
        std::env::remove_var("FLEZHA_CONTROL_IDLE_TIMEOUT_MS");
        drop(release);
        ctrl.join().ok();
    }

    /// A controller-side connect for these tests: drain the init frame, write
    /// `frame`, then close.
    fn controller_answering(ctrl_end: UnixStream, frame: Vec<u8>) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut ctrl_end = ctrl_end;
            let mut buf = [0u8; 1024];
            let _ = ctrl_end.read(&mut buf);
            let _ = ctrl_end.write_all(&frame);
        })
    }

    fn start_answer(frame: Vec<u8>) -> (ClientResponseMessage, Duration, Vec<RunnerMessage>) {
        let (stream, ctrl_end) = stream_pair();
        let ctrl = controller_answering(ctrl_end, frame);
        let (itx, irx) = unbounded();
        let (otx, orx) = unbounded();
        let started = std::time::Instant::now();
        let _ = sock_conn(
            "test_ns".to_string(),
            Arc::new(RwLock::new(vec!["probe".to_string()])),
            stream,
            Some(Arc::new(Mutex::new(itx))),
            Arc::new(Mutex::new(otx)),
        );
        let answer = irx
            .recv_timeout(Duration::from_secs(5))
            .expect("the start is answered");
        let waited = started.elapsed();
        ctrl.join().ok();
        let forwarded = std::iter::from_fn(|| orx.recv_timeout(Duration::from_millis(500)).ok())
            .take_while(|message| !matches!(message, RunnerMessage::ControlLost { .. }))
            .collect();
        (answer, waited, forwarded)
    }

    #[test]
    fn a_refused_connect_answers_the_start_with_the_controllers_error() {
        let refusal = flz_transport::errors::FlezhaError::new(
            "runner.namespace_not_proven",
            Default::default(),
        );
        let frame = payloader::build_without_metadata(
            "control_command",
            &pb::CommandRequest {
                reference_id: String::new(),
                command: Some(Command::ConnectRefused(pb::ConnectRefused {
                    error: Some(pb::Error::from(&refusal)),
                })),
            }
            .encode_to_vec(),
        );

        let (answer, waited, forwarded) = start_answer(frame);

        match answer {
            ClientResponseMessage::ConnectRefused(error) => assert_eq!(error, refusal),
            _ => panic!("a refused connect must not be confirmed as connected"),
        }
        assert!(
            waited < CONNECT_ANSWER_GRACE,
            "the refusal is answered at once"
        );
        assert!(
            forwarded.is_empty(),
            "the refusal is the reader's to answer, not a command to dispatch"
        );
    }

    #[test]
    fn an_answered_connect_confirms_the_start_before_the_grace() {
        let assign = pb::CommandRequest {
            reference_id: "ref-assign".to_string(),
            command: Some(Command::AssignRunnerId(pb::AssignRunnerId {
                runner_id: "r1".to_string(),
            })),
        };
        let frame = payloader::build_without_metadata("control_command", &assign.encode_to_vec());

        let (answer, waited, forwarded) = start_answer(frame);

        assert!(matches!(
            answer,
            ClientResponseMessage::RunnerIdConfirmation
        ));
        assert!(
            waited < CONNECT_ANSWER_GRACE,
            "the first frame confirms at once"
        );
        assert_eq!(forwarded.len(), 1, "the frame itself is dispatched");
    }

    // A controller that wedges after the handshake without a clean close must
    // not hold the reader either: the idle watchdog drives a reconnect.
    #[test]
    fn reconnects_when_controller_wedges_after_handshake() {
        let _guard = env_test_guard();
        std::env::set_var("FLEZHA_CONTROL_ACK_TIMEOUT_MS", "5000");
        std::env::set_var("FLEZHA_CONTROL_IDLE_TIMEOUT_MS", "500");

        let (stream, ctrl_end) = stream_pair();

        // Fake controller: drain the init frame, send ONE control frame (the
        // ack), then go silent — the post-handshake wedge — until the test
        // lets it go.
        let (release, held) = std::sync::mpsc::channel::<()>();
        let ctrl = thread::spawn(move || {
            let mut ctrl_end = ctrl_end;
            let mut buf = [0u8; 1024];
            let _ = ctrl_end.read(&mut buf);
            let ack = payloader::build_without_metadata("control_command", b"\x08\x01");
            let _ = ctrl_end.write_all(&ack);
            let _ = held.recv();
        });

        let (itx, _irx) = unbounded();
        let (otx, orx) = unbounded();

        let _ = sock_conn(
            "test_ns".to_string(),
            Arc::new(RwLock::new(vec!["probe".to_string()])),
            stream,
            Some(Arc::new(Mutex::new(itx))),
            Arc::new(Mutex::new(otx)),
        );

        // First the acked frame is forwarded, then — after the idle window —
        // a Reconnect must arrive.
        let mut saw_command = false;
        let mut reconnected = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            match orx.recv_timeout(Duration::from_millis(200)) {
                Ok(RunnerMessage::HandleCommandAction(..)) => saw_command = true,
                Ok(RunnerMessage::ControlLost { .. }) => {
                    reconnected = true;
                    break;
                }
                _ => {}
            }
        }

        assert!(saw_command, "the acked first frame must be forwarded");
        assert!(
            reconnected,
            "SDK must post Reconnect when the controller wedges after the handshake \
             (idle watchdog past FLEZHA_CONTROL_IDLE_TIMEOUT_MS)"
        );

        std::env::remove_var("FLEZHA_CONTROL_ACK_TIMEOUT_MS");
        std::env::remove_var("FLEZHA_CONTROL_IDLE_TIMEOUT_MS");
        drop(release);
        ctrl.join().ok();
    }

    // The handshake presents the self-owned stable identity on EVERY connect
    // (initial and reconnect alike): the controller resolves a reconnect from
    // the always-present stable id + existing bindings, so the retired
    // `previous_runner_id` field is gone and there is no initial-only gate.
    #[test]
    fn handshake_presents_a_non_empty_stable_id_on_initial_connect() {
        let _guard = env_test_guard();

        let (stream, ctrl_end) = stream_pair();
        let (itx, _irx) = unbounded();
        let (otx, _orx) = unbounded();

        // Some(itx) marks this as an initial connect.
        let _ = sock_conn(
            "test_ns".to_string(),
            Arc::new(RwLock::new(vec!["probe".to_string()])),
            stream,
            Some(Arc::new(Mutex::new(itx))),
            Arc::new(Mutex::new(otx)),
        );

        let ctrl: FlezhaStream = FlezhaStream::unix(ctrl_end);
        let (payload_id, message, _meta) = payloader::extract_payload(&ctrl).unwrap();
        assert_eq!(payload_id, "connect_control_channel");
        let json = String::from_utf8(message).unwrap();
        assert!(
            json.contains("\"stable_id\":\"") && !json.contains("\"stable_id\":\"\""),
            "initial connect must present a non-empty stable_id, got: {json}"
        );
        assert!(
            !json.contains("previous_runner_id"),
            "the retired previous_runner_id field must be gone, got: {json}"
        );
    }

    // fails when: a runner that reconnects after a controller crash stays
    // unregistered forever. The controller reads only the first frame of a
    // pending connection and needs it to be the handshake; a keepalive tick
    // queued during the reconnect backoff is written the moment `sock_conn`
    // returns, so the handshake must already be on the wire by then.
    #[test]
    fn the_handshake_is_on_the_wire_before_the_stream_is_handed_back() {
        let _guard = env_test_guard();

        let (stream, ctrl_end) = stream_pair();
        let (otx, _orx) = unbounded();
        let (_answered, stream) = sock_conn(
            "test_ns".to_string(),
            Arc::new(RwLock::new(vec!["probe".to_string()])),
            stream,
            None,
            Arc::new(Mutex::new(otx)),
        );
        send_keepalive_ping(&stream).unwrap();

        let ctrl: FlezhaStream = FlezhaStream::unix(ctrl_end);
        let (first, _message, _meta) = payloader::extract_payload(&ctrl).unwrap();
        assert_eq!(first, "connect_control_channel");
        let (second, _message, _meta) = payloader::extract_payload(&ctrl).unwrap();
        assert_eq!(second, liveness::CONTROL_PING_PAYLOAD_ID);
    }

    // The namespace credential rides the connect frame, and only when this
    // runner holds one: a deployment authenticating with a certificate, or one
    // whose controller enforces nothing, sends the frame it always sent.
    #[test]
    fn handshake_presents_the_namespace_secret_only_when_one_is_set() {
        let _guard = env_test_guard();

        let init_json = |secret: Option<&str>| {
            match secret {
                Some(value) => std::env::set_var("FLEZHA_NAMESPACE_SECRET", value),
                None => std::env::remove_var("FLEZHA_NAMESPACE_SECRET"),
            }

            let (stream, ctrl_end) = stream_pair();
            let (otx, _orx) = unbounded();
            let _ = sock_conn(
                "test_ns".to_string(),
                Arc::new(RwLock::new(vec!["probe".to_string()])),
                stream,
                None,
                Arc::new(Mutex::new(otx)),
            );
            let ctrl: FlezhaStream = FlezhaStream::unix(ctrl_end);
            let (_id, message, _meta) = payloader::extract_payload(&ctrl).unwrap();
            String::from_utf8(message).unwrap()
        };

        let with_secret = init_json(Some("flzt_abc"));
        assert!(
            with_secret.contains("\"namespace_secret\":\"flzt_abc\""),
            "a runner holding a secret must present it, got: {with_secret}"
        );

        let without = init_json(None);
        assert!(
            !without.contains("namespace_secret"),
            "a runner holding none must not carry the field, got: {without}"
        );
    }

    // A reconnect (itx == None) must STILL present the stable id — identity is
    // re-presented every connect so the controller can swap the binding.
    #[test]
    fn handshake_presents_a_non_empty_stable_id_on_reconnect_too() {
        let _guard = env_test_guard();

        let (stream, ctrl_end) = stream_pair();
        let (otx, _orx) = unbounded();

        // None marks this as a reconnect.
        let _ = sock_conn(
            "test_ns".to_string(),
            Arc::new(RwLock::new(vec!["probe".to_string()])),
            stream,
            None,
            Arc::new(Mutex::new(otx)),
        );

        let ctrl: FlezhaStream = FlezhaStream::unix(ctrl_end);
        let (_payload_id, message, _meta) = payloader::extract_payload(&ctrl).unwrap();
        let json = String::from_utf8(message).unwrap();
        assert!(
            json.contains("\"stable_id\":\"") && !json.contains("\"stable_id\":\"\""),
            "reconnect must also present a non-empty stable_id, got: {json}"
        );
    }

    // `send_keepalive_ping` (the keepalive timer's write) must put a bare
    // `Ping` on the dedicated control_ping payload, so the controller recognizes
    // it without entering command dispatch.
    #[test]
    fn keepalive_ping_is_a_bare_ping_on_control_ping_payload() {
        let (stream, ctrl_end) = stream_pair();
        send_keepalive_ping(&stream).unwrap();

        let ctrl: FlezhaStream = FlezhaStream::unix(ctrl_end);
        let (payload_id, message, _meta) = payloader::extract_payload(&ctrl).unwrap();
        assert_eq!(payload_id, liveness::CONTROL_PING_PAYLOAD_ID);
        let ping = Ping::decode(message.as_slice()).expect("ping decodes");
        assert!(ping.nonce.starts_with("ka-"));
    }

    #[test]
    fn the_diagnosis_is_produced_once_however_many_backoffs_follow() {
        let produced = std::cell::Cell::new(0u32);
        let produce = || {
            produced.set(produced.get() + 1);
            "why the connect failed".to_string()
        };

        let mut gate = OnceOnly::pending();
        assert_eq!(
            gate.first(produce).as_deref(),
            Some("why the connect failed")
        );
        for _ in 0..5 {
            assert_eq!(gate.first(produce), None);
        }
        assert_eq!(
            produced.get(),
            1,
            "the probe itself must not run again either"
        );

        // A later reconnect is a new retry loop, and diagnoses again.
        assert!(OnceOnly::pending().first(produce).is_some());
        assert_eq!(produced.get(), 2);
    }
}
