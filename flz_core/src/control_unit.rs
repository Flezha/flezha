use std::collections::HashMap;
use std::io::{self, Write};
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use crossbeam::channel::Sender;
use flz_transport::service::payloader;
use flz_transport::transport::config::{
    self as transport_config, get_transport_type, TransportType,
};
use flz_transport::transport::liveness::{self, ControlLiveness, ReadAction};
use flz_transport::transport::quic::QuicClient;
use flz_transport::transport::FlezhaStream;
use flz_transport::utility::last_good::LastGood;
use flz_transport::utility::{socket_dir, socket_probe};
use prost::Message;
use serde::Serialize;

use crate::error::Error;
use crate::pb::Ping;
use crate::runner::{ClientResponseMessage, RunnerMessage};
use crate::util::nonce_suffix;

/// The QUIC address this process last connected through.
static LAST_GOOD_QUIC: LastGood<SocketAddr> = LastGood::new();

type ControlConn = (
    thread::JoinHandle<()>,
    Arc<FlezhaStream>,
    Option<Arc<QuicClient>>,
);

#[derive(Serialize)]
struct FactoryInfo {
    id: String,
    description: Option<String>,
    /// The class's declared language, from the registry; `0` for a factory
    /// with no class, which the controller refuses to place.
    sdk_lang: u8,
}

#[derive(Serialize)]
struct ControlInitMessage {
    namespace: String,
    factory_ids: Vec<String>,
    factories: Vec<FactoryInfo>,
    /// The runner's stable identity, on every connect; the controller's
    /// binding key.
    stable_id: String,
}

pub fn open(
    namespace: String,
    declared: Arc<RwLock<Vec<String>>>,
    itx: Arc<Mutex<Sender<ClientResponseMessage>>>,
    otx: Arc<Mutex<Sender<RunnerMessage>>>,
) -> ControlConn {
    open_with_retry(namespace, declared, Some(itx), otx)
}

/// A reconnect sends no startup confirmation.
pub fn open_reconnect(
    namespace: String,
    declared: Arc<RwLock<Vec<String>>>,
    otx: Arc<Mutex<Sender<RunnerMessage>>>,
) -> ControlConn {
    open_with_retry(namespace, declared, None, otx)
}

fn open_with_retry(
    namespace: String,
    declared: Arc<RwLock<Vec<String>>>,
    itx: Option<Arc<Mutex<Sender<ClientResponseMessage>>>>,
    otx: Arc<Mutex<Sender<RunnerMessage>>>,
) -> ControlConn {
    let mut attempt = 0u32;
    let mut diagnosis = OnceOnly::pending();
    let max_backoff = Duration::from_secs(30);

    loop {
        match try_open(
            namespace.clone(),
            Arc::clone(&declared),
            itx.clone(),
            otx.clone(),
        ) {
            Ok(result) => {
                if attempt > 0 {
                    eprintln!("[flezha] Connected after {} attempts", attempt);
                }
                return result;
            }
            Err(e) => {
                attempt += 1;
                // Once, on entry to the retry loop: which of the four
                // situations this is and what fixes it. Repeating it every
                // backoff teaches the operator to skip the line that matters.
                if let Some(text) = diagnosis.first(connect_diagnosis) {
                    eprintln!("[flezha] {}", text);
                }
                // Full jitter on the exponential backoff so a controller
                // restart that drops the whole fleet doesn't produce a
                // synchronized reconnect storm (shared with the Java SDK via
                // the same `liveness::jittered` schedule).
                let backoff = liveness::jittered(std::cmp::min(
                    Duration::from_millis(100 * 2u64.pow(attempt.min(8))),
                    max_backoff,
                ));
                eprintln!(
                    "[flezha] Connection attempt {} failed: {}. Retrying in {:?}...",
                    attempt, e, backoff
                );
                thread::sleep(backoff);
            }
        }
    }
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
            let (handle, stream) = try_open_uds(namespace, declared, itx, otx)?;
            Ok((handle, stream, None))
        }
        #[cfg(not(unix))]
        TransportType::Uds => Err(Error::Connect(
            "UDS transport is not available on this platform; set FLEZHA_TRANSPORT=quic"
                .to_string(),
        )),
        TransportType::Quic => try_open_quic(namespace, declared, itx, otx),
    }
}

#[cfg(unix)]
fn try_open_uds(
    namespace: String,
    declared: Arc<RwLock<Vec<String>>>,
    itx: Option<Arc<Mutex<Sender<ClientResponseMessage>>>>,
    otx: Arc<Mutex<Sender<RunnerMessage>>>,
) -> Result<(thread::JoinHandle<()>, Arc<FlezhaStream>), Error> {
    let socket_path = socket_dir::control_socket_path()?;

    let stream = UnixStream::connect(socket_path.clone()).map_err(|e| {
        Error::Connect(format!(
            "Failed to connect to control socket {}: {}",
            socket_path, e
        ))
    })?;

    // The directory that answered is where this runner's data sockets are
    // opened, and where a reconnect returns to.
    socket_dir::note_connected(&socket_path);

    let stream_box = Arc::new(FlezhaStream::Unix(stream));

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
                let (handle, stream) = sock_conn(namespace, declared, stream_box, itx, otx);

                return Ok((handle, stream, Some(client)));
            }
            // Every failure names the address it was dialled against — a
            // failure that names no address cannot be acted on.
            Err(e) => failures.push(format!("{}: {}", addr, e)),
        }
    }

    Err(Error::Connect(format!(
        "Failed to connect to a QUIC controller — {}",
        failures.join("; ")
    )))
}

/// Every address the list resolves to on THIS attempt, last-good first, plus
/// the entries that failed to resolve.
fn quic_candidates() -> Result<(Vec<SocketAddr>, Vec<String>), Error> {
    let entries = transport_config::quic_addresses()?;
    let (resolved, unresolved) = transport_config::resolve_entries(&entries);
    if resolved.is_empty() {
        return Err(Error::Connect(format!(
            "no {} entry resolved — {}",
            transport_config::QUIC_ADDRESSES_VAR,
            unresolved.join("; ")
        )));
    }
    Ok((LAST_GOOD_QUIC.prefer(resolved), unresolved))
}

/// The diagnosis every SDK and `flz runner check` render.
fn connect_diagnosis() -> String {
    socket_probe::connect_diagnosis(get_transport_type(), None, None)
}

fn sock_conn(
    namespace: String,
    declared: Arc<RwLock<Vec<String>>>,
    stream_box: Arc<FlezhaStream>,
    itx: Option<Arc<Mutex<Sender<ClientResponseMessage>>>>,
    otx: Arc<Mutex<Sender<RunnerMessage>>>,
) -> (thread::JoinHandle<()>, Arc<FlezhaStream>) {
    let stream_clone = Arc::clone(&stream_box);

    let handle = thread::spawn(move || {
        // Shared (Rust + Java) control-channel liveness: an ack-deadline until
        // the controller's first frame, then a steady-state idle watchdog that
        // re-arms on every frame. Catches both the warm-restart "accept but
        // never ack" hang and a controller that wedges after the handshake.
        let mut live = ControlLiveness::new();
        let _ = stream_clone.set_read_timeout(Some(live.current_deadline()));

        // Present the self-owned stable identity on every connect (initial and
        // reconnect): the controller resolves a reconnect from the
        // always-present stable id + the existing bindings (a binding swap), so
        // there is no separate reconnect frame and no `itx`-gated behaviour.
        if let Err(e) = send_init_msg(namespace, &declared, stream_clone.clone()) {
            tracing::warn!(
                target: "flezha::transport",
                kind = ?e.kind(),
                "control channel init send failed (peer vanished mid-handshake); reconnecting: {}",
                e
            );
            let _ = otx
                .lock()
                .unwrap()
                .send(RunnerMessage::Reconnect("write_error"));
            return;
        }

        // Send startup confirmation only on initial connection
        if let Some(itx) = itx {
            itx.lock()
                .unwrap()
                .send(ClientResponseMessage::RunnerIdConfirmation)
                .unwrap();
        }

        read_loop(&stream_clone, &otx, &mut live);
    });

    (handle, stream_box)
}

/// Feed control frames to the coordinator until the channel fails, then ask
/// for a reconnect with the classified reason.
fn read_loop(
    stream: &Arc<FlezhaStream>,
    otx: &Arc<Mutex<Sender<RunnerMessage>>>,
    live: &mut ControlLiveness,
) {
    loop {
        match payloader::extract_payload(&**stream) {
            Ok((payload_id, message, metadata)) => {
                live.note_frame();
                let _ = stream.set_read_timeout(Some(live.current_deadline()));
                otx.lock()
                    .unwrap()
                    .send(RunnerMessage::HandleCommandAction(
                        payload_id, message, metadata,
                    ))
                    .unwrap();
            }
            Err(e) => match live.classify_error(&e) {
                ReadAction::Retry => continue,
                ReadAction::Reconnect => {
                    let reason = classify_reconnect_reason(&e, live.acked());
                    if !live.acked() {
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
                    let _ = otx.lock().unwrap().send(RunnerMessage::Reconnect(reason));
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
    payloader::build_from_binary(
        liveness::CONTROL_PING_PAYLOAD_ID,
        &ping.encode_to_vec(),
        HashMap::new(),
    )
}

pub fn send_keepalive_ping(stream: &FlezhaStream) -> io::Result<()> {
    let frame = build_keepalive_ping();
    (&*stream).write_all(&frame).map_err(|e| {
        tracing::warn!(
            target: "flezha::transport",
            kind = ?e.kind(),
            "control keepalive ping write failed: {}", e
        );
        e
    })
}

/// The keepalive timer only signals the coordinator, so it outlives every
/// reconnect; jittered so a fleet does not ping in lockstep.
pub fn spawn_keepalive(otx: Arc<Mutex<Sender<RunnerMessage>>>) -> thread::JoinHandle<()> {
    thread::spawn(move || loop {
        thread::sleep(liveness::jittered(liveness::control_keepalive_interval()));
        if otx
            .lock()
            .unwrap()
            .send(RunnerMessage::SendControlKeepalive)
            .is_err()
        {
            // Coordinator gone (runner shutting down) — stop the timer.
            break;
        }
    })
}

/// The heartbeat timer, shaped as `spawn_keepalive`.
pub fn spawn_data_heartbeat(otx: Arc<Mutex<Sender<RunnerMessage>>>) -> thread::JoinHandle<()> {
    thread::spawn(move || loop {
        thread::sleep(liveness::jittered(liveness::data_heartbeat_interval()));
        if otx
            .lock()
            .unwrap()
            .send(RunnerMessage::SendDataHeartbeat)
            .is_err()
        {
            // Coordinator gone (runner shutting down) — stop the timer.
            break;
        }
    })
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
        .map(|id| FactoryInfo {
            id: id.clone(),
            description: None,
            sdk_lang: crate::operative::registry::sdk_lang_for_factory(id),
        })
        .collect();

    // Loaded once per process (cached); the controller binds operatives to it.
    let stable_id = crate::persistent_id::stable_id(&namespace, &factory_ids);

    let control_init_message = ControlInitMessage {
        namespace,
        factory_ids,
        factories,
        stable_id,
    };

    let control_message = serde_json::to_string(&control_init_message).unwrap();
    let payload =
        payloader::build_from_str("connect_control_channel", &control_message, HashMap::new());

    // A failed init write means the peer vanished between connect and write
    // (e.g. a controller that died mid-handshake); surface it so the caller
    // drives a reconnect instead of panicking the reader thread.
    (&*stream_clone).write_all(&payload)
}

#[cfg(test)]
mod tests {
    use std::io::Read;

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
            Ok(RunnerMessage::Reconnect(_))
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
            let ack = payloader::build_from_binary("control_command", b"\x08\x01", HashMap::new());
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
                Ok(RunnerMessage::Reconnect(_)) => {
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

        let ctrl: FlezhaStream = FlezhaStream::Unix(ctrl_end);
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

        let ctrl: FlezhaStream = FlezhaStream::Unix(ctrl_end);
        let (_payload_id, message, _meta) = payloader::extract_payload(&ctrl).unwrap();
        let json = String::from_utf8(message).unwrap();
        assert!(
            json.contains("\"stable_id\":\"") && !json.contains("\"stable_id\":\"\""),
            "reconnect must also present a non-empty stable_id, got: {json}"
        );
    }

    // `send_keepalive_ping` (the coordinator's write path) must put a bare
    // `Ping` on the dedicated control_ping payload, so the controller recognizes
    // it without entering command dispatch.
    #[test]
    fn keepalive_ping_is_a_bare_ping_on_control_ping_payload() {
        let (stream, ctrl_end) = stream_pair();
        send_keepalive_ping(&stream).unwrap();

        let ctrl: FlezhaStream = FlezhaStream::Unix(ctrl_end);
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
