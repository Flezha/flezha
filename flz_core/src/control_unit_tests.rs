use std::io::{Read, Write};
use std::sync::mpsc;

use crossbeam::channel::unbounded;

use super::*;
use crate::test_support::stream_pair;

/// `sock_conn` as a runner of `test_ns` declaring one channel; an `itx`
/// marks an initial connect, `None` a reconnect.
fn connect(
    stream: Arc<FlezhaStream>,
    itx: Option<Sender<ClientResponseMessage>>,
    otx: Sender<RunnerMessage>,
) -> (ConnectAnswered, Arc<FlezhaStream>) {
    sock_conn(
        "test_ns".to_string(),
        Arc::new(RwLock::new(vec!["probe".to_string()])),
        stream,
        itx.map(|itx| Arc::new(Mutex::new(itx))),
        Arc::new(Mutex::new(otx)),
    )
}

/// A controller-side connect for these tests: drain the init frame, write
/// `reply`, then hold the connection open until `release`'s sender drops.
fn fake_controller(
    ctrl_end: UnixStream,
    reply: Vec<u8>,
    release: mpsc::Receiver<()>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut ctrl_end = ctrl_end;
        let mut buf = [0u8; 1024];
        let _ = ctrl_end.read(&mut buf);
        let _ = ctrl_end.write_all(&reply);
        let _ = release.recv();
    })
}

/// Connect over a fresh pair, as an initial connect when `initial`, and
/// return the first frame the controller reads: its payload id and its JSON.
fn handshake(initial: bool) -> (String, String) {
    let (stream, ctrl_end) = stream_pair();
    let (itx, _irx) = unbounded();
    let (otx, _orx) = unbounded();
    let _ = connect(stream, initial.then_some(itx), otx);
    let ctrl = FlezhaStream::unix(ctrl_end);
    let (payload_id, message, _meta) = payloader::extract_payload(&ctrl).unwrap();
    (payload_id, String::from_utf8(message).unwrap())
}

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

    // Fake non-servicing controller: never acknowledges the runner, until
    // the test lets it go.
    let (release, held) = mpsc::channel();
    let ctrl = fake_controller(ctrl_end, Vec::new(), held);

    let (itx, _irx) = unbounded();
    let (otx, orx) = unbounded();
    let _ = connect(stream, Some(itx), otx);

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

fn start_answer(frame: Vec<u8>) -> (ClientResponseMessage, Duration, Vec<RunnerMessage>) {
    let (stream, ctrl_end) = stream_pair();
    // A sender dropped at once: the controller closes right after `frame`.
    let ctrl = fake_controller(ctrl_end, frame, mpsc::channel().1);
    let (itx, irx) = unbounded();
    let (otx, orx) = unbounded();
    let started = std::time::Instant::now();
    let _ = connect(stream, Some(itx), otx);
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
    let refusal =
        flz_transport::errors::FlezhaError::new("runner.namespace_not_proven", Default::default());
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

    // Fake controller: send ONE control frame (the ack), then go silent —
    // the post-handshake wedge — until the test lets it go.
    let (release, held) = mpsc::channel();
    let ack = payloader::build_without_metadata("control_command", b"\x08\x01");
    let ctrl = fake_controller(ctrl_end, ack, held);

    let (itx, _irx) = unbounded();
    let (otx, orx) = unbounded();
    let _ = connect(stream, Some(itx), otx);

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

    let (payload_id, json) = handshake(true);
    assert_eq!(payload_id, "connect_control_channel");
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
    let (_answered, stream) = connect(stream, None, otx);
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
        handshake(false).1
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

// The connect frame and `UpdateFactories` announce one struct in one form.
#[test]
fn the_handshake_announces_each_factory_as_its_factory_decl() {
    let _guard = env_test_guard();

    let (_payload_id, json) = handshake(false);
    let frame: serde_json::Value = serde_json::from_str(&json).unwrap();
    let decl = crate::relay::registry::advertised_for_factory("probe");
    assert_eq!(frame["factories"], serde_json::json!([decl]));
}

// A reconnect (itx == None) must STILL present the stable id — identity is
// re-presented every connect so the controller can swap the binding.
#[test]
fn handshake_presents_a_non_empty_stable_id_on_reconnect_too() {
    let _guard = env_test_guard();

    let (_payload_id, json) = handshake(false);
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
