use std::io::Write;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};
use std::thread;

use flz_transport::service::payloader;
use flz_transport::transport::config::{get_transport_type, TransportType};
use flz_transport::transport::quic::{QuicBidiStream, QuicClient};
use flz_transport::transport::{protocol, FlezhaStream};
#[cfg(unix)]
use flz_transport::utility::socket_dir;

use super::{CommandAction, Reply};
use crate::flusher::{SendError, SendFrameFn};
use crate::pb::ConnectSocket;
use crate::routing::OperatorDispatchHandle;
use crate::runner::RunnerNode;
use crate::state_sink;

impl CommandAction for ConnectSocket {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        errors: &mut Vec<crate::Error>,
    ) -> Reply {
        let connected = match get_transport_type() {
            #[cfg(unix)]
            TransportType::Uds => connect_uds(self, runner),
            #[cfg(not(unix))]
            TransportType::Uds => Err("UDS transport is not available on this platform".into()),
            TransportType::Quic => connect_quic(self, runner),
        };
        answer(runner, connected, errors)
    }
}

/// The `ConnectSocket` answer, one for both transports. A refusal is the
/// command's error and ends the handshake at the controller; a pair set that
/// came up is noted first, so the control reader takes the controller's next
/// frame as the session carrying on.
fn answer(
    runner: &RunnerNode,
    connected: Result<(), String>,
    errors: &mut Vec<crate::Error>,
) -> Reply {
    match connected {
        Ok(()) => runner.note_connect_answered(),
        Err(reason) => errors.push(crate::Error::Connect(
            crate::Code::DataStreamsFailed.with([("reason", reason.into())]),
        )),
    }
    Reply {
        payload_id: "connect_sockets",
        bytes: Vec::new(),
    }
}

/// One key's pair as a transport opened it: the primary, the secondary, and
/// the secondary's handle the runner's table keeps.
type OpenedPair = (FlezhaStream, FlezhaStream, FlezhaStream);

/// The pair set over UDS. Controller→SDK reads on primary, SDK→Controller
/// writes on secondary: reads and writes on one descriptor cost ~4× in
/// throughput, and the Java SDK pairs them the same way.
#[cfg(unix)]
fn connect_uds(cmd: &ConnectSocket, runner: &mut RunnerNode) -> Result<(), String> {
    let runner_id = runner.get_runner_id();
    connect_pairs(cmd, runner, "uds", |_key| {
        let primary = connect_unix(&runner_id, "primary")?;
        let secondary = connect_unix(&runner_id, "secondary")?;
        let stored = secondary
            .try_clone()
            .map_err(|e| format!("secondary stream clone failed: {e}"))?;
        Ok((
            FlezhaStream::Unix(primary),
            FlezhaStream::Unix(secondary),
            FlezhaStream::Unix(stored),
        ))
    })
}

/// The pair set over QUIC, each stream opening with its type byte.
fn connect_quic(cmd: &ConnectSocket, runner: &mut RunnerNode) -> Result<(), String> {
    let client = runner
        .get_quic_client()
        .ok_or_else(|| "QUIC client not available".to_string())?;
    connect_pairs(cmd, runner, "quic", |key| {
        let primary = open_quic(&client, "primary", protocol::STREAM_TYPE_PRIMARY, key)?;
        let secondary = open_quic(&client, "secondary", protocol::STREAM_TYPE_SECONDARY, key)?;
        let stored = FlezhaStream::Quic(secondary.clone_handle());
        Ok((
            FlezhaStream::Quic(primary),
            FlezhaStream::Quic(secondary),
            stored,
        ))
    })
}

fn open_quic(
    client: &QuicClient,
    half: &str,
    stream_type: u8,
    key: u32,
) -> Result<QuicBidiStream, String> {
    let stream = client
        .open_bidi_stream()
        .map_err(|e| format!("{half} stream open failed: {e}"))?;
    write_stream_type(&stream, stream_type, key)
        .map_err(|e| format!("{half} stream type write failed: {e}"))?;
    Ok(stream)
}

/// Bring up a pair set, all of it or none of it. A step the runner cannot take
/// fails its connect and says so, rather than ending the coordinator thread and
/// with it the control channel.
///
/// Nothing is installed until every pair is up, which is the whole point: a
/// half-installed set answers the data heartbeat on the secondaries it did
/// open, so `send_data_heartbeat` reports delivered, `note_transport_failure`
/// never fires and nothing reconnects, while the primaries beside them have
/// no reader. The runner reads healthy and loses every input. Leaving the map
/// untouched keeps the failure visible instead.
fn connect_pairs(
    cmd: &ConnectSocket,
    runner: &RunnerNode,
    transport: &'static str,
    mut open_pair: impl FnMut(u32) -> Result<OpenedPair, String>,
) -> Result<(), String> {
    let mut stream_pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)> = Vec::new();
    let mut fresh: Vec<(u32, FlezhaStream)> = Vec::new();

    for key in 0..cmd.sockets_count {
        let (primary, secondary, stored) = open_pair(key)?;
        fresh.push((key, stored));
        stream_pairs.push((primary, Arc::new(secondary)));
    }

    runner
        .get_io_streams()
        .write()
        .map_err(|_| "the data stream table is poisoned".to_string())?
        .extend(fresh);
    install_state_sink(&stream_pairs);
    spawn_listener(runner, transport, stream_pairs);
    Ok(())
}

/// The stream-type byte a fresh QUIC stream opens with, through the class so
/// no writer on this wire chooses its own answer to a refusal.
fn write_stream_type(stream: &QuicBidiStream, stream_type: u8, key: u32) -> std::io::Result<()> {
    state_sink::write(
        state_sink::Class::Control,
        Some(&FlezhaStream::Quic(stream.clone_handle())),
        vec![stream_type, key as u8],
    )
    .refused()
}

/// Install the global `state_sink` wrapping the FIRST
/// secondary stream from the freshly-connected pairs. The flusher uses
/// this sink to ship `cell_state_change` and
/// `cell_state_snapshot` frames; one secondary is enough because
/// the WAL is single-writer per cell and the controller serializes the
/// per-Cell writes server-side.
fn install_state_sink(pairs: &[(FlezhaStream, Arc<FlezhaStream>)]) {
    // Credit never carries across sessions: drop the old window (waking any
    // parked sender into the legacy path) until the controller re-offers
    // `CreditWindowInit` for the fresh attach.
    crate::credit::global().disarm();
    if let Some((_, secondary)) = pairs.first() {
        // Mirror the secondary stream: the ring's replay puts an
        // already-framed entry back on it, and a `CloseInterval` and a
        // restored timer ride it when they have no connection of their own.
        state_sink::set_secondary_stream(Some(Arc::clone(secondary)));
        let sink_stream = Arc::clone(secondary);
        // The Mutex serializes writes from the single flusher thread —
        // belt-and-braces given that the flusher has only one writer.
        let writer = Arc::new(Mutex::new(sink_stream));
        let send_fn: SendFrameFn = Arc::new(
            move |payload_id: &str, frame: &[u8]| -> Result<(), SendError> {
                let payload = payloader::build_without_metadata(payload_id, frame);
                let stream = writer.lock().map_err(|e| SendError::Other(e.to_string()))?;
                (&**stream)
                    .write_all(&payload)
                    .map_err(|e| SendError::TransportDown(e.to_string()))
            },
        );
        state_sink::install_global_sink(send_fn);
    }
}

fn spawn_listener(
    runner: &RunnerNode,
    transport: &'static str,
    pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>,
) {
    let handle = runner.operator_dispatch_handle();
    let _ = thread::spawn(move || listen_incoming(transport, pairs, handle));
}

fn run_data_reader(
    transport: &'static str,
    key: u32,
    primary: Arc<FlezhaStream>,
    secondary: Arc<FlezhaStream>,
    handle: OperatorDispatchHandle,
) {
    let mut reader = std::io::BufReader::with_capacity(64 * 1024, &*primary);
    let mut frames: u64 = 0;
    let exit_reason;
    loop {
        match payloader::extract_payload(&mut reader) {
            Ok((instance_id, payload, metadata)) => {
                frames += 1;
                handle.dispatch(&instance_id, payload, &metadata, Arc::clone(&secondary));
            }
            Err(e) => {
                exit_reason = e;
                break;
            }
        }
    }
    tracing::warn!(
        target: "flezha::transport",
        transport, stream_key = key, frames_seen = frames,
        kind = ?exit_reason.kind(),
        "data stream reader exiting: {}",
        exit_reason
    );
}

fn listen_incoming(
    transport: &'static str,
    pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>,
    handle: OperatorDispatchHandle,
) {
    let mut handles = Vec::new();
    for (key, (primary, secondary)) in pairs.into_iter().enumerate() {
        let handle = handle.clone();
        let primary = Arc::new(primary);
        let h = thread::spawn(move || {
            run_data_reader(transport, key as u32, primary, secondary, handle);
        });
        handles.push(h);
    }
    for h in handles {
        let _ = h.join();
    }
}

#[cfg(unix)]
fn connect_unix(runner_id: &str, stream_type: &str) -> Result<UnixStream, String> {
    // Resolved from the directory the control channel actually reached, so a
    // data socket can never land in a different one.
    let socket_path = socket_dir::stream_socket_path(runner_id, stream_type.to_string())
        .map_err(|e| format!("{stream_type} stream socket unresolved: {e}"))?;

    // The controller binds the per-runner data sockets immediately before
    // sending ConnectSocket, but bind+connect can race on busy CI. Retry with
    // bounded backoff (up to ~3s) before giving up; matches the control
    // channel's own open_with_retry pattern in `runner::control_unit`.
    let mut attempt = 0u32;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        match UnixStream::connect(&socket_path) {
            Ok(s) => return Ok(s),
            Err(e) => {
                if std::time::Instant::now() >= deadline {
                    return Err(format!(
                        "{stream_type} stream open failed: {socket_path} after {attempt} attempts: {e}"
                    ));
                }
                attempt += 1;
                let backoff = std::cmp::min(
                    std::time::Duration::from_millis(10 * 2u64.pow(attempt.min(6))),
                    std::time::Duration::from_millis(500),
                );
                std::thread::sleep(backoff);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::time::Duration;

    use flz_transport::transport::liveness::ControlLiveness;
    use flz_transport::transport::quic::QuicClient;
    use flz_transport::transport::quic_pair::{cert_path, SocketPeer};
    use prost::Message;

    use super::*;
    use crate::pb::command_request::Command;
    use crate::pb::{CommandRequest, CommandResponse};
    use crate::ErrorModel;

    /// A runner dialled at `peer`, ready to be handed a `ConnectSocket`.
    fn runner_on(peer: &SocketPeer) -> (RunnerNode, Arc<QuicClient>) {
        let client = Arc::new(
            QuicClient::connect(peer.addr(), &cert_path("ca.pem")).expect("the peer answers"),
        );
        let mut runner = RunnerNode::new("connect_test".to_string());
        runner.set_quic_client(Some(Arc::clone(&client)));
        (runner, client)
    }

    /// The connect is reached on a QUIC runner whose client is not there:
    /// the reconnect that fell back, or a controller gone before the command
    /// landed. The command must refuse; aborting takes the control channel
    /// that would have retried down with it.
    #[test]
    fn a_connect_with_no_quic_client_refuses() {
        let mut runner = RunnerNode::new("connect_test".to_string());

        let refusal = connect_quic(&ConnectSocket { sockets_count: 1 }, &mut runner)
            .expect_err("the refusal is the command's answer");

        assert!(
            refusal.contains("QUIC client"),
            "the refusal names what was missing: {refusal}"
        );
    }

    /// The controller dies between `ConnectSocket` and the stream open. Every
    /// open on the connection now fails, and the runner must live to be told
    /// to try again.
    #[test]
    fn a_connect_whose_peer_vanished_refuses() {
        let mut peer = SocketPeer::listening(128);
        let (mut runner, client) = runner_on(&peer);

        // The driver drops every stream's chunk channel as it exits, so a
        // stream opened before the close reads end-of-stream once the driver
        // is gone. That is the wait, without opening streams to find out.
        let mut probe = client.open_bidi_stream().expect("the connection is live");
        probe
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("arm the probe deadline");
        peer.vanish();
        assert_eq!(
            probe.read(&mut [0u8; 1]).expect("the probe reads"),
            0,
            "the client's driver thread must have exited"
        );

        let refusal = connect_quic(&ConnectSocket { sockets_count: 4 }, &mut runner)
            .expect_err("the first refusal ends the connect");

        assert!(
            refusal.contains("primary"),
            "the refusal names the stream it could not open: {refusal}"
        );
    }

    /// A pair set is all or nothing. A half-installed set is worse than none:
    /// its secondaries take the data heartbeat and answer it, so
    /// `send_data_heartbeat` reports delivered, `note_transport_failure` never
    /// fires and nothing reconnects, while the primaries beside them have no
    /// reader and every input on them is lost.
    #[test]
    fn a_refused_pair_set_installs_no_stream() {
        let peer = SocketPeer::listening(4);
        let (mut runner, client) = runner_on(&peer);

        // The map as the connect finds it. The key is outside the range this
        // attempt writes, so an install is visible rather than an overwrite.
        let previous = client.open_bidi_stream().expect("the connection is live");
        runner
            .get_io_streams()
            .write()
            .unwrap()
            .insert(7, FlezhaStream::Quic(previous));

        connect_quic(&ConnectSocket { sockets_count: 2 }, &mut runner)
            .expect_err("the fourth stream is refused");
        let io_streams = runner.get_io_streams();
        let installed = io_streams.read().unwrap();
        let mut keys: Vec<u32> = installed.keys().copied().collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![7],
            "a refused connect installs no stream and removes none"
        );
    }

    /// A controller that can take some of the pair set and not the rest: the
    /// third stream opens, the fourth is refused. The already-open pair is no
    /// reason to abort, and the refusal names the half that failed.
    #[test]
    fn a_pair_set_refused_partway_refuses_rather_than_aborting() {
        let peer = SocketPeer::listening(3);
        let (mut runner, _client) = runner_on(&peer);

        let refusal = connect_quic(&ConnectSocket { sockets_count: 2 }, &mut runner)
            .expect_err("the first refusal ends the connect");

        assert!(
            refusal.contains("secondary"),
            "the refusal names the stream it could not open: {refusal}"
        );
    }

    /// Only a pair set that came up tells the control reader the connect was
    /// answered; a refusal is the command's error and admits nothing.
    #[test]
    fn only_a_connect_that_came_up_is_noted_as_answered() {
        let mut runner = RunnerNode::new("connect_test".to_string());
        let mut live = ControlLiveness::new();
        runner.set_connect_answered(live.connect_answered());
        let mut errors: Vec<crate::Error> = Vec::new();

        let reply = answer(&runner, Err("refused".to_string()), &mut errors);
        live.note_frame();
        assert_eq!(reply.payload_id, "connect_sockets");
        let codes: Vec<String> = errors.iter().map(ErrorModel::code).collect();
        assert_eq!(codes, ["runner.data_streams_failed"]);
        assert_eq!(errors[0].to_error().details["reason"], "refused");
        assert!(
            !live.handshake_completed(),
            "a refused connect admits nothing"
        );

        answer(&runner, Ok(()), &mut errors);
        live.note_frame();
        assert!(live.handshake_completed());
    }

    /// The default transport's two refusals, driven through the command
    /// dispatch on a thread of its own, the way the coordinator runs it: that
    /// thread owns the control stream, so it must answer and live.
    #[cfg(unix)]
    fn refused_uds_connect(socket_dir: &std::path::Path) -> CommandResponse {
        let _guard = crate::runner::env_test_guard();
        std::env::set_var("FLEZHA_TRANSPORT", "uds");
        std::env::set_var("FLEZHA_SOCKET_FILES_PATH", socket_dir);

        let (stream, controller) = crate::test_support::stream_pair();
        let frame = CommandRequest {
            reference_id: "ref-connect".to_string(),
            command: Some(Command::ConnectSocket(ConnectSocket { sockets_count: 1 })),
        }
        .encode_to_vec();

        let coordinator = thread::spawn(move || {
            let mut runner = RunnerNode::new("connect_test".to_string());
            runner.assign_runner_id("unbound".to_string());
            crate::command::handle_command(&mut runner, &stream, "control_command", &frame);
        });
        let survived = coordinator.join().is_ok();

        std::env::remove_var("FLEZHA_TRANSPORT");
        std::env::remove_var("FLEZHA_SOCKET_FILES_PATH");
        assert!(
            survived,
            "the coordinator thread unwound on a refusable connect"
        );

        let (_, reply, _) = payloader::extract_payload(&controller).expect("an answer was written");
        CommandResponse::decode(&reply[..]).expect("the answer decodes")
    }

    #[cfg(unix)]
    #[test]
    fn a_data_socket_the_controller_has_not_bound_is_refused() {
        let dir = std::env::temp_dir().join(format!("flezha-unbound-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("socket dir");

        let response = refused_uds_connect(&dir);
        let _ = std::fs::remove_dir_all(&dir);

        assert_eq!(response.status, "error");
        assert_eq!(response.errors[0].code, "runner.data_streams_failed");
        assert!(
            response.errors[0]
                .details_json
                .contains("primary_stream_unbound"),
            "the refusal names the socket it could not dial: {:?}",
            response.errors
        );
    }

    #[cfg(unix)]
    #[test]
    fn an_unresolvable_data_socket_directory_is_refused() {
        // Longer than any Unix domain socket address, so nothing could bind a
        // socket in it and nothing is dialled.
        let dir = std::path::PathBuf::from(format!("/tmp/{}", "d".repeat(120)));

        let response = refused_uds_connect(&dir);

        assert_eq!(response.status, "error");
        assert!(
            response.errors[0]
                .details_json
                .contains(&*dir.to_string_lossy()),
            "the refusal names the directory: {:?}",
            response.errors
        );
    }
}
