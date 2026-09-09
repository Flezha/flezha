use std::io::Write;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};
use std::thread;

use flz_transport::service::payloader;
use flz_transport::transport::config::{get_transport_type, TransportType};
use flz_transport::transport::{protocol, FlezhaStream};
#[cfg(unix)]
use flz_transport::utility::socket_dir;

use super::{CommandAction, Reply};
use crate::flusher::{SendError, SendFrameFn};
use crate::pb::ConnectSocket;
use crate::routing::OperativeDispatchHandle;
use crate::runner::RunnerNode;
use crate::state_sink;

impl CommandAction for ConnectSocket {
    fn handle(&self, runner: &mut RunnerNode, _message: &str, _errors: &mut Vec<String>) -> Reply {
        match get_transport_type() {
            #[cfg(unix)]
            TransportType::Uds => handle_uds(self, runner),
            #[cfg(not(unix))]
            TransportType::Uds => {
                _errors.push("UDS transport is not available on this platform".to_string());
                connected_reply()
            }
            TransportType::Quic => handle_quic(self, runner, _errors),
        }
    }
}

#[cfg(unix)]
fn handle_uds(cmd: &ConnectSocket, runner: &mut RunnerNode) -> Reply {
    let io_streams = runner.get_io_streams();
    let runner_id = runner.get_runner_id();

    // Pair each primary with its secondary explicitly. Controller→SDK reads
    // on primary; SDK→Controller writes on secondary. Putting reads + writes
    // on the same FD hurts throughput ~4× (Java SDK writes on the
    // secondary; matching that here keeps the throughput floor).
    let mut stream_pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)> = Vec::new();

    for key in 0..cmd.sockets_count {
        let primary = connect_unix(&runner_id, "primary".to_string());
        let secondary = connect_unix(&runner_id, "secondary".to_string());

        let secondary_for_store = FlezhaStream::Unix(secondary.try_clone().unwrap());
        io_streams.write().unwrap().insert(key, secondary_for_store);

        let secondary_for_write = Arc::new(FlezhaStream::Unix(secondary));
        stream_pairs.push((FlezhaStream::Unix(primary), secondary_for_write));
    }

    install_state_sink(&stream_pairs);
    spawn_listener_uds(runner, stream_pairs);
    connected_reply()
}

fn handle_quic(cmd: &ConnectSocket, runner: &mut RunnerNode, errors: &mut Vec<String>) -> Reply {
    if let Err(refusal) = connect_quic(cmd, runner) {
        errors.push(refusal);
    }
    connected_reply()
}

/// Bring up the pair set over QUIC, all of it or none of it. A step the
/// runner cannot take fails its connect and says so, rather than ending the
/// coordinator thread and with it the control channel.
///
/// Nothing is installed until every pair is up, which is the whole point: a
/// half-installed set answers the data heartbeat on the secondaries it did
/// open, so `send_data_heartbeat` reports delivered, `note_transport_failure`
/// never fires and nothing reconnects, while the primaries beside them have
/// no reader. The runner reads healthy and loses every input. Leaving the map
/// untouched keeps the failure visible instead.
fn connect_quic(cmd: &ConnectSocket, runner: &mut RunnerNode) -> Result<(), String> {
    let quic_client = runner
        .get_quic_client()
        .ok_or_else(|| "QUIC client not available".to_string())?;

    let mut stream_pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)> = Vec::new();
    let mut fresh: Vec<(u32, FlezhaStream)> = Vec::new();

    for key in 0..cmd.sockets_count {
        let primary = quic_client
            .open_bidi_stream()
            .map_err(|e| format!("primary stream open failed: {e}"))?;
        write_stream_type(&primary, protocol::STREAM_TYPE_PRIMARY, key)
            .map_err(|e| format!("primary stream type write failed: {e}"))?;

        let secondary = quic_client
            .open_bidi_stream()
            .map_err(|e| format!("secondary stream open failed: {e}"))?;
        write_stream_type(&secondary, protocol::STREAM_TYPE_SECONDARY, key)
            .map_err(|e| format!("secondary stream type write failed: {e}"))?;

        fresh.push((key, FlezhaStream::Quic(secondary.clone_handle())));

        let secondary_for_write = Arc::new(FlezhaStream::Quic(secondary));
        stream_pairs.push((FlezhaStream::Quic(primary), secondary_for_write));
    }

    runner.get_io_streams().write().unwrap().extend(fresh);
    install_state_sink(&stream_pairs);
    spawn_listener_quic(runner, stream_pairs);
    Ok(())
}

/// The `ConnectSocket` answer; the errors the caller collected are what say
/// whether the pairs came up.
fn connected_reply() -> Reply {
    Reply {
        payload_id: "connect_sockets",
        bytes: Vec::new(),
    }
}

/// The stream-type byte a fresh QUIC stream opens with, through the class so
/// no writer on this wire chooses its own answer to a refusal.
fn write_stream_type(
    stream: &flz_transport::transport::quic::QuicBidiStream,
    stream_type: u8,
    key: u32,
) -> std::io::Result<()> {
    state_sink::write(
        state_sink::Class::Control,
        Some(&FlezhaStream::Quic(stream.clone_handle())),
        vec![stream_type, key as u8],
    )
    .refused()
}

/// Install the global `state_sink` wrapping the FIRST
/// secondary stream from the freshly-connected pairs. The flusher uses
/// this sink to ship `entity_state_change` and
/// `entity_state_snapshot` frames; one secondary is enough because
/// the WAL is single-writer per entity and the controller serializes the
/// per-Entity writes server-side.
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

#[cfg(unix)]
fn spawn_listener_uds(runner: &RunnerNode, pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>) {
    let handle = runner.operative_dispatch_handle();
    let _ = thread::spawn(move || listen_incoming_uds(pairs, handle));
}

fn spawn_listener_quic(runner: &RunnerNode, pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>) {
    let handle = runner.operative_dispatch_handle();
    let _ = thread::spawn(move || listen_incoming_quic(pairs, handle));
}

fn run_data_reader(
    transport: &'static str,
    key: u32,
    primary: Arc<FlezhaStream>,
    secondary: Arc<FlezhaStream>,
    handle: OperativeDispatchHandle,
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

#[cfg(unix)]
fn listen_incoming_uds(
    pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>,
    handle: OperativeDispatchHandle,
) {
    let mut handles = Vec::new();
    for (key, (primary, secondary)) in pairs.into_iter().enumerate() {
        let handle = handle.clone();
        let primary = Arc::new(primary);
        let h = thread::spawn(move || {
            run_data_reader("uds", key as u32, primary, secondary, handle);
        });
        handles.push(h);
    }
    for h in handles {
        let _ = h.join();
    }
}

fn listen_incoming_quic(
    mut pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>,
    handle: OperativeDispatchHandle,
) {
    let mut handles = Vec::new();
    for (key, (primary, secondary)) in pairs.drain(..).enumerate() {
        let handle = handle.clone();
        let primary = Arc::new(primary);
        let h = thread::spawn(move || {
            run_data_reader("quic", key as u32, primary, secondary, handle);
        });
        handles.push(h);
    }
    for h in handles {
        let _ = h.join();
    }
}

#[cfg(unix)]
fn connect_unix(runner_id: &str, stream_type: String) -> UnixStream {
    // Resolved from the directory the control channel actually reached, so a
    // data socket can never land in a different one.
    let socket_path = socket_dir::stream_socket_path(runner_id, stream_type)
        .unwrap_or_else(|e| panic!("Cannot resolve the data socket directory: {}", e));

    // The controller binds the per-runner data sockets immediately before
    // sending ConnectSocket, but bind+connect can race on busy CI. Retry with
    // bounded backoff (up to ~3s) before giving up; matches the control
    // channel's own open_with_retry pattern in `runner::control_unit`.
    let mut attempt = 0u32;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        match UnixStream::connect(&socket_path) {
            Ok(s) => return s,
            Err(e) => {
                if std::time::Instant::now() >= deadline {
                    panic!(
                        "Failed to connect to the socket {} after {} attempts: {}",
                        socket_path, attempt, e
                    );
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

    use flz_transport::transport::quic::QuicClient;
    use flz_transport::transport::quic_pair::{cert_path, SocketPeer};

    use super::*;

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
        let mut errors: Vec<String> = Vec::new();

        let reply = handle_quic(
            &ConnectSocket { sockets_count: 1 },
            &mut runner,
            &mut errors,
        );

        assert_eq!(reply.payload_id, "connect_sockets");
        assert_eq!(errors.len(), 1, "the refusal is the command's answer");
        assert!(
            errors[0].contains("QUIC client"),
            "the refusal names what was missing: {}",
            errors[0]
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

        let mut errors: Vec<String> = Vec::new();
        let reply = handle_quic(
            &ConnectSocket { sockets_count: 4 },
            &mut runner,
            &mut errors,
        );

        assert_eq!(reply.payload_id, "connect_sockets");
        assert_eq!(errors.len(), 1, "the first refusal ends the connect");
        assert!(
            errors[0].contains("primary"),
            "the refusal names the stream it could not open: {}",
            errors[0]
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

        let mut errors: Vec<String> = Vec::new();
        handle_quic(
            &ConnectSocket { sockets_count: 2 },
            &mut runner,
            &mut errors,
        );

        assert_eq!(errors.len(), 1, "the fourth stream is refused");
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

        let mut errors: Vec<String> = Vec::new();
        let reply = handle_quic(
            &ConnectSocket { sockets_count: 2 },
            &mut runner,
            &mut errors,
        );

        assert_eq!(reply.payload_id, "connect_sockets");
        assert_eq!(errors.len(), 1, "the first refusal ends the connect");
        assert!(
            errors[0].contains("secondary"),
            "the refusal names the stream it could not open: {}",
            errors[0]
        );
    }
}
