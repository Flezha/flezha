//! Client-side transport entry points for the standalone Elixir SDK deployment.
//!
//! These wrap `flz_transport`'s client (`UnixStream`, `QuicClient`) and payloader
//! and return the SAME `StreamHandle` resource the server drivers produce, so
//! every per-stream NIF op — and the `Flezha.Transport.Stream` dispatch seam —
//! covers a client handle with no new Elixir code. Each client stream runs a
//! blocking reader thread (decoding frames with the shared payloader and
//! delivering the standard `{:flz_data, ...}` / `{:flz_closed, ...}` tuples) and
//! a writer thread draining the handle's command channel. No new protocol logic
//! lives here; framing, QUIC and TLS stay in Rust where they are already tested.

use std::io::Write;
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam::channel::{unbounded, Receiver};
use flz_transport::service::payloader::{build_from_binary_into, extract_payload};
use flz_transport::transport::config::{self as transport_config, QuicEntry, TransportType};
use flz_transport::transport::protocol::{STREAM_TYPE_PRIMARY, STREAM_TYPE_SECONDARY};
use flz_transport::transport::quic::QuicClient;
use flz_transport::transport::send_gate::SendGate;
use flz_transport::transport::FlezhaStream;
use flz_transport::utility::socket_dir::{self, Role};
use flz_transport::utility::socket_probe;
use rustler::{Encoder, Env, LocalPid, NifResult, ResourceArc, Term};

use crate::atoms;
use crate::driver::{CloseReason, ServerCommand};
use crate::driver_msg::{send_closed_msg, send_data_msg};
use crate::frame_buffer::ParsedFrame;
use crate::resources::{StreamHandle, StreamHandleArc, StreamWaker};

// Client stream ids share nothing with server ids — a client owner never mixes
// the two, so a private monotonic counter is enough.
static CLIENT_STREAM_ID: AtomicU64 = AtomicU64::new(1);

/// A QUIC client connection, held by Elixir so it can open data streams on it.
pub struct QuicClientConn {
    pub client: QuicClient,
}

pub type QuicClientConnArc = ResourceArc<QuicClientConn>;

/// Connect a UDS client stream with the same bounded retry the Rust client uses
/// (exponential from 10 ms, cap 500 ms, ~3 s deadline — the controller binds a
/// data socket immediately before `ConnectSocket`, and bind+connect races).
#[rustler::nif(schedule = "DirtyIo")]
fn client_connect_uds<'a>(
    env: Env<'a>,
    socket_path: String,
    owner_pid: LocalPid,
) -> NifResult<Term<'a>> {
    match connect_uds_with_retry(&socket_path) {
        Ok(stream) => {
            // The directory that answered is where this runner's later sockets
            // are opened, and where a reconnect returns to — the same slot the
            // Rust client records.
            socket_dir::note_connected(&socket_path);

            let handle = spawn_client_stream(FlezhaStream::Unix(stream), owner_pid);
            Ok((atoms::ok(), handle).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Where this process's controller is: the resolved absolute socket directory
/// for `role` ("controller" or "runner") and the QUIC candidate list, both from
/// `flz_transport`. The Elixir SDK and the controller ask for the answer here
/// rather than deriving it, so the ladder exists once for every SDK.
///
/// `socket_dir` and `quic_addresses` carry the CALLER's reading of the two
/// variables: the BEAM keeps its own environment table, so a value set with
/// `System.put_env/2` is invisible to `std::env::var` here. Passing them keeps
/// the rule in one place while letting the caller's environment win.
#[rustler::nif(schedule = "DirtyIo")]
fn resolve_connect<'a>(
    env: Env<'a>,
    role: String,
    socket_dir: Option<String>,
    quic_addresses: Option<String>,
) -> NifResult<Term<'a>> {
    let role = match role.as_str() {
        "controller" => Role::Controller,
        "runner" => Role::Runner,
        other => {
            return Ok((
                atoms::error(),
                format!(
                    "unknown role {:?}; expected \"controller\" or \"runner\"",
                    other
                ),
            )
                .encode(env))
        }
    };

    let resolved_dir = match socket_dir::resolve_socket_dir(role, socket_dir.as_deref()) {
        Ok(resolved) => resolved.path.display().to_string(),
        Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
    };

    let entries = match transport_config::quic_addresses_with(quic_addresses.as_deref()) {
        Ok(entries) => entries.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
        Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
    };

    Ok((atoms::ok(), resolved_dir, entries).encode(env))
}

/// A runner's data-socket path in `socket_dir` — `flz_transport`'s own
/// composition, the one every other SDK reaches through `flz_core`. The
/// controller and a runner ask for it here rather than spelling it, so one
/// runner cannot end up with two names; the runner-id field is bounded there
/// and a path a Unix domain socket address cannot hold is refused by name and
/// length.
#[rustler::nif]
fn stream_socket_path(
    socket_dir: String,
    runner_id: String,
    stream_type: String,
) -> Result<String, String> {
    socket_dir::stream_socket_path_in(Path::new(&socket_dir), &runner_id, &stream_type)
        .map_err(|refusal| refusal.to_string())
}

/// The control-socket path in `socket_dir` — the composer beside
/// `stream_socket_path/3`, so the controller that listens and the Elixir SDK
/// runner that dials read one file name from `flz_transport` rather than each
/// spelling it.
#[rustler::nif]
fn control_socket_path_in(socket_dir: String) -> Result<String, String> {
    socket_dir::control_socket_path_in(Path::new(&socket_dir))
        .map_err(|refusal| refusal.to_string())
}

/// The runner-id ceiling a data-socket name applies. Read by the controller so
/// the id it MINTS fits the socket it will open, rather than being shortened
/// into a name the controller never reports.
#[rustler::nif]
fn stream_socket_id_max() -> usize {
    socket_dir::stream_socket_id_max()
}

/// Why this node cannot reach a controller over `transport`, in the words the
/// Rust runner and `flz runner check` use. `socket_dir` and `quic_addresses`
/// carry the caller's environment view, as in `resolve_connect/3`.
#[rustler::nif(schedule = "DirtyIo")]
fn connect_diagnosis(
    transport: String,
    socket_dir: Option<String>,
    quic_addresses: Option<String>,
) -> String {
    let transport = match transport.as_str() {
        "quic" => TransportType::Quic,
        _ => TransportType::Uds,
    };

    socket_probe::connect_diagnosis(transport, socket_dir.as_deref(), quic_addresses.as_deref())
}

/// Open a QUIC client connection, verifying the server against `ca_cert_path`.
/// The entry is resolved here, on this attempt — never parsed once and cached.
#[rustler::nif(schedule = "DirtyIo")]
fn client_connect_quic<'a>(
    env: Env<'a>,
    address: String,
    ca_cert_path: String,
) -> NifResult<Term<'a>> {
    let entry = match QuicEntry::parse(&address) {
        Ok(entry) => entry,
        Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
    };
    let (addrs, mut failures) = transport_config::resolve_entries(&[entry]);
    if addrs.is_empty() {
        return Ok((atoms::error(), failures.join("; ")).encode(env));
    }

    for addr in addrs {
        match QuicClient::connect(addr, &ca_cert_path) {
            Ok(client) => {
                let conn = ResourceArc::new(QuicClientConn { client });
                return Ok((atoms::ok(), conn).encode(env));
            }
            Err(e) => failures.push(format!("{}: {}", addr, e)),
        }
    }

    Ok((atoms::error(), failures.join("; ")).encode(env))
}

/// Open a bidirectional QUIC stream and write the 2-byte `[type, index]` preamble
/// (types 1/2); type 0 is the control stream and writes no preamble.
#[rustler::nif(schedule = "DirtyIo")]
fn client_open_stream<'a>(
    env: Env<'a>,
    conn: QuicClientConnArc,
    stream_type: u8,
    index: u8,
    owner_pid: LocalPid,
) -> NifResult<Term<'a>> {
    match conn.client.open_bidi_stream() {
        Ok(mut bidi) => {
            if stream_type == STREAM_TYPE_PRIMARY || stream_type == STREAM_TYPE_SECONDARY {
                if let Err(e) = bidi.write_all(&[stream_type, index]) {
                    return Ok((atoms::error(), e.to_string()).encode(env));
                }
                let _ = bidi.flush();
            }

            let handle = spawn_client_stream(FlezhaStream::Quic(bidi), owner_pid);
            Ok((atoms::ok(), handle).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

fn connect_uds_with_retry(path: &str) -> std::io::Result<UnixStream> {
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut attempt: u32 = 0;

    loop {
        match UnixStream::connect(path) {
            Ok(stream) => return Ok(stream),
            Err(e) => {
                if Instant::now() >= deadline {
                    return Err(e);
                }

                let backoff = std::cmp::min(10u64 * (1u64 << attempt.min(6)), 500);
                thread::sleep(Duration::from_millis(backoff));
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

// Spawn the reader + writer threads for a connected stream and return a handle
// indistinguishable from a server-side one.
fn spawn_client_stream(stream: FlezhaStream, owner_pid: LocalPid) -> StreamHandleArc {
    let stream_id = CLIENT_STREAM_ID.fetch_add(1, Ordering::Relaxed);
    let owner = Arc::new(Mutex::new(owner_pid));
    let (cmd_tx, cmd_rx) = unbounded::<ServerCommand>();

    // A client stream's writer thread drains the command channel itself and
    // holds no send buffer of the drivers' kind, so there is no depth to
    // signal about: the handle carries a gate that cannot raise, so that every
    // per-stream NIF op is the same op on both sides. It still discharges each
    // frame's reservation as the writer takes it, because `send_frame` counts
    // one on every stream and a ledger only one side keeps is not a ledger.
    let gate = Arc::new(SendGate::ungated());

    let read_stream = stream
        .clone_handle()
        .expect("clone client stream for reader");
    spawn_reader(read_stream, stream_id, Arc::clone(&owner));
    spawn_writer(stream, cmd_rx, Arc::clone(&gate));

    ResourceArc::new(StreamHandle {
        stream_id,
        cmd_tx,
        waker: StreamWaker::Channel,
        owner,
        gate,
    })
}

/// Reader: decode frames and deliver to the CURRENT owner (shared via `owner`,
/// updated by `controlling_process/2`), the same contract the server drivers
/// ship.
fn spawn_reader(mut read_stream: FlezhaStream, stream_id: u64, owner: Arc<Mutex<LocalPid>>) {
    thread::spawn(move || loop {
        match extract_payload(&mut read_stream) {
            Ok((payload_id, message, metadata)) => {
                let frame = ParsedFrame {
                    payload_id,
                    message,
                    metadata,
                };

                let who = *owner.lock().unwrap();
                send_data_msg(who, stream_id, frame);
            }

            Err(e) => {
                let reason = CloseReason::from(e.kind());

                let who = *owner.lock().unwrap();
                send_closed_msg(who, stream_id, &reason);
                break;
            }
        }
    });
}

/// Writer: drain the command channel. Client streams are active from birth and
/// route by the shared `owner`, so SetActive/SetPassive/UpdateOwner are inert.
fn spawn_writer(
    mut write_stream: FlezhaStream,
    cmd_rx: Receiver<ServerCommand>,
    gate: Arc<SendGate>,
) {
    thread::spawn(move || {
        let mut buf = Vec::new();

        while let Ok(cmd) = cmd_rx.recv() {
            match cmd {
                ServerCommand::SendFrame {
                    payload_id,
                    message,
                    metadata,
                    ..
                } => {
                    gate.release(message.len());
                    buf.clear();
                    build_from_binary_into(&mut buf, &payload_id, &message, &metadata);
                    if write_stream.write_all(&buf).is_err() {
                        break;
                    }
                }

                ServerCommand::CloseStream { .. } => {
                    let _ = write_stream.shutdown(Shutdown::Both);
                    break;
                }

                _ => {}
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use std::os::unix::net::UnixListener;

    use super::*;

    #[test]
    fn connect_retry_succeeds_against_a_live_listener() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("flz-client-test-{}.sock", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let _listener = UnixListener::bind(&path).unwrap();

        let stream = connect_uds_with_retry(path.to_str().unwrap());
        assert!(stream.is_ok());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn connect_retry_gives_up_after_the_deadline_on_a_missing_socket() {
        let path = "/nonexistent/dir/definitely/not/a/socket.sock";
        let start = Instant::now();
        let result = connect_uds_with_retry(path);
        assert!(result.is_err());
        // The ~3 s deadline bounds the wait (allow slack for slow CI).
        assert!(start.elapsed() <= Duration::from_secs(5));
    }

    #[test]
    fn stream_type_preamble_constants_match_the_protocol() {
        assert_eq!(STREAM_TYPE_PRIMARY, 1);
        assert_eq!(STREAM_TYPE_SECONDARY, 2);
    }
}
