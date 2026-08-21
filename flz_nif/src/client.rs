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

use std::io::{ErrorKind, Write};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam::channel::unbounded;
use rustler::{Encoder, Env, LocalPid, NifResult, ResourceArc, Term};

use flz_transport::service::payloader::{build_from_binary_into, extract_payload};
use flz_transport::transport::protocol::{STREAM_TYPE_PRIMARY, STREAM_TYPE_SECONDARY};
use flz_transport::transport::quic::QuicClient;
use flz_transport::transport::FlezhaStream;

use crate::atoms;
use crate::driver_msg::{send_closed_msg, send_data_msg};
use crate::frame_buffer::ParsedFrame;
use crate::resources::{StreamHandle, StreamHandleArc, StreamWaker};
use crate::uds_driver::ServerCommand;

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
            let handle = spawn_client_stream(FlezhaStream::Unix(stream), owner_pid);
            Ok((atoms::ok(), handle).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Open a QUIC client connection, verifying the server against `ca_cert_path`.
#[rustler::nif(schedule = "DirtyIo")]
fn client_connect_quic<'a>(
    env: Env<'a>,
    address: String,
    ca_cert_path: String,
) -> NifResult<Term<'a>> {
    match QuicClient::connect(&address, &ca_cert_path) {
        Ok(client) => {
            let conn = ResourceArc::new(QuicClientConn { client });
            Ok((atoms::ok(), conn).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
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

    // Reader: decode frames and deliver to the CURRENT owner (shared via `owner`,
    // updated by `controlling_process/2`), the same contract the server drivers ship.
    let mut read_stream = stream.try_clone().expect("clone client stream for reader");
    let reader_owner = Arc::clone(&owner);
    thread::spawn(move || loop {
        match extract_payload(&mut read_stream) {
            Ok((payload_id, message, metadata)) => {
                let frame = ParsedFrame {
                    payload_id,
                    message,
                    metadata,
                };

                let who = *reader_owner.lock().unwrap();
                send_data_msg(who, stream_id, frame);
            }

            Err(e) => {
                let reason = match e.kind() {
                    ErrorKind::UnexpectedEof => "connection_closed",
                    ErrorKind::InvalidData => "frame_too_large",
                    _ => "read_error",
                };

                let who = *reader_owner.lock().unwrap();
                send_closed_msg(who, stream_id, reason);
                break;
            }
        }
    });

    // Writer: drain the command channel. Client streams are active from birth and
    // route by the shared `owner`, so SetActive/SetPassive/UpdateOwner are inert.
    let mut write_stream = stream;
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
                    buf.clear();
                    build_from_binary_into(&mut buf, &payload_id, &message, &metadata);
                    if write_stream.write_all(&buf).is_err() {
                        break;
                    }
                }

                ServerCommand::SendData { data, .. } => {
                    if write_stream.write_all(&data).is_err() {
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

    ResourceArc::new(StreamHandle {
        stream_id,
        cmd_tx,
        waker: StreamWaker::Channel,
        owner,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;

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
