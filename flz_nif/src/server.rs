use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread;

use crossbeam::channel::{self, Receiver, Sender};
use flz_transport::transport::tls;
use rustler::{Encoder, Env, LocalPid, NifResult, ResourceArc, Term};

use crate::atoms;
use crate::driver::{quic, uds, DriverReady, ServerCommand};
use crate::driver_stats::{DriverStats, DriverStatsOut, StreamStatsOut};
use crate::resources::{TransportServer, TransportServerArc};

/// What a driver thread is handed: the commands the NIF queues for it, a sender
/// of its own so an accepted stream can queue one, the readiness the caller is
/// waiting on, and the self-report it publishes into.
type Wiring = (
    Receiver<ServerCommand>,
    Sender<ServerCommand>,
    Sender<std::io::Result<DriverReady>>,
    Arc<DriverStats>,
);

/// Start one server transport and wait for its driver to report ready.
///
/// Every transport is started this way: the command channel, the driver thread,
/// the readiness handshake and the handle Elixir holds are the same whichever
/// wire runs underneath, so a transport supplies only its thread name and what
/// it does with the wiring it is handed.
fn start<'a, F>(env: Env<'a>, thread_name: &str, run: F) -> NifResult<Term<'a>>
where
    F: FnOnce(Wiring) + Send + 'static,
{
    let (cmd_tx, cmd_rx) = channel::unbounded::<ServerCommand>();
    let (ready_tx, ready_rx) = channel::bounded(1);
    let stats = Arc::new(DriverStats::new());

    let wiring = (cmd_rx, cmd_tx.clone(), ready_tx, Arc::clone(&stats));
    let driver_handle = thread::Builder::new()
        .name(thread_name.into())
        .spawn(move || run(wiring))
        .map_err(|e| rustler::Error::Term(Box::new(format!("spawn error: {}", e))))?;

    // A driver that could not bind never answers, and dropping its sender is
    // how the caller hears about it.
    let ready = ready_rx
        .recv()
        .map_err(|_| rustler::Error::Term(Box::new("driver init failed".to_string())))?
        .map_err(|e| rustler::Error::Term(Box::new(format!("driver init: {}", e))))?;

    let server = ResourceArc::new(TransportServer {
        cmd_tx,
        waker: ready.waker,
        driver_handle: Mutex::new(Some(driver_handle)),
        stats,
    });

    Ok((atoms::ok(), server).encode(env))
}

/// Start a UDS server listening on `socket_path`.
/// Returns `{:ok, server_ref}` or `{:error, reason}`.
///
/// The acceptor_pid receives `{:flz_connection, stream_ref}` for each new connection.
#[rustler::nif]
fn start_uds_server(env: Env, socket_path: String, acceptor_pid: LocalPid) -> NifResult<Term> {
    let path = PathBuf::from(&socket_path);
    start(
        env,
        "flz-uds-driver",
        move |(cmd_rx, cmd_tx, ready_tx, stats)| {
            uds::run(path, acceptor_pid, cmd_rx, cmd_tx, ready_tx, stats)
        },
    )
}

/// Start a QUIC server listening on `bind_address`.
/// Returns `{:ok, server_ref}` or `{:error, reason}`.
///
/// A non-empty `client_ca_path` requires every peer to present a client
/// certificate chaining to that trust anchor; `""` disables client auth
/// (the explicit opt-out the caller warns about).
///
/// The acceptor_pid receives `{:flz_connection, stream_ref}` for each new QUIC stream.
#[rustler::nif]
fn start_quic_server(
    env: Env,
    bind_address: String,
    cert_path: String,
    key_path: String,
    client_ca_path: String,
    acceptor_pid: LocalPid,
) -> NifResult<Term> {
    let addr: std::net::SocketAddr = bind_address
        .parse()
        .map_err(|e| rustler::Error::Term(Box::new(format!("invalid address: {}", e))))?;

    let server_config = tls::build_quic_server_config(&cert_path, &key_path, &client_ca_path)
        .map_err(|e| rustler::Error::Term(Box::new(format!("TLS config error: {}", e))))?;

    start(
        env,
        "flz-quic-driver",
        move |(cmd_rx, cmd_tx, ready_tx, stats)| {
            quic::run(
                addr,
                server_config,
                acceptor_pid,
                cmd_rx,
                cmd_tx,
                ready_tx,
                stats,
            )
        },
    )
}

/// Stop a running transport server.
#[rustler::nif]
fn stop_server(env: Env, server: TransportServerArc) -> NifResult<Term> {
    let _ = server.cmd_tx.send(ServerCommand::Shutdown);
    let _ = server.waker.wake();
    Ok(atoms::ok().encode(env))
}

/// Self-reported driver liveness for this server — the same map whichever
/// server driver produced it. Read by the `:flezha_transport` observability
/// source ~1 Hz — never on a data path.
#[rustler::nif]
fn driver_stats(env: Env, server: TransportServerArc) -> NifResult<Term> {
    let stats = &server.stats;

    let streams = match stats.streams.lock() {
        Ok(guard) => guard
            .iter()
            .map(|(&stream_id, s)| StreamStatsOut {
                stream_id,
                write_buffer_len: s.write_buffer_len,
                write_buffer_dropped: s.write_buffer_dropped,
                frames_written: s.frames_written,
                frames_read: s.frames_read,
                last_write_ms: s.last_write_ms,
                last_read_ms: s.last_read_ms,
            })
            .collect(),
        Err(_) => Vec::new(),
    };

    let out = DriverStatsOut {
        now_ms: stats.now_ms(),
        loop_iterations: stats.loop_iterations.load(Ordering::Relaxed),
        last_loop_ms: stats.last_loop_ms.load(Ordering::Relaxed),
        cmd_queue_depth: stats.cmd_queue_depth.load(Ordering::Relaxed),
        streams,
    };

    Ok(out.encode(env))
}
