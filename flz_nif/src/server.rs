use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread;

use crossbeam::channel;
use flz_transport::transport::tls;
use rustler::{Encoder, Env, LocalPid, NifResult, ResourceArc, Term};

use crate::driver_stats::{DriverStats, DriverStatsOut, StreamStatsOut};
use crate::resources::{TransportServer, TransportServerArc};
use crate::uds_driver::{self, ServerCommand};
use crate::{atoms, quic_driver};

/// Start a UDS server listening on `socket_path`.
/// Returns `{:ok, server_ref}` or `{:error, reason}`.
///
/// The acceptor_pid receives `{:flz_connection, stream_ref}` for each new connection.
#[rustler::nif]
fn start_uds_server(env: Env, socket_path: String, acceptor_pid: LocalPid) -> NifResult<Term> {
    let path = PathBuf::from(&socket_path);
    let (cmd_tx, cmd_rx) = channel::unbounded::<ServerCommand>();
    let (ready_tx, ready_rx) = channel::bounded(1);

    let cmd_tx_clone = cmd_tx.clone();
    let stats = Arc::new(DriverStats::new());
    let stats_clone = Arc::clone(&stats);

    let driver_handle = thread::Builder::new()
        .name("flz-uds-driver".into())
        .spawn(move || {
            uds_driver::run_uds_driver(
                path,
                acceptor_pid,
                cmd_rx,
                cmd_tx_clone,
                ready_tx,
                stats_clone,
            );
        })
        .map_err(|e| rustler::Error::Term(Box::new(format!("spawn error: {}", e))))?;

    // Wait for driver to be ready
    let ready = ready_rx
        .recv()
        .map_err(|_| rustler::Error::Term(Box::new("driver init failed".to_string())))?
        .map_err(|e| rustler::Error::Term(Box::new(format!("driver init: {}", e))))?;

    let server = ResourceArc::new(TransportServer {
        cmd_tx,
        waker: ready.waker,
        driver_handle: Mutex::new(Some(driver_handle)),
        stats: Some(stats),
    });

    Ok((atoms::ok(), server).encode(env))
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

    let (cmd_tx, cmd_rx) = channel::unbounded::<ServerCommand>();
    let (ready_tx, ready_rx) = channel::bounded(1);

    let cmd_tx_clone = cmd_tx.clone();

    let driver_handle = thread::Builder::new()
        .name("flz-quic-driver".into())
        .spawn(move || {
            quic_driver::run_quic_driver(
                addr,
                server_config,
                acceptor_pid,
                cmd_rx,
                cmd_tx_clone,
                ready_tx,
            );
        })
        .map_err(|e| rustler::Error::Term(Box::new(format!("spawn error: {}", e))))?;

    let ready = ready_rx
        .recv()
        .map_err(|_| rustler::Error::Term(Box::new("driver init failed".to_string())))?
        .map_err(|e| rustler::Error::Term(Box::new(format!("driver init: {}", e))))?;

    let server = ResourceArc::new(TransportServer {
        cmd_tx,
        waker: ready.waker,
        driver_handle: Mutex::new(Some(driver_handle)),
        stats: None,
    });

    Ok((atoms::ok(), server).encode(env))
}

/// Stop a running transport server.
#[rustler::nif]
fn stop_server(env: Env, server: TransportServerArc) -> NifResult<Term> {
    let _ = server.cmd_tx.send(ServerCommand::Shutdown);
    let _ = server.waker.wake();
    Ok(atoms::ok().encode(env))
}

/// Self-reported driver liveness for this server. Returns a map for UDS
/// servers, or `:undefined` for QUIC (no per-stream stats yet). Read by the
/// `:flezha_transport` observability source ~1 Hz — never on a data path.
#[rustler::nif]
fn driver_stats(env: Env, server: TransportServerArc) -> NifResult<Term> {
    let stats = match &server.stats {
        Some(s) => s,
        None => return Ok(atoms::undefined().encode(env)),
    };

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
