use std::path::PathBuf;
use std::sync::Mutex;
use std::thread;

use crossbeam::channel;
use rustler::{Encoder, Env, LocalPid, NifResult, ResourceArc, Term};

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

    let driver_handle = thread::Builder::new()
        .name("flz-uds-driver".into())
        .spawn(move || {
            uds_driver::run_uds_driver(path, acceptor_pid, cmd_rx, cmd_tx_clone, ready_tx);
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
    });

    Ok((atoms::ok(), server).encode(env))
}

/// Start a QUIC server listening on `bind_address`.
/// Returns `{:ok, server_ref}` or `{:error, reason}`.
///
/// The acceptor_pid receives `{:flz_connection, stream_ref}` for each new QUIC stream.
#[rustler::nif]
fn start_quic_server(
    env: Env,
    bind_address: String,
    cert_path: String,
    key_path: String,
    acceptor_pid: LocalPid,
) -> NifResult<Term> {
    let addr: std::net::SocketAddr = bind_address
        .parse()
        .map_err(|e| rustler::Error::Term(Box::new(format!("invalid address: {}", e))))?;

    let server_config = quic_driver::build_quic_server_config(&cert_path, &key_path)
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
