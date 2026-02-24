use std::path::PathBuf;
use std::sync::Mutex;
use std::thread;

use crossbeam::channel;
use rustler::ResourceArc;
use rustler::{Encoder, Env, LocalPid, NifResult, Term};

use crate::atoms;
use crate::resources::{TransportServer, TransportServerArc};
use crate::uds_driver::{self, ServerCommand};

/// Start a UDS server listening on `socket_path`.
/// Returns `{:ok, server_ref}` or `{:error, reason}`.
///
/// The acceptor_pid receives `{:flz_connection, stream_ref}` for each new connection.
#[rustler::nif]
fn start_uds_server(
    env: Env,
    socket_path: String,
    acceptor_pid: LocalPid,
) -> NifResult<Term> {
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

/// Stop a running transport server.
#[rustler::nif]
fn stop_server(env: Env, server: TransportServerArc) -> NifResult<Term> {
    let _ = server.cmd_tx.send(ServerCommand::Shutdown);
    let _ = server.waker.wake();
    Ok(atoms::ok().encode(env))
}
