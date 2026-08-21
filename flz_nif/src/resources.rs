use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crossbeam::channel::Sender;
use mio::Waker;
use rustler::{LocalPid, ResourceArc};

use crate::driver_stats::DriverStats;
use crate::uds_driver::ServerCommand;

/// How a stream nudges its driver after a command is queued. Server streams
/// wake a `mio` poll; client streams' writer threads block on the command
/// channel itself, so no wake is needed.
pub enum StreamWaker {
    Mio(Arc<Waker>),
    Channel,
}

impl StreamWaker {
    pub fn wake(&self) {
        if let StreamWaker::Mio(waker) = self {
            let _ = waker.wake();
        }
    }
}

/// Opaque handle to a running transport server, held by Elixir as a NIF resource.
pub struct TransportServer {
    pub cmd_tx: Sender<ServerCommand>,
    pub waker: Arc<Waker>,
    pub driver_handle: Mutex<Option<JoinHandle<()>>>,
    // Self-reported driver liveness. `None` for the QUIC driver, which does not
    // yet surface per-stream stats (the silent-stall incident was UDS).
    pub stats: Option<Arc<DriverStats>>,
}

impl Drop for TransportServer {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(ServerCommand::Shutdown);
        let _ = self.waker.wake();
        if let Ok(mut handle) = self.driver_handle.lock() {
            if let Some(h) = handle.take() {
                let _ = h.join();
            }
        }
    }
}

/// Opaque handle to a single stream (connection) on the server.
pub struct StreamHandle {
    pub stream_id: u64,
    pub cmd_tx: Sender<ServerCommand>,
    pub waker: StreamWaker,
    pub owner: Arc<Mutex<LocalPid>>,
}

impl Drop for StreamHandle {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(ServerCommand::CloseStream {
            stream_id: self.stream_id,
        });
        let _ = self.waker.wake();
    }
}

pub type TransportServerArc = ResourceArc<TransportServer>;
pub type StreamHandleArc = ResourceArc<StreamHandle>;
