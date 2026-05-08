use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crossbeam::channel::Sender;
use mio::Waker;
use rustler::{LocalPid, ResourceArc};

use crate::uds_driver::ServerCommand;

/// Opaque handle to a running transport server, held by Elixir as a NIF resource.
pub struct TransportServer {
    pub cmd_tx: Sender<ServerCommand>,
    pub waker: Arc<Waker>,
    pub driver_handle: Mutex<Option<JoinHandle<()>>>,
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
    pub waker: Arc<Waker>,
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
