use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crossbeam::channel::Sender;
use flz_transport::transport::send_gate::SendGate;
use mio::Waker;
use rustler::{LocalPid, ResourceArc};

use crate::driver::ServerCommand;
use crate::driver_stats::DriverStats;

/// Opaque handle to a running transport server, held by Elixir as a NIF resource.
pub struct TransportServer {
    pub cmd_tx: Sender<ServerCommand>,
    pub waker: Arc<Waker>,
    pub driver_handle: Mutex<Option<JoinHandle<()>>>,
    // Self-reported driver liveness, in one shape for both server drivers.
    pub stats: Arc<DriverStats>,
}

impl TransportServer {
    /// Shut the driver down and wait for its thread to end, so the socket it
    /// listened on is released by the time this returns. A second call is a
    /// no-op.
    pub fn stop(&self) {
        let _ = self.cmd_tx.send(ServerCommand::Shutdown);
        let _ = self.waker.wake();
        if let Ok(mut handle) = self.driver_handle.lock() {
            if let Some(h) = handle.take() {
                let _ = h.join();
            }
        }
    }
}

impl Drop for TransportServer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Opaque handle to a single stream (connection) on the server.
pub struct StreamHandle {
    pub stream_id: u64,
    pub cmd_tx: Sender<ServerCommand>,
    /// Nudges the driver's poll after a command is queued.
    pub waker: Arc<Waker>,
    pub owner: Arc<Mutex<LocalPid>>,
    /// Shared with the driver's stream state: the driver raises it as its send
    /// buffer fills, `send_frame` reads it before queueing anything more.
    pub gate: Arc<SendGate>,
    /// The DER certificate the peer presented on this stream's connection, read
    /// once at the handshake. `None` for every UDS stream (no handshake) and for
    /// a QUIC peer that presented none. The connect path reads it to learn which
    /// namespace the connection proved.
    pub peer_cert: Option<Vec<u8>>,
    /// The process id of the peer, as the kernel recorded it when the peer
    /// connected. Only a UDS peer has one, and only where the platform can say.
    pub peer_pid: Option<u32>,
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
