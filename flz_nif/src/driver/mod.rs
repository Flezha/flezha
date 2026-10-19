//! One driver loop, and one implementation per server transport.
//!
//! [`DriverLoop`] owns every job that is the same whichever wire is under it:
//! the iteration clock, the liveness heartbeat, the command drain and all six
//! `ServerCommand` arms, send-side framing, the send-gate ledger, the gate
//! observation, the per-iteration stats snapshot, the frame parse and delivery,
//! and the close report. A [`Transport`] supplies only wire operations, and a
//! close reason is one `CloseReason` variant spelled once for every wire.
//!
//! A capability added here reaches every transport at once. That is the point:
//! a capability one transport has and another lacks is a defect of the loop,
//! not a backlog item for the transport that lacks it, and a transport with a
//! loop of its own is how such a gap hides.

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};

use crossbeam::channel::{Receiver, Sender};
use flz_transport::transport::close_reason::CloseReason;
use flz_transport::transport::send_gate::{SendGate, Transition};
use mio::{Interest, Poll, Token, Waker};
use rustler::{LocalPid, ResourceArc};

use crate::driver_msg::{report_gate, send_closed_msg};
use crate::driver_stats::{DriverStats, StreamCounters, StreamSnapshot};
use crate::frame_buffer::{FrameBuffer, Inbox};
use crate::resources::StreamHandle;

pub(crate) mod quic;
pub(crate) mod uds;

#[cfg(test)]
#[path = "conformance_tests.rs"]
mod tests;

/// How much room one read is offered before the loop frames what it took.
const READ_CHUNK: usize = 65536;

/// The poll token of a transport's own listening or receiving source.
const SOURCE_TOKEN: Token = Token(0);
/// The poll token the NIF wakes to get a queued command noticed before the
/// poll's own timeout would have.
const WAKER_TOKEN: Token = Token(1);

/// The startup handshake every transport runs: a poll of its own, its source
/// registered on it, and the waker handed back to the NIF. Written once, so a
/// transport that forgets to answer `ready_tx` is not a transport that can
/// exist.
fn open_poll<S: mio::event::Source + ?Sized>(
    source: &mut S,
    interest: Interest,
    ready_tx: &Sender<io::Result<DriverReady>>,
) -> io::Result<(Poll, Arc<Waker>)> {
    let poll = Poll::new()?;
    poll.registry().register(source, SOURCE_TOKEN, interest)?;
    let waker = Arc::new(Waker::new(poll.registry(), WAKER_TOKEN)?);
    let _ = ready_tx.send(Ok(DriverReady {
        waker: Arc::clone(&waker),
    }));
    Ok((poll, waker))
}

/// Hand a bound wire to the one loop, or say why it could not bind. A caller
/// that never answered `ready_tx` is how the NIF learns of the failure; this is
/// the user's copy of it.
pub(crate) fn drive<T: Transport>(
    wire: io::Result<T>,
    stats: Arc<DriverStats>,
    cmd_rx: Receiver<ServerCommand>,
    name: &str,
) {
    match wire {
        Ok(wire) => DriverLoop::new(wire, stats).run(cmd_rx),
        Err(e) => flz_transport::notice!(Error, "{name} driver fatal error: {e}"),
    }
}

/// Commands sent from NIF functions to the driver thread.
pub enum ServerCommand {
    SendFrame {
        stream_id: u64,
        payload_id: String,
        message: Vec<u8>,
        metadata: HashMap<String, String>,
    },
    SetActive {
        stream_id: u64,
        /// Deliver one frame, then hold the rest until the next activation.
        once: bool,
    },
    SetPassive {
        stream_id: u64,
    },
    CloseStream {
        stream_id: u64,
    },
    UpdateOwner {
        stream_id: u64,
        new_owner: LocalPid,
    },
    Shutdown,
}

/// A frame this side could not encode, because a metadata field is longer
/// than the wire can carry. The frame is dropped; the connection is not, because retrying a
/// value that cannot be represented would refuse identically for ever.
pub(crate) fn report_refused_frame(payload_id: &str, error: &std::io::Error) {
    flz_transport::notice!(Warn, "frame refused for payload {payload_id}: {error}");
}

/// Start result passed back from a driver thread to the NIF via channel.
pub struct DriverReady {
    pub waker: Arc<Waker>,
}

/// Who ended a stream. A wire that has already ended one has nothing left to
/// tell its peer, so only a close the NIF asked for says so on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Ended {
    ByRequest,
    ByWire,
}

/// What one flush handed to the wire.
pub(crate) enum Written {
    /// The wire took this many bytes, which have been drained from the buffer.
    /// The depth left behind is what moves the gate.
    Took(usize),
    /// The wire itself says the peer will take nothing more now, so the gate is
    /// raised on its word rather than inferred from a byte count.
    Blocked,
}

/// What one poll pass found: the streams to read, the streams to flush, and the
/// streams the wire ended along with why.
#[derive(Default)]
pub(crate) struct Pass {
    pub readable: Vec<u64>,
    pub writable: Vec<u64>,
    pub closed: Vec<(u64, CloseReason)>,
}

/// One stream, as the loop holds it. Every field here is loop business; the
/// wire's own handle on the stream is `wire` and the loop never reads it.
pub(crate) struct StreamState<W> {
    pub id: u64,
    pub owner: Arc<Mutex<LocalPid>>,
    pub frame_buffer: FrameBuffer,
    pub inbox: Inbox,
    pub write_buffer: Vec<u8>,
    /// Shared with the `StreamHandle` Elixir holds, and carrying the ceiling.
    pub gate: Arc<SendGate>,
    /// Liveness counters (single-threaded driver, no contention). Copied into
    /// the DriverStats snapshot once per loop iteration.
    pub counters: StreamCounters,
    pub wire: W,
}

/// Every open stream a driver holds, addressed by the id Elixir knows it as.
pub(crate) type Streams<W> = HashMap<u64, StreamState<W>>;

impl<W> StreamState<W> {
    pub(crate) fn new(id: u64, owner: Arc<Mutex<LocalPid>>, gate: Arc<SendGate>, wire: W) -> Self {
        StreamState {
            id,
            owner,
            frame_buffer: FrameBuffer::new(),
            inbox: Inbox::new(),
            write_buffer: Vec::new(),
            gate,
            counters: StreamCounters::default(),
            wire,
        }
    }

    /// One accepted stream's loop state and the handle Elixir holds: they share
    /// the owner and the send gate the loop signals through. Written once
    /// because an accepted stream is the same thing on every wire.
    ///
    /// `peer_cert` is what the wire's handshake proved about the peer, and only
    /// QUIC has one to give; `peer_pid` is the peer's process id, which only a
    /// UDS socket knows.
    pub(crate) fn accept(
        id: u64,
        acceptor: LocalPid,
        cmd_tx: &Sender<ServerCommand>,
        waker: &Arc<Waker>,
        wire: W,
        peer_cert: Option<Vec<u8>>,
        peer_pid: Option<u32>,
    ) -> (Self, ResourceArc<StreamHandle>) {
        let owner = Arc::new(Mutex::new(acceptor));
        let gate = Arc::new(SendGate::new());
        let handle = ResourceArc::new(StreamHandle {
            stream_id: id,
            cmd_tx: cmd_tx.clone(),
            waker: Arc::clone(waker),
            owner: Arc::clone(&owner),
            gate: Arc::clone(&gate),
            peer_cert,
            peer_pid,
        });
        (StreamState::new(id, owner, gate, wire), handle)
    }

    /// The send-gate ledger, in one place. The frame has arrived, so it stops
    /// being counted as queued and starts being counted as buffered; the
    /// last-resort ceiling turns it away and counts that; only a frame the
    /// ceiling took is counted as written. Every reservation gets exactly one
    /// release, including the ones that never travel.
    pub(crate) fn offer(&mut self, frame: &[u8], reserved: usize) {
        self.gate.release(reserved);
        if self.gate.offer(&mut self.write_buffer, frame) {
            self.counters.frames_written += 1;
        }
    }

    /// Move the gate on what one flush left behind, and stamp the clock only
    /// for a write the peer actually took.
    pub(crate) fn observe(&mut self, written: Written, now_ms: u64) -> Transition {
        match written {
            Written::Took(n) => {
                if n > 0 {
                    self.counters.last_write_ms = now_ms;
                }
                self.gate.observe(self.write_buffer.len())
            }
            Written::Blocked => self.gate.raise(),
        }
    }
}

/// The loop's read scratch: one allocation reused for every stream, so a
/// transport that reads straight off a socket copies nothing into it and one
/// that assembles its own chunks copies them exactly once.
pub(crate) struct ReadBuf {
    bytes: Vec<u8>,
    filled: usize,
    from: usize,
}

impl ReadBuf {
    fn new() -> Self {
        ReadBuf {
            bytes: Vec::new(),
            filled: 0,
            from: 0,
        }
    }

    /// Room for one more read, at least [`READ_CHUNK`] bytes.
    pub(crate) fn spare(&mut self) -> &mut [u8] {
        if self.bytes.len() < self.filled + READ_CHUNK {
            self.bytes.resize(self.filled + READ_CHUNK, 0);
        }
        &mut self.bytes[self.filled..]
    }

    /// The first `n` bytes of the last [`ReadBuf::spare`] are the stream's.
    pub(crate) fn commit(&mut self, n: usize) {
        self.filled += n;
    }

    /// Take bytes a transport assembled itself.
    pub(crate) fn append(&mut self, data: &[u8]) {
        self.bytes.truncate(self.filled);
        self.bytes.extend_from_slice(data);
        self.filled = self.bytes.len();
    }

    /// The first `n` bytes belong to the wire, not to the frame stream.
    pub(crate) fn skip(&mut self, n: usize) {
        self.from = (self.from + n).min(self.filled);
    }

    /// What this read took, ready to be framed.
    pub(crate) fn taken(&self) -> &[u8] {
        &self.bytes[self.from..self.filled]
    }

    fn clear(&mut self) {
        self.filled = 0;
        self.from = 0;
    }
}

/// One server transport's wire. Everything a transport does not declare here is
/// the loop's, and a transport that restates one of the loop's jobs is the
/// defect this contract exists to prevent.
pub(crate) trait Transport {
    /// What the wire needs to address one stream. The loop carries it and never
    /// reads it.
    type Wire;

    /// Wait for the wire, at most until this transport's own deadline. The
    /// loop's heartbeat is taken the moment this returns, so a wedge anywhere
    /// later in the iteration freezes the counter that reports it.
    fn wait(&mut self);

    /// What the wait found. New streams are accepted into `streams` here,
    /// because who may open one is a property of the wire.
    fn pass(&mut self, streams: &mut Streams<Self::Wire>) -> Pass;

    /// Take up to one chunk of what one stream holds into `into`. `Some` is the
    /// reason the wire gives for ending the stream; `None` with nothing taken
    /// means the wire has no more right now.
    fn read(
        &mut self,
        state: &mut StreamState<Self::Wire>,
        into: &mut ReadBuf,
    ) -> Option<CloseReason>;

    /// Hand the wire as much of `state.write_buffer` as it will take now,
    /// draining what it took.
    fn write(&mut self, state: &mut StreamState<Self::Wire>) -> Result<Written, CloseReason>;

    /// Push whatever the wire has buffered onto the medium. A transport whose
    /// write already reached the medium has nothing to do here.
    fn flush(&mut self);

    /// Release one stream's wire resources.
    fn release(&mut self, state: &mut StreamState<Self::Wire>, ended: Ended);

    /// Close the server down; the loop drops every stream afterwards.
    fn shutdown(&mut self, streams: &mut Streams<Self::Wire>);
}

/// The one transport server driver loop.
pub(crate) struct DriverLoop<T: Transport> {
    transport: T,
    stats: Arc<DriverStats>,
    streams: Streams<T::Wire>,
    read_buf: ReadBuf,
    /// The driver-elapsed clock of the iteration in flight, refreshed once at
    /// the top of the loop so every stamp taken during it agrees. It is the
    /// loop's, not a transport's: no wire operation needs the time.
    now_ms: u64,
}

impl<T: Transport> DriverLoop<T> {
    pub(crate) fn new(transport: T, stats: Arc<DriverStats>) -> Self {
        DriverLoop {
            transport,
            stats,
            streams: HashMap::new(),
            read_buf: ReadBuf::new(),
            now_ms: 0,
        }
    }

    /// Wait, report liveness, take the pass the wire found, drain the command
    /// queue, flush what is still buffered, publish the self-report. Returns
    /// when the NIF asks the driver to shut down.
    pub(crate) fn run(mut self, cmd_rx: Receiver<ServerCommand>) {
        loop {
            self.transport.wait();

            // Liveness heartbeat: bump once per iteration before any work, so a
            // frozen counter (with a stale last_loop_ms) pinpoints a wedged
            // thread.
            self.now_ms = self.stats.now_ms();
            self.stats.mark_loop(self.now_ms, cmd_rx.len() as u64);

            let pass = self.transport.pass(&mut self.streams);
            for id in pass.writable {
                self.flush(id);
            }
            for id in pass.readable {
                self.read(id);
            }
            for (id, reason) in pass.closed {
                self.report_and_release(id, reason);
            }
            self.transport.flush();

            while let Ok(cmd) = cmd_rx.try_recv() {
                if self.handle_command(cmd) {
                    return;
                }
            }

            // After processing all commands, try to flush pending writes.
            let buffered: Vec<u64> = self
                .streams
                .iter()
                .filter(|(_, s)| !s.write_buffer.is_empty())
                .map(|(&id, _)| id)
                .collect();
            for id in buffered {
                self.flush(id);
            }
            self.transport.flush();

            let streams = self.snapshot_streams();
            self.stats.set_streams(streams);
        }
    }

    /// One NIF command. `true` means the driver was asked to shut down. Only
    /// `SendFrame` and `Shutdown` reach the wire at all.
    fn handle_command(&mut self, cmd: ServerCommand) -> bool {
        match cmd {
            ServerCommand::SendFrame {
                stream_id,
                payload_id,
                message,
                metadata,
            } => {
                let reserved = message.len();
                let frame = match flz_transport::service::payloader::build_from_binary(
                    &payload_id,
                    &message,
                    metadata,
                ) {
                    Ok(frame) => frame,
                    Err(e) => {
                        report_refused_frame(&payload_id, &e);
                        return false;
                    }
                };
                if let Some(state) = self.streams.get_mut(&stream_id) {
                    state.offer(&frame, reserved);
                    self.flush(stream_id);
                }
            }
            ServerCommand::SetActive { stream_id, once } => {
                if let Some(state) = self.streams.get_mut(&stream_id) {
                    let owner = *state.owner.lock().unwrap();
                    state.inbox.activate(owner, stream_id, once);
                }
            }
            ServerCommand::SetPassive { stream_id } => {
                if let Some(state) = self.streams.get_mut(&stream_id) {
                    state.inbox.deactivate();
                }
            }
            ServerCommand::CloseStream { stream_id } => self.release(stream_id, Ended::ByRequest),
            ServerCommand::UpdateOwner {
                stream_id,
                new_owner,
            } => {
                if let Some(state) = self.streams.get_mut(&stream_id) {
                    *state.owner.lock().unwrap() = new_owner;
                }
            }
            ServerCommand::Shutdown => {
                self.transport.shutdown(&mut self.streams);
                self.streams.clear();
                return true;
            }
        }
        false
    }

    /// One stream's turn to read: the wire is drained a chunk at a time, each
    /// chunk framed and delivered to the current owner, and a close the wire
    /// reported taken after the bytes that came with it.
    fn read(&mut self, id: u64) {
        loop {
            let (took, ended) = {
                let Self {
                    transport,
                    streams,
                    read_buf,
                    now_ms,
                    ..
                } = self;
                let Some(state) = streams.get_mut(&id) else {
                    return;
                };
                read_buf.clear();
                let closed = transport.read(state, read_buf);
                let data = read_buf.taken();
                if data.is_empty() {
                    (false, closed)
                } else {
                    match state.frame_buffer.feed(data) {
                        Ok(frames) => {
                            let owner = *state.owner.lock().unwrap();
                            state.counters.last_read_ms = *now_ms;
                            state.counters.frames_read += frames.len() as u64;
                            state.inbox.deliver(owner, id, frames);
                            (true, closed)
                        }
                        Err(_) => (false, Some(CloseReason::FrameTooLarge)),
                    }
                }
            };
            if let Some(reason) = ended {
                self.report_and_release(id, reason);
                return;
            }
            if !took {
                return;
            }
        }
    }

    /// Flush one stream's buffered bytes and tell its owner if that moved the
    /// gate; a stream the wire broke is reported and released.
    fn flush(&mut self, id: u64) {
        let broke = {
            let Self {
                transport,
                streams,
                now_ms,
                ..
            } = self;
            let Some(state) = streams.get_mut(&id) else {
                return;
            };
            match transport.write(state) {
                Ok(written) => {
                    let change = state.observe(written, *now_ms);
                    report_gate(&state.owner, id, change);
                    None
                }
                Err(reason) => {
                    state.write_buffer.clear();
                    Some(reason)
                }
            }
        };
        if let Some(reason) = broke {
            self.report_and_release(id, reason);
        }
    }

    /// Tell the owner why its stream ended, then let the wire release it.
    fn report_and_release(&mut self, id: u64, reason: CloseReason) {
        if let Some(state) = self.streams.get(&id) {
            let owner = *state.owner.lock().unwrap();
            send_closed_msg(owner, id, &reason);
        }
        self.release(id, Ended::ByWire);
    }

    fn release(&mut self, id: u64, ended: Ended) {
        if let Some(mut state) = self.streams.remove(&id) {
            self.transport.release(&mut state, ended);
        }
    }

    /// The per-stream liveness snapshot, rebuilt once per iteration (off the
    /// per-frame path). A frozen loop means this never refreshes — matching the
    /// stale last_loop_ms that flags the wedge.
    fn snapshot_streams(&self) -> HashMap<u64, StreamSnapshot> {
        self.streams
            .iter()
            .map(|(&id, s)| (id, s.counters.snapshot(&s.write_buffer, &s.gate)))
            .collect()
    }
}

/// What the conformance table needs to stand a wire up with no BEAM behind it.
#[cfg(test)]
pub(crate) mod support {
    use super::*;

    /// A pid value for a table that never sends to it: the cases assert the
    /// loop's own bookkeeping, and every `{:flz_*, ...}` send is driven from
    /// Elixir by `test/flezha/transport/driver_parity_test.exs`.
    fn nowhere_pid() -> LocalPid {
        // SAFETY: `ErlNifPid` is `#[repr(C)]` over one `ERL_NIF_TERM`, an
        // integer, so all-zero is a valid value of it. It is only ever a value
        // here, and rustler exposes no other way to name a pid outside a NIF
        // call.
        LocalPid::from_c_arg(unsafe { std::mem::zeroed() })
    }

    /// One stream with a fresh gate and an owner nothing is sent to.
    pub(crate) fn detached_stream<W>(id: u64, wire: W) -> StreamState<W> {
        StreamState::new(
            id,
            Arc::new(Mutex::new(nowhere_pid())),
            Arc::new(SendGate::new()),
            wire,
        )
    }

    /// The poll, waker and command sender a wire needs to exist, with nothing
    /// on the other end of either channel.
    pub(crate) fn detached_poll<S: mio::event::Source + ?Sized>(
        source: &mut S,
    ) -> (Poll, Arc<Waker>, Sender<ServerCommand>, LocalPid) {
        let (ready_tx, _ready_rx) = crossbeam::channel::unbounded();
        let (poll, waker) = open_poll(source, Interest::READABLE, &ready_tx).expect("a poll");
        let (cmd_tx, _cmd_rx) = crossbeam::channel::unbounded();
        (poll, waker, cmd_tx, nowhere_pid())
    }
}
