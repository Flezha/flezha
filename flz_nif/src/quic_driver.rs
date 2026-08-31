use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use crossbeam::channel::{Receiver, Sender};
use flz_transport::transport::quic::{expire_timer, flush_transmits, send_transmit};
use flz_transport::transport::send_gate::{SendGate, Transition};
use mio::net::UdpSocket;
use mio::{Events, Interest, Poll, Token, Waker};
use quinn_proto::{
    ConnectionHandle, DatagramEvent, Dir, EndpointConfig, Event, ServerConfig, StreamEvent,
    StreamId,
};
use rustler::LocalPid;

use crate::driver_msg::{
    report_gate, send_closed_msg, send_connection_msg, send_refused_msg, send_stream_msg,
};
use crate::frame_buffer::{FrameBuffer, Inbox};
use crate::resources::StreamHandle;
use crate::uds_driver::ServerCommand;

const UDP_TOKEN: Token = Token(0);
const WAKER_TOKEN: Token = Token(1);

/// Result passed back from driver thread to NIF.
pub struct QuicDriverReady {
    pub waker: Arc<Waker>,
}

/// Whether the stream has consumed its type-byte prefix.
///
/// Data streams (non-control) send `[type_byte, index_byte]` before framed data.
/// The control channel (first bidi stream per connection) has no prefix.
enum TypeByteState {
    /// Control channel — no prefix, data goes straight to FrameBuffer.
    ControlChannel,
    /// Waiting for the 2-byte prefix. Accumulated bytes so far.
    AwaitingPrefix(Vec<u8>),
    /// Prefix consumed, type and index known. Data goes to FrameBuffer.
    DataStream { _stream_type: u8, _index: u8 },
}

/// Per-QUIC-stream state within a connection.
struct QuicStreamState {
    /// Unique ID exposed to Elixir (maps to this specific QUIC stream).
    nif_stream_id: u64,
    owner: Arc<Mutex<LocalPid>>,
    frame_buffer: FrameBuffer,
    inbox: Inbox,
    write_buffer: Vec<u8>,
    /// Shared with the `StreamHandle` Elixir holds, and carrying the ceiling.
    gate: Arc<SendGate>,
    type_byte_state: TypeByteState,
}

/// Per-QUIC-connection state.
struct ConnectionState {
    connection: quinn_proto::Connection,
    /// Maps quinn-proto StreamId → per-stream state.
    streams: HashMap<StreamId, QuicStreamState>,
    /// Number of bidi streams accepted on this connection.
    stream_count: u32,
    /// The control channel's nif stream id — a stable per-connection routing
    /// key. Every data stream on this connection is tagged with it in
    /// `{:flz_stream, ...}` so the controller routes by connection identity
    /// (which owns the session token) instead of a shared "current runner"
    /// scalar, eliminating the concurrent-connect race. Set when the first
    /// (control) stream is accepted.
    control_nif_id: u64,
}

/// Run the QUIC server driver loop. Called from a spawned thread.
pub fn run_quic_driver(
    bind_address: SocketAddr,
    server_config: ServerConfig,
    acceptor_pid: LocalPid,
    cmd_rx: Receiver<ServerCommand>,
    cmd_tx: Sender<ServerCommand>,
    ready_tx: Sender<io::Result<QuicDriverReady>>,
) {
    let outcome = bind_endpoint(bind_address, &ready_tx).and_then(|(poll, udp, waker)| {
        QuicDriver {
            acceptor_pid,
            cmd_tx,
            waker,
            poll,
            udp,
            endpoint: quinn_proto::Endpoint::new(
                Arc::new(EndpointConfig::default()),
                Some(Arc::new(server_config)),
                true,
                None,
            ),
            connections: HashMap::new(),
            nif_stream_map: HashMap::new(),
            next_nif_stream_id: 1,
            pending_data_streams: HashMap::new(),
            transmit_buf: Vec::new(),
        }
        .run(cmd_rx)
    });
    if let Err(e) = outcome {
        eprintln!("[flz_nif] QUIC driver fatal error: {}", e);
    }
}

/// Bind the UDP socket, register it with a fresh poll and hand the NIF its
/// waker.
fn bind_endpoint(
    bind_address: SocketAddr,
    ready_tx: &Sender<io::Result<QuicDriverReady>>,
) -> io::Result<(Poll, UdpSocket, Arc<Waker>)> {
    let std_udp = std::net::UdpSocket::bind(bind_address)?;
    std_udp.set_nonblocking(true)?;
    let mut udp = UdpSocket::from_std(std_udp);

    let poll = Poll::new()?;
    poll.registry()
        .register(&mut udp, UDP_TOKEN, Interest::READABLE | Interest::WRITABLE)?;

    let waker = Arc::new(Waker::new(poll.registry(), WAKER_TOKEN)?);

    let _ = ready_tx.send(Ok(QuicDriverReady {
        waker: Arc::clone(&waker),
    }));
    Ok((poll, udp, waker))
}

/// The QUIC server driver: the endpoint, every accepted connection and its
/// streams, and the NIF-facing stream ids that address them.
struct QuicDriver {
    acceptor_pid: LocalPid,
    cmd_tx: Sender<ServerCommand>,
    waker: Arc<Waker>,
    poll: Poll,
    udp: UdpSocket,
    endpoint: quinn_proto::Endpoint,
    connections: HashMap<ConnectionHandle, ConnectionState>,
    /// Maps NIF stream_id → (ConnectionHandle, StreamId) for command dispatch.
    nif_stream_map: HashMap<u64, (ConnectionHandle, StreamId)>,
    next_nif_stream_id: u64,
    /// Data streams pending type-byte identification, keyed by NIF stream_id.
    pending_data_streams: HashMap<u64, rustler::ResourceArc<StreamHandle>>,
    transmit_buf: Vec<u8>,
}

impl QuicDriver {
    fn run(mut self, cmd_rx: Receiver<ServerCommand>) -> io::Result<()> {
        let mut events = Events::with_capacity(256);
        let mut udp_buf = vec![0u8; 65536];

        loop {
            let timeout = self.poll_timeout();
            let _ = self.poll.poll(&mut events, Some(timeout));
            let now = Instant::now();
            for conn_state in self.connections.values_mut() {
                expire_timer(&mut conn_state.connection, now);
            }
            self.handle_datagrams(&mut udp_buf, now);

            // Pump endpoint events and poll connection events.
            // Events are collected first to avoid borrow conflicts with
            // drain_readable.
            let conn_handles: Vec<ConnectionHandle> = self.connections.keys().copied().collect();
            let mut lost_connections: Vec<ConnectionHandle> = Vec::new();
            for ch in &conn_handles {
                for event in self.pump_connection(*ch) {
                    self.handle_stream_event(*ch, event, &mut lost_connections);
                }
            }
            for ch in &lost_connections {
                self.connections.remove(ch);
            }
            self.flush_all(&conn_handles);

            while let Ok(cmd) = cmd_rx.try_recv() {
                if self.handle_command(cmd) {
                    return Ok(());
                }
            }

            // Flush pending writes for all streams across all connections.
            let conn_handles: Vec<ConnectionHandle> = self.connections.keys().copied().collect();
            for ch in &conn_handles {
                self.flush_stream_writes(*ch);
            }
            self.flush_all(&conn_handles);
        }
    }

    /// How long to poll: until the nearest connection timer, 50 ms at most.
    fn poll_timeout(&mut self) -> Duration {
        let now = Instant::now();
        let mut min_timeout = Duration::from_millis(50);
        for conn_state in self.connections.values_mut() {
            if let Some(deadline) = conn_state.connection.poll_timeout() {
                let remaining = deadline.saturating_duration_since(now);
                if remaining < min_timeout {
                    min_timeout = remaining;
                }
            }
        }
        min_timeout
    }

    /// Read every datagram the socket holds into the endpoint: a connection's
    /// event to its connection, a stateless response back onto the wire, a
    /// new connection accepted with its handshake packets flushed.
    fn handle_datagrams(&mut self, udp_buf: &mut [u8], now: Instant) {
        while let Ok((n, remote)) = self.udp.recv_from(udp_buf) {
            let data = BytesMut::from(&udp_buf[..n]);
            let mut resp_buf = Vec::new();
            let Some(event) = self
                .endpoint
                .handle(now, remote, None, None, data, &mut resp_buf)
            else {
                continue;
            };
            match event {
                DatagramEvent::ConnectionEvent(ch, event) => {
                    if let Some(conn_state) = self.connections.get_mut(&ch) {
                        conn_state.connection.handle_event(event);
                    }
                }
                DatagramEvent::Response(transmit) => {
                    let _ = send_transmit(&self.udp, &transmit, &resp_buf);
                }
                DatagramEvent::NewConnection(incoming) => {
                    let mut accept_buf = Vec::new();
                    match self.endpoint.accept(incoming, now, &mut accept_buf, None) {
                        Ok((ch, mut connection)) => {
                            let _ =
                                flush_transmits(&mut connection, &self.udp, &mut self.transmit_buf);
                            self.connections.insert(
                                ch,
                                ConnectionState {
                                    connection,
                                    streams: HashMap::new(),
                                    stream_count: 0,
                                    control_nif_id: 0,
                                },
                            );
                        }
                        Err(e) => {
                            eprintln!("[flz_nif] QUIC accept error: {:?}", e);
                        }
                    }
                }
            }
        }
    }

    /// Pump endpoint ↔ connection events for one connection and take every
    /// event it has to report.
    fn pump_connection(&mut self, ch: ConnectionHandle) -> Vec<Event> {
        let Some(conn_state) = self.connections.get_mut(&ch) else {
            return Vec::new();
        };
        while let Some(endpoint_event) = conn_state.connection.poll_endpoint_events() {
            if let Some(conn_event) = self.endpoint.handle_event(ch, endpoint_event) {
                conn_state.connection.handle_event(conn_event);
            }
        }
        let mut conn_events: Vec<Event> = Vec::new();
        while let Some(event) = conn_state.connection.poll() {
            conn_events.push(event);
        }
        conn_events
    }

    /// One connection event: newly opened streams are accepted and drained
    /// (quinn-proto may not fire a Readable event for data received before
    /// accept).
    fn handle_stream_event(
        &mut self,
        ch: ConnectionHandle,
        event: Event,
        lost_connections: &mut Vec<ConnectionHandle>,
    ) {
        match event {
            Event::Stream(StreamEvent::Opened { dir: Dir::Bi }) => {
                for accepted_id in self.accept_streams(ch) {
                    self.drain_readable(ch, accepted_id);
                }
            }
            Event::Stream(StreamEvent::Readable { id }) => self.drain_readable(ch, id),
            Event::Stream(StreamEvent::Writable { id }) => self.flush_one(ch, id),
            Event::Stream(StreamEvent::Finished { id }) => {
                let conn_state = self.connections.get_mut(&ch).unwrap();
                if let Some(stream_state) = conn_state.streams.remove(&id) {
                    self.nif_stream_map.remove(&stream_state.nif_stream_id);
                    let owner = *stream_state.owner.lock().unwrap();
                    send_closed_msg(owner, stream_state.nif_stream_id, "stream_finished");
                }
            }
            Event::ConnectionLost { reason } => {
                self.connection_lost(ch, reason);
                lost_connections.push(ch);
            }
            _ => {}
        }
    }

    /// A lost connection closes every stream; one that never opened a stream
    /// is reported as refused (a missing or untrusted client certificate
    /// lands there).
    fn connection_lost(&mut self, ch: ConnectionHandle, reason: quinn_proto::ConnectionError) {
        let conn_state = self.connections.get_mut(&ch).unwrap();
        if conn_state.streams.is_empty()
            && matches!(&reason, quinn_proto::ConnectionError::TransportError(_))
        {
            send_refused_msg(self.acceptor_pid, &reason.to_string());
        }
        for (_, stream_state) in conn_state.streams.drain() {
            self.nif_stream_map.remove(&stream_state.nif_stream_id);
            let owner = *stream_state.owner.lock().unwrap();
            send_closed_msg(
                owner,
                stream_state.nif_stream_id,
                &format!("connection_lost: {}", reason),
            );
        }
    }

    /// Flush one stream's buffered writes; a broken stream is closed and
    /// reported.
    fn flush_one(&mut self, ch: ConnectionHandle, qstream_id: StreamId) {
        let Some(conn_state) = self.connections.get_mut(&ch) else {
            return;
        };
        let mut broken: Option<(u64, LocalPid)> = None;
        if let Some(stream_state) = conn_state.streams.get_mut(&qstream_id) {
            let (is_broken, change) = flush_quic_write(
                &mut conn_state.connection,
                qstream_id,
                &mut stream_state.write_buffer,
                &stream_state.gate,
            );
            report_gate(&stream_state.owner, stream_state.nif_stream_id, change);
            if is_broken {
                broken = Some((
                    stream_state.nif_stream_id,
                    *stream_state.owner.lock().unwrap(),
                ));
            }
        }
        if let Some((nif_id, owner)) = broken {
            conn_state.streams.remove(&qstream_id);
            self.nif_stream_map.remove(&nif_id);
            send_closed_msg(owner, nif_id, "write_error");
        }
    }

    /// Accept every bidi stream waiting on a connection. The first stream per
    /// connection is the control channel (no type prefix) and is announced at
    /// once, tagging the connection with its nif id; a later one is a data
    /// stream (2-byte type+index prefix) whose announcement waits for the
    /// prefix. Answers the accepted stream ids.
    fn accept_streams(&mut self, ch: ConnectionHandle) -> Vec<StreamId> {
        let conn_state = self.connections.get_mut(&ch).unwrap();
        let mut accepted_streams: Vec<StreamId> = Vec::new();
        while let Some(stream_id) = conn_state.connection.streams().accept(Dir::Bi) {
            let nif_id = self.next_nif_stream_id;
            self.next_nif_stream_id += 1;

            let is_control = conn_state.stream_count == 0;
            conn_state.stream_count += 1;

            let type_byte_state = if is_control {
                TypeByteState::ControlChannel
            } else {
                TypeByteState::AwaitingPrefix(Vec::with_capacity(2))
            };

            let (state, stream_handle) = new_stream(
                nif_id,
                self.acceptor_pid,
                &self.cmd_tx,
                &self.waker,
                type_byte_state,
            );
            conn_state.streams.insert(stream_id, state);

            self.nif_stream_map.insert(nif_id, (ch, stream_id));

            if is_control {
                conn_state.control_nif_id = nif_id;
                send_connection_msg(self.acceptor_pid, stream_handle);
            } else {
                self.pending_data_streams.insert(nif_id, stream_handle);
            }

            accepted_streams.push(stream_id);
        }
        accepted_streams
    }

    /// Flush every connection's pending transmits to the UDP socket.
    fn flush_all(&mut self, conn_handles: &[ConnectionHandle]) {
        for ch in conn_handles {
            if let Some(conn_state) = self.connections.get_mut(ch) {
                let _ = flush_transmits(
                    &mut conn_state.connection,
                    &self.udp,
                    &mut self.transmit_buf,
                );
            }
        }
    }

    /// Flush every stream's buffered writes on one connection; a broken stream
    /// is closed and reported.
    fn flush_stream_writes(&mut self, ch: ConnectionHandle) {
        let Some(conn_state) = self.connections.get_mut(&ch) else {
            return;
        };
        let stream_ids: Vec<StreamId> = conn_state
            .streams
            .iter()
            .filter(|(_, s)| !s.write_buffer.is_empty())
            .map(|(&id, _)| id)
            .collect();
        let mut broken_streams: Vec<(StreamId, u64, LocalPid)> = Vec::new();
        for qstream_id in stream_ids {
            if let Some(stream_state) = conn_state.streams.get_mut(&qstream_id) {
                let (broken, change) = flush_quic_write(
                    &mut conn_state.connection,
                    qstream_id,
                    &mut stream_state.write_buffer,
                    &stream_state.gate,
                );
                report_gate(&stream_state.owner, stream_state.nif_stream_id, change);
                if broken {
                    broken_streams.push((
                        qstream_id,
                        stream_state.nif_stream_id,
                        *stream_state.owner.lock().unwrap(),
                    ));
                }
            }
        }
        for (qstream_id, nif_id, owner) in broken_streams {
            conn_state.streams.remove(&qstream_id);
            self.nif_stream_map.remove(&nif_id);
            send_closed_msg(owner, nif_id, "write_error");
        }
    }

    /// One NIF command. `true` means the driver was asked to shut down.
    fn handle_command(&mut self, cmd: ServerCommand) -> bool {
        match cmd {
            ServerCommand::SendFrame {
                stream_id: nif_id,
                payload_id,
                message,
                metadata,
            } => {
                let frame = flz_transport::service::payloader::build_from_binary(
                    &payload_id,
                    &message,
                    metadata,
                );
                self.write_stream(nif_id, frame);
            }
            ServerCommand::SetActive { stream_id: nif_id } => {
                if let Some(stream_state) = self.stream_mut(nif_id) {
                    let owner = *stream_state.owner.lock().unwrap();
                    stream_state.inbox.activate(owner, nif_id);
                }
            }
            ServerCommand::SetPassive { stream_id: nif_id } => {
                if let Some(stream_state) = self.stream_mut(nif_id) {
                    stream_state.inbox.deactivate();
                }
            }
            ServerCommand::CloseStream { stream_id: nif_id } => self.close_stream(nif_id),
            ServerCommand::UpdateOwner {
                stream_id: nif_id,
                new_owner,
            } => {
                if let Some(stream_state) = self.stream_mut(nif_id) {
                    *stream_state.owner.lock().unwrap() = new_owner;
                }
            }
            ServerCommand::Shutdown => {
                self.shutdown();
                return true;
            }
        }
        false
    }

    /// Stop reading and finish writing the stream a NIF id addresses.
    fn close_stream(&mut self, nif_id: u64) {
        let Some((ch, qstream_id)) = self.nif_stream_map.remove(&nif_id) else {
            return;
        };
        if let Some(conn_state) = self.connections.get_mut(&ch) {
            conn_state.streams.remove(&qstream_id);
            let _ = conn_state
                .connection
                .recv_stream(qstream_id)
                .stop(quinn_proto::VarInt::from_u32(0));
            let _ = conn_state.connection.send_stream(qstream_id).finish();
        }
    }

    /// Close every connection and flush its closing transmits.
    fn shutdown(&mut self) {
        let now = Instant::now();
        for (_, mut conn_state) in self.connections.drain() {
            conn_state.connection.close(
                now,
                quinn_proto::VarInt::from_u32(0),
                b"shutdown".to_vec().into(),
            );
            let _ = flush_transmits(
                &mut conn_state.connection,
                &self.udp,
                &mut self.transmit_buf,
            );
        }
    }

    /// The stream a NIF id addresses, if it is still open.
    fn stream_mut(&mut self, nif_id: u64) -> Option<&mut QuicStreamState> {
        let &(ch, qstream_id) = self.nif_stream_map.get(&nif_id)?;
        self.connections.get_mut(&ch)?.streams.get_mut(&qstream_id)
    }

    /// Queue `data` on a stream and flush what it will take now.
    fn write_stream(&mut self, nif_id: u64, data: Vec<u8>) {
        let Some(&(ch, qstream_id)) = self.nif_stream_map.get(&nif_id) else {
            return;
        };
        if let Some(stream_state) = self.stream_mut(nif_id) {
            stream_state
                .gate
                .offer(&mut stream_state.write_buffer, &data);
        }
        self.flush_one(ch, qstream_id);
    }

    /// Forget a stream the peer ended: its state, its NIF id, and any
    /// announcement still pending on its type prefix.
    fn forget_stream(&mut self, ch: ConnectionHandle, id: StreamId, nif_id: u64) {
        if let Some(conn_state) = self.connections.get_mut(&ch) {
            conn_state.streams.remove(&id);
        }
        self.nif_stream_map.remove(&nif_id);
        self.pending_data_streams.remove(&nif_id);
    }

    /// Drain readable data from a QUIC stream, feed into FrameBuffer, deliver
    /// frames. For data streams, handles the 2-byte type+index prefix before
    /// switching to framed mode; a just-identified data stream is announced
    /// with the connection's routing key (the control stream's nif id).
    fn drain_readable(&mut self, ch: ConnectionHandle, id: StreamId) {
        let Some(conn_state) = self.connections.get_mut(&ch) else {
            return;
        };
        let conn_id = conn_state.control_nif_id;
        let Some(stream_state) = conn_state.streams.get_mut(&id) else {
            return;
        };
        let nif_id = stream_state.nif_stream_id;
        let owner = *stream_state.owner.lock().unwrap();

        let all_data = match read_chunks(&mut conn_state.connection, id, nif_id) {
            Ok(data) => data,
            Err(reason) => {
                send_closed_msg(owner, nif_id, reason);
                self.forget_stream(ch, id, nif_id);
                return;
            }
        };
        if all_data.is_empty() {
            return;
        }

        let Some((consumed, identified)) =
            consume_type_prefix(&mut stream_state.type_byte_state, &all_data)
        else {
            return;
        };
        if let Some((stream_type, index)) = identified {
            if let Some(handle) = self.pending_data_streams.remove(&nif_id) {
                send_stream_msg(self.acceptor_pid, handle, stream_type, index, conn_id);
            }
        }
        let data_for_framing = &all_data[consumed..];
        if data_for_framing.is_empty() {
            return;
        }
        match stream_state.frame_buffer.feed(data_for_framing) {
            Ok(frames) => stream_state.inbox.deliver(owner, nif_id, frames),
            Err(e) => {
                eprintln!("[flz_nif] QUIC frame too large on nif_id={}: {}", nif_id, e);
                send_closed_msg(owner, nif_id, "frame_too_large");
                self.forget_stream(ch, id, nif_id);
            }
        }
    }
}

/// Everything readable on a stream right now. `Err` carries the close reason
/// for a reset or a read error.
fn read_chunks(
    connection: &mut quinn_proto::Connection,
    id: StreamId,
    nif_id: u64,
) -> Result<Vec<u8>, &'static str> {
    let mut recv = connection.recv_stream(id);
    let mut chunks = match recv.read(true) {
        Ok(chunks) => chunks,
        Err(e) => {
            eprintln!("[flz_nif] QUIC read error on nif_id={}: {:?}", nif_id, e);
            return Err("read_error");
        }
    };
    let mut all_data = Vec::new();
    loop {
        match chunks.next(usize::MAX) {
            Ok(Some(chunk)) => all_data.extend_from_slice(&chunk.bytes),
            Ok(None) => break,
            Err(quinn_proto::ReadError::Blocked) => break,
            Err(quinn_proto::ReadError::Reset(_)) => {
                let _ = chunks.finalize();
                return Err("stream_reset");
            }
        }
    }
    let _ = chunks.finalize();
    Ok(all_data)
}

/// Take a data stream's 2-byte type+index prefix out of `data`. Answers how
/// many bytes belong to the prefix and, the moment it completes, the
/// `(type, index)` it named; `None` while the prefix is still incomplete.
fn consume_type_prefix(
    state: &mut TypeByteState,
    data: &[u8],
) -> Option<(usize, Option<(u8, u8)>)> {
    match state {
        TypeByteState::ControlChannel | TypeByteState::DataStream { .. } => Some((0, None)),
        TypeByteState::AwaitingPrefix(buf) => {
            let need = 2 - buf.len();
            let take = std::cmp::min(need, data.len());
            buf.extend_from_slice(&data[..take]);
            if buf.len() < 2 {
                return None;
            }
            let (stream_type, index) = (buf[0], buf[1]);
            *state = TypeByteState::DataStream {
                _stream_type: stream_type,
                _index: index,
            };
            Some((take, Some((stream_type, index))))
        }
    }
}

/// One accepted stream's driver state and the handle Elixir holds: they share
/// the owner and the send gate the driver signals through.
fn new_stream(
    nif_id: u64,
    acceptor_pid: LocalPid,
    cmd_tx: &Sender<ServerCommand>,
    waker: &Arc<Waker>,
    type_byte_state: TypeByteState,
) -> (QuicStreamState, rustler::ResourceArc<StreamHandle>) {
    let owner = Arc::new(Mutex::new(acceptor_pid));
    let gate = Arc::new(SendGate::new());
    let handle = rustler::ResourceArc::new(StreamHandle {
        stream_id: nif_id,
        cmd_tx: cmd_tx.clone(),
        waker: crate::resources::StreamWaker::Mio(Arc::clone(waker)),
        owner: Arc::clone(&owner),
        gate: Arc::clone(&gate),
    });
    let state = QuicStreamState {
        nif_stream_id: nif_id,
        owner,
        frame_buffer: FrameBuffer::new(),
        inbox: Inbox::new(),
        write_buffer: Vec::new(),
        gate,
        type_byte_state,
    };
    (state, handle)
}

/// Flush what the peer will take of a stream's buffered bytes. Answers whether
/// the stream broke, and whether that flush moved its send gate.
fn flush_quic_write(
    connection: &mut quinn_proto::Connection,
    qstream_id: StreamId,
    write_buffer: &mut Vec<u8>,
    gate: &SendGate,
) -> (bool, Transition) {
    if write_buffer.is_empty() {
        return (false, gate.observe(0));
    }

    match connection.send_stream(qstream_id).write(write_buffer) {
        Ok(written) => {
            write_buffer.drain(..written);
            (false, gate.observe(write_buffer.len()))
        }
        // The peer's flow-control window is full: the protocol has already
        // computed the backpressure, so it raises the gate directly rather
        // than waiting for a byte count to say the same thing.
        Err(quinn_proto::WriteError::Blocked) => (false, gate.raise()),
        Err(_) => {
            write_buffer.clear();
            (true, None)
        }
    }
}

#[cfg(test)]
#[path = "quic_driver_tests.rs"]
mod tests;
