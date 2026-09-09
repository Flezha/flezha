//! The QUIC wire: a UDP endpoint, the connections on it, the streams
//! multiplexed inside each connection, and the two-byte type prefix a data
//! stream opens with. Multiplexing is a QUIC concept and stays here — the loop
//! knows only streams.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use crossbeam::channel::{Receiver, Sender};
use flz_transport::transport::quic::{expire_timer, flush_transmits, send_transmit};
use mio::net::UdpSocket;
use mio::{Events, Interest, Poll, Waker};
use quinn_proto::{
    ConnectionHandle, DatagramEvent, Dir, EndpointConfig, Event, ServerConfig, StreamEvent,
    StreamId,
};
use rustler::LocalPid;

use super::{
    CloseReason, DriverReady, Ended, Pass, ReadBuf, ServerCommand, StreamState, Streams, Transport,
    Written,
};
use crate::driver_msg::{send_connection_msg, send_refused_msg, send_stream_msg};
use crate::driver_stats::DriverStats;
use crate::resources::StreamHandle;

/// Whether the stream has consumed its type-byte prefix.
///
/// Data streams (non-control) send `[type_byte, index_byte]` before framed data.
/// The control channel (first bidi stream per connection) has no prefix.
enum TypeByteState {
    /// Control channel — no prefix, data goes straight to the frame buffer.
    ControlChannel,
    /// Waiting for the 2-byte prefix. Accumulated bytes so far.
    AwaitingPrefix(Vec<u8>),
    /// Prefix consumed, type and index known. Data goes to the frame buffer.
    DataStream { _stream_type: u8, _index: u8 },
}

/// How the QUIC wire addresses one stream: the connection it rides, the
/// quinn-proto stream inside it, and the prefix machine in front of its bytes.
pub(crate) struct QuicWire {
    ch: ConnectionHandle,
    qstream_id: StreamId,
    type_byte_state: TypeByteState,
}

/// Per-QUIC-connection state. The streams map is a routing table only: a
/// stream's own state belongs to the driver loop.
struct ConnectionState {
    connection: quinn_proto::Connection,
    /// Maps quinn-proto StreamId → the NIF stream id the loop holds it under.
    streams: HashMap<StreamId, u64>,
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

/// Run the QUIC server driver. Called from a spawned thread.
pub fn run(
    bind_address: SocketAddr,
    server_config: ServerConfig,
    acceptor_pid: LocalPid,
    cmd_rx: Receiver<ServerCommand>,
    cmd_tx: Sender<ServerCommand>,
    ready_tx: Sender<io::Result<DriverReady>>,
    stats: Arc<DriverStats>,
) {
    let wire = bind(bind_address, server_config, acceptor_pid, cmd_tx, &ready_tx);
    super::drive(wire, stats, cmd_rx, "QUIC");
}

/// Bind the UDP socket and open the endpoint and poll it answers on.
fn bind(
    bind_address: SocketAddr,
    server_config: ServerConfig,
    acceptor_pid: LocalPid,
    cmd_tx: Sender<ServerCommand>,
    ready_tx: &Sender<io::Result<DriverReady>>,
) -> io::Result<Quic> {
    let std_udp = std::net::UdpSocket::bind(bind_address)?;
    std_udp.set_nonblocking(true)?;
    let mut udp = UdpSocket::from_std(std_udp);

    let (poll, waker) =
        super::open_poll(&mut udp, Interest::READABLE | Interest::WRITABLE, ready_tx)?;

    Ok(Quic {
        acceptor_pid,
        cmd_tx,
        waker,
        poll,
        udp,
        events: Events::with_capacity(256),
        udp_buf: vec![0u8; 65536],
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
    })
}

/// The QUIC wire.
pub(crate) struct Quic {
    acceptor_pid: LocalPid,
    cmd_tx: Sender<ServerCommand>,
    waker: Arc<Waker>,
    poll: Poll,
    udp: UdpSocket,
    events: Events,
    udp_buf: Vec<u8>,
    endpoint: quinn_proto::Endpoint,
    connections: HashMap<ConnectionHandle, ConnectionState>,
    /// Maps NIF stream_id → (ConnectionHandle, StreamId) for command dispatch.
    nif_stream_map: HashMap<u64, (ConnectionHandle, StreamId)>,
    next_nif_stream_id: u64,
    /// Data streams pending type-byte identification, keyed by NIF stream_id.
    pending_data_streams: HashMap<u64, rustler::ResourceArc<StreamHandle>>,
    transmit_buf: Vec<u8>,
}

impl Quic {
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
    fn handle_datagrams(&mut self, now: Instant) {
        while let Ok((n, remote)) = self.udp.recv_from(&mut self.udp_buf) {
            let data = BytesMut::from(&self.udp_buf[..n]);
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

    /// One connection event, sorted into the pass; answers the connection if
    /// this event was its death. Newly opened streams are accepted and marked
    /// readable, because quinn-proto may not fire a Readable event for data
    /// received before the accept.
    fn handle_stream_event(
        &mut self,
        ch: ConnectionHandle,
        event: Event,
        streams: &mut Streams<QuicWire>,
        pass: &mut Pass,
    ) -> Option<ConnectionHandle> {
        match event {
            Event::Stream(StreamEvent::Opened { dir: Dir::Bi }) => {
                for accepted in self.accept_streams(ch, streams) {
                    pass.readable.push(accepted);
                }
            }
            Event::Stream(StreamEvent::Readable { id }) => {
                if let Some(nif_id) = self.nif_id_of(ch, id) {
                    pass.readable.push(nif_id);
                }
            }
            Event::Stream(StreamEvent::Writable { id }) => {
                if let Some(nif_id) = self.nif_id_of(ch, id) {
                    pass.writable.push(nif_id);
                }
            }
            Event::ConnectionLost { reason } => {
                self.connection_lost(ch, reason, pass);
                return Some(ch);
            }
            _ => {}
        }
        None
    }

    fn nif_id_of(&self, ch: ConnectionHandle, id: StreamId) -> Option<u64> {
        self.connections.get(&ch)?.streams.get(&id).copied()
    }

    /// A lost connection closes every stream; one that never opened a stream
    /// is reported as refused (a missing or untrusted client certificate
    /// lands there).
    fn connection_lost(
        &mut self,
        ch: ConnectionHandle,
        reason: quinn_proto::ConnectionError,
        pass: &mut Pass,
    ) {
        let Some(conn_state) = self.connections.get(&ch) else {
            return;
        };
        if conn_state.streams.is_empty()
            && matches!(&reason, quinn_proto::ConnectionError::TransportError(_))
        {
            send_refused_msg(self.acceptor_pid, &reason.to_string());
        }
        let lost = CloseReason::ConnectionLost(reason.to_string());
        for &nif_id in conn_state.streams.values() {
            pass.closed.push((nif_id, lost.clone()));
        }
    }

    /// Accept every bidi stream waiting on a connection. The first stream per
    /// connection is the control channel (no type prefix) and is announced at
    /// once, tagging the connection with its nif id; a later one is a data
    /// stream (2-byte type+index prefix) whose announcement waits for the
    /// prefix. Answers the accepted streams' NIF ids.
    fn accept_streams(
        &mut self,
        ch: ConnectionHandle,
        streams: &mut Streams<QuicWire>,
    ) -> Vec<u64> {
        let mut accepted: Vec<u64> = Vec::new();
        let Some(conn_state) = self.connections.get_mut(&ch) else {
            return accepted;
        };
        while let Some(qstream_id) = conn_state.connection.streams().accept(Dir::Bi) {
            let nif_id = self.next_nif_stream_id;
            self.next_nif_stream_id += 1;

            let is_control = conn_state.stream_count == 0;
            conn_state.stream_count += 1;
            conn_state.streams.insert(qstream_id, nif_id);

            let type_byte_state = if is_control {
                conn_state.control_nif_id = nif_id;
                TypeByteState::ControlChannel
            } else {
                TypeByteState::AwaitingPrefix(Vec::with_capacity(2))
            };

            let (state, handle) = StreamState::accept(
                nif_id,
                self.acceptor_pid,
                &self.cmd_tx,
                &self.waker,
                QuicWire {
                    ch,
                    qstream_id,
                    type_byte_state,
                },
                // Only the control stream is asked what the handshake proved:
                // reading the identity clones the whole verified chain, and the
                // connect path is the one reader.
                if is_control {
                    peer_leaf_der(&conn_state.connection)
                } else {
                    None
                },
            );
            streams.insert(nif_id, state);
            self.nif_stream_map.insert(nif_id, (ch, qstream_id));

            if is_control {
                send_connection_msg(self.acceptor_pid, handle);
            } else {
                self.pending_data_streams.insert(nif_id, handle);
            }

            accepted.push(nif_id);
        }
        accepted
    }
}

impl Transport for Quic {
    type Wire = QuicWire;

    fn wait(&mut self) {
        let timeout = self.poll_timeout();
        let _ = self.poll.poll(&mut self.events, Some(timeout));
    }

    fn pass(&mut self, streams: &mut Streams<QuicWire>) -> Pass {
        let mut pass = Pass::default();
        let now = Instant::now();
        for conn_state in self.connections.values_mut() {
            expire_timer(&mut conn_state.connection, now);
        }
        self.handle_datagrams(now);

        // Events are collected per connection first, because handling one
        // reaches back into the connection that reported it.
        let conn_handles: Vec<ConnectionHandle> = self.connections.keys().copied().collect();
        let mut lost: Vec<ConnectionHandle> = Vec::new();
        for ch in &conn_handles {
            for event in self.pump_connection(*ch) {
                lost.extend(self.handle_stream_event(*ch, event, streams, &mut pass));
            }
        }
        for ch in &lost {
            self.connections.remove(ch);
        }
        pass
    }

    /// Everything readable on a QUIC stream right now, past the two-byte
    /// type+index prefix a data stream opens with. A stream identified by that
    /// prefix is announced here, with the connection's routing key. A close the
    /// wire reported rides back with the bytes that came before it.
    fn read(
        &mut self,
        state: &mut StreamState<QuicWire>,
        into: &mut ReadBuf,
    ) -> Option<CloseReason> {
        let ch = state.wire.ch;
        let qstream_id = state.wire.qstream_id;
        let conn_state = self.connections.get_mut(&ch)?;
        let conn_id = conn_state.control_nif_id;
        let closed = read_chunks(&mut conn_state.connection, qstream_id, state.id, into);
        if into.taken().is_empty() {
            return closed;
        }

        let Some((consumed, identified)) =
            consume_type_prefix(&mut state.wire.type_byte_state, into.taken())
        else {
            // The prefix is still incomplete: every byte taken was prefix.
            into.clear();
            return closed;
        };
        into.skip(consumed);
        if let Some((stream_type, index)) = identified {
            if let Some(handle) = self.pending_data_streams.remove(&state.id) {
                send_stream_msg(self.acceptor_pid, handle, stream_type, index, conn_id);
            }
        }
        closed
    }

    fn write(&mut self, state: &mut StreamState<QuicWire>) -> Result<Written, CloseReason> {
        if state.write_buffer.is_empty() {
            return Ok(Written::Took(0));
        }
        let Some(conn_state) = self.connections.get_mut(&state.wire.ch) else {
            return Ok(Written::Took(0));
        };
        match conn_state
            .connection
            .send_stream(state.wire.qstream_id)
            .write(&state.write_buffer)
        {
            Ok(written) => {
                state.write_buffer.drain(..written);
                Ok(Written::Took(written))
            }
            // The peer's flow-control window is full: the protocol has already
            // computed the backpressure, so the gate is raised on its word
            // rather than on a byte count saying the same thing.
            Err(quinn_proto::WriteError::Blocked) => Ok(Written::Blocked),
            Err(_) => Err(CloseReason::WriteError),
        }
    }

    /// A QUIC write reaches a connection, not the medium: every connection's
    /// pending transmits go out here.
    fn flush(&mut self) {
        for conn_state in self.connections.values_mut() {
            let _ = flush_transmits(
                &mut conn_state.connection,
                &self.udp,
                &mut self.transmit_buf,
            );
        }
    }

    fn release(&mut self, state: &mut StreamState<QuicWire>, ended: Ended) {
        self.nif_stream_map.remove(&state.id);
        self.pending_data_streams.remove(&state.id);
        let Some(conn_state) = self.connections.get_mut(&state.wire.ch) else {
            return;
        };
        conn_state.streams.remove(&state.wire.qstream_id);
        if ended == Ended::ByRequest {
            // Only a close the NIF asked for is worth telling the peer: a wire
            // that ended the stream itself has nothing left to hear.
            let _ = conn_state
                .connection
                .recv_stream(state.wire.qstream_id)
                .stop(quinn_proto::VarInt::from_u32(0));
            let _ = conn_state
                .connection
                .send_stream(state.wire.qstream_id)
                .finish();
        }
    }

    /// Close every connection and flush its closing transmits; the streams go
    /// with them.
    fn shutdown(&mut self, _streams: &mut Streams<QuicWire>) {
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
}

/// The leaf certificate the peer presented, DER-encoded, or `None` when it
/// presented none.
///
/// `peer_identity` answers the whole verified chain and only after the
/// handshake completes; the leaf is the one that carries the peer's own SPIFFE
/// SAN, so it is the one the connect path reads its namespace from. rustls has
/// already verified the chain against the configured trust anchor by the time a
/// stream can be accepted, so nothing here re-verifies it.
fn peer_leaf_der(connection: &quinn_proto::Connection) -> Option<Vec<u8>> {
    let identity = connection.crypto_session().peer_identity()?;
    let chain = identity
        .downcast::<Vec<rustls::pki_types::CertificateDer<'static>>>()
        .ok()?;
    chain.first().map(|leaf| leaf.as_ref().to_vec())
}

/// Everything readable on a stream right now, into the loop's read buffer, and
/// the reason the wire gives for ending it.
///
/// `Ok(None)` from `Chunks::next` is the peer's FIN with every byte read, and
/// `Err(Blocked)` is "nothing more right now" — the two say different things
/// and are answered differently, so a peer ending a QUIC stream reports the
/// same close as a peer ending a UDS one.
fn read_chunks(
    connection: &mut quinn_proto::Connection,
    id: StreamId,
    nif_id: u64,
    into: &mut ReadBuf,
) -> Option<CloseReason> {
    let mut recv = connection.recv_stream(id);
    let mut chunks = match recv.read(true) {
        Ok(chunks) => chunks,
        Err(e) => {
            eprintln!("[flz_nif] QUIC read error on nif_id={}: {:?}", nif_id, e);
            return Some(CloseReason::ReadError);
        }
    };
    let mut closed = None;
    loop {
        match chunks.next(usize::MAX) {
            Ok(Some(chunk)) => into.append(&chunk.bytes),
            Ok(None) => {
                closed = Some(CloseReason::ConnectionClosed);
                break;
            }
            Err(quinn_proto::ReadError::Blocked) => break,
            Err(quinn_proto::ReadError::Reset(_)) => {
                closed = Some(CloseReason::StreamReset);
                break;
            }
        }
    }
    let _ = chunks.finalize();
    closed
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

/// What the conformance table drives the QUIC half of the contract against.
#[cfg(test)]
pub(super) mod conformance {
    use super::*;

    /// A QUIC wire with no socket traffic behind it, holding one already
    /// accepted connection carrying one stream.
    pub(crate) fn wire(
        ch: ConnectionHandle,
        connection: quinn_proto::Connection,
        qstream_id: StreamId,
        id: u64,
    ) -> (Quic, StreamState<QuicWire>) {
        let std_udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        std_udp.set_nonblocking(true).unwrap();
        let mut udp = UdpSocket::from_std(std_udp);
        let (poll, waker, cmd_tx, acceptor_pid) = super::super::support::detached_poll(&mut udp);

        let wire = Quic {
            acceptor_pid,
            cmd_tx,
            waker,
            poll,
            udp,
            events: Events::with_capacity(8),
            udp_buf: vec![0u8; 1024],
            endpoint: quinn_proto::Endpoint::new(
                Arc::new(EndpointConfig::default()),
                None,
                true,
                None,
            ),
            connections: HashMap::new(),
            nif_stream_map: HashMap::from([(id, (ch, qstream_id))]),
            next_nif_stream_id: id + 1,
            pending_data_streams: HashMap::new(),
            transmit_buf: Vec::new(),
        };
        let mut wire = wire;
        wire.adopt(ch, connection, qstream_id, id);
        let state = super::super::support::detached_stream(
            id,
            QuicWire {
                ch,
                qstream_id,
                type_byte_state: TypeByteState::ControlChannel,
            },
        );
        (wire, state)
    }

    /// The connection lives in the wire while the driver writes through it and
    /// back in `quic_pair::Pair` while the pair routes datagrams.
    impl Quic {
        pub(crate) fn surrender(&mut self) -> (ConnectionHandle, quinn_proto::Connection) {
            let ch = *self.connections.keys().next().expect("a connection");
            let state = self.connections.remove(&ch).unwrap();
            (ch, state.connection)
        }

        pub(crate) fn adopt(
            &mut self,
            ch: ConnectionHandle,
            connection: quinn_proto::Connection,
            qstream_id: StreamId,
            id: u64,
        ) {
            self.connections.insert(
                ch,
                ConnectionState {
                    connection,
                    streams: HashMap::from([(qstream_id, id)]),
                    stream_count: 1,
                    control_nif_id: id,
                },
            );
        }
    }
}
