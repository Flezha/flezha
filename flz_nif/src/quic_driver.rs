use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use crossbeam::channel::{Receiver, Sender};
use mio::net::UdpSocket;
use mio::{Events, Interest, Poll, Token, Waker};
use quinn_proto::{
    ConnectionHandle, DatagramEvent, Dir, EndpointConfig, Event, ServerConfig, StreamEvent,
    StreamId, Transmit,
};
use rustler::{Encoder, LocalPid, OwnedEnv, Term};

use crate::atoms;
use crate::frame_buffer::{FrameBuffer, ParsedFrame};
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
    active: bool,
    passive_queue: Vec<ParsedFrame>,
    write_buffer: Vec<u8>,
    type_byte_state: TypeByteState,
}

/// Per-QUIC-connection state.
struct ConnectionState {
    connection: quinn_proto::Connection,
    /// Maps quinn-proto StreamId → per-stream state.
    streams: HashMap<StreamId, QuicStreamState>,
    /// Number of bidi streams accepted on this connection.
    stream_count: u32,
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
    match init_and_run(
        bind_address,
        server_config,
        acceptor_pid,
        cmd_rx,
        cmd_tx,
        ready_tx,
    ) {
        Ok(()) => {}
        Err(e) => eprintln!("[flz_nif] QUIC driver fatal error: {}", e),
    }
}

fn init_and_run(
    bind_address: SocketAddr,
    server_config: ServerConfig,
    acceptor_pid: LocalPid,
    cmd_rx: Receiver<ServerCommand>,
    cmd_tx: Sender<ServerCommand>,
    ready_tx: Sender<io::Result<QuicDriverReady>>,
) -> io::Result<()> {
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

    driver_loop(
        bind_address,
        server_config,
        acceptor_pid,
        cmd_rx,
        cmd_tx,
        &waker,
        poll,
        udp,
    )
}

// reason: each arg is part of the QUIC driver's required wiring (poll, sockets, channels, pid); splitting would invent arbitrary bag-of-state structs
#[allow(clippy::too_many_arguments)]
fn driver_loop(
    _bind_address: SocketAddr,
    server_config: ServerConfig,
    acceptor_pid: LocalPid,
    cmd_rx: Receiver<ServerCommand>,
    cmd_tx: Sender<ServerCommand>,
    _waker: &Arc<Waker>,
    mut poll: Poll,
    udp: UdpSocket,
) -> io::Result<()> {
    let mut endpoint = quinn_proto::Endpoint::new(
        Arc::new(EndpointConfig::default()),
        Some(Arc::new(server_config)),
        true,
        None,
    );

    let mut events = Events::with_capacity(256);
    let mut connections: HashMap<ConnectionHandle, ConnectionState> = HashMap::new();
    let mut udp_buf = vec![0u8; 65536];
    let mut transmit_buf = Vec::new();

    // Maps NIF stream_id → (ConnectionHandle, StreamId) for command dispatch.
    let mut nif_stream_map: HashMap<u64, (ConnectionHandle, StreamId)> = HashMap::new();
    let mut next_nif_stream_id: u64 = 1;

    // Data streams pending type-byte identification (QUIC only).
    // Keyed by NIF stream_id.
    let mut pending_data_streams: HashMap<u64, rustler::ResourceArc<StreamHandle>> = HashMap::new();

    loop {
        // Compute poll timeout from all active connections.
        let now = Instant::now();
        let mut min_timeout = Duration::from_millis(50);
        for conn_state in connections.values_mut() {
            if let Some(deadline) = conn_state.connection.poll_timeout() {
                let remaining = deadline.saturating_duration_since(now);
                if remaining < min_timeout {
                    min_timeout = remaining;
                }
            }
        }

        let _ = poll.poll(&mut events, Some(min_timeout));
        let now = Instant::now();

        // Handle timers for all connections.
        for conn_state in connections.values_mut() {
            if let Some(deadline) = conn_state.connection.poll_timeout() {
                if now >= deadline {
                    conn_state.connection.handle_timeout(now);
                }
            }
        }

        // Read incoming UDP datagrams.
        loop {
            match udp.recv_from(&mut udp_buf) {
                Ok((n, remote)) => {
                    let data = BytesMut::from(&udp_buf[..n]);
                    let mut resp_buf = Vec::new();
                    if let Some(event) =
                        endpoint.handle(now, remote, None, None, data, &mut resp_buf)
                    {
                        match event {
                            DatagramEvent::ConnectionEvent(ch, event) => {
                                if let Some(conn_state) = connections.get_mut(&ch) {
                                    conn_state.connection.handle_event(event);
                                }
                            }
                            DatagramEvent::Response(transmit) => {
                                let _ = send_transmit(&udp, &transmit, &resp_buf);
                            }
                            DatagramEvent::NewConnection(incoming) => {
                                // New QUIC connection incoming.
                                // Accept new QUIC connection via the endpoint.
                                let mut accept_buf = Vec::new();
                                match endpoint.accept(incoming, now, &mut accept_buf, None) {
                                    Ok((ch, mut connection)) => {
                                        // Connection accepted.
                                        // Flush initial handshake packets.
                                        let _ = flush_connection_transmits(
                                            &mut connection,
                                            &udp,
                                            &mut transmit_buf,
                                        );

                                        connections.insert(
                                            ch,
                                            ConnectionState {
                                                connection,
                                                streams: HashMap::new(),
                                                stream_count: 0,
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
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }

        // Pump endpoint events and poll connection events.
        // We collect events first to avoid borrow conflicts with drain_readable.
        let conn_handles: Vec<ConnectionHandle> = connections.keys().copied().collect();
        let mut lost_connections: Vec<ConnectionHandle> = Vec::new();

        for ch in &conn_handles {
            let conn_state = match connections.get_mut(ch) {
                Some(c) => c,
                None => continue,
            };

            // Pump endpoint ↔ connection events.
            while let Some(endpoint_event) = conn_state.connection.poll_endpoint_events() {
                if let Some(conn_event) = endpoint.handle_event(*ch, endpoint_event) {
                    conn_state.connection.handle_event(conn_event);
                }
            }

            // Collect events to process (avoids borrow conflicts).
            let mut conn_events: Vec<Event> = Vec::new();
            while let Some(event) = conn_state.connection.poll() {
                conn_events.push(event);
            }

            for event in conn_events {
                // Re-borrow conn_state for each event since drain_readable may
                // need &mut connections for the same connection.
                match event {
                    Event::Connected => {}
                    Event::Stream(StreamEvent::Opened { dir: Dir::Bi }) => {
                        // Accept new bidi streams on this connection.
                        let conn_state = connections.get_mut(ch).unwrap();
                        let mut accepted_streams: Vec<StreamId> = Vec::new();
                        while let Some(stream_id) = conn_state.connection.streams().accept(Dir::Bi)
                        {
                            let nif_id = next_nif_stream_id;
                            next_nif_stream_id += 1;

                            // First stream per connection = control channel (no type prefix).
                            // Subsequent streams = data streams (2-byte type+index prefix).
                            let is_control = conn_state.stream_count == 0;
                            conn_state.stream_count += 1;

                            let type_byte_state = if is_control {
                                TypeByteState::ControlChannel
                            } else {
                                TypeByteState::AwaitingPrefix(Vec::with_capacity(2))
                            };

                            let owner = Arc::new(Mutex::new(acceptor_pid));

                            let stream_handle = rustler::ResourceArc::new(StreamHandle {
                                stream_id: nif_id,
                                cmd_tx: cmd_tx.clone(),
                                waker: Arc::clone(_waker),
                                owner: Arc::clone(&owner),
                            });

                            conn_state.streams.insert(
                                stream_id,
                                QuicStreamState {
                                    nif_stream_id: nif_id,
                                    owner,
                                    frame_buffer: FrameBuffer::new(),
                                    active: false,
                                    passive_queue: Vec::new(),
                                    write_buffer: Vec::new(),
                                    type_byte_state,
                                },
                            );

                            nif_stream_map.insert(nif_id, (*ch, stream_id));

                            // For control channel, send connection message immediately.
                            // For data streams, defer until type bytes are read.
                            if is_control {
                                // Control channel — notify Elixir immediately.
                                send_connection_msg(acceptor_pid, stream_handle);
                            } else {
                                // Data stream — defer notification until type prefix is read.
                                // Store the handle for later delivery once type is known.
                                pending_data_streams.insert(nif_id, stream_handle);
                            }

                            accepted_streams.push(stream_id);
                        }

                        // After accepting streams, immediately drain any data that was
                        // buffered in quinn-proto before the accept. quinn-proto may not
                        // fire a Readable event for data received before accept().
                        for accepted_id in accepted_streams {
                            drain_readable(
                                *ch,
                                accepted_id,
                                &mut connections,
                                &mut nif_stream_map,
                                &mut pending_data_streams,
                                acceptor_pid,
                            );
                        }
                    }
                    Event::Stream(StreamEvent::Readable { id }) => {
                        drain_readable(
                            *ch,
                            id,
                            &mut connections,
                            &mut nif_stream_map,
                            &mut pending_data_streams,
                            acceptor_pid,
                        );
                    }
                    Event::Stream(StreamEvent::Writable { id }) => {
                        let conn_state = connections.get_mut(ch).unwrap();
                        if let Some(stream_state) = conn_state.streams.get_mut(&id) {
                            flush_quic_write(&mut conn_state.connection, id, stream_state);
                        }
                    }
                    Event::Stream(StreamEvent::Finished { id }) => {
                        let conn_state = connections.get_mut(ch).unwrap();
                        if let Some(stream_state) = conn_state.streams.remove(&id) {
                            nif_stream_map.remove(&stream_state.nif_stream_id);
                            let owner = *stream_state.owner.lock().unwrap();
                            send_closed_msg(owner, stream_state.nif_stream_id, "stream_finished");
                        }
                    }
                    Event::ConnectionLost { reason } => {
                        // Connection lost — clean up all streams.
                        let conn_state = connections.get_mut(ch).unwrap();
                        for (_, stream_state) in conn_state.streams.drain() {
                            nif_stream_map.remove(&stream_state.nif_stream_id);
                            let owner = *stream_state.owner.lock().unwrap();
                            send_closed_msg(
                                owner,
                                stream_state.nif_stream_id,
                                &format!("connection_lost: {}", reason),
                            );
                        }
                        lost_connections.push(*ch);
                    }
                    _ => {}
                }
            }
        }

        // Remove lost connections.
        for ch in &lost_connections {
            connections.remove(ch);
        }

        // Flush outgoing packets for all connections.
        for ch in &conn_handles {
            if let Some(conn_state) = connections.get_mut(ch) {
                let _ =
                    flush_connection_transmits(&mut conn_state.connection, &udp, &mut transmit_buf);
            }
        }

        // Process NIF commands.
        while let Ok(cmd) = cmd_rx.try_recv() {
            match cmd {
                ServerCommand::SendData {
                    stream_id: nif_id,
                    data,
                } => {
                    if let Some(&(ch, qstream_id)) = nif_stream_map.get(&nif_id) {
                        if let Some(conn_state) = connections.get_mut(&ch) {
                            if let Some(stream_state) = conn_state.streams.get_mut(&qstream_id) {
                                stream_state.write_buffer.extend_from_slice(&data);
                                flush_quic_write(
                                    &mut conn_state.connection,
                                    qstream_id,
                                    stream_state,
                                );
                            }
                        }
                    }
                }
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
                    if let Some(&(ch, qstream_id)) = nif_stream_map.get(&nif_id) {
                        if let Some(conn_state) = connections.get_mut(&ch) {
                            if let Some(stream_state) = conn_state.streams.get_mut(&qstream_id) {
                                stream_state.write_buffer.extend_from_slice(&frame);
                                flush_quic_write(
                                    &mut conn_state.connection,
                                    qstream_id,
                                    stream_state,
                                );
                            }
                        }
                    }
                }
                ServerCommand::SetActive { stream_id: nif_id } => {
                    if let Some(&(ch, qstream_id)) = nif_stream_map.get(&nif_id) {
                        if let Some(conn_state) = connections.get_mut(&ch) {
                            if let Some(stream_state) = conn_state.streams.get_mut(&qstream_id) {
                                stream_state.active = true;
                                let owner = *stream_state.owner.lock().unwrap();
                                let queued: Vec<_> = stream_state.passive_queue.drain(..).collect();
                                for frame in queued {
                                    send_data_msg(owner, nif_id, frame);
                                }
                            }
                        }
                    }
                }
                ServerCommand::SetPassive { stream_id: nif_id } => {
                    if let Some(&(ch, qstream_id)) = nif_stream_map.get(&nif_id) {
                        if let Some(conn_state) = connections.get_mut(&ch) {
                            if let Some(stream_state) = conn_state.streams.get_mut(&qstream_id) {
                                stream_state.active = false;
                            }
                        }
                    }
                }
                ServerCommand::CloseStream { stream_id: nif_id } => {
                    if let Some((ch, qstream_id)) = nif_stream_map.remove(&nif_id) {
                        if let Some(conn_state) = connections.get_mut(&ch) {
                            conn_state.streams.remove(&qstream_id);
                            let _ = conn_state
                                .connection
                                .recv_stream(qstream_id)
                                .stop(quinn_proto::VarInt::from_u32(0));
                            let _ = conn_state.connection.send_stream(qstream_id).finish();
                        }
                    }
                }
                ServerCommand::UpdateOwner {
                    stream_id: nif_id,
                    new_owner,
                } => {
                    if let Some(&(ch, qstream_id)) = nif_stream_map.get(&nif_id) {
                        if let Some(conn_state) = connections.get_mut(&ch) {
                            if let Some(stream_state) = conn_state.streams.get_mut(&qstream_id) {
                                *stream_state.owner.lock().unwrap() = new_owner;
                            }
                        }
                    }
                }
                ServerCommand::Shutdown => {
                    let now = Instant::now();
                    for (_, mut conn_state) in connections.drain() {
                        conn_state.connection.close(
                            now,
                            quinn_proto::VarInt::from_u32(0),
                            b"shutdown".to_vec().into(),
                        );
                        let _ = flush_connection_transmits(
                            &mut conn_state.connection,
                            &udp,
                            &mut transmit_buf,
                        );
                    }
                    return Ok(());
                }
            }
        }

        // Flush pending writes for all streams across all connections.
        let conn_handles: Vec<ConnectionHandle> = connections.keys().copied().collect();
        for ch in conn_handles {
            if let Some(conn_state) = connections.get_mut(&ch) {
                let stream_ids: Vec<StreamId> = conn_state
                    .streams
                    .iter()
                    .filter(|(_, s)| !s.write_buffer.is_empty())
                    .map(|(&id, _)| id)
                    .collect();
                for qstream_id in stream_ids {
                    if let Some(stream_state) = conn_state.streams.get_mut(&qstream_id) {
                        flush_quic_write(&mut conn_state.connection, qstream_id, stream_state);
                    }
                }
                let _ =
                    flush_connection_transmits(&mut conn_state.connection, &udp, &mut transmit_buf);
            }
        }
    }
}

/// Drain readable data from a QUIC stream, feed into FrameBuffer, deliver frames.
/// For data streams, handles the 2-byte type+index prefix before switching to framed mode.
fn drain_readable(
    ch: ConnectionHandle,
    id: StreamId,
    connections: &mut HashMap<ConnectionHandle, ConnectionState>,
    nif_stream_map: &mut HashMap<u64, (ConnectionHandle, StreamId)>,
    pending_data_streams: &mut HashMap<u64, rustler::ResourceArc<StreamHandle>>,
    acceptor_pid: LocalPid,
) {
    let conn_state = match connections.get_mut(&ch) {
        Some(c) => c,
        None => return,
    };

    let stream_state = match conn_state.streams.get_mut(&id) {
        Some(s) => s,
        None => return,
    };

    let nif_id = stream_state.nif_stream_id;

    match conn_state.connection.recv_stream(id).read(true) {
        Ok(mut chunks) => {
            let mut all_data = Vec::new();
            loop {
                match chunks.next(usize::MAX) {
                    Ok(Some(chunk)) => {
                        all_data.extend_from_slice(&chunk.bytes);
                    }
                    Ok(None) => break,
                    Err(quinn_proto::ReadError::Blocked) => break,
                    Err(quinn_proto::ReadError::Reset(_)) => {
                        let _ = chunks.finalize();
                        let owner = *stream_state.owner.lock().unwrap();
                        send_closed_msg(owner, nif_id, "stream_reset");
                        conn_state.streams.remove(&id);
                        nif_stream_map.remove(&nif_id);
                        pending_data_streams.remove(&nif_id);
                        return;
                    }
                }
            }
            let _ = chunks.finalize();

            if all_data.is_empty() {
                return;
            }

            // Handle type-byte prefix for data streams.
            let data_for_framing = match &mut stream_state.type_byte_state {
                TypeByteState::ControlChannel => {
                    // Control channel — all data goes to FrameBuffer directly.
                    all_data.as_slice()
                }
                TypeByteState::AwaitingPrefix(buf) => {
                    // Accumulate until we have 2 bytes.
                    let need = 2 - buf.len();
                    let take = std::cmp::min(need, all_data.len());
                    buf.extend_from_slice(&all_data[..take]);

                    if buf.len() >= 2 {
                        let stream_type = buf[0];
                        let index = buf[1];
                        stream_state.type_byte_state = TypeByteState::DataStream {
                            _stream_type: stream_type,
                            _index: index,
                        };

                        // Type prefix consumed — deliver stream notification.
                        // Deliver the {:flz_stream, stream_ref, type, index} message.
                        if let Some(handle) = pending_data_streams.remove(&nif_id) {
                            send_stream_msg(acceptor_pid, handle, stream_type, index);
                        }

                        // Remaining data after the 2-byte prefix.
                        &all_data[take..]
                    } else {
                        // Still need more bytes for the prefix.
                        return;
                    }
                }
                TypeByteState::DataStream { .. } => {
                    // Prefix already consumed — all data goes to FrameBuffer.
                    all_data.as_slice()
                }
            };

            if !data_for_framing.is_empty() {
                let frames = stream_state.frame_buffer.feed(data_for_framing);
                let owner = *stream_state.owner.lock().unwrap();

                if stream_state.active {
                    for frame in frames {
                        send_data_msg(owner, nif_id, frame);
                    }
                } else {
                    stream_state.passive_queue.extend(frames);
                }
            }
        }
        Err(e) => {
            eprintln!("[flz_nif] QUIC read error on nif_id={}: {:?}", nif_id, e);
            let owner = *stream_state.owner.lock().unwrap();
            send_closed_msg(owner, nif_id, "read_error");
            conn_state.streams.remove(&id);
            nif_stream_map.remove(&nif_id);
            pending_data_streams.remove(&nif_id);
        }
    }
}

/// Attempt to flush buffered write data to a QUIC stream.
fn flush_quic_write(
    connection: &mut quinn_proto::Connection,
    qstream_id: StreamId,
    stream_state: &mut QuicStreamState,
) {
    if stream_state.write_buffer.is_empty() {
        return;
    }

    match connection
        .send_stream(qstream_id)
        .write(&stream_state.write_buffer)
    {
        Ok(written) => {
            stream_state.write_buffer.drain(..written);
        }
        Err(quinn_proto::WriteError::Blocked) => {
            // Keep buffer, wait for Writable event.
        }
        Err(_) => {
            // Write error — discard buffer.
            stream_state.write_buffer.clear();
        }
    }
}

/// Flush all pending transmits from a connection to the UDP socket.
fn flush_connection_transmits(
    connection: &mut quinn_proto::Connection,
    udp: &UdpSocket,
    buf: &mut Vec<u8>,
) -> io::Result<()> {
    let now = Instant::now();
    while let Some(transmit) = connection.poll_transmit(now, 1, buf) {
        send_transmit(udp, &transmit, buf)?;
        buf.clear();
    }
    Ok(())
}

/// Send a single UDP datagram.
fn send_transmit(udp: &UdpSocket, transmit: &Transmit, buf: &[u8]) -> io::Result<()> {
    let data = &buf[..transmit.size];
    match udp.send_to(data, transmit.destination) {
        Ok(_) => Ok(()),
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(()),
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// Erlang message helpers (same pattern as uds_driver.rs)
// ---------------------------------------------------------------------------

fn send_data_msg(owner: LocalPid, stream_id: u64, frame: ParsedFrame) {
    let mut env = OwnedEnv::new();
    let _ = env.send_and_clear(&owner, |env| {
        let metadata_pairs: Vec<(Term, Term)> = frame
            .metadata
            .iter()
            .map(|(k, v)| (k.as_str().encode(env), v.as_str().encode(env)))
            .collect();

        let metadata_term = Term::map_from_pairs(env, &metadata_pairs)
            .unwrap_or_else(|_| rustler::types::atom::nil().encode(env));

        let mut msg_binary = rustler::NewBinary::new(env, frame.message.len());
        msg_binary.as_mut_slice().copy_from_slice(&frame.message);

        rustler::types::tuple::make_tuple(
            env,
            &[
                atoms::flz_data().encode(env),
                stream_id.encode(env),
                frame.payload_id.as_str().encode(env),
                rustler::Binary::from(msg_binary).encode(env),
                metadata_term,
            ],
        )
    });
}

fn send_closed_msg(owner: LocalPid, stream_id: u64, reason: &str) {
    let reason_owned = reason.to_string();
    let mut env = OwnedEnv::new();
    let _ = env.send_and_clear(&owner, |env| {
        rustler::types::tuple::make_tuple(
            env,
            &[
                atoms::flz_closed().encode(env),
                stream_id.encode(env),
                reason_owned.as_str().encode(env),
            ],
        )
    });
}

fn send_connection_msg(acceptor: LocalPid, stream_handle: rustler::ResourceArc<StreamHandle>) {
    let mut env = OwnedEnv::new();
    let _ = env.send_and_clear(&acceptor, |env| {
        rustler::types::tuple::make_tuple(
            env,
            &[
                atoms::flz_connection().encode(env),
                stream_handle.encode(env),
            ],
        )
    });
}

/// Send {:flz_stream, stream_handle, type, index} for QUIC data streams.
fn send_stream_msg(
    acceptor: LocalPid,
    stream_handle: rustler::ResourceArc<StreamHandle>,
    stream_type: u8,
    index: u8,
) {
    let mut env = OwnedEnv::new();
    let _ = env.send_and_clear(&acceptor, |env| {
        rustler::types::tuple::make_tuple(
            env,
            &[
                atoms::flz_stream().encode(env),
                stream_handle.encode(env),
                (stream_type as u64).encode(env),
                (index as u64).encode(env),
            ],
        )
    });
}

/// Build a rustls ServerConfig and wrap it into a quinn-proto ServerConfig.
pub fn build_quic_server_config(cert_path: &str, key_path: &str) -> io::Result<ServerConfig> {
    // Read certificate chain.
    let cert_pem = std::fs::read(cert_path)?;
    let mut cert_reader = io::BufReader::new(&cert_pem[..]);
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        rustls_pemfile::certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "No certificates found in cert file",
        ));
    }

    // Read private key.
    let key_pem = std::fs::read(key_path)?;
    let mut key_reader = io::BufReader::new(&key_pem[..]);
    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "No private key found in key file",
            )
        })?;

    // Build rustls server config.
    let mut server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    server_crypto.alpn_protocols = vec![b"flezha".to_vec()];

    let quic_server_config = quinn_proto::crypto::rustls::QuicServerConfig::try_from(server_crypto)
        .map_err(io::Error::other)?;

    let mut server_config = ServerConfig::with_crypto(Arc::new(quic_server_config));

    // Transport config matching client settings.
    let mut transport = quinn_proto::TransportConfig::default();
    transport.keep_alive_interval(Some(Duration::from_secs(5)));
    transport.stream_receive_window(8_388_608u32.into());
    transport.receive_window(16_777_216u32.into());
    transport.send_window(8_388_608);
    // Allow enough bidi streams for control + data streams.
    transport.max_concurrent_bidi_streams(128u32.into());
    server_config.transport_config(Arc::new(transport));

    Ok(server_config)
}
