use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use crossbeam::channel;
use mio::net::UdpSocket;
use mio::{Events, Interest, Poll, Token, Waker};
use quinn_proto::{
    ClientConfig, ConnectionHandle, DatagramEvent, Dir, EndpointConfig, Event, IdleTimeout,
    StreamEvent, StreamId, Transmit,
};

use super::send_gate::SendGate;

const UDP_TOKEN: Token = Token(0);
const WAKER_TOKEN: Token = Token(1);

/// How many chunks a stream's reader may fall behind by before the driver
/// suspends that stream. Production and every test slot take the bound from
/// here, so a fixture cannot quietly run against a channel that never fills.
const CHUNK_CHANNEL_DEPTH: usize = 64;

enum DriverCommand {
    OpenStream {
        read_tx: channel::Sender<Bytes>,
        reply: channel::Sender<io::Result<StreamId>>,
    },
    Write {
        stream_id: StreamId,
        data: Vec<u8>,
        reply: channel::Sender<io::Result<usize>>,
        /// Take the frame now or refuse it, never wait for the buffer to drain.
        nowait: bool,
    },
    Shutdown,
}

struct StreamSlot {
    read_tx: channel::Sender<Bytes>,
    write_buf: Vec<u8>,
    /// Writers whose bytes are in `write_buf`, in the order they appended.
    /// Every one is answered when the buffer drains: a writer blocks on its
    /// reply, so this is where the backpressure of a stalled peer is felt.
    pending_write_replies: Vec<(usize, channel::Sender<io::Result<usize>>)>,
    /// The ceiling and its counter, in the one form every send buffer in the
    /// tree shares. The gate's raised bit is unused here: this stream signals
    /// congestion by withholding the reply, which is a stronger signal.
    gate: SendGate,
    /// The chunk a full reader channel would not take, held until it has room.
    /// It is `write_buf` in the other direction: what the far end will not
    /// take yet waits in the slot, and the protocol carries the signal.
    held_chunk: Option<Bytes>,
}

impl StreamSlot {
    /// The slot a freshly opened stream starts with.
    fn new(read_tx: channel::Sender<Bytes>) -> Self {
        Self {
            read_tx,
            write_buf: Vec::new(),
            pending_write_replies: Vec::new(),
            gate: SendGate::new(),
            held_chunk: None,
        }
    }

    /// Hand one chunk to this stream's reader, and answer whether draining may
    /// go on. `try_send` and never `send`: a full channel is a reader that is
    /// behind, and waiting for it stops the one loop that also expires this
    /// connection's timers, pumps its socket, takes commands and serves every
    /// other stream on it. A refused chunk is held rather than dropped,
    /// because a QUIC stream is ordered bytes and a hole in it cannot be
    /// repaired, and the caller stops draining: the rest stays in the window
    /// QUIC's own flow control backpressures the peer.
    fn offer_chunk(&mut self, chunk: Bytes) -> bool {
        match self.read_tx.try_send(chunk) {
            Ok(()) => true,
            Err(channel::TrySendError::Full(chunk)) => {
                self.held_chunk = Some(chunk);
                false
            }
            // The reader is gone; there is nobody left to drain this for.
            Err(channel::TrySendError::Disconnected(_)) => false,
        }
    }
}

struct ChannelReader {
    rx: channel::Receiver<Bytes>,
    buf: Bytes,
    // Honors `FlezhaStream::set_read_timeout` on QUIC: when set, a read that
    // waits longer than this for the next chunk returns `WouldBlock` instead
    // of blocking forever, so control-channel liveness works over QUIC.
    read_timeout: Option<Duration>,
    // The driver's waker, so a read that makes room in a full channel resumes
    // the stream the driver suspended. `None` outside a real connection.
    waker: Option<Arc<Waker>>,
}

impl Read for ChannelReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        // Serve from buffered chunk if available
        if !self.buf.is_empty() {
            let n = std::cmp::min(out.len(), self.buf.len());
            out[..n].copy_from_slice(&self.buf[..n]);
            self.buf = self.buf.slice(n..);
            return Ok(n);
        }

        // Buffer exhausted — wait for next chunk from driver, honoring the
        // read deadline if one is armed.
        let chunk = match self.read_timeout {
            Some(d) => match self.rx.recv_timeout(d) {
                Ok(c) => c,
                Err(channel::RecvTimeoutError::Timeout) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "QUIC read timed out",
                    ))
                }
                Err(channel::RecvTimeoutError::Disconnected) => return Ok(0),
            },
            None => match self.rx.recv() {
                Ok(c) => c,
                Err(_) => return Ok(0), // Channel closed = stream finished
            },
        };
        // A chunk taken from a full channel makes room for the one the driver
        // holds, so the driver is woken rather than left to the 5 s keep-alive.
        // Asked after the wait, not before it: a burst can fill the channel
        // while this read waits. Only the driver adds to the channel and only
        // this read takes from it, so a full one is never missed.
        if self.rx.len() + 1 >= CHUNK_CHANNEL_DEPTH {
            if let Some(waker) = &self.waker {
                let _ = waker.wake();
            }
        }

        let n = std::cmp::min(out.len(), chunk.len());
        out[..n].copy_from_slice(&chunk[..n]);
        if n < chunk.len() {
            self.buf = chunk.slice(n..);
        }
        Ok(n)
    }
}

/// The transport settings both ends of a Flezha QUIC connection run: a 5 s
/// keep-alive and an idle timeout aligned to the control-liveness window, so
/// a wedged peer is declared dead in the same window on QUIC as on UDS
/// instead of inheriting the quinn default (keep-alive << idle, so a live
/// connection never idles out), plus the receive/send windows. The server
/// adds its stream limit on top.
pub fn transport_config() -> quinn_proto::TransportConfig {
    let mut transport = quinn_proto::TransportConfig::default();
    transport.keep_alive_interval(Some(Duration::from_secs(5)));
    if let Ok(idle) = IdleTimeout::try_from(crate::transport::liveness::control_idle_timeout()) {
        transport.max_idle_timeout(Some(idle));
    }
    transport.stream_receive_window(8_388_608u32.into());
    transport.receive_window(16_777_216u32.into());
    transport.send_window(8_388_608);
    transport
}

/// Holds a QUIC connection driven by a background I/O thread using quinn-proto.
pub struct QuicClient {
    cmd_tx: channel::Sender<DriverCommand>,
    waker: Arc<Waker>,
    _driver_handle: thread::JoinHandle<()>,
}

impl QuicClient {
    /// Connect to a QUIC server at the given address using the CA cert for
    /// verification. When the configured client cert/key pair exists on disk
    /// it is presented for mutual TLS — the server requires it by default.
    /// Dial one already-resolved address. Resolution is the caller's, and
    /// happens per attempt: an address parsed once at startup is the failure
    /// the candidate list exists to remove.
    pub fn connect(addr: SocketAddr, ca_cert_path: &str) -> io::Result<QuicClient> {
        let mut client_config = ClientConfig::new(Arc::new(
            quinn_proto::crypto::rustls::QuicClientConfig::try_from(
                super::tls::build_quic_client_crypto(ca_cert_path)?,
            )
            .map_err(io::Error::other)?,
        ));
        client_config.transport_config(Arc::new(transport_config()));

        // --- UDP socket + mio Poll ---
        let std_udp = std::net::UdpSocket::bind("0.0.0.0:0")?;
        std_udp.set_nonblocking(true)?;
        let mut udp = UdpSocket::from_std(std_udp);

        let mut poll = Poll::new()?;
        poll.registry()
            .register(&mut udp, UDP_TOKEN, Interest::READABLE | Interest::WRITABLE)?;
        let waker = Arc::new(Waker::new(poll.registry(), WAKER_TOKEN)?);

        // --- quinn-proto Endpoint + Connection ---
        let mut endpoint =
            quinn_proto::Endpoint::new(Arc::new(EndpointConfig::default()), None, true, None);

        let (conn_handle, mut connection) = endpoint
            .connect(Instant::now(), client_config, addr, "localhost")
            .map_err(|e| io::Error::other(format!("QUIC connect error: {}", e)))?;

        // --- Blocking handshake ---
        drive_handshake(&mut poll, &udp, &mut endpoint, conn_handle, &mut connection)?;

        // --- Spawn driver thread ---
        let (cmd_tx, cmd_rx) = channel::unbounded::<DriverCommand>();
        let waker_clone = Arc::clone(&waker);

        let driver_handle = thread::Builder::new()
            .name("quic-io-driver".into())
            .spawn(move || {
                driver_loop(poll, udp, endpoint, conn_handle, connection, cmd_rx);
            })
            .map_err(io::Error::other)?;

        Ok(QuicClient {
            cmd_tx,
            waker: waker_clone,
            _driver_handle: driver_handle,
        })
    }

    /// Open a new bidirectional stream on this connection.
    pub fn open_bidi_stream(&self) -> io::Result<QuicBidiStream> {
        let (read_tx, read_rx) = channel::bounded::<Bytes>(CHUNK_CHANNEL_DEPTH);
        let (reply_tx, reply_rx) = channel::bounded::<io::Result<StreamId>>(1);

        self.cmd_tx
            .send(DriverCommand::OpenStream {
                read_tx,
                reply: reply_tx,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Driver thread gone"))?;
        self.waker.wake().map_err(io::Error::other)?;

        let stream_id = reply_rx
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Driver reply dropped"))??;

        Ok(QuicBidiStream {
            stream_id,
            cmd_tx: self.cmd_tx.clone(),
            waker: Arc::clone(&self.waker),
            read_rx: Arc::new(Mutex::new(ChannelReader {
                rx: read_rx,
                buf: Bytes::new(),
                read_timeout: None,
                waker: Some(Arc::clone(&self.waker)),
            })),
        })
    }
}

impl Drop for QuicClient {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(DriverCommand::Shutdown);
        let _ = self.waker.wake();
    }
}

/// A bidirectional QUIC stream that implements std::io::Read and Write.
///
/// All QUIC I/O is driven by a background driver thread using quinn-proto.
/// App threads communicate with the driver via crossbeam channels.
pub struct QuicBidiStream {
    stream_id: StreamId,
    cmd_tx: channel::Sender<DriverCommand>,
    waker: Arc<Waker>,
    read_rx: Arc<Mutex<ChannelReader>>,
}

impl QuicBidiStream {
    /// Arm (or clear) the read deadline. Shared with all clones via the
    /// `Arc<Mutex<ChannelReader>>`, so the control reader and any writer see
    /// one deadline. Backs `FlezhaStream::set_read_timeout` on QUIC.
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.read_rx
            .lock()
            .map_err(|e| io::Error::other(format!("Lock poisoned: {}", e)))?
            .read_timeout = timeout;
        Ok(())
    }

    /// One frame onto the stream, and the answer the driver sends back for it.
    /// The two `Write` impls are one call here, so a writer holding an
    /// `Arc<FlezhaStream>` and one holding the handle cannot drift.
    fn write_frame(&self, buf: &[u8]) -> io::Result<usize> {
        self.command_write(buf, false)
    }

    /// One frame the stream takes at once or refuses with `WouldBlock`: the
    /// caller waits for the driver's turn, never for the peer.
    pub fn try_write_frame(&self, buf: &[u8]) -> io::Result<usize> {
        self.command_write(buf, true)
    }

    fn command_write(&self, buf: &[u8], nowait: bool) -> io::Result<usize> {
        let (reply_tx, reply_rx) = channel::bounded(1);
        self.cmd_tx
            .send(DriverCommand::Write {
                stream_id: self.stream_id,
                data: buf.to_vec(),
                reply: reply_tx,
                nowait,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "QUIC write channel closed"))?;
        self.waker.wake().map_err(io::Error::other)?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Driver reply dropped"))?
    }

    /// Clone by sharing the write channel and read channel. Infallible: the
    /// handle shares channels, it opens nothing.
    pub fn clone_handle(&self) -> QuicBidiStream {
        QuicBidiStream {
            stream_id: self.stream_id,
            cmd_tx: self.cmd_tx.clone(),
            waker: Arc::clone(&self.waker),
            read_rx: Arc::clone(&self.read_rx),
        }
    }
}

impl Read for QuicBidiStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_rx
            .lock()
            .map_err(|e| io::Error::other(format!("Lock poisoned: {}", e)))?
            .read(buf)
    }
}

impl Write for QuicBidiStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_frame(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// Read/Write for &QuicBidiStream (needed for Arc<FlezhaStream>)
impl Read for &QuicBidiStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_rx
            .lock()
            .map_err(|e| io::Error::other(format!("Lock poisoned: {}", e)))?
            .read(buf)
    }
}

impl Write for &QuicBidiStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_frame(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn drive_handshake(
    poll: &mut Poll,
    udp: &UdpSocket,
    endpoint: &mut quinn_proto::Endpoint,
    conn_handle: ConnectionHandle,
    connection: &mut quinn_proto::Connection,
) -> io::Result<()> {
    let mut events = Events::with_capacity(64);
    let mut udp_buf = vec![0u8; 65536];
    let mut transmit_buf = Vec::new();

    // Flush initial handshake packets
    flush_transmits(connection, udp, &mut transmit_buf)?;

    loop {
        poll.poll(&mut events, Some(poll_timeout(connection)))?;
        let now = Instant::now();
        expire_timer(connection, now);
        pump_udp(udp, &mut udp_buf, endpoint, conn_handle, connection, now)?;
        pump_endpoint_events(endpoint, conn_handle, connection);

        // Flush outgoing packets
        flush_transmits(connection, udp, &mut transmit_buf)?;

        // Check for Connected event
        while let Some(event) = connection.poll() {
            match event {
                Event::Connected => return Ok(()),
                Event::ConnectionLost { reason } => {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionRefused,
                        format!("QUIC handshake failed: {}", reason),
                    ));
                }
                _ => {}
            }
        }
    }
}

fn driver_loop(
    mut poll: Poll,
    udp: UdpSocket,
    mut endpoint: quinn_proto::Endpoint,
    conn_handle: ConnectionHandle,
    mut connection: quinn_proto::Connection,
    cmd_rx: channel::Receiver<DriverCommand>,
) {
    let mut events = Events::with_capacity(256);
    let mut udp_buf = vec![0u8; 65536];
    let mut transmit_buf = Vec::new();
    let mut streams: HashMap<StreamId, StreamSlot> = HashMap::new();

    loop {
        let _ = poll.poll(&mut events, Some(poll_timeout(&mut connection)));
        let now = Instant::now();
        expire_timer(&mut connection, now);
        let _ = pump_udp(
            &udp,
            &mut udp_buf,
            &mut endpoint,
            conn_handle,
            &mut connection,
            now,
        );
        while let Ok(cmd) = cmd_rx.try_recv() {
            if handle_command(cmd, &mut connection, &mut streams, now) {
                let _ = flush_transmits(&mut connection, &udp, &mut transmit_buf);
                return;
            }
        }
        pump_endpoint_events(&mut endpoint, conn_handle, &mut connection);
        if dispatch_stream_events(&mut connection, &mut streams) {
            return;
        }
        resume_suspended_streams(&mut connection, &mut streams);
        let _ = flush_transmits(&mut connection, &udp, &mut transmit_buf);
    }
}

/// How long to poll: until the connection's next timer, 100 ms at most.
fn poll_timeout(connection: &mut quinn_proto::Connection) -> Duration {
    connection
        .poll_timeout()
        .map(|deadline| deadline.saturating_duration_since(Instant::now()))
        .unwrap_or(Duration::from_millis(100))
}

/// Fire the connection's timer if it has come due.
pub fn expire_timer(connection: &mut quinn_proto::Connection, now: Instant) {
    if let Some(deadline) = connection.poll_timeout() {
        if now >= deadline {
            connection.handle_timeout(now);
        }
    }
}

/// Read every datagram the socket holds into the endpoint, feeding this
/// connection's events to it and answering stateless responses on the spot.
/// Shared by the handshake and the steady-state loop.
pub fn pump_udp(
    udp: &UdpSocket,
    udp_buf: &mut [u8],
    endpoint: &mut quinn_proto::Endpoint,
    conn_handle: ConnectionHandle,
    connection: &mut quinn_proto::Connection,
    now: Instant,
) -> io::Result<()> {
    loop {
        match udp.recv_from(udp_buf) {
            Ok((n, remote)) => {
                let data = BytesMut::from(&udp_buf[..n]);
                let mut resp_buf = Vec::new();
                if let Some(event) = endpoint.handle(now, remote, None, None, data, &mut resp_buf) {
                    match event {
                        DatagramEvent::ConnectionEvent(ch, event) => {
                            if ch == conn_handle {
                                connection.handle_event(event);
                            }
                        }
                        DatagramEvent::Response(transmit) => {
                            send_transmit(udp, &transmit, &resp_buf)?;
                        }
                        DatagramEvent::NewConnection(_) => {}
                    }
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}

/// One app-thread command. `true` means the driver was asked to shut down:
/// the connection is closed and the caller flushes and exits.
fn handle_command(
    cmd: DriverCommand,
    connection: &mut quinn_proto::Connection,
    streams: &mut HashMap<StreamId, StreamSlot>,
    now: Instant,
) -> bool {
    match cmd {
        DriverCommand::OpenStream { read_tx, reply } => match connection.streams().open(Dir::Bi) {
            Some(stream_id) => {
                streams.insert(stream_id, StreamSlot::new(read_tx));
                let _ = reply.send(Ok(stream_id));
            }
            None => {
                let _ = reply.send(Err(io::Error::other("Stream limit reached")));
            }
        },
        DriverCommand::Write {
            stream_id,
            data,
            reply,
            nowait,
        } => match streams.get_mut(&stream_id) {
            Some(slot) if nowait => write_or_refuse(connection, stream_id, slot, data, reply),
            Some(slot) => write_or_buffer(connection, stream_id, slot, data, reply),
            None => {
                let _ = reply.send(Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "Unknown stream ID",
                )));
            }
        },
        DriverCommand::Shutdown => {
            connection.close(
                now,
                quinn_proto::VarInt::from_u32(0),
                b"shutdown".to_vec().into(),
            );
            return true;
        }
    }
    false
}

/// Write an app thread's bytes onto its stream, buffering what the stream will
/// not take now and deferring the reply until the buffer drains.
///
/// Two rules make several writers on one stream safe, and this is the only
/// place either can be stated. **Bytes already buffered are bytes this frame
/// queues behind**: writing past them would put this frame on the wire ahead
/// of the last one's remainder. And **a buffered writer is appended, never
/// replaced** — its reply sender is kept beside its bytes, so it is answered
/// rather than dropped.
///
/// Only the ceiling can refuse, and it answers the caller with an error: a
/// `Write` that returns `Ok(n)` has promised `n` bytes were accepted, so it
/// may never be the answer to a frame this function discarded.
fn write_or_buffer(
    connection: &mut quinn_proto::Connection,
    stream_id: StreamId,
    slot: &mut StreamSlot,
    data: Vec<u8>,
    reply: channel::Sender<io::Result<usize>>,
) {
    let total_len = data.len();

    if !slot.write_buf.is_empty() {
        if slot.gate.offer(&mut slot.write_buf, &data) {
            slot.pending_write_replies.push((total_len, reply));
        } else {
            let _ = reply.send(Err(refused_at_ceiling(slot.write_buf.len())));
        }
        return;
    }

    match connection.send_stream(stream_id).write(&data) {
        Ok(written) if written == total_len => {
            let _ = reply.send(Ok(total_len));
        }
        // Partially taken, or the peer's flow-control window is shut: buffer
        // what is left and answer once it has gone out.
        Ok(written) => {
            slot.write_buf.extend_from_slice(&data[written..]);
            slot.pending_write_replies.push((total_len, reply));
        }
        Err(quinn_proto::WriteError::Blocked) => {
            slot.write_buf.extend_from_slice(&data);
            slot.pending_write_replies.push((total_len, reply));
        }
        Err(e) => {
            let _ = reply.send(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!("QUIC write error: {}", e),
            )));
        }
    }
}

/// [`write_or_buffer`] for a writer that must not wait: refused with
/// `WouldBlock` when bytes are already queued or the window is shut. A frame
/// the window took part of is queued whole and answered at once, because half
/// a frame cannot be taken back off the stream.
fn write_or_refuse(
    connection: &mut quinn_proto::Connection,
    stream_id: StreamId,
    slot: &mut StreamSlot,
    data: Vec<u8>,
    reply: channel::Sender<io::Result<usize>>,
) {
    let answer = if !slot.write_buf.is_empty() {
        Err(io::ErrorKind::WouldBlock.into())
    } else {
        match connection.send_stream(stream_id).write(&data) {
            Ok(written) => {
                slot.write_buf.extend_from_slice(&data[written..]);
                Ok(data.len())
            }
            Err(quinn_proto::WriteError::Blocked) => Err(io::ErrorKind::WouldBlock.into()),
            Err(e) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!("QUIC write error: {}", e),
            )),
        }
    };
    let _ = reply.send(answer);
}

/// The one answer a refused frame gets. `WouldBlock` and not `BrokenPipe`:
/// the stream is healthy, the peer is behind, and the caller may try again.
fn refused_at_ceiling(buffered: usize) -> io::Error {
    tracing::warn!(
        target: "flezha::transport",
        transport = "quic",
        buffered,
        "QUIC send buffer at its ceiling; frame refused"
    );
    io::Error::new(
        io::ErrorKind::WouldBlock,
        "QUIC send buffer at its ceiling; frame not written",
    )
}

/// Serve every event the connection has for its streams. `true` when the
/// connection was lost and the driver should exit.
fn dispatch_stream_events(
    connection: &mut quinn_proto::Connection,
    streams: &mut HashMap<StreamId, StreamSlot>,
) -> bool {
    while let Some(event) = connection.poll() {
        match event {
            Event::Stream(StreamEvent::Readable { id }) => {
                drain_readable(connection, id, streams);
            }
            Event::Stream(StreamEvent::Writable { id }) => {
                flush_write_buffer(connection, id, streams);
            }
            Event::ConnectionLost { reason } => {
                tracing::warn!(
                    target: "flezha::transport",
                    transport = "quic",
                    "QUIC connection lost; driver thread exiting: {}", reason
                );
                return true;
            }
            _ => {}
        }
    }
    false
}

/// Drain readable data from a stream and push to the channel, dropping the
/// slot the moment the stream itself has ended.
///
/// `Ok(None)` from `Chunks::next` is the peer's FIN with every byte read and a
/// reset ends the stream too, while `Err(Blocked)` is only "nothing more right
/// now": they say different things and only the first two end anything. Ending
/// it drops the chunk sender with the slot, and that disconnect is the only way
/// a reader on ONE stream of a live connection hears that its stream is gone.
/// Left in the map, that reader waits for a frame the peer will never send
/// until the connection itself dies.
///
/// Draining stops the moment the reader will not take another chunk, and what
/// is not read stays in the receive window: see [`StreamSlot::offer_chunk`].
fn drain_readable(
    connection: &mut quinn_proto::Connection,
    id: StreamId,
    streams: &mut HashMap<StreamId, StreamSlot>,
) {
    let slot = match streams.get_mut(&id) {
        Some(s) => s,
        None => return,
    };
    // Whatever an earlier pass held goes first, or this stream's bytes arrive
    // out of the order the peer wrote them in.
    if let Some(held) = slot.held_chunk.take() {
        if !slot.offer_chunk(held) {
            return;
        }
    }

    let mut ended = false;
    if let Ok(mut chunks) = connection.recv_stream(id).read(true) {
        loop {
            match chunks.next(usize::MAX) {
                Ok(Some(chunk)) => {
                    if !slot.offer_chunk(chunk.bytes) {
                        break; // Suspended, or the consumer is gone
                    }
                }
                Ok(None) => {
                    ended = true; // The peer's FIN, every byte read
                    break;
                }
                Err(quinn_proto::ReadError::Blocked) => break,
                Err(e) => {
                    tracing::warn!(
                        target: "flezha::transport",
                        transport = "quic", stream_id = ?id,
                        "QUIC stream read error: {}", e
                    );
                    ended = true; // A reset ends the stream as surely as a FIN
                    break;
                }
            }
        }
        let _ = chunks.finalize();
    }
    if ended {
        streams.remove(&id);
    }
}

/// Retry every stream a full chunk channel suspended. A suspended stream has
/// no event of its own to wait for, so the loop asks on every pass; a reader
/// that has made room is served at once rather than at the connection's next
/// timer, which the 5 s keep-alive can put seconds away.
fn resume_suspended_streams(
    connection: &mut quinn_proto::Connection,
    streams: &mut HashMap<StreamId, StreamSlot>,
) {
    let suspended: Vec<StreamId> = streams
        .iter()
        .filter(|(_, slot)| slot.held_chunk.is_some())
        .map(|(id, _)| *id)
        .collect();
    for id in suspended {
        drain_readable(connection, id, streams);
    }
}

/// Flush buffered write data for a stream that became writable.
fn flush_write_buffer(
    connection: &mut quinn_proto::Connection,
    id: StreamId,
    streams: &mut HashMap<StreamId, StreamSlot>,
) {
    let slot = match streams.get_mut(&id) {
        Some(s) => s,
        None => return,
    };

    if slot.write_buf.is_empty() {
        return;
    }

    match connection.send_stream(id).write(&slot.write_buf) {
        Ok(written) => {
            slot.write_buf.drain(..written);
            // An empty buffer means every appended writer's bytes have gone
            // out, in the order they appended them — so every one is owed its
            // answer, not just the last.
            if slot.write_buf.is_empty() {
                for (total, reply) in slot.pending_write_replies.drain(..) {
                    let _ = reply.send(Ok(total));
                }
            }
        }
        Err(quinn_proto::WriteError::Blocked) => {
            // Still blocked, keep buffer
        }
        Err(e) => {
            slot.write_buf.clear();
            for (_, reply) in slot.pending_write_replies.drain(..) {
                let _ = reply.send(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    format!("QUIC write error: {}", e),
                )));
            }
        }
    }
}

/// Pump endpoint events back and forth between endpoint and connection.
fn pump_endpoint_events(
    endpoint: &mut quinn_proto::Endpoint,
    conn_handle: ConnectionHandle,
    connection: &mut quinn_proto::Connection,
) {
    while let Some(endpoint_event) = connection.poll_endpoint_events() {
        if let Some(conn_event) = endpoint.handle_event(conn_handle, endpoint_event) {
            connection.handle_event(conn_event);
        }
    }
}

/// Flush all pending transmits from the connection to the UDP socket.
pub fn flush_transmits(
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

/// Send a single transmit (datagram) on the UDP socket.
pub fn send_transmit(udp: &UdpSocket, transmit: &Transmit, buf: &[u8]) -> io::Result<()> {
    let data = &buf[..transmit.size];
    match udp.send_to(data, transmit.destination) {
        Ok(_) => Ok(()),
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
#[path = "quic_client_tests.rs"]
mod client_tests;

#[cfg(test)]
#[path = "quic_tests.rs"]
mod tests;
