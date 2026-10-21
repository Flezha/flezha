use std::collections::HashMap;
use std::io::{self, Read};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use crossbeam::channel;
use mio::net::UdpSocket;
use mio::{Events, Interest, Poll, Token, Waker};
use quinn_proto::{
    ClientConfig, ConnectionHandle, Dir, EndpointConfig, Event, StreamEvent, StreamId,
};

use super::send_gate::SendGate;
use super::Wire;

#[path = "quic_endpoint.rs"]
mod endpoint;
pub use endpoint::{expire_timer, flush_transmits, pump_udp, send_transmit, transport_config};
use endpoint::{poll_timeout, pump_endpoint_events};

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
    /// This burst's refusal at the ceiling was said; an emptied buffer re-arms it.
    ceiling_said: bool,
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
            ceiling_said: false,
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

/// A bidirectional QUIC stream, read and written as a [`Wire`].
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
}

impl Wire for QuicBidiStream {
    fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_rx
            .lock()
            .map_err(|e| io::Error::other(format!("Lock poisoned: {}", e)))?
            .read(buf)
    }

    fn write(&self, buf: &[u8]) -> io::Result<usize> {
        self.command_write(buf, false)
    }

    /// One command: the driver answers only once the whole frame is on the stream.
    fn write_frame(&self, frame: &[u8]) -> io::Result<()> {
        self.command_write(frame, false).map(drop)
    }

    /// The caller waits for the driver's turn, never for the peer.
    fn try_write_frame(&self, frame: &[u8]) -> io::Result<()> {
        self.command_write(frame, true).map(drop)
    }

    /// Shares the write and read channels: infallible, it opens nothing.
    fn clone_handle(&self) -> io::Result<QuicBidiStream> {
        Ok(QuicBidiStream {
            stream_id: self.stream_id,
            cmd_tx: self.cmd_tx.clone(),
            waker: Arc::clone(&self.waker),
            read_rx: Arc::clone(&self.read_rx),
        })
    }

    /// The deadline lives in the shared reader, so the control reader and
    /// every clone see one; without it control liveness would not hold on QUIC.
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.read_rx
            .lock()
            .map_err(|e| io::Error::other(format!("Lock poisoned: {}", e)))?
            .read_timeout = timeout;
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
            let _ = reply.send(Err(refused_at_ceiling(slot)));
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
            let _ = reply.send(Err(write_failed(&e)));
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
            Err(e) => Err(write_failed(&e)),
        }
    };
    let _ = reply.send(answer);
}

/// What a writer is told when the stream itself failed under it.
fn write_failed(e: &quinn_proto::WriteError) -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, format!("QUIC write error: {e}"))
}

/// The one answer a refused frame gets. `WouldBlock` and not `BrokenPipe`:
/// the stream is healthy, the peer is behind, and the caller may try again.
/// Said once per burst: a caller retrying on `WouldBlock` would repeat it per frame.
fn refused_at_ceiling(slot: &mut StreamSlot) -> io::Error {
    if !std::mem::replace(&mut slot.ceiling_said, true) {
        let buffered = slot.write_buf.len();
        crate::notice!(
            Warn,
            "QUIC send buffer at its ceiling ({buffered} bytes); frames refused"
        );
    }
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
                crate::notice!(
                    Warn,
                    "QUIC connection lost; driver thread exiting: {reason}"
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
    let Some(slot) = streams.get_mut(&id) else {
        return;
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
                    crate::notice!(Warn, "QUIC stream {id:?} read error: {e}");
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
    let Some(slot) = streams.get_mut(&id) else {
        return;
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
                slot.ceiling_said = false;
                for (total, reply) in slot.pending_write_replies.drain(..) {
                    let _ = reply.send(Ok(total));
                }
            }
        }
        Err(quinn_proto::WriteError::Blocked) => {} // Still blocked: keep the buffer.
        Err(e) => {
            slot.write_buf.clear();
            for (_, reply) in slot.pending_write_replies.drain(..) {
                let _ = reply.send(Err(write_failed(&e)));
            }
        }
    }
}

#[cfg(test)]
#[path = "quic_client_tests.rs"]
mod client_tests;

#[cfg(test)]
#[path = "quic_tests.rs"]
mod tests;
