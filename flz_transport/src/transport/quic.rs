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

use super::config::{max_write_buffer_bytes, would_overflow_write_buffer};

const UDP_TOKEN: Token = Token(0);
const WAKER_TOKEN: Token = Token(1);

enum DriverCommand {
    OpenStream {
        read_tx: channel::Sender<Bytes>,
        reply: channel::Sender<io::Result<StreamId>>,
    },
    Write {
        stream_id: StreamId,
        data: Vec<u8>,
        reply: channel::Sender<io::Result<usize>>,
    },
    Shutdown,
}

struct StreamSlot {
    read_tx: channel::Sender<Bytes>,
    write_buf: Vec<u8>,
    pending_write_reply: Option<(usize, channel::Sender<io::Result<usize>>)>,
    max_write_buffer_bytes: usize,
    write_buffer_dropped: u64,
}

struct ChannelReader {
    rx: channel::Receiver<Bytes>,
    buf: Bytes,
    // Honors `FlezhaStream::set_read_timeout` on QUIC: when set, a read that
    // waits longer than this for the next chunk returns `WouldBlock` instead
    // of blocking forever, so control-channel liveness works over QUIC.
    read_timeout: Option<Duration>,
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

        let n = std::cmp::min(out.len(), chunk.len());
        out[..n].copy_from_slice(&chunk[..n]);
        if n < chunk.len() {
            self.buf = chunk.slice(n..);
        }
        Ok(n)
    }
}

/// Load the client identity for mutual TLS. `Ok(None)` when the pair is not
/// on disk (the caller falls back to no client auth and the server's posture
/// decides); a present-but-unreadable pair is an error, not a silent
/// downgrade.
fn load_client_identity(
    cert_path: &str,
    key_path: &str,
) -> io::Result<
    Option<(
        Vec<rustls::pki_types::CertificateDer<'static>>,
        rustls::pki_types::PrivateKeyDer<'static>,
    )>,
> {
    if !std::path::Path::new(cert_path).exists() || !std::path::Path::new(key_path).exists() {
        return Ok(None);
    }

    let chain = super::tls::load_certs(cert_path)?;
    if chain.is_empty() {
        return Ok(None);
    }

    Ok(Some((chain, super::tls::load_private_key(key_path)?)))
}

/// The client's TLS posture: the CA the server is verified against, the
/// client identity for mutual TLS when its pair is on disk (the server
/// requires it by default), and the `flezha` ALPN.
fn client_crypto(ca_cert_path: &str) -> io::Result<rustls::ClientConfig> {
    let root_store = super::tls::load_root_store(ca_cert_path)?;
    let builder = rustls::ClientConfig::builder().with_root_certificates(root_store);

    let client_cert_path = super::config::quic_client_cert_path();
    let client_key_path = super::config::quic_client_key_path();

    let mut crypto = match load_client_identity(&client_cert_path, &client_key_path)? {
        Some((chain, key)) => builder
            .with_client_auth_cert(chain, key)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
        None => builder.with_no_client_auth(),
    };
    crypto.alpn_protocols = vec![b"flezha".to_vec()];
    Ok(crypto)
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
            quinn_proto::crypto::rustls::QuicClientConfig::try_from(client_crypto(ca_cert_path)?)
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
        let (read_tx, read_rx) = channel::bounded::<Bytes>(64);
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
        let (reply_tx, reply_rx) = channel::bounded(1);
        self.cmd_tx
            .send(DriverCommand::Write {
                stream_id: self.stream_id,
                data: buf.to_vec(),
                reply: reply_tx,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "QUIC write channel closed"))?;
        self.waker.wake().map_err(io::Error::other)?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Driver reply dropped"))?
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
        let (reply_tx, reply_rx) = channel::bounded(1);
        self.cmd_tx
            .send(DriverCommand::Write {
                stream_id: self.stream_id,
                data: buf.to_vec(),
                reply: reply_tx,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "QUIC write channel closed"))?;
        self.waker.wake().map_err(io::Error::other)?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Driver reply dropped"))?
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
                streams.insert(
                    stream_id,
                    StreamSlot {
                        read_tx,
                        write_buf: Vec::new(),
                        pending_write_reply: None,
                        max_write_buffer_bytes: max_write_buffer_bytes(),
                        write_buffer_dropped: 0,
                    },
                );
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
        } => match streams.get_mut(&stream_id) {
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

/// Write an app thread's bytes onto its stream, buffering what the stream
/// will not take now and deferring the reply until the buffer drains. At the
/// send-buffer cap the frame is dropped rather than growing the buffer
/// without bound (DropNewest — the peer is not draining; the data stream is
/// at-least-once with downstream dedup).
fn write_or_buffer(
    connection: &mut quinn_proto::Connection,
    stream_id: StreamId,
    slot: &mut StreamSlot,
    data: Vec<u8>,
    reply: channel::Sender<io::Result<usize>>,
) {
    let total_len = data.len();
    if would_overflow_write_buffer(slot.write_buf.len(), total_len, slot.max_write_buffer_bytes) {
        slot.write_buffer_dropped += 1;
        tracing::warn!(
            target: "flezha::transport",
            transport = "quic",
            dropped = slot.write_buffer_dropped,
            "QUIC send buffer at cap; dropping frame"
        );
        let _ = reply.send(Ok(total_len));
        return;
    }
    match connection.send_stream(stream_id).write(&data) {
        Ok(written) => {
            if written < total_len {
                // Partial write — buffer remainder, defer reply
                slot.write_buf.extend_from_slice(&data[written..]);
                slot.pending_write_reply = Some((total_len, reply));
            } else {
                let _ = reply.send(Ok(total_len));
            }
        }
        Err(quinn_proto::WriteError::Blocked) => {
            // Fully blocked — buffer everything, defer reply
            slot.write_buf = data;
            slot.pending_write_reply = Some((total_len, reply));
        }
        Err(e) => {
            let _ = reply.send(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!("QUIC write error: {}", e),
            )));
        }
    }
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

/// Drain readable data from a stream and push to the channel.
fn drain_readable(
    connection: &mut quinn_proto::Connection,
    id: StreamId,
    streams: &mut HashMap<StreamId, StreamSlot>,
) {
    let slot = match streams.get(&id) {
        Some(s) => s,
        None => return,
    };
    let read_tx = slot.read_tx.clone();

    if let Ok(mut chunks) = connection.recv_stream(id).read(true) {
        loop {
            match chunks.next(usize::MAX) {
                Ok(Some(chunk)) => {
                    if read_tx.send(chunk.bytes).is_err() {
                        break; // Consumer dropped
                    }
                }
                Ok(None) => break, // No more data
                Err(quinn_proto::ReadError::Blocked) => break,
                Err(e) => {
                    tracing::warn!(
                        target: "flezha::transport",
                        transport = "quic", stream_id = ?id,
                        "QUIC stream read error: {}", e
                    );
                    break;
                }
            }
        }
        let _ = chunks.finalize();
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
            if slot.write_buf.is_empty() {
                // All buffered data sent — reply to the app
                if let Some((total, reply)) = slot.pending_write_reply.take() {
                    let _ = reply.send(Ok(total));
                }
            }
        }
        Err(quinn_proto::WriteError::Blocked) => {
            // Still blocked, keep buffer
        }
        Err(e) => {
            slot.write_buf.clear();
            if let Some((_, reply)) = slot.pending_write_reply.take() {
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
mod tests {
    use super::*;

    // fails when: QUIC `set_read_timeout` is a no-op (the pre-change behavior),
    // so `ChannelReader::read` blocks forever on an idle stream and control-
    // channel liveness is silently skipped on QUIC.
    #[test]
    fn channel_reader_honors_read_timeout_when_idle() {
        let (_tx, rx) = channel::bounded::<Bytes>(4);
        let mut reader = ChannelReader {
            rx,
            buf: Bytes::new(),
            read_timeout: Some(Duration::from_millis(120)),
        };

        let started = Instant::now();
        let mut out = [0u8; 64];
        let res = reader.read(&mut out);

        assert!(
            matches!(&res, Err(e) if e.kind() == io::ErrorKind::WouldBlock),
            "idle QUIC read with a deadline must return WouldBlock, got {:?}",
            res
        );
        assert!(
            started.elapsed() >= Duration::from_millis(100),
            "read returned before the deadline elapsed"
        );
    }

    #[test]
    fn channel_reader_returns_data_then_eof_on_disconnect() {
        let (tx, rx) = channel::bounded::<Bytes>(4);
        tx.send(Bytes::from_static(b"hello")).unwrap();
        drop(tx); // disconnect after one chunk

        let mut reader = ChannelReader {
            rx,
            buf: Bytes::new(),
            read_timeout: Some(Duration::from_millis(500)),
        };

        let mut out = [0u8; 64];
        let n = reader.read(&mut out).unwrap();
        assert_eq!(&out[..n], b"hello");
        // Disconnected channel = stream finished = EOF (Ok(0)), not WouldBlock.
        assert_eq!(reader.read(&mut out).unwrap(), 0);
    }
}
