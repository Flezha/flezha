//! The QUIC peers tests drive against: [`Pair`], a client and server
//! exchanging datagrams in memory with no socket, thread or clock, and
//! [`SocketPeer`], the least controller a real [`super::quic::QuicClient`]
//! can dial.
//!
//! Both send buffers in the tree sit behind an in-memory pair, the client's
//! in [`super::quic`] and the server driver's in `flz_nif`, and both exist
//! for the same condition: a peer that completes the handshake and stops
//! reading, so its flow-control window is never re-opened. `SocketPeer` is
//! for the other half: a caller that holds a whole `QuicClient` and needs its
//! peer to go away, or to refuse the next stream. One harness serves them
//! all; it is compiled only under `cfg(test)` or the `test-support` feature.

use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use quinn_proto::{
    ClientConfig, Connection, ConnectionHandle, DatagramEvent, Dir, Endpoint, EndpointConfig,
    Event, ServerConfig, StreamId, VarInt,
};

use super::quic::{expire_timer, transport_config};
use super::tls::build_quic_server_config;

const CLIENT_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40001);
const SERVER_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40002);

/// The repo's development certificates, which both endpoints below use.
pub fn cert_path(name: &str) -> String {
    format!(
        "{}/../../storage/app/certs/{}",
        env!("CARGO_MANIFEST_DIR"),
        name
    )
}

/// The server side of the pair, built by the same function a real QUIC server
/// boots with. No client auth: these tests are about send buffers, not mTLS.
pub fn server_config() -> ServerConfig {
    build_quic_server_config(&cert_path("server.pem"), &cert_path("server.key"), "").unwrap()
}

fn client_config() -> ClientConfig {
    let roots = super::tls::load_root_store(&cert_path("ca.pem")).unwrap();
    let mut crypto = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"flezha".to_vec()];
    let mut config = ClientConfig::new(Arc::new(
        quinn_proto::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
    ));
    config.transport_config(Arc::new(transport_config()));
    config
}

/// A connected client/server pair. Whichever side a test writes from, the
/// other one only reads when the test asks it to — which is the stalled peer
/// every send buffer exists for.
pub struct Pair {
    client_ep: Endpoint,
    server_ep: Endpoint,
    client_ch: ConnectionHandle,
    pub client: quinn_proto::Connection,
    pub server: Option<(ConnectionHandle, quinn_proto::Connection)>,
}

impl Pair {
    pub fn new() -> Pair {
        let mut client_ep = Endpoint::new(Arc::new(EndpointConfig::default()), None, true, None);
        let server_ep = Endpoint::new(
            Arc::new(EndpointConfig::default()),
            Some(Arc::new(server_config())),
            true,
            None,
        );
        let (client_ch, client) = client_ep
            .connect(Instant::now(), client_config(), SERVER_ADDR, "localhost")
            .unwrap();
        Pair {
            client_ep,
            server_ep,
            client_ch,
            client,
            server: None,
        }
    }

    /// The server connection, once the handshake has produced one.
    pub fn server_conn(&mut self) -> &mut quinn_proto::Connection {
        &mut self.server.as_mut().expect("server connection").1
    }

    /// One round of the exchange: every datagram either side has to send is
    /// delivered to the other, endpoint events are pumped, timers fire.
    pub fn step(&mut self) {
        let now = Instant::now();
        expire_timer(&mut self.client, now);
        if let Some((_, conn)) = self.server.as_mut() {
            expire_timer(conn, now);
        }

        let mut buf = Vec::new();
        while let Some(transmit) = self.client.poll_transmit(now, 1, &mut buf) {
            let data = BytesMut::from(&buf[..transmit.size]);
            buf.clear();
            self.deliver_to_server(data, now);
        }

        if let Some((_, conn)) = self.server.as_mut() {
            let mut buf = Vec::new();
            while let Some(transmit) = conn.poll_transmit(now, 1, &mut buf) {
                let data = BytesMut::from(&buf[..transmit.size]);
                buf.clear();
                let mut resp = Vec::new();
                if let Some(DatagramEvent::ConnectionEvent(ch, event)) =
                    self.client_ep
                        .handle(now, SERVER_ADDR, None, None, data, &mut resp)
                {
                    if ch == self.client_ch {
                        self.client.handle_event(event);
                    }
                }
            }
        }

        while let Some(event) = self.client.poll_endpoint_events() {
            if let Some(event) = self.client_ep.handle_event(self.client_ch, event) {
                self.client.handle_event(event);
            }
        }
        if let Some((ch, conn)) = self.server.as_mut() {
            while let Some(event) = conn.poll_endpoint_events() {
                if let Some(event) = self.server_ep.handle_event(*ch, event) {
                    conn.handle_event(event);
                }
            }
        }
    }

    /// Hand one datagram to the server endpoint, accepting the connection the
    /// first time one arrives.
    fn deliver_to_server(&mut self, data: BytesMut, now: Instant) {
        let mut resp = Vec::new();
        let Some(event) = self
            .server_ep
            .handle(now, CLIENT_ADDR, None, None, data, &mut resp)
        else {
            return;
        };
        match event {
            DatagramEvent::ConnectionEvent(ch, event) => {
                if let Some((server_ch, conn)) = self.server.as_mut() {
                    if *server_ch == ch {
                        conn.handle_event(event);
                    }
                }
            }
            DatagramEvent::NewConnection(incoming) => {
                let mut accept_buf = Vec::new();
                let accepted = self
                    .server_ep
                    .accept(incoming, now, &mut accept_buf, None)
                    .unwrap();
                self.server = Some(accepted);
            }
            DatagramEvent::Response(_) => {}
        }
    }

    /// Drain what the client can read on `stream_id`, re-opening the server's
    /// flow-control window. A reproduction never calls it; its control does.
    pub fn client_reads(&mut self, stream_id: StreamId) -> usize {
        drain(&mut self.client, stream_id)
    }

    /// The same for the server, which is the reading peer when the test drives
    /// the client's send buffer.
    pub fn server_reads(&mut self, stream_id: StreamId) -> usize {
        drain(self.server_conn(), stream_id)
    }

    /// Run the handshake to `Event::Connected` on the client.
    pub fn connect(&mut self) {
        for _ in 0..200 {
            self.step();
            while let Some(event) = self.client.poll() {
                if matches!(event, Event::Connected) {
                    return;
                }
            }
        }
        panic!("QUIC handshake did not complete in 200 rounds");
    }

    /// Open one bidi stream from the client and accept it on the server, the
    /// way a runner's data stream reaches the driver.
    pub fn open_stream(&mut self) -> StreamId {
        let id = self.client.streams().open(Dir::Bi).unwrap();
        self.client.send_stream(id).write(b"open").unwrap();
        for _ in 0..200 {
            self.step();
            let conn = self.server_conn();
            while conn.poll().is_some() {}
            if let Some(accepted) = conn.streams().accept(Dir::Bi) {
                assert_eq!(accepted, id, "stream ids match across the pair");
                return id;
            }
        }
        panic!("the server never saw the client's stream");
    }
}

impl Default for Pair {
    fn default() -> Pair {
        Pair::new()
    }
}

fn drain(conn: &mut quinn_proto::Connection, stream_id: StreamId) -> usize {
    let mut total = 0;
    let mut recv = conn.recv_stream(stream_id);
    let Ok(mut chunks) = recv.read(true) else {
        return 0;
    };
    while let Ok(Some(chunk)) = chunks.next(usize::MAX) {
        total += chunk.bytes.len();
    }
    let _ = chunks.finalize();
    total
}

/// A QUIC peer on a real UDP socket: the least server that completes a
/// handshake, so a test can hold the [`super::quic::QuicClient`] a runner
/// dials instead of the driver's internals. It never reads a stream and never
/// answers one; what it exists to control is whether the client can open the
/// next one, and whether the connection is still there at all.
pub struct SocketPeer {
    addr: SocketAddr,
    gone: Arc<AtomicBool>,
    serving: Option<JoinHandle<()>>,
}

impl SocketPeer {
    /// Listen on the loopback, advertising `max_bidi_streams` to whoever
    /// dials. The count is the lever: a client that asks for one more than
    /// this is refused its next open, in the middle of a pair set.
    pub fn listening(max_bidi_streams: u32) -> SocketPeer {
        // `storage/app/` is runtime data, so no checkout carries the development
        // certificates and the failure would otherwise be an open() on a path.
        for name in ["ca.pem", "server.pem", "server.key"] {
            assert!(
                std::path::Path::new(&cert_path(name)).exists(),
                "{name} is missing; write the development set with \
                 `mix run --no-start -e 'Flezha.Transport.CertGenerator.ensure_certs()'`"
            );
        }

        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind the peer socket");
        socket
            .set_read_timeout(Some(Duration::from_millis(20)))
            .expect("arm the peer's read deadline");
        let addr = socket.local_addr().expect("the peer's address");

        let mut config = server_config();
        let mut transport = transport_config();
        transport.max_concurrent_bidi_streams(max_bidi_streams.into());
        config.transport_config(Arc::new(transport));

        let gone = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&gone);
        let serving = thread::spawn(move || serve(socket, config, flag));
        SocketPeer {
            addr,
            gone,
            serving: Some(serving),
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Close every connection and stop listening: the controller dying between
    /// one command and the next. Returns once the close has gone out, so the
    /// client's driver thread is on its way to exiting.
    pub fn vanish(&mut self) {
        self.gone.store(true, Ordering::Relaxed);
        if let Some(serving) = self.serving.take() {
            let _ = serving.join();
        }
    }
}

impl Drop for SocketPeer {
    fn drop(&mut self) {
        self.vanish();
    }
}

/// The peer's loop: accept whoever dials, keep the connection alive, and on
/// the vanish flag close it and flush the close before returning.
fn serve(socket: UdpSocket, config: ServerConfig, gone: Arc<AtomicBool>) {
    let mut endpoint = Endpoint::new(
        Arc::new(EndpointConfig::default()),
        Some(Arc::new(config)),
        true,
        None,
    );
    let mut conns: Vec<(ConnectionHandle, Connection)> = Vec::new();
    let mut closing: Option<Instant> = None;
    let mut buf = vec![0u8; 65536];

    loop {
        let now = Instant::now();
        if closing.is_none() && gone.load(Ordering::Relaxed) {
            for (_, conn) in conns.iter_mut() {
                conn.close(now, VarInt::from_u32(0), b"peer gone".to_vec().into());
            }
            closing = Some(now);
        }

        if let Ok((n, from)) = socket.recv_from(&mut buf) {
            let data = BytesMut::from(&buf[..n]);
            let mut resp = Vec::new();
            match endpoint.handle(now, from, None, None, data, &mut resp) {
                Some(DatagramEvent::NewConnection(incoming)) => {
                    let mut accept_buf = Vec::new();
                    if let Ok(accepted) = endpoint.accept(incoming, now, &mut accept_buf, None) {
                        conns.push(accepted);
                    }
                }
                Some(DatagramEvent::ConnectionEvent(ch, event)) => {
                    if let Some((_, conn)) = conns.iter_mut().find(|(handle, _)| *handle == ch) {
                        conn.handle_event(event);
                    }
                }
                Some(DatagramEvent::Response(transmit)) => {
                    let _ = socket.send_to(&resp[..transmit.size], transmit.destination);
                }
                None => {}
            }
        }

        let mut out = Vec::new();
        for (handle, conn) in conns.iter_mut() {
            expire_timer(conn, now);
            while let Some(event) = conn.poll_endpoint_events() {
                if let Some(event) = endpoint.handle_event(*handle, event) {
                    conn.handle_event(event);
                }
            }
            while conn.poll().is_some() {}
            while let Some(transmit) = conn.poll_transmit(now, 1, &mut out) {
                let _ = socket.send_to(&out[..transmit.size], transmit.destination);
                out.clear();
            }
        }

        // A close is one datagram; give the loop a few more turns so a
        // retransmit still has a socket to go out on.
        if closing.is_some_and(|at| at.elapsed() > Duration::from_millis(200)) {
            return;
        }
    }
}
