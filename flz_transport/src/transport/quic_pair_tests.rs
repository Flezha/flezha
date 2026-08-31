//! A QUIC client and server exchanging datagrams in memory, for tests that
//! need a real `quinn_proto` connection and no socket, thread or clock.
//!
//! Both send buffers in the tree sit behind such a connection — the client's
//! in [`super::quic`] and the server driver's in `flz_nif` — and both exist
//! for the same condition: a peer that completes the handshake and stops
//! reading, so its flow-control window is never re-opened. One harness serves
//! both; it is compiled only under `cfg(test)` or the `test-support` feature.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use quinn_proto::{
    ClientConfig, ConnectionHandle, DatagramEvent, Dir, Endpoint, EndpointConfig, Event,
    ServerConfig, StreamId,
};

use super::quic::{expire_timer, transport_config};
use super::tls::build_quic_server_config;

const CLIENT_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40001);
const SERVER_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40002);

/// The repo's development certificates, which both endpoints below use.
pub fn cert_path(name: &str) -> String {
    format!("{}/../app_data/certs/{}", env!("CARGO_MANIFEST_DIR"), name)
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
