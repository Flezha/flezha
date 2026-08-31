//! The QUIC server driver's per-stream send buffer, driven against a peer that
//! completes the handshake and never reads.
//!
//! The pair is in memory: two `quinn_proto::Endpoint`s whose datagrams are
//! handed to each other directly, so the whole exchange is deterministic and
//! needs no socket, no thread and no clock.

use std::net::{Ipv4Addr, SocketAddr};

use flz_transport::transport::send_gate::{GateChange, SendGate, Transition};
use quinn_proto::{ClientConfig, Endpoint};

use super::*;

const CLIENT_ADDR: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 40001);
const SERVER_ADDR: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 40002);

fn cert_path(name: &str) -> String {
    format!("{}/../app_data/certs/{}", env!("CARGO_MANIFEST_DIR"), name)
}

fn client_config() -> ClientConfig {
    let roots = flz_transport::transport::tls::load_root_store(&cert_path("ca.pem")).unwrap();
    let mut crypto = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"flezha".to_vec()];
    let mut config = ClientConfig::new(Arc::new(
        quinn_proto::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
    ));
    config.transport_config(Arc::new(flz_transport::transport::quic::transport_config()));
    config
}

/// A client and a server connection exchanging datagrams in memory. The client
/// never reads a stream, so its flow-control window is never re-opened — the
/// condition both drivers' send buffers exist for.
struct Pair {
    client_ep: Endpoint,
    server_ep: Endpoint,
    client_ch: ConnectionHandle,
    client: quinn_proto::Connection,
    server: Option<(ConnectionHandle, quinn_proto::Connection)>,
    /// The driver's per-stream send buffer, held here so one helper can run
    /// the driver's whole write path over it.
    write_buffer: Vec<u8>,
}

impl Pair {
    fn new() -> Pair {
        let server_config = build_quic_server_config(
            &cert_path("server.pem"),
            &cert_path("server.key"),
            // No client auth: this test is about the send buffer, not mTLS.
            "",
        )
        .unwrap();
        let mut client_ep = Endpoint::new(Arc::new(EndpointConfig::default()), None, true, None);
        let server_ep = Endpoint::new(
            Arc::new(EndpointConfig::default()),
            Some(Arc::new(server_config)),
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
            write_buffer: Vec::new(),
        }
    }

    /// One round of the exchange: every datagram either side has to send is
    /// delivered to the other, endpoint events are pumped, timers fire.
    fn step(&mut self) {
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

    /// Drain what the client can read on `stream_id`, re-opening its
    /// flow-control window. The reproduction never calls this; its control does.
    fn client_reads(&mut self, stream_id: StreamId) -> usize {
        let mut total = 0;
        let mut recv = self.client.recv_stream(stream_id);
        let Ok(mut chunks) = recv.read(true) else {
            return 0;
        };
        while let Ok(Some(chunk)) = chunks.next(usize::MAX) {
            total += chunk.bytes.len();
        }
        let _ = chunks.finalize();
        total
    }

    /// Run the handshake to `Event::Connected` on the client.
    fn connect(&mut self) {
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
    fn open_stream(&mut self) -> StreamId {
        let id = self.client.streams().open(Dir::Bi).unwrap();
        self.client.send_stream(id).write(b"open").unwrap();
        for _ in 0..200 {
            self.step();
            let (_, conn) = self.server.as_mut().unwrap();
            while conn.poll().is_some() {}
            if let Some(accepted) = conn.streams().accept(Dir::Bi) {
                assert_eq!(accepted, id, "stream ids match across the pair");
                return id;
            }
        }
        panic!("the server never saw the client's stream");
    }
}

/// The driver's write path for one frame, exactly as `write_stream` runs it:
/// offer at the ceiling, then flush what the peer will take. Returns the gate
/// transition the driver would have reported to the stream's owner.
fn offer_and_flush(
    pair: &mut Pair,
    stream_id: StreamId,
    gate: &SendGate,
    frame: &[u8],
) -> Transition {
    gate.offer(&mut pair.write_buffer, frame);
    let (_, conn) = pair.server.as_mut().unwrap();
    let (broken, change) = flush_quic_write(conn, stream_id, &mut pair.write_buffer, gate);
    assert!(!broken, "the stream must not break");
    pair.step();
    change
}

/// A gate that never engages — the *ungated* driver, which is what
/// `quic_driver.rs` was before this change. Every gate test needs it: a gate
/// that never engages looks exactly like a gate that works.
fn ungated() -> SendGate {
    SendGate::with_watermarks(usize::MAX, usize::MAX, usize::MAX)
}

/// KI-56, and the negative control for the bound below. `write_stream` extends
/// the per-stream buffer with no bound, and `flush_quic_write` treats
/// `WriteError::Blocked` — the peer's flow-control window being full — as a
/// no-op that leaves the bytes buffered. Against a peer that never reads, an
/// ungated buffer therefore grows past `max_write_buffer_bytes()` and keeps
/// going, with nothing counted.
#[test]
fn an_ungated_quic_send_buffer_grows_past_the_cap_when_the_peer_stops_reading() {
    let cap = flz_transport::transport::config::max_write_buffer_bytes();
    let gate = ungated();
    let mut pair = Pair::new();
    pair.connect();
    let stream_id = pair.open_stream();

    let frame = vec![b'x'; 64 * 1024];
    let mut offered = 0usize;
    for _ in 0..400 {
        offer_and_flush(&mut pair, stream_id, &gate, &frame);
        offered += frame.len();
    }
    let after_first_burst = pair.write_buffer.len();

    // Offer as much again. A bounded buffer would hold at its bound; this one
    // takes every byte, which is what "no cap at all" means.
    for _ in 0..400 {
        offer_and_flush(&mut pair, stream_id, &gate, &frame);
        offered += frame.len();
    }

    println!(
        "ungated quic write_buffer: offered={} buffered={} cap={} (x{:.1} the cap); \
         after first burst={} growth={} refused={}",
        offered,
        pair.write_buffer.len(),
        cap,
        pair.write_buffer.len() as f64 / cap as f64,
        after_first_burst,
        pair.write_buffer.len() - after_first_burst,
        gate.refused(),
    );
    assert!(
        after_first_burst > cap,
        "KI-56: an ungated QUIC send buffer holds {} bytes against a {} byte cap",
        after_first_burst,
        cap
    );
    assert_eq!(
        pair.write_buffer.len() - after_first_burst,
        400 * frame.len(),
        "KI-56: the second burst is buffered whole — there is no bound to hold at"
    );
    assert_eq!(gate.refused(), 0, "an ungated gate refuses nothing");
}

/// The same drive with the driver's real gate. The buffer holds at its
/// ceiling, the gate is raised so the sender is told, and every frame the
/// ceiling turned away is counted — where before it was neither bounded nor
/// counted.
#[test]
fn the_quic_send_buffer_holds_at_its_ceiling_and_raises_the_gate() {
    let cap = flz_transport::transport::config::max_write_buffer_bytes();
    let gate = SendGate::new();
    let mut pair = Pair::new();
    pair.connect();
    let stream_id = pair.open_stream();

    let frame = vec![b'x'; 64 * 1024];
    let mut raises = 0;
    for _ in 0..800 {
        if offer_and_flush(&mut pair, stream_id, &gate, &frame) == Some(GateChange::Raised) {
            raises += 1;
        }
    }

    println!(
        "gated quic write_buffer: buffered={} cap={} raised={} refused={} raises={}",
        pair.write_buffer.len(),
        cap,
        gate.is_raised(),
        gate.refused(),
        raises,
    );
    assert!(
        pair.write_buffer.len() <= cap,
        "the buffer must hold at the ceiling, held {} against {}",
        pair.write_buffer.len(),
        cap
    );
    assert!(
        gate.is_raised(),
        "a stalled peer must leave the gate raised"
    );
    assert_eq!(raises, 1, "one crossing, one wake — not one per frame");
    assert!(
        gate.refused() > 0,
        "the ceiling turned frames away and counted every one"
    );
}

/// The control for both: the same harness against a peer that DOES read leaves
/// the buffer empty, refuses nothing and lowers the gate again. Without it,
/// "the buffer grew" is not evidence about the peer.
#[test]
fn the_quic_send_buffer_drains_and_the_gate_lowers_when_the_peer_reads() {
    let gate = SendGate::new();
    let mut pair = Pair::new();
    pair.connect();
    let stream_id = pair.open_stream();

    let frame = vec![b'x'; 64 * 1024];
    let mut read_total = 0usize;
    for _ in 0..400 {
        offer_and_flush(&mut pair, stream_id, &gate, &frame);
        read_total += pair.client_reads(stream_id);
        pair.step();
    }
    for _ in 0..200 {
        let (_, conn) = pair.server.as_mut().unwrap();
        flush_quic_write(conn, stream_id, &mut pair.write_buffer, &gate);
        pair.step();
        read_total += pair.client_reads(stream_id);
    }

    println!(
        "quic write_buffer with a draining peer: buffered={} read={} refused={} raised={}",
        pair.write_buffer.len(),
        read_total,
        gate.refused(),
        gate.is_raised(),
    );
    assert_eq!(
        pair.write_buffer.len(),
        0,
        "a draining peer leaves nothing buffered"
    );
    assert_eq!(gate.refused(), 0, "nothing was refused");
    assert!(!gate.is_raised(), "the gate is down for a healthy peer");
}
