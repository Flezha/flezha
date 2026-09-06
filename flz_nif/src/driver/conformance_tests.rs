//! One table of cases, run against every [`Transport`] implementation.
//!
//! A table nothing enters cannot say that a capability one wire has and another
//! lacks is a defect. This one says it: each case is written once and driven
//! over `Uds` and over `Quic`, so a difference between the wires is a failing
//! build rather than a documented difference.
//!
//! What it cannot enter is the send to Elixir — `report_gate`, `send_data_msg`
//! and `send_closed_msg` need a live BEAM. Every case therefore asserts the
//! bookkeeping the loop does around those sends, and the sends themselves are
//! driven from Elixir by `test/system/transport_driver_parity_test.exs`.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;

use flz_transport::transport::quic_pair::Pair;
use flz_transport::transport::send_gate::GateChange;

use super::*;

/// One wire, connected to a peer the case drives. Every method is written
/// against the [`Transport`] contract, so the table below never names a
/// transport.
trait Wire {
    type T: Transport;

    /// The wire and the stream, as the two disjoint things `DriverLoop` holds
    /// them: a transport method takes one of each.
    fn halves(&mut self) -> (&mut Self::T, &mut StreamState<<Self::T as Transport>::Wire>);

    fn state(&mut self) -> &mut StreamState<<Self::T as Transport>::Wire> {
        self.halves().1
    }

    /// Carry whatever the last write handed to the wire out to the peer.
    fn carry(&mut self);
    /// Let the peer take everything waiting for it, answering how much.
    fn peer_reads(&mut self) -> usize;
    /// Send `data` from the peer to the driver.
    fn peer_writes(&mut self, data: &[u8]);
    /// End the stream from the peer's side, cleanly.
    fn peer_ends(&mut self);
}

/// One frame's whole round trip through the gate: the sender reserves against
/// it in `stream.rs`, the loop releases that reservation and offers the frame,
/// the wire takes what it will, and the loop observes the depth left behind.
///
/// Both halves are here because the ledger is only balanced across them: a
/// reservation the loop fails to release stays counted for ever, and the gate
/// latches raised with an empty buffer under it.
fn offer_and_flush<W: Wire>(wire: &mut W, frame: &[u8], now_ms: u64) -> Transition {
    let reserved = frame.len();
    let queued = wire.state().gate.queued(reserved);
    wire.state().offer(frame, reserved);
    let written = {
        let (transport, state) = wire.halves();
        transport.write(state).expect("the stream must not break")
    };
    let observed = wire.state().observe(written, now_ms);
    wire.carry();
    // The gate is a latch, so at most one of the two halves reports a crossing.
    queued.or(observed)
}

/// One read, taken the way `DriverLoop::read` takes it.
fn read_once<W: Wire>(wire: &mut W, into: &mut ReadBuf) -> Option<CloseReason> {
    into.clear();
    let (transport, state) = wire.halves();
    transport.read(state, into)
}

// --- the UDS wire ------------------------------------------------------------

struct UdsFixture {
    transport: uds::Uds,
    state: StreamState<uds::UdsWire>,
    peer: UnixStream,
}

impl UdsFixture {
    fn new(id: u64) -> UdsFixture {
        let (driver_end, peer) = UnixStream::pair().expect("a socket pair");
        peer.set_nonblocking(true).expect("a non-blocking peer");
        let (transport, state) = uds::conformance::wire(driver_end, id);
        UdsFixture {
            transport,
            state,
            peer,
        }
    }
}

impl Wire for UdsFixture {
    type T = uds::Uds;

    fn halves(&mut self) -> (&mut uds::Uds, &mut StreamState<uds::UdsWire>) {
        (&mut self.transport, &mut self.state)
    }

    /// A Unix socket write already reached the peer.
    fn carry(&mut self) {}

    fn peer_reads(&mut self) -> usize {
        let mut buf = vec![0u8; 256 * 1024];
        let mut total = 0;
        while let Ok(n) = self.peer.read(&mut buf) {
            if n == 0 {
                break;
            }
            total += n;
        }
        total
    }

    fn peer_writes(&mut self, data: &[u8]) {
        self.peer.write_all(data).expect("the peer writes");
    }

    fn peer_ends(&mut self) {
        self.peer
            .shutdown(std::net::Shutdown::Both)
            .expect("the peer ends the stream");
    }
}

// --- the QUIC wire -----------------------------------------------------------

struct QuicFixture {
    transport: quic::Quic,
    state: StreamState<quic::QuicWire>,
    pair: Pair,
    qstream_id: quinn_proto::StreamId,
    id: u64,
}

impl QuicFixture {
    fn new(id: u64) -> QuicFixture {
        let mut pair = Pair::new();
        pair.connect();
        let qstream_id = pair.open_stream();
        let (ch, connection) = pair.server.take().expect("a server connection");
        let (transport, state) = quic::conformance::wire(ch, connection, qstream_id, id);
        let mut fixture = QuicFixture {
            transport,
            state,
            pair,
            qstream_id,
            id,
        };
        // `Pair::open_stream` opens the stream by writing to it, so the driver
        // has those bytes waiting. UDS opens a stream by connecting and has
        // none; drain them here so a case starts from the same place on both.
        let mut opened = ReadBuf::new();
        assert_eq!(read_once(&mut fixture, &mut opened), None);
        assert_eq!(opened.taken(), b"open");
        fixture
    }
}

impl Wire for QuicFixture {
    type T = quic::Quic;

    fn halves(&mut self) -> (&mut quic::Quic, &mut StreamState<quic::QuicWire>) {
        (&mut self.transport, &mut self.state)
    }

    /// A QUIC write reaches a connection, and the pair routes its datagrams:
    /// the connection lives in the wire while the driver writes through it and
    /// back in the pair while the pair steps.
    fn carry(&mut self) {
        let (ch, connection) = self.transport.surrender();
        self.pair.server = Some((ch, connection));
        self.pair.step();
        let (ch, connection) = self.pair.server.take().expect("a server connection");
        self.transport
            .adopt(ch, connection, self.qstream_id, self.id);
    }

    fn peer_reads(&mut self) -> usize {
        let read = self.pair.client_reads(self.qstream_id);
        self.carry();
        read
    }

    fn peer_writes(&mut self, data: &[u8]) {
        self.pair
            .client
            .send_stream(self.qstream_id)
            .write(data)
            .expect("the peer writes");
        self.carry();
    }

    fn peer_ends(&mut self) {
        self.pair
            .client
            .send_stream(self.qstream_id)
            .finish()
            .expect("the peer ends the stream");
        self.carry();
    }
}

// --- the table ---------------------------------------------------------------

/// Every case, over every transport implementation. Adding a transport means
/// adding one line here and nothing else.
macro_rules! conformance {
    ($($case:ident),* $(,)?) => {
        $(
            mod $case {
                #[test]
                fn uds() {
                    super::$case(&mut super::UdsFixture::new(7));
                }

                #[test]
                fn quic() {
                    super::$case(&mut super::QuicFixture::new(7));
                }
            }
        )*
    };
}

conformance!(
    a_command_reaches_the_wire,
    the_gate_ledger_balances_against_a_stalled_peer,
    a_draining_peer_leaves_the_gate_down,
    the_snapshot_carries_what_the_driver_did,
    a_peer_ending_the_stream_reads_the_same,
    bytes_the_peer_sent_are_framed,
);

/// The `SendFrame` arm: the frame is built once by the loop, the wire takes it
/// and the peer gets it.
fn a_command_reaches_the_wire<W: Wire>(wire: &mut W) {
    let frame = flz_transport::service::payloader::build_from_binary(
        "p-1",
        b"hello",
        std::collections::HashMap::new(),
    );
    let change = offer_and_flush(wire, &frame, 11);
    assert_eq!(change, None, "one small frame crosses no watermark");
    assert!(
        wire.state().write_buffer.is_empty(),
        "a healthy peer leaves nothing buffered"
    );
    assert_eq!(wire.state().counters.frames_written, 1);
    assert_eq!(
        wire.state().counters.last_write_ms,
        11,
        "a write the peer took stamps the loop's clock"
    );
    assert_eq!(
        wire.peer_reads(),
        frame.len(),
        "the peer got the frame the loop built"
    );
}

/// KI-56's shape, over both wires: a peer that never reads fills the buffer to
/// its ceiling, the gate is raised exactly once, and every frame the ceiling
/// turned away is counted. The invariant is that a reservation is released
/// exactly once — including the ones that never travel — so a refused frame
/// neither latches the gate nor counts as written.
fn the_gate_ledger_balances_against_a_stalled_peer<W: Wire>(wire: &mut W) {
    let cap = flz_transport::transport::config::max_write_buffer_bytes();
    let frame = vec![b'x'; 64 * 1024];
    let mut raises = 0;
    for _ in 0..800 {
        if offer_and_flush(wire, &frame, 1) == Some(GateChange::Raised) {
            raises += 1;
        }
    }

    let state = wire.state();
    println!(
        "stalled peer: buffered={} cap={} raised={} refused={} written={} raises={}",
        state.write_buffer.len(),
        cap,
        state.gate.is_raised(),
        state.gate.refused(),
        state.counters.frames_written,
        raises,
    );
    assert!(
        state.write_buffer.len() <= cap,
        "the buffer must hold at the ceiling, held {} against {}",
        state.write_buffer.len(),
        cap
    );
    assert!(
        state.gate.is_raised(),
        "a stalled peer must leave the gate raised"
    );
    assert_eq!(raises, 1, "one crossing, one wake — not one per frame");
    assert!(
        state.gate.refused() > 0,
        "the ceiling turned frames away and counted every one"
    );
    assert_eq!(
        state.counters.frames_written + state.gate.refused(),
        800,
        "every offered frame is either written or counted as refused"
    );
}

/// The control for the case above: the same drive against a peer that DOES read
/// leaves the buffer empty, refuses nothing and never raises the gate. Without
/// it, "the buffer grew" is not evidence about the peer.
fn a_draining_peer_leaves_the_gate_down<W: Wire>(wire: &mut W) {
    let frame = vec![b'x'; 64 * 1024];
    let mut read_total = 0;
    for _ in 0..200 {
        offer_and_flush(wire, &frame, 1);
        read_total += wire.peer_reads();
    }

    let state = wire.state();
    println!(
        "draining peer: buffered={} read={} refused={} raised={}",
        state.write_buffer.len(),
        read_total,
        state.gate.refused(),
        state.gate.is_raised(),
    );
    assert_eq!(state.gate.refused(), 0, "nothing was refused");
    assert!(
        !state.gate.is_raised(),
        "the gate is down for a healthy peer"
    );
    assert!(read_total > 0, "the peer took bytes");
}

/// The stats snapshot the `driver_stats` NIF reads carries the fields each case
/// moves, and it carries them for every transport because one loop produces it.
fn the_snapshot_carries_what_the_driver_did<W: Wire>(wire: &mut W) {
    let idle = {
        let state = wire.state();
        state.counters.snapshot(&state.write_buffer, &state.gate)
    };
    assert_eq!(idle.frames_written, 0);
    assert_eq!(
        idle.last_write_ms, 0,
        "a stream that never wrote is unstamped"
    );

    let frame = vec![b'x'; 64 * 1024];
    for i in 0..800u64 {
        offer_and_flush(wire, &frame, i + 1);
        wire.peer_reads();
    }

    let state = wire.state();
    let snap = state.counters.snapshot(&state.write_buffer, &state.gate);
    println!(
        "snapshot: frames_written={} last_write_ms={} write_buffer_len={} dropped={}",
        snap.frames_written, snap.last_write_ms, snap.write_buffer_len, snap.write_buffer_dropped
    );
    assert_eq!(
        snap.frames_written + snap.write_buffer_dropped,
        800,
        "every offered frame is either written or counted as refused"
    );
    assert!(snap.frames_written > 0, "the peer took frames");
    assert!(
        snap.last_write_ms > 0,
        "a write the peer took stamps the clock"
    );
    assert_eq!(
        snap.write_buffer_len,
        state.write_buffer.len() as u64,
        "the snapshot reports the live buffer depth"
    );
}

/// The close vocabulary: a peer that ends the stream cleanly is
/// `connection_closed` on every wire, never one word here and a word of its own
/// there.
fn a_peer_ending_the_stream_reads_the_same<W: Wire>(wire: &mut W) {
    let mut buf = ReadBuf::new();
    wire.peer_ends();

    let mut reason = None;
    for _ in 0..200 {
        reason = read_once(wire, &mut buf);
        if reason.is_some() {
            break;
        }
        wire.carry();
    }

    assert_eq!(
        reason,
        Some(CloseReason::ConnectionClosed),
        "a peer ending the stream is one word for every transport"
    );
    assert_eq!(
        reason.unwrap().to_string(),
        "connection_closed",
        "and it renders once"
    );
}

/// The read half: bytes the peer sent arrive in the loop's own buffer, ready to
/// be framed, whichever wire carried them.
fn bytes_the_peer_sent_are_framed<W: Wire>(wire: &mut W) {
    let frame = flz_transport::service::payloader::build_from_binary(
        "p-2",
        b"from the peer",
        std::collections::HashMap::new(),
    );
    wire.peer_writes(&frame);

    let mut buf = ReadBuf::new();
    let mut taken = Vec::new();
    for _ in 0..200 {
        let reason = read_once(wire, &mut buf);
        taken.extend_from_slice(buf.taken());
        assert_eq!(reason, None, "a live stream reports no close");
        if taken.len() >= frame.len() {
            break;
        }
        wire.carry();
    }

    assert_eq!(taken, frame, "the loop reads exactly what the peer sent");
    let frames = wire
        .state()
        .frame_buffer
        .feed(&taken)
        .expect("the frame parses");
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].payload_id, "p-2");
    assert_eq!(frames[0].message, b"from the peer");
}

// --- what the vocabulary declares --------------------------------------------

/// Every reason is declared once and rendered once, including the two only one
/// wire can produce. A word written as a literal at the site that produces it
/// is what this set exists to prevent.
#[test]
fn the_close_vocabulary_is_one_declared_set() {
    let rendered: Vec<String> = [
        CloseReason::ConnectionClosed,
        CloseReason::StreamReset,
        CloseReason::ReadError,
        CloseReason::WriteError,
        CloseReason::FrameTooLarge,
        CloseReason::ConnectionLost("aborted by peer".into()),
    ]
    .iter()
    .map(CloseReason::to_string)
    .collect();

    assert_eq!(
        rendered,
        [
            "connection_closed",
            "stream_reset",
            "read_error",
            "write_error",
            "frame_too_large",
            "connection_lost: aborted by peer",
        ]
    );
}
