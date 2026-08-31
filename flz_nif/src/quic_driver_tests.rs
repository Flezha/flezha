//! The QUIC server driver's per-stream send buffer, driven against a peer that
//! completes the handshake and never reads.
//!
//! The connected pair is `flz_transport`'s shared in-memory harness, the same
//! one the client's own send-buffer suite drives, so the two buffers are
//! exercised against one definition of a stalled peer.

use flz_transport::transport::quic_pair::Pair;
use flz_transport::transport::send_gate::{GateChange, SendGate, Transition};

use super::*;

/// The driver's write path for one frame, exactly as `write_stream` runs it:
/// offer at the ceiling, then flush what the peer will take. Returns the gate
/// transition the driver would have reported to the stream's owner.
fn offer_and_flush(
    pair: &mut Pair,
    write_buffer: &mut Vec<u8>,
    stream_id: StreamId,
    gate: &SendGate,
    frame: &[u8],
) -> Transition {
    gate.offer(write_buffer, frame);
    let (broken, change) = flush_quic_write(pair.server_conn(), stream_id, write_buffer, gate);
    assert!(!broken, "the stream must not break");
    pair.step();
    change
}

/// A gate that never engages: watermarks and ceiling at `usize::MAX`. It is
/// the negative control every gate drive owes, because a gate that never
/// engages looks exactly like a gate that works.
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
    let mut buffer = Vec::new();

    let frame = vec![b'x'; 64 * 1024];
    let mut offered = 0usize;
    for _ in 0..400 {
        offer_and_flush(&mut pair, &mut buffer, stream_id, &gate, &frame);
        offered += frame.len();
    }
    let after_first_burst = buffer.len();

    // Offer as much again. A bounded buffer would hold at its bound; this one
    // takes every byte, which is what "no cap at all" means.
    for _ in 0..400 {
        offer_and_flush(&mut pair, &mut buffer, stream_id, &gate, &frame);
        offered += frame.len();
    }

    println!(
        "ungated quic write_buffer: offered={} buffered={} cap={} (x{:.1} the cap); \
         after first burst={} growth={} refused={}",
        offered,
        buffer.len(),
        cap,
        buffer.len() as f64 / cap as f64,
        after_first_burst,
        buffer.len() - after_first_burst,
        gate.refused(),
    );
    assert!(
        after_first_burst > cap,
        "KI-56: an ungated QUIC send buffer holds {} bytes against a {} byte cap",
        after_first_burst,
        cap
    );
    assert_eq!(
        buffer.len() - after_first_burst,
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
    let mut buffer = Vec::new();

    let frame = vec![b'x'; 64 * 1024];
    let mut raises = 0;
    for _ in 0..800 {
        if offer_and_flush(&mut pair, &mut buffer, stream_id, &gate, &frame)
            == Some(GateChange::Raised)
        {
            raises += 1;
        }
    }

    println!(
        "gated quic write_buffer: buffered={} cap={} raised={} refused={} raises={}",
        buffer.len(),
        cap,
        gate.is_raised(),
        gate.refused(),
        raises,
    );
    assert!(
        buffer.len() <= cap,
        "the buffer must hold at the ceiling, held {} against {}",
        buffer.len(),
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
    let mut buffer = Vec::new();

    let frame = vec![b'x'; 64 * 1024];
    let mut read_total = 0usize;
    for _ in 0..400 {
        offer_and_flush(&mut pair, &mut buffer, stream_id, &gate, &frame);
        read_total += pair.client_reads(stream_id);
        pair.step();
    }
    for _ in 0..200 {
        flush_quic_write(pair.server_conn(), stream_id, &mut buffer, &gate);
        pair.step();
        read_total += pair.client_reads(stream_id);
    }

    println!(
        "quic write_buffer with a draining peer: buffered={} read={} refused={} raised={}",
        buffer.len(),
        read_total,
        gate.refused(),
        gate.is_raised(),
    );
    assert_eq!(buffer.len(), 0, "a draining peer leaves nothing buffered");
    assert_eq!(gate.refused(), 0, "nothing was refused");
    assert!(!gate.is_raised(), "the gate is down for a healthy peer");
}
