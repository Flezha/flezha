//! The QUIC client's reader: the deadline `set_read_timeout` arms, what a
//! disconnected chunk channel means, and what ends one stream of a live
//! connection, which is a case driven over UDS as well so the two wires are
//! held to one answer.

use std::net::Shutdown;
use std::os::unix::net::UnixStream;

use super::client_tests::Client;
use super::*;
use crate::service::payloader::extract_payload;
use crate::transport::quic_pair::Pair;
use crate::transport::FlezhaStream;

// A QUIC read timeout is a real deadline: an idle stream returns
// `TimedOut`, so control-channel liveness holds on QUIC as on UDS.
#[test]
fn channel_reader_honors_read_timeout_when_idle() {
    let (_tx, rx) = channel::bounded::<Bytes>(4);
    let mut reader = ChannelReader {
        rx,
        buf: Bytes::new(),
        read_timeout: Some(Duration::from_millis(120)),
        waker: None,
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
        waker: None,
    };

    let mut out = [0u8; 64];
    let n = reader.read(&mut out).unwrap();
    assert_eq!(&out[..n], b"hello");
    // Disconnected channel = stream finished = EOF (Ok(0)), not WouldBlock.
    assert_eq!(reader.read(&mut out).unwrap(), 0);
}

/// One way a peer can end one stream, and the wire it ends it on.
///
/// The two QUIC cases are what this file measures: each drives the real
/// `dispatch_stream_events` and `drain_readable` against a real `quinn_proto`
/// peer. `UdsShutdown` is a **control**, not a second measurement: it is
/// `UnixStream` and `read_exact`, so it cannot go red for any reason inside
/// Flezha. It is here to show what the answer is supposed to look like.
#[derive(Debug, Clone, Copy)]
enum Case {
    UdsShutdown,
    QuicFinish,
    QuicReset,
}

/// One connected stream, ended by the peer while the connection lives on, read
/// the way `flz_nif`'s client reader reads it. Answers the error
/// `extract_payload` gives, which is the whole input to the close that reader
/// reports.
fn peer_ends_one_stream(case: Case) -> io::Error {
    if let Case::UdsShutdown = case {
        let (mine, peer) = UnixStream::pair().expect("a socket pair");
        peer.shutdown(Shutdown::Write)
            .expect("the peer ends its half");
        return extract_payload(&mut FlezhaStream::Unix(mine)).expect_err("the stream has ended");
    }

    let mut c = Client::connected();
    match case {
        // What a controller-side release sends, beside its STOP_SENDING.
        Case::QuicReset => c
            .pair
            .server_conn()
            .send_stream(c.stream_id)
            .reset(quinn_proto::VarInt::from_u32(7))
            .expect("the peer resets its half"),
        _ => c
            .pair
            .server_conn()
            .send_stream(c.stream_id)
            .finish()
            .expect("the peer ends its half"),
    }
    c.pair.step();
    assert!(
        !dispatch_stream_events(&mut c.pair.client, &mut c.streams),
        "{:?}: one stream ending must not be read as the connection dying",
        case
    );

    // `QuicBidiStream::read` is this call behind a lock, so reading the reader
    // is reading the stream. The deadline is what makes an unfixed run fail
    // instead of waiting for a frame that never comes.
    let mut reader = ChannelReader {
        rx: c.read_rx.clone(),
        buf: Bytes::new(),
        read_timeout: Some(Duration::from_millis(250)),
        waker: None,
    };
    extract_payload(&mut reader).expect_err("the stream has ended")
}

// A peer ending ONE stream is an end of stream to that stream's reader. Over
// QUIC it was not: the driver kept the chunk sender in its stream map until the
// whole connection died, so a runner went on reading a stream the controller
// had already released. Both ways of ending it are driven, because the fix
// answers `Ok(None)` and `Err(Reset)` together and a case that drove only the
// FIN would leave half of it unpinned.
#[test]
fn ending_one_quic_stream_reads_as_end_of_stream_like_the_uds_control() {
    for case in [Case::UdsShutdown, Case::QuicFinish, Case::QuicReset] {
        let err = peer_ends_one_stream(case);
        assert_eq!(
            err.kind(),
            io::ErrorKind::UnexpectedEof,
            "{:?}: a peer ending one stream must read as end of stream, got {}",
            case,
            err
        );
    }
}

/// One connection carrying a stream whose reader has stopped, and one other
/// stream beside it. Both chunk channels take production's own bound, so a
/// case here cannot come out green against a channel that never fills.
///
/// The receivers are handed back rather than kept, because the drive runs on
/// its own thread: a fixture that owned them would keep the channel connected
/// and a wedged drive could never be released.
struct Congested {
    pair: Pair,
    slow: StreamId,
    other: StreamId,
    streams: HashMap<StreamId, StreamSlot>,
    /// Everything the peer wrote on the slow stream, in order, which is what a
    /// resumed reader has to end up holding.
    written: Vec<u8>,
}

/// The peer writes `frames` separate 1 KiB frames on the slow stream, each
/// its own chunk, so the count is the channel's and not the byte total's, and
/// one frame on the other stream. Nothing reads the slow stream.
fn congested(
    frames: usize,
) -> (
    Congested,
    channel::Receiver<Bytes>,
    channel::Receiver<Bytes>,
) {
    let mut pair = Pair::new();
    pair.connect();
    let slow = pair.open_stream();
    let other = pair.open_stream();

    let (slow_tx, slow_rx) = channel::bounded(CHUNK_CHANNEL_DEPTH);
    let (other_tx, other_rx) = channel::bounded(CHUNK_CHANNEL_DEPTH);
    let streams = HashMap::from([
        (slow, client_tests::driver_slot(slow_tx)),
        (other, client_tests::driver_slot(other_tx)),
    ]);

    let mut written = Vec::new();
    for i in 0..frames {
        // Each frame carries its own byte, so a chunk delivered out of order
        // or twice fails the comparison rather than passing on length alone.
        let frame = vec![(i % 251) as u8; 1024];
        let n = pair
            .server_conn()
            .send_stream(slow)
            .write(&frame)
            .expect("the peer writes on the slow stream");
        assert_eq!(n, frame.len(), "the peer's own window took the whole frame");
        written.extend_from_slice(&frame);
        pair.step();
    }
    pair.server_conn()
        .send_stream(other)
        .write(CONTROL_FRAME)
        .expect("the peer writes on the other stream");
    pair.step();

    (
        Congested {
            pair,
            slow,
            other,
            streams,
            written,
        },
        slow_rx,
        other_rx,
    )
}

const CONTROL_FRAME: &[u8] = b"control-frame";
const CONGESTING_FRAMES: usize = 400;

// One reader that stops reading costs its own stream and nothing else. The
// chunk channel is bounded, and a `send` that waits for room waits on the one
// driver thread, the same thread that expires this connection's timers, pumps
// its socket, takes commands and serves every other stream on it. So what is
// measured is not whether the slow stream stalled, which it must, but whether
// the drive returned at all and whether the other stream was served.
#[test]
fn one_reader_that_stops_reading_does_not_stall_the_rest_of_the_connection() {
    let (mut c, slow_rx, other_rx) = congested(CONGESTING_FRAMES);

    let (done_tx, done_rx) = channel::bounded::<bool>(1);
    let drive = std::thread::spawn(move || {
        let lost = dispatch_stream_events(&mut c.pair.client, &mut c.streams);
        let _ = done_tx.send(lost);
        c
    });

    let finished = done_rx.recv_timeout(Duration::from_secs(5));
    let served = other_rx.try_recv();
    let backed_up = slow_rx.len();

    // Release a drive that is wedged, so a red run leaves no thread behind:
    // the only thing a blocked `send` answers to is a dropped receiver.
    drop(slow_rx);
    drop(other_rx);
    let c = drive.join().expect("the drive thread ends");

    assert_eq!(
        backed_up, CHUNK_CHANNEL_DEPTH,
        "the case must fill the slow reader's channel, or it is testing nothing"
    );
    assert_eq!(
        finished,
        Ok(false),
        "the driver loop must not wait on a reader that is behind; \
         it serves every other stream on this connection, and its timers"
    );
    assert_eq!(
        served.as_deref(),
        Ok(CONTROL_FRAME),
        "the other stream's reader is served while the slow one is suspended"
    );
    assert!(
        c.streams.contains_key(&c.other),
        "neither stream ended, so both slots stay"
    );
}

// The other half of not waiting: a stream suspended because its reader was
// behind resumes when the reader catches up, and every byte the peer wrote
// arrives once, in the order it wrote them. A chunk dropped at a full channel
// would leave a hole in an ordered byte stream that no later read repairs, so
// the refused chunk is held and offered again first.
#[test]
fn a_suspended_stream_resumes_and_delivers_every_byte_in_order() {
    let (mut c, slow_rx, _other_rx) = congested(CONGESTING_FRAMES);

    dispatch_stream_events(&mut c.pair.client, &mut c.streams);
    assert_eq!(
        slow_rx.len(),
        CHUNK_CHANNEL_DEPTH,
        "the case must congest the slow reader, or it is testing nothing"
    );

    let mut read: Vec<u8> = Vec::new();
    // Generously more passes than the 400 chunks need at 64 a pass, so a
    // stream that stops resuming fails on what arrived rather than by hanging.
    for _ in 0..CONGESTING_FRAMES {
        while let Ok(chunk) = slow_rx.try_recv() {
            read.extend_from_slice(&chunk);
        }
        if read.len() >= c.written.len() {
            break;
        }
        resume_suspended_streams(&mut c.pair.client, &mut c.streams);
    }

    assert_eq!(
        read.len(),
        c.written.len(),
        "every byte the peer wrote reaches the reader once it drains"
    );
    assert!(read == c.written, "and in the order the peer wrote them");
    assert!(
        c.streams[&c.slow].held_chunk.is_none(),
        "nothing stays held once the reader has caught up"
    );
}
