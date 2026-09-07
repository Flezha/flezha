//! The QUIC client's reader: the deadline `set_read_timeout` arms, what a
//! disconnected chunk channel means, and what ends one stream of a live
//! connection, which is a case driven over UDS as well so the two wires are
//! held to one answer.

use std::net::Shutdown;
use std::os::unix::net::UnixStream;

use super::client_tests::Client;
use super::*;
use crate::service::payloader::extract_payload;
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
