//! The QUIC client's reader: the deadline `set_read_timeout` arms, and
//! what a disconnected chunk channel means.

use super::*;

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
