//! The QUIC **client**'s per-stream send buffer, driven against a peer that
//! completes the handshake and never reads. This is the buffer every SDK
//! compiles in: `flz_core` and `flz_core_ffi` both reach the controller
//! through it.
//!
//! Each drive calls `write_or_buffer` exactly as `DriverCommand::Write` does,
//! and reads the answer off the reply channel a real `QuicBidiStream::write`
//! blocks on — so what these tests assert is what a caller is told.

use super::*;
use crate::transport::quic_pair::Pair;

type Reply = channel::Receiver<io::Result<usize>>;

/// The client driver's stream map with one stream in it, driven the way
/// `handle_command` and `dispatch_stream_events` drive the real one.
struct Client {
    pair: Pair,
    stream_id: StreamId,
    streams: HashMap<StreamId, StreamSlot>,
    /// The slot's reader end, held so the chunk channel stays connected for
    /// the life of the drive, as the real driver's `ChannelReader` does.
    _read_rx: channel::Receiver<Bytes>,
}

impl Client {
    fn connected() -> Client {
        let mut pair = Pair::new();
        pair.connect();
        let stream_id = pair.open_stream();
        let (read_tx, read_rx) = channel::unbounded();
        let streams = HashMap::from([(
            stream_id,
            StreamSlot {
                read_tx,
                write_buf: Vec::new(),
                pending_write_replies: Vec::new(),
                gate: SendGate::new(),
            },
        )]);
        Client {
            pair,
            stream_id,
            streams,
            _read_rx: read_rx,
        }
    }

    fn slot(&self) -> &StreamSlot {
        &self.streams[&self.stream_id]
    }

    /// One `DriverCommand::Write`, and the answer its caller would receive.
    /// `None` means the reply is still deferred, which is the stream telling
    /// the writer to wait — the client's whole backpressure mechanism.
    fn write(&mut self, data: Vec<u8>) -> (Reply, Option<io::Result<usize>>) {
        let (reply_tx, reply_rx) = channel::bounded(1);
        let slot = self.streams.get_mut(&self.stream_id).unwrap();
        write_or_buffer(&mut self.pair.client, self.stream_id, slot, data, reply_tx);
        let answer = reply_rx.try_recv().ok();
        (reply_rx, answer)
    }

    /// A writable event plus a peer that reads: one drain pass.
    fn drain_pass(&mut self) {
        self.pair.server_reads(self.stream_id);
        self.pair.step();
        flush_write_buffer(&mut self.pair.client, self.stream_id, &mut self.streams);
        self.pair.step();
    }

    /// Fill the peer's flow-control window so the next write blocks. Returns
    /// the receiver of the write that is now buffered and still owed an answer.
    fn block(&mut self) -> Reply {
        let frame = vec![b'a'; 256 * 1024];
        for _ in 0..200 {
            let (rx, answer) = self.write(frame.clone());
            self.pair.step();
            if answer.is_none() {
                return rx;
            }
        }
        panic!("the peer's flow-control window never closed");
    }
}

/// A second writer must not evict the first's buffered remainder, and must not
/// drop the reply sender the first one is blocked on. Both statements in the
/// blocked arm assigned, so the first writer's bytes vanished from the stream
/// mid-frame and its `recv()` reported a sender nobody holds.
#[test]
fn a_second_blocked_write_appends_behind_the_first_and_leaves_it_answerable() {
    let mut c = Client::connected();

    let first_rx = c.block();
    let buffered_after_first = c.slot().write_buf.len();
    assert!(
        buffered_after_first > 0,
        "the first writer left bytes behind"
    );

    let second = vec![b'b'; 64 * 1024];
    let (second_rx, second_answer) = c.write(second.clone());
    assert!(second_answer.is_none(), "a blocked write defers its reply");

    assert_eq!(
        c.slot().write_buf.len(),
        buffered_after_first + second.len(),
        "the second frame appends; it must not replace what is already queued"
    );
    assert!(
        c.slot().write_buf.ends_with(&second),
        "the second frame queues BEHIND the first's remainder, never ahead of it"
    );
    assert_eq!(
        c.slot().pending_write_replies.len(),
        2,
        "both writers are still owed an answer"
    );

    // Both senders are alive. A dropped one reports `BrokenPipe` naming
    // neither the stream nor the loss, which is what the first writer got.
    assert!(matches!(
        first_rx.try_recv(),
        Err(channel::TryRecvError::Empty)
    ));
    assert!(matches!(
        second_rx.try_recv(),
        Err(channel::TryRecvError::Empty)
    ));

    for _ in 0..400 {
        c.drain_pass();
        if c.slot().write_buf.is_empty() {
            break;
        }
    }
    assert!(
        c.slot().write_buf.is_empty(),
        "a reading peer drains the buffer"
    );
    assert_eq!(first_rx.try_recv().unwrap().unwrap(), 256 * 1024);
    assert_eq!(second_rx.try_recv().unwrap().unwrap(), second.len());
}

/// A frame the ceiling turns away is never reported as written. `Ok(n)` from a
/// `Write` promises `n` bytes were accepted; a caller has no other way to tell
/// a refusal from a real write.
#[test]
fn a_frame_refused_at_the_ceiling_is_reported_as_refused_not_as_written() {
    let cap = crate::transport::config::max_write_buffer_bytes();
    let mut c = Client::connected();

    let _first_rx = c.block();
    // Fill to the ceiling without crossing it, then offer one frame too many.
    let filler = vec![b'c'; 512 * 1024];
    let mut fills = 0;
    while c.slot().write_buf.len() + filler.len() <= cap {
        let (_rx, answer) = c.write(filler.clone());
        assert!(
            answer.is_none(),
            "buffered writes defer, they do not answer"
        );
        fills += 1;
        assert!(
            fills <= 2 + cap / filler.len(),
            "the buffer is not accumulating: {} bytes after {} appends of {}",
            c.slot().write_buf.len(),
            fills,
            filler.len()
        );
    }
    let buffered = c.slot().write_buf.len();

    let (_rx, answer) = c.write(vec![b'd'; cap]);

    let answer = answer.expect("a refused frame is answered at once, not deferred");
    let err = answer.expect_err("a discarded frame must never answer Ok(n)");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(
        c.slot().write_buf.len(),
        buffered,
        "the refused frame left the buffer alone"
    );
    assert_eq!(c.slot().gate.refused(), 1, "and it was counted");
}

/// The control: against a peer that reads, nothing is refused and every writer
/// is answered with its own byte count.
#[test]
fn a_reading_peer_answers_every_writer_and_refuses_nothing() {
    let mut c = Client::connected();

    let frame = vec![b'e'; 32 * 1024];
    let mut answers = Vec::new();
    let mut deferred = Vec::new();
    for _ in 0..40 {
        match c.write(frame.clone()) {
            (_, Some(answer)) => answers.push(answer),
            (rx, None) => deferred.push(rx),
        }
        c.drain_pass();
    }
    for _ in 0..200 {
        c.drain_pass();
    }
    for rx in deferred {
        answers.push(
            rx.recv()
                .expect("a deferred writer is answered, never dropped"),
        );
    }

    assert_eq!(
        c.slot().write_buf.len(),
        0,
        "a draining peer leaves nothing"
    );
    assert_eq!(c.slot().gate.refused(), 0, "nothing was refused");
    assert_eq!(answers.len(), 40, "every writer got exactly one answer");
    for answer in answers {
        assert_eq!(
            answer.unwrap(),
            frame.len(),
            "every writer is told its own byte count"
        );
    }
}
