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
        (slow, StreamSlot::new(slow_tx)),
        (other, StreamSlot::new(other_tx)),
    ]);

    let written = peer_writes(&mut pair, slow, frames);
    pair.server_conn()
        .send_stream(other)
        .write(CONTROL_FRAME)
        .expect("the peer writes on the other stream");
    deliver(&mut pair);

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

/// Step until the client has received one more stream frame than it had. One
/// step does not promise that: the peer paces its sends by the clock, and a
/// thread preempted between steps inflates the RTT it paces by, until it holds
/// frames back and later sends them together as fewer, larger chunks.
fn deliver(pair: &mut Pair) {
    let received = pair.client.stats().frame_rx.stream;
    let deadline = Instant::now() + Duration::from_secs(5);
    pair.step();
    while pair.client.stats().frame_rx.stream == received {
        assert!(Instant::now() < deadline, "the peer never sent its frame");
        pair.step();
    }
}

/// The peer writes `frames` separate frames on `stream`, each delivered before
/// the next is written, so every frame lands as its own chunk. Answers the
/// bytes it wrote, in order.
fn peer_writes(pair: &mut Pair, stream: StreamId, frames: usize) -> Vec<u8> {
    let mut written = Vec::new();
    for i in 0..frames {
        // Each frame carries its own byte, so a chunk delivered out of order
        // or twice fails the comparison rather than passing on length alone.
        let frame = vec![(i % 251) as u8; FRAME_LEN];
        let n = pair
            .server_conn()
            .send_stream(stream)
            .write(&frame)
            .expect("the peer writes on the stream");
        assert_eq!(n, frame.len(), "the peer's own window took the whole frame");
        written.extend_from_slice(&frame);
        deliver(pair);
    }
    written
}

const FRAME_LEN: usize = 1024;
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

/// A reader already waiting on an empty channel has to wake the driver when a
/// burst fills that channel under it, because nothing else will: the driver
/// holds the chunk the channel would not take, and a suspended stream has no
/// event of its own. Every case above drives the resume by hand with no waker,
/// so none of them can see a wake that is never sent.
///
/// Linux only: the order the case needs is forced with the scheduler, see
/// `run_on`.
#[cfg(target_os = "linux")]
mod reader_wake {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;

    /// How long a driver with nothing armed but the keep-alive
    /// `transport_config` sets sleeps between passes: the connection's next
    /// timer, which a suspended stream must not have to wait for.
    const KEEP_ALIVE: Duration = Duration::from_secs(5);

    /// How long one burst's verdict may take. When `KEEP_ALIVE` runs out, the
    /// driver makes a pass of its own that would hand the reader a held chunk
    /// with no wake at all. That timer starts when the driver parks after the
    /// burst, which is after this bound starts, so no verdict reached inside it
    /// can be the timer's doing; the second to spare covers timer rounding.
    const VERDICT_BOUND: Duration = Duration::from_secs(4);

    /// How long the reader may take to catch up before the next burst. Nothing
    /// is held while it does, so this only bounds how long a reader starved of
    /// CPU takes to be reported as one.
    const CATCH_UP_BOUND: Duration = Duration::from_secs(30);

    /// The lost-wake state is the verdict only once it holds on this many
    /// samples in a row, `SAMPLE` apart, and not for one moment between steps.
    const STABLE_SAMPLES: usize = 5;
    const SAMPLE: Duration = Duration::from_millis(10);

    /// The smallest burst a reader can fall behind on: one chunk more than its
    /// channel holds, so the driver has to hold one and suspend the stream.
    const BURST_FRAMES: usize = CHUNK_CHANNEL_DEPTH + 1;
    const BURST_BYTES: usize = BURST_FRAMES * FRAME_LEN;

    /// One lost wake fails the case. Another thread waking on the shared CPU
    /// mid-pass can hand it to the reader early, which serves that one burst
    /// by luck; four bursts keep that from hiding the defect for a whole run.
    const BURSTS: usize = 4;

    /// What the case asks of the driver thread, sent the way `write_frame`
    /// sends a `DriverCommand`: onto a channel, then a wake.
    enum Drive {
        /// The peer's next burst lands in this pass, as a socket's datagrams do.
        Burst,
        Stop,
    }

    /// What the driver thread publishes at the end of every pass, so the case
    /// is judged from state and never from how long anything took.
    #[derive(Default)]
    struct Published {
        /// Whether a chunk is held, as of the last pass.
        holding: AtomicBool,
        /// Burst passes finished.
        served: AtomicUsize,
        /// Bursts whose own pass, its resume included, ended with a chunk held.
        suspended: AtomicUsize,
        /// Holds released by a pass the reader's wake asked for.
        woken: AtomicUsize,
    }

    impl Published {
        fn publish(&self, burst: bool, released_on_wake: bool, holding: bool) {
            self.holding.store(holding, Ordering::SeqCst);
            if released_on_wake {
                self.woken.fetch_add(1, Ordering::SeqCst);
            }
            if burst {
                if holding {
                    self.suspended.fetch_add(1, Ordering::SeqCst);
                }
                self.served.fetch_add(1, Ordering::SeqCst);
            }
        }

        /// What a burst's verdict is measured from: `suspended`, then `woken`.
        fn marks(&self) -> (usize, usize) {
            let suspended = self.suspended.load(Ordering::SeqCst);
            (suspended, self.woken.load(Ordering::SeqCst))
        }

        /// Whether burst `burst`, sent at `marks`, is settled: its own pass
        /// held nothing, so no wake was owed, or a pass the reader's wake asked
        /// for has released what it held.
        fn settled(&self, burst: usize, (suspended, woken): (usize, usize)) -> bool {
            self.served.load(Ordering::SeqCst) > burst
                && (self.suspended.load(Ordering::SeqCst) == suspended
                    || self.woken.load(Ordering::SeqCst) > woken)
        }
    }

    /// Pin the calling thread to `cpu`, and with `beneath`, make it a thread
    /// whose own wake never preempts a normal one there.
    ///
    /// This gives the case the order production gets from timing. A release
    /// build's pass fills the channel in microseconds, inside the time a parked
    /// reader takes to wake, so the reader finds a full channel it never saw
    /// fill. A debug build's pass is a hundred times slower and a woken reader
    /// keeps pace with it, so the channel never fills at all. On one CPU,
    /// beneath the driver, the reader runs once the driver's pass is over.
    fn run_on(cpu: usize, beneath: bool) {
        // SAFETY: syscalls on the calling thread, each handed a value that
        // outlives the call.
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_SET(cpu, &mut set);
            let size = std::mem::size_of::<libc::cpu_set_t>();
            assert_eq!(libc::sched_setaffinity(0, size, &set), 0, "pin to {cpu}");
            if beneath {
                let param = libc::sched_param { sched_priority: 0 };
                let idle = libc::sched_setscheduler(0, libc::SCHED_IDLE, &param);
                assert_eq!(idle, 0, "SCHED_IDLE");
            }
        }
    }

    /// Run `work` on a thread `run_on` places, and answer the thread's id once
    /// it is there, with its handle.
    fn spawn_on<T: Send + 'static>(
        cpu: usize,
        beneath: bool,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> (libc::pid_t, thread::JoinHandle<T>) {
        let (tid_tx, tid_rx) = channel::bounded(1);
        let handle = thread::spawn(move || {
            run_on(cpu, beneath);
            // SAFETY: a syscall that takes nothing.
            let _ = tid_tx.send(unsafe { libc::gettid() });
            work()
        });
        (tid_rx.recv().expect("the thread starts"), handle)
    }

    /// Whether thread `tid` of this process is asleep. Neither thread here
    /// sleeps anywhere but in its wait: the reader in `recv`, the driver in
    /// `poll`.
    fn asleep(tid: libc::pid_t) -> bool {
        std::fs::read_to_string(format!("/proc/self/task/{tid}/stat"))
            .ok()
            .and_then(|stat| {
                stat.rsplit_once(") ")
                    .map(|(_, rest)| rest.starts_with('S'))
            })
            .unwrap_or(false)
    }

    /// Poll `met` until it holds or `bound` passes. The sleep is the poll
    /// interval, and it also leaves the CPU to a reader beneath this thread.
    fn within(bound: Duration, mut met: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + bound;
        while !met() {
            if Instant::now() >= deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(1));
        }
        true
    }

    fn holding(c: &Client) -> bool {
        c.streams.values().any(|slot| slot.held_chunk.is_some())
    }

    /// The driver thread, waiting the way `driver_loop` waits and on nothing
    /// else: a real `mio::Poll` with the connection's `Waker` on it, no socket,
    /// and no timer nearer than the keep-alive. So a pass the waker did not ask
    /// for is the keep-alive's, and a waker pass that is no burst's is the
    /// reader's. Answers every byte the peer wrote.
    fn drive_on_the_waker_alone(
        mut c: Client,
        mut poll: Poll,
        commands: channel::Receiver<Drive>,
        report: &Published,
    ) -> Vec<u8> {
        let mut events = Events::with_capacity(8);
        let mut written = Vec::new();
        loop {
            let _ = poll.poll(&mut events, Some(KEEP_ALIVE));
            let woken = events.iter().any(|event| event.token() == WAKER_TOKEN);
            let mut burst = false;
            while let Ok(command) = commands.try_recv() {
                match command {
                    Drive::Burst => {
                        written.extend(peer_writes(&mut c.pair, c.stream_id, BURST_FRAMES));
                        burst = true;
                    }
                    Drive::Stop => return written,
                }
            }
            let was_holding = holding(&c);
            dispatch_stream_events(&mut c.pair.client, &mut c.streams);
            resume_suspended_streams(&mut c.pair.client, &mut c.streams);
            let held = holding(&c);
            report.publish(burst, woken && was_holding && !held, held);
        }
    }

    /// The reader, reading until it has every burst or the driver has gone.
    fn read_everything(mut reader: ChannelReader, progress: &AtomicUsize) -> Vec<u8> {
        let mut read = Vec::new();
        let mut out = [0u8; FRAME_LEN];
        while read.len() < BURSTS * BURST_BYTES {
            match reader.read(&mut out) {
                Ok(n) if n > 0 => {
                    read.extend_from_slice(&out[..n]);
                    progress.store(read.len(), Ordering::Relaxed);
                }
                _ => break,
            }
        }
        read
    }

    /// The driver and the reader under test, and what the case reads of them.
    struct Case {
        commands: channel::Sender<Drive>,
        waker: Arc<Waker>,
        report: Arc<Published>,
        progress: Arc<AtomicUsize>,
        /// The reader's channel, never read from here: only asked if it is empty.
        chunks: channel::Receiver<Bytes>,
        driver_tid: libc::pid_t,
        reader_tid: libc::pid_t,
        driver: thread::JoinHandle<Vec<u8>>,
        reading: thread::JoinHandle<Vec<u8>>,
    }

    impl Case {
        /// The driver and the reader on this thread's CPU, the reader beneath.
        fn start() -> Case {
            // SAFETY: a syscall that takes nothing.
            let cpu = usize::try_from(unsafe { libc::sched_getcpu() }).expect("a cpu");
            let c = Client::connected();
            let chunks = c.read_rx.clone();
            let poll = Poll::new().expect("a poll");
            let waker = Arc::new(Waker::new(poll.registry(), WAKER_TOKEN).expect("a waker"));
            let reader = ChannelReader {
                rx: c.read_rx.clone(),
                buf: Bytes::new(),
                read_timeout: None,
                waker: Some(Arc::clone(&waker)),
            };
            let (commands, commands_rx) = channel::unbounded();
            let (report, progress) = (Arc::new(Published::default()), Arc::default());
            let (published, read) = (Arc::clone(&report), Arc::clone(&progress));
            let (driver_tid, driver) = spawn_on(cpu, false, move || {
                drive_on_the_waker_alone(c, poll, commands_rx, &published)
            });
            let (reader_tid, reading) = spawn_on(cpu, true, move || read_everything(reader, &read));
            Case {
                commands,
                waker,
                report,
                progress,
                chunks,
                driver_tid,
                reader_tid,
                driver,
                reading,
            }
        }

        fn got(&self) -> usize {
            self.progress.load(Ordering::Relaxed)
        }

        /// Ask the driver for something, the way `write_frame` does.
        fn ask(&self, command: Drive) {
            let _ = self.commands.send(command);
            let _ = self.waker.wake();
        }

        /// The wake lost: the driver parked on a held chunk and the reader
        /// asleep on an empty channel, which only the keep-alive would change.
        fn wake_lost(&self) -> bool {
            self.report.holding.load(Ordering::SeqCst)
                && asleep(self.driver_tid)
                && asleep(self.reader_tid)
                && self.chunks.is_empty()
        }
    }

    /// Send the bursts one at a time, each to a reader already waiting for it,
    /// and judge each. Answers what went wrong, if anything did.
    fn feed(case: &Case) -> Result<(), String> {
        for burst in 0..BURSTS {
            let before = burst * BURST_BYTES;
            // The reader has everything so far and is asleep, so the check it
            // makes before it blocks has already seen an empty channel; the
            // driver is parked, so its last pass is published.
            let waiting =
                || case.got() == before && asleep(case.reader_tid) && asleep(case.driver_tid);
            if !within(CATCH_UP_BOUND, waiting) {
                return Err(format!(
                    "the reader never came to wait for burst {}: it was starved of CPU, so \
                     the run proves nothing about the wake",
                    burst + 1
                ));
            }
            let marks = case.report.marks();
            case.ask(Drive::Burst);
            judge(case, burst, before, marks)?;
        }
        Ok(())
    }

    /// One burst's verdict, read from state: settled is the wake arriving or no
    /// wake owed, and the lost-wake state held for `STABLE_SAMPLES` is the wake
    /// lost. Neither inside `VERDICT_BOUND` is a starved reader, which proves
    /// nothing either way.
    fn judge(
        case: &Case,
        burst: usize,
        before: usize,
        marks: (usize, usize),
    ) -> Result<(), String> {
        let deadline = Instant::now() + VERDICT_BOUND;
        let mut lost = 0;
        while Instant::now() < deadline {
            if case.report.settled(burst, marks) {
                return Ok(());
            }
            lost = if case.wake_lost() { lost + 1 } else { 0 };
            if lost == STABLE_SAMPLES {
                return Err(format!(
                    "burst {} of {BURSTS}: the driver is parked holding a chunk and the reader \
                     is asleep on an empty channel after {} of the burst's {BURST_FRAMES} \
                     chunks; the wake that resumes the stream was never sent",
                    burst + 1,
                    case.got().saturating_sub(before) / FRAME_LEN
                ));
            }
            thread::sleep(SAMPLE);
        }
        Err(format!(
            "burst {} of {BURSTS}: in {VERDICT_BOUND:?} the reader neither released the held \
             chunk nor went to sleep without it; a SCHED_IDLE reader gets almost no CPU beside \
             a busy task, so the run proves nothing about the wake",
            burst + 1
        ))
    }

    #[test]
    fn a_burst_to_a_reader_already_waiting_arrives_without_a_timer() {
        let case = Case::start();
        let failed = feed(&case).err();

        // Release both threads however the case went: the driver on its waker,
        // the reader on the disconnect the driver's exit makes.
        case.ask(Drive::Stop);
        let suspended = case.report.suspended.load(Ordering::SeqCst);
        let written = case.driver.join().expect("the driver ends");
        let read = case.reading.join().expect("the reader ends");

        if let Some(why) = failed {
            panic!("{why}");
        }
        // One suspended burst is the whole test: it met a reader still inside
        // the wait it began on an empty channel. A burst the reader kept pace
        // with proves nothing, and no suspended burst at all is a scheduler
        // that stopped giving the case its order.
        assert!(
            suspended > 0,
            "no burst filled the reader's channel and suspended the stream, so no \
             wake was needed and the case tested nothing"
        );
        assert!(
            read == written,
            "every byte the peer wrote arrives once, in order"
        );
    }
}
