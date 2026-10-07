//! The reconnect ladder against a controller that refuses the session.
//!
//! `try_open` answers `Ok` on `connect(2)` alone — the handshake is written by
//! the thread `sock_conn` spawns — so a controller that accepts the socket and
//! then closes it without admitting the runner never reaches the ladder's
//! `Err` arm. The controller driven here is exactly the one
//! `Transport.Server` presents when `connect_handler` refuses: accept, close,
//! say nothing. The loop is the coordinator's — open, take the `Reconnect` the
//! dead session posts, open again.
//!
//! The reset is the other half. A session is admitted only once its handshake
//! completed: the runner answered `ConnectSocket` and the controller carried
//! on with another frame. A controller that answers the first frame and closes
//! is refusing too, and is charged the ladder like one.
//!
//! An integration test rather than a unit one: the ladder's state and the
//! socket directory are both process-wide, so this needs a binary of its own.

use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam::channel::{unbounded, Receiver, Sender};
use flz_core::control_unit;
use flz_core::runner::RunnerMessage;
use flz_transport::service::payloader;
use flz_transport::utility::socket_dir;

mod common;

/// Sessions the refusal phase drives. Open 1 is immediate; opens 2, 3 and 4
/// are preceded by rungs 1, 2 and 3 of the `:sdk_reconnect` ladder —
/// `100 * 2^rung` ms, full-jittered into `[base/2, base]` — so the phase
/// cannot finish in less than 100 + 200 + 400 ms.
const REFUSED_SESSIONS: usize = 4;
const LADDER_FLOOR: Duration = Duration::from_millis(700);
/// The same three rungs at their unjittered ceiling (200 + 400 + 800 ms),
/// with room for the connects themselves: a wait that outruns this is the
/// ladder climbing where it should have reset.
const LADDER_CEILING: Duration = Duration::from_secs(3);

struct Controller {
    path: String,
    accepted: Receiver<()>,
    frames_next: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Controller {
    /// Accept every connection and close it. With `frames_next` raised, write
    /// that many control frames first, each only after the runner's test has
    /// released it: the `assign_runner_id` a real controller writes, then
    /// whatever it sends once it has carried on with the session.
    fn refusing(dir: &Path, release: Receiver<()>) -> Controller {
        let path = socket_dir::control_socket_path_in(dir).expect("control socket path");
        let listener = UnixListener::bind(&path).expect("bind control socket");

        let frames_next = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let (noted, accepted) = unbounded();

        let thread = {
            let frames_next = Arc::clone(&frames_next);
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                for conn in listener.incoming() {
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    let Ok(mut conn) = conn else { return };
                    let frames = frames_next.swap(0, Ordering::Relaxed);
                    if frames > 0 {
                        let mut buf = [0u8; 4096];
                        let _ = conn.read(&mut buf);
                    }
                    for _ in 0..frames {
                        let _ = release.recv();
                        let frame = payloader::build_from_binary(
                            "control_command",
                            b"\x08\x01",
                            std::collections::HashMap::new(),
                        )
                        .unwrap();
                        let _ = conn.write_all(&frame);
                    }
                    let _ = noted.send(());
                }
            })
        };

        Controller {
            path,
            accepted,
            frames_next,
            stop,
            thread: Some(thread),
        }
    }

    fn write_next_session(&self, frames: usize) {
        self.frames_next.store(frames, Ordering::Relaxed);
    }

    fn accepts(&self) -> usize {
        self.accepted.try_iter().count()
    }
}

impl Drop for Controller {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        // Unblock the parked `accept(2)` so the acceptor leaves rather than
        // outliving the test.
        let _ = UnixStream::connect(&self.path);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn declared() -> Arc<RwLock<Vec<String>>> {
    Arc::new(RwLock::new(vec!["probe".to_string()]))
}

/// Wait out the session's death, skipping the frames it forwarded first.
fn await_reconnect(orx: &Receiver<RunnerMessage>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match orx.recv_deadline(deadline) {
            Ok(RunnerMessage::ControlLost { .. }) => return,
            Ok(_) => continue,
            Err(e) => panic!("a session the controller closed must post a Reconnect: {e}"),
        }
    }
}

/// The next frame the session forwards, which is the reader having read it.
fn await_frame(orx: &Receiver<RunnerMessage>) {
    let frame = orx.recv_timeout(Duration::from_secs(5));
    assert!(
        matches!(frame, Ok(RunnerMessage::HandleCommandAction(..))),
        "the controller's frame was not forwarded"
    );
}

/// One reconnect: open it and wait for the `Reconnect` its death posts.
/// Answers how long the open itself took — the ladder's wait plus one
/// `connect(2)`.
fn one_session(otx: &Arc<Mutex<Sender<RunnerMessage>>>, orx: &Receiver<RunnerMessage>) -> Duration {
    let started = Instant::now();
    let conn = control_unit::open_reconnect("kb111_ns".to_string(), declared(), otx.clone());
    let opened = started.elapsed();
    await_reconnect(orx);
    drop(conn);
    opened
}

/// A runner STARTING is `open`, not `open_reconnect` — the entry point a host
/// reaches through `flz_core_start`, and it is owed none of the previous
/// runner's ladder: the count belongs to the runner, and start-close-start in
/// one process is a documented flow.
fn assert_a_fresh_start_does_not_wait(
    otx: &Arc<Mutex<Sender<RunnerMessage>>>,
    orx: &Receiver<RunnerMessage>,
) {
    let (itx, _irx) = unbounded();
    let started = Instant::now();
    let conn = control_unit::open(
        "kb111_ns".to_string(),
        declared(),
        Arc::new(Mutex::new(itx)),
        otx.clone(),
    );
    let waited = started.elapsed();
    await_reconnect(orx);
    drop(conn);

    assert!(
        waited < Duration::from_millis(100),
        "a starting runner waited {:?} before its first connect(2); it inherited the \
         ladder of the runner that ran before it",
        waited
    );
}

/// Sessions the controller accepts and closes without admitting the runner,
/// back to back, as the coordinator drives them. Answers what they took.
fn drive_refused_sessions(
    otx: &Arc<Mutex<Sender<RunnerMessage>>>,
    orx: &Receiver<RunnerMessage>,
) -> Duration {
    let started = Instant::now();
    for _ in 0..REFUSED_SESSIONS {
        one_session(otx, orx);
    }
    started.elapsed()
}

#[test]
fn a_refused_session_backs_the_reconnect_ladder_off() {
    let dir = common::isolated_socket_dir("kb111");
    let (release, released) = unbounded();
    let controller = Controller::refusing(&dir, released);

    let (otx, orx) = unbounded();
    let otx = Arc::new(Mutex::new(otx));

    let refusal_phase = drive_refused_sessions(&otx, &orx);

    assert_eq!(
        controller.accepts(),
        REFUSED_SESSIONS,
        "one accept per session"
    );
    assert!(
        refusal_phase >= LADDER_FLOOR,
        "{} refused sessions took {:?}; the ladder's own arithmetic puts three rungs \
         at {:?} or more, so a refused session is reconnecting with no backoff at all",
        REFUSED_SESSIONS,
        refusal_phase,
        LADDER_FLOOR
    );
    assert!(
        refusal_phase < LADDER_CEILING,
        "{} refused sessions took {:?}, past the ladder's ceiling of {:?}",
        REFUSED_SESSIONS,
        refusal_phase,
        LADDER_CEILING
    );

    assert_a_fresh_start_does_not_wait(&otx, &orx);

    // A controller that writes its first frame and closes has not admitted
    // the runner: the rung climbs past the fresh start's zero.
    controller.write_next_session(1);
    let conn = control_unit::open_reconnect("kb111_ns".to_string(), declared(), otx.clone());
    release.send(()).unwrap();
    await_frame(&orx);
    await_reconnect(&orx);
    drop(conn);

    let after_first_frame = one_session(&otx, &orx);
    assert!(
        after_first_frame >= Duration::from_millis(100),
        "the reconnect after a session that only answered its first frame waited {:?}; \
         the first frame reset the ladder",
        after_first_frame
    );

    // A completed handshake is what the ladder resets on: the runner answers
    // `ConnectSocket` and the controller carries on with a further frame.
    controller.write_next_session(2);
    let (answered, stream, quic) =
        control_unit::open_reconnect("kb111_ns".to_string(), declared(), otx.clone());
    release.send(()).unwrap();
    await_frame(&orx);
    answered.note();
    release.send(()).unwrap();
    await_frame(&orx);
    await_reconnect(&orx);
    drop((stream, quic));

    let after_admitted = one_session(&otx, &orx);
    assert!(
        after_admitted < Duration::from_millis(100),
        "the reconnect after an admitted session waited {:?}; a controller restart \
         must not be charged the refusal ladder",
        after_admitted
    );

    drop(controller);
    let _ = std::fs::remove_dir_all(&dir);
}
