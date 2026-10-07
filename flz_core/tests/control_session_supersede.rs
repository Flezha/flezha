//! A control session the runner replaced must not end the session that
//! replaced it.
//!
//! The coordinator opens a new session whenever something other than the
//! control reader asks it to — a data-stream write flood, a controller request.
//! The replaced session's reader still holds its socket, and a controller that
//! leaves that socket open stops writing to it. One idle window later that
//! reader's watchdog fires; honoured against the live session, it ends a
//! healthy session whose own reader then does the same to the next one, so
//! the runner reconnects once per idle window for ever.
//!
//! An integration test of its own: the coordinator, the socket directory and
//! the liveness knobs are all process-wide.

use std::collections::HashMap;
use std::io::Write;
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam::channel::unbounded;
use flz_core::coordinator;
use flz_core::runner::RunnerMessage;
use flz_core::shutdown::ShutdownBridge;
use flz_transport::service::payloader;
use flz_transport::transport::liveness::CONTROL_PING_PAYLOAD_ID;
use flz_transport::utility::socket_dir;

mod common;

const IDLE_WINDOW: Duration = Duration::from_millis(300);

/// Accepts every control connection, keeps each one open, and writes a ping
/// echo only to the newest — the controller a superseded session faces.
fn controller(path: &str) -> Arc<Mutex<Vec<UnixStream>>> {
    let listener = UnixListener::bind(path).expect("bind control socket");
    let sessions: Arc<Mutex<Vec<UnixStream>>> = Arc::new(Mutex::new(Vec::new()));

    let accepted = Arc::clone(&sessions);
    thread::spawn(move || {
        for conn in listener.incoming().flatten() {
            accepted.lock().unwrap().push(conn);
        }
    });

    let live = Arc::clone(&sessions);
    thread::spawn(move || {
        let echo = payloader::build_from_binary(CONTROL_PING_PAYLOAD_ID, b"", HashMap::new())
            .expect("ping frame");
        loop {
            if let Some(newest) = live.lock().unwrap().last_mut() {
                let _ = newest.write_all(&echo);
            }
            thread::sleep(Duration::from_millis(50));
        }
    });

    sessions
}

fn await_sessions(sessions: &Mutex<Vec<UnixStream>>, count: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while sessions.lock().unwrap().len() < count {
        assert!(
            Instant::now() < deadline,
            "the runner never opened control session {count}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

/// Fails as: after one data-plane reconnect the runner drops its control
/// session every idle window, and its relays stall on every rebuild.
#[test]
fn a_superseded_session_does_not_end_its_successor() {
    let dir = common::isolated_socket_dir("supersede");
    std::env::set_var(
        "FLEZHA_CONTROL_IDLE_TIMEOUT_MS",
        IDLE_WINDOW.as_millis().to_string(),
    );
    let path = socket_dir::control_socket_path_in(&dir).expect("control socket path");
    let sessions = controller(&path);

    let (otx, orx) = unbounded();
    let (ctx, _crx) = unbounded();
    let _coordinator = coordinator::start(
        "supersede_ns".to_string(),
        Arc::new(RwLock::new(vec!["probe".to_string()])),
        (otx.clone(), orx),
        ctx,
        ShutdownBridge::new(),
    );
    await_sessions(&sessions, 1);
    // Established: past the ack window and onto the idle watchdog.
    thread::sleep(IDLE_WINDOW);

    otx.send(RunnerMessage::Reconnect("broken_pipe_flood"))
        .expect("coordinator is running");
    await_sessions(&sessions, 2);

    thread::sleep(IDLE_WINDOW * 5);

    assert_eq!(
        sessions.lock().unwrap().len(),
        2,
        "the replaced session's reader ended the live session that replaced it"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
