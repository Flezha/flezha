//! The session against a runner thread of the test's own: it answers a
//! removal with the coordinator's own live-instance check and hands every
//! control request it is sent back to the test.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam::channel::{Receiver, Sender};

use super::*;
use crate::runner::RunnerNode;
use crate::test_support::host_class::steps_json;

/// A started session whose runner thread is `node`'s: the factory ids of every
/// `UpdateFactories` it ships arrive on the receiver.
fn live_session(node: Arc<Mutex<RunnerNode>>) -> (Session, Receiver<Vec<String>>) {
    let (runner_tx, runner_rx) = crossbeam::channel::unbounded::<RunnerMessage>();
    let (sent_tx, sent) = crossbeam::channel::unbounded();
    std::thread::spawn(move || coordinate(&node, &runner_rx, &sent_tx));
    let session = Session::default();
    *session.started() = Some(runner_tx);
    (session, sent)
}

fn coordinate(
    node: &Mutex<RunnerNode>,
    runner_rx: &Receiver<RunnerMessage>,
    sent_tx: &Sender<Vec<String>>,
) {
    while let Ok(message) = runner_rx.recv() {
        match message {
            RunnerMessage::RemoveFactory { factory_id, reply } => {
                node.lock().unwrap().remove_factory(&factory_id, &reply);
            }
            RunnerMessage::EmitControlRequest { command, .. } => {
                if let pb::command_request::Command::UpdateFactories(update) = *command {
                    let ids = update.factories.into_iter().map(|f| f.id).collect();
                    let _ = sent_tx.send(ids);
                }
            }
            _ => {}
        }
    }
}

fn declare(session: &Session, id: &str) {
    session
        .declare(id, || -> Result<(), SessionError> {
            crate::host::register_host_class_json(steps_json(id, &["in"], 1).as_bytes())
                .expect("the class registers");
            Ok(())
        })
        .expect("the class declares");
}

fn shipped(sent: &Receiver<Vec<String>>) -> Vec<String> {
    sent.recv_timeout(Duration::from_secs(5))
        .expect("an UpdateFactories frame")
}

#[test]
fn a_post_start_declare_and_undeclare_ship_the_full_set() {
    let (session, sent) = live_session(Arc::new(Mutex::new(RunnerNode::new("ns".into()))));
    declare(&session, "session_ship_a");
    assert_eq!(shipped(&sent), ["session_ship_a"]);
    declare(&session, "session_ship_b");
    assert_eq!(shipped(&sent), ["session_ship_a", "session_ship_b"]);

    session
        .undeclare("session_ship_b")
        .expect("an idle factory goes");
    assert_eq!(shipped(&sent), ["session_ship_a"]);
    assert!(crate::relay::registry::get_class("session_ship_b").is_none());
}

#[test]
fn a_duplicate_or_unknown_id_is_refused_and_ships_nothing() {
    let (session, sent) = live_session(Arc::new(Mutex::new(RunnerNode::new("ns".into()))));
    declare(&session, "session_dup");
    shipped(&sent);

    let again = session.declare("session_dup", || -> Result<(), SessionError> {
        panic!("a duplicate registers nothing")
    });
    assert!(matches!(again, Err(SessionError::DuplicateFactoryId(id)) if id == "session_dup"));
    let unknown = session.undeclare("session_never");
    assert_eq!(
        unknown.unwrap_err().to_error().code,
        "runtime.class_not_registered"
    );
    assert!(sent.try_recv().is_err(), "a refusal shipped a frame");
}

#[test]
fn an_undeclare_with_a_live_instance_is_refused_by_the_coordinator() {
    let node = Arc::new(Mutex::new(RunnerNode::new("ns".into())));
    let (session, sent) = live_session(Arc::clone(&node));
    declare(&session, "session_live");
    shipped(&sent);
    node.lock()
        .unwrap()
        .start_relay(
            "session_live".into(),
            "session_live_1".into(),
            crate::relay::instance::KeyGroupRange::UNPARTITIONED,
            Default::default(),
        )
        .expect("the relay starts");

    let refused = session.undeclare("session_live").unwrap_err().to_error();
    assert_eq!(refused.code, "runtime.relay_type_in_use");
    assert_eq!(refused.details["factory_id"], "session_live");
    assert_eq!(refused.details["live_instances"], 1);
    assert!(
        sent.try_recv().is_err(),
        "a refused removal shipped a frame"
    );
    assert!(crate::relay::registry::get_class("session_live").is_some());
}

#[test]
fn an_unanswered_removal_holds_no_lock_and_is_never_applied_after_it_gives_up() {
    let _env = crate::runner::env_test_guard();
    flz_transport::env::hand_over_connect_settings(vec![(START_WAIT_VAR, "2000".to_string())]);
    // A coordinator that never answers: one reconnecting through an outage.
    let (runner_tx, runner_rx) = crossbeam::channel::unbounded::<RunnerMessage>();
    let session = Arc::new(Session::default());
    *session.started() = Some(runner_tx);
    declare(&session, "session_unanswered");

    let (done_tx, done) = crossbeam::channel::unbounded();
    let undeclaring = Arc::clone(&session);
    std::thread::spawn(move || {
        let _ = done_tx.send(undeclaring.undeclare("session_unanswered"));
    });
    let reply = loop {
        match runner_rx.recv_timeout(Duration::from_secs(5)) {
            Ok(RunnerMessage::RemoveFactory { reply, .. }) => break reply,
            Ok(_) => {}
            Err(e) => panic!("no removal reached the runner thread: {e}"),
        }
    };
    declare(&session, "session_unanswered_other");
    assert!(
        done.try_recv().is_err(),
        "the declare waited for the removal's answer"
    );

    let refused = done
        .recv_timeout(Duration::from_secs(10))
        .expect("the wait is bounded");
    flz_transport::env::hand_over_connect_settings(Vec::new());
    assert_eq!(
        refused.unwrap_err().to_error().code,
        "runtime.runner_unavailable"
    );
    // The coordinator reaches the removal after its caller gave up.
    RunnerNode::new("ns".into()).remove_factory("session_unanswered", &reply);
    assert!(crate::relay::registry::get_class("session_unanswered").is_some());
    let declared = session.declared.read().unwrap().clone();
    assert_eq!(declared, ["session_unanswered", "session_unanswered_other"]);
}

#[test]
fn the_start_wait_is_a_setting_and_zero_waits_for_ever() {
    let _env = crate::runner::env_test_guard();
    let handed = |value: &str| {
        flz_transport::env::hand_over_connect_settings(vec![(START_WAIT_VAR, value.to_string())]);
        start_wait()
    };
    assert_eq!(handed("0"), None);
    assert_eq!(handed("250"), Some(Duration::from_millis(250)));
    flz_transport::env::hand_over_connect_settings(Vec::new());
    assert_eq!(start_wait(), Some(Duration::from_secs(10)));

    let (reply_tx, replies) = crossbeam::channel::unbounded();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        let _ = reply_tx.send(ClientResponseMessage::RunnerIdConfirmation);
    });
    assert!(await_confirmation(&replies, None).is_ok());
}

#[test]
fn a_dead_runner_thread_is_a_connect_failed_result() {
    let (reply_tx, replies) = crossbeam::channel::unbounded::<ClientResponseMessage>();
    // The runner thread panicking or exiting before it confirms drops every
    // sender, so the receive fails: a typed result, not a panic.
    drop(reply_tx);
    match await_confirmation(&replies, None) {
        Err(SessionError::ConnectFailed(reason)) => {
            assert!(reason.contains("exited before confirming"), "got: {reason}");
        }
        other => panic!("expected ConnectFailed, got: {other:?}"),
    }
}

#[test]
fn an_unanswered_start_is_unconfirmed_and_stops_dialling() {
    if let Some(stderr) = crate::test_support::passes_alone(
        "session::tests::an_unanswered_start_is_unconfirmed_and_stops_dialling",
    ) {
        assert!(
            stderr.contains("[flezha] warning: Connection attempt 1 failed: "),
            "{stderr}"
        );
        let dialling = |line: &str| {
            [
                "warning: control socket absent at ",
                "warning: Connection attempt ",
                "Control channel attempt ",
            ]
            .iter()
            .any(|shape| line.starts_with(&format!("[flezha] {shape}")))
        };
        let stray: Vec<_> = stderr
            .lines()
            .filter(|line| line.starts_with("[flezha]") && !dialling(line))
            .collect();
        assert!(stray.is_empty(), "lines the dial does not write: {stray:?}");
        return;
    }
    let dir = std::env::temp_dir().join(format!("flz-session-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("FLEZHA_RUNNER_ID_PATH", dir.join("runner.id"));
    let settings = vec![
        ("FLEZHA_SOCKET_FILES_PATH", dir.display().to_string()),
        (START_WAIT_VAR, "300".to_string()),
    ];

    let session = Session::default();
    let refused = session.start("session-ns", settings);
    let error = refused.unwrap_err().to_error();
    assert_eq!(error.code, "runtime.connect_unconfirmed");
    assert_eq!(error.details["wait_ms"], 300);
    assert!(!session.is_started());
    // The runner thread holds the declared set while it dials; it lets go
    // once the abandoned start stops retrying.
    assert!(
        crate::test_support::wait_until(Duration::from_secs(10), Duration::from_millis(10), || {
            Arc::strong_count(&session.declared) == 1
        }),
        "the abandoned start kept dialling"
    );
}
