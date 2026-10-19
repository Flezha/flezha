use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, OnceLock};

use super::*;

/// The hook and the queue are process-wide: the tests that touch them take turns.
pub(crate) static TURN: Mutex<()> = Mutex::new(());

const TARGET: &str = "flz_transport::notice::tests";

/// Run the test `name` of this module alone in a child: `None` inside it, the
/// child's `[flezha]` lines of standard error outside, once it passed.
fn flezha_stderr_of_child(name: &str) -> Option<Vec<String>> {
    const CHILD: &str = "FLZ_NOTICE_TEST_CHILD";
    if std::env::var_os(CHILD).is_some() {
        return None;
    }
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &format!("notice::tests::{name}"), "--nocapture"])
        .env(CHILD, "1")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success() && stdout.contains("1 passed"),
        "{stdout}{stderr}"
    );
    let flezha = stderr.lines().filter(|l| l.starts_with("[flezha]"));
    Some(flezha.map(str::to_string).collect())
}

#[test]
fn without_a_hook_a_notice_reaches_standard_error() {
    let Some(lines) = flezha_stderr_of_child("without_a_hook_a_notice_reaches_standard_error")
    else {
        notice(Level::Warn, TARGET, "a warning");
        notice(Level::Error, TARGET, "an error");
        notice(Level::Always, TARGET, "a banner");
        return;
    };
    assert_eq!(
        lines,
        [
            "[flezha] warning: a warning",
            "[flezha] error: an error",
            "[flezha] a banner"
        ]
    );
}

#[test]
fn a_hook_receives_the_level_target_and_text() {
    let _turn = TURN.lock().unwrap_or_else(|p| p.into_inner());
    let seen: Arc<Mutex<Vec<Notice>>> = Arc::default();
    let into = Arc::clone(&seen);
    set_hook(Box::new(move |level, target, text| {
        if target == TARGET {
            into.lock().unwrap().push(Notice {
                level,
                target,
                text: text.to_string(),
            });
        }
    }));
    notice(Level::Error, TARGET, "the text");
    crate::notice!(Info, "formatted {}", 7);
    clear_hook();
    assert_eq!(
        *seen.lock().unwrap(),
        [
            Notice {
                level: Level::Error,
                target: TARGET,
                text: "the text".to_string()
            },
            Notice {
                level: Level::Info,
                target: TARGET,
                text: "formatted 7".to_string()
            }
        ]
    );
}

#[test]
fn a_full_queue_drops_the_newest_and_the_next_take_says_how_many() {
    let _turn = TURN.lock().unwrap_or_else(|p| p.into_inner());
    let _ = take(Duration::ZERO);
    let before = NOTICES_DROPPED_TOTAL.load(Ordering::Relaxed);
    for i in 0..QUEUE_CAPACITY + 3 {
        push(Level::Info, TARGET, &i.to_string());
    }
    let batch = take(Duration::ZERO);
    assert_eq!(batch.len(), QUEUE_CAPACITY + 1);
    assert_eq!(
        batch[QUEUE_CAPACITY - 1].text,
        (QUEUE_CAPACITY - 1).to_string()
    );
    assert_eq!(
        batch[QUEUE_CAPACITY],
        Notice {
            level: Level::Warn,
            target: "flz_transport::notice",
            text: "3 notices dropped: the notice queue was full".to_string()
        }
    );
    assert_eq!(NOTICES_DROPPED_TOTAL.load(Ordering::Relaxed) - before, 3);
    assert!(take(Duration::ZERO).is_empty());
}

#[test]
fn a_take_waits_for_the_first_notice() {
    let _turn = TURN.lock().unwrap_or_else(|p| p.into_inner());
    let _ = take(Duration::ZERO);
    let pusher = std::thread::spawn(|| push(Level::Warn, TARGET, "late"));
    let batch = take(Duration::from_secs(30));
    pusher.join().unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].text, "late");
}

#[test]
fn a_forwarded_notice_reaches_the_oldest_receiver_that_takes_it() {
    static RECEIVERS: Mutex<Option<Vec<u8>>> = Mutex::new(None);
    static TAKEN: OnceLock<crossbeam::channel::Sender<(u8, Notice)>> = OnceLock::new();
    static TWO_GONE: AtomicBool = AtomicBool::new(false);
    // Receiver 1 is gone from the start, receiver 2 once `TWO_GONE` is set.
    fn send(receiver: u8, n: &Notice) -> bool {
        let gone = receiver == 1 || (receiver == 2 && TWO_GONE.load(Ordering::SeqCst));
        !gone && TAKEN.get().unwrap().send((receiver, n.clone())).is_ok()
    }
    let name = "a_forwarded_notice_reaches_the_oldest_receiver_that_takes_it";
    let Some(lines) = flezha_stderr_of_child(name) else {
        let (tx, rx) = crossbeam::channel::unbounded();
        TAKEN.set(tx).unwrap();
        forward_to(&RECEIVERS, 1, send).unwrap();
        forward_to(&RECEIVERS, 2, send).unwrap();
        forward_to(&RECEIVERS, 2, send).unwrap();
        notice(Level::Info, TARGET, "kept");
        let taken = rx.recv_timeout(Duration::from_secs(30)).unwrap();
        assert_eq!((taken.0, taken.1.text.as_str()), (2, "kept"));
        assert_eq!(*RECEIVERS.lock().unwrap(), Some(vec![2]));
        TWO_GONE.store(true, Ordering::SeqCst);
        notice(Level::Warn, TARGET, "unheard");
        // Receiver 2 is forgotten once it refused; the notice after it is
        // sent after the unheard one was written.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while *RECEIVERS.lock().unwrap() != Some(vec![]) {
            assert!(
                std::time::Instant::now() < deadline,
                "receiver 2 is never forgotten"
            );
            std::thread::yield_now();
        }
        forward_to(&RECEIVERS, 3, send).unwrap();
        notice(Level::Info, TARGET, "last");
        let taken = rx.recv_timeout(Duration::from_secs(30)).unwrap();
        assert_eq!((taken.0, taken.1.text.as_str()), (3, "last"));
        return;
    };
    assert_eq!(lines, ["[flezha] warning: unheard"]);
}
