//! The core's notices over the C ABI: a C callback takes them on the thread
//! that wrote them, and a host that drains takes them as a batch, on the
//! answers of the one pool a context opens. Each case runs in a child process
//! of this test binary: the hook is process-global, and what reaches standard
//! error can only be read from outside.

use std::ffi::{c_char, CStr};
use std::process::Command;
use std::sync::Mutex;

use flz_core_ffi::{veneer, *};
use flz_transport::notice::{notice, Level};

const CHILD: &str = "FLZ_ABI_NOTICE_CHILD";

/// Run `test` of this module alone in a child: `None` inside it, the child's
/// `[flezha]` lines of standard error outside, once it passed.
fn stderr_of_child(test: &str) -> Option<Vec<String>> {
    if std::env::var_os(CHILD).is_some() {
        return None;
    }
    let out = Command::new(std::env::current_exe().expect("the test binary"))
        .args(["--exact", &format!("notice::{test}"), "--nocapture"])
        .env(CHILD, "1")
        .output()
        .expect("the child runs");
    let (stdout, stderr) = (
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.success() && stdout.contains("1 passed"),
        "{stdout}{stderr}"
    );
    Some(
        stderr
            .lines()
            .filter(|l| l.starts_with("[flezha]"))
            .map(str::to_string)
            .collect(),
    )
}

static SEEN: Mutex<Vec<(i32, String)>> = Mutex::new(Vec::new());

extern "C" fn record(level: i32, message: *const c_char) {
    // SAFETY: the core hands a NUL-terminated text borrowed for the call.
    let text = unsafe { CStr::from_ptr(message) }.to_string_lossy();
    SEEN.lock().unwrap().push((level, text.into_owned()));
}

#[test]
fn a_registered_callback_takes_every_notice_and_null_gives_them_back() {
    let Some(stderr) =
        stderr_of_child("a_registered_callback_takes_every_notice_and_null_gives_them_back")
    else {
        flz_core_set_log_callback(Some(record));
        notice(Level::Warn, "t", "through the callback");
        notice(Level::Always, "t", "the banner");
        flz_core_set_log_callback(None);
        notice(Level::Error, "t", "on standard error");
        assert_eq!(
            *SEEN.lock().unwrap(),
            [
                (2, "through the callback".to_string()),
                (4, "the banner".to_string())
            ]
        );
        return;
    };
    assert_eq!(stderr, ["[flezha] error: on standard error"]);
}

#[test]
fn the_first_take_starts_holding_and_each_take_hands_out_what_was_held() {
    let Some(stderr) =
        stderr_of_child("the_first_take_starts_holding_and_each_take_hands_out_what_was_held")
    else {
        notice(Level::Info, "t", "before any take");
        assert_eq!(veneer::take_notices(), []);
        notice(Level::Warn, "flz_core::retry", "held");
        notice(Level::Always, "t", "the banner");
        let notice_of = |level: &str, target: &str, text: &str| veneer::Notice {
            level: level.into(),
            target: target.into(),
            text: text.into(),
        };
        assert_eq!(
            veneer::take_notices(),
            [
                notice_of("warn", "flz_core::retry", "held"),
                notice_of("always", "t", "the banner")
            ]
        );
        assert_eq!(veneer::take_notices(), []);
        return;
    };
    assert_eq!(stderr, ["[flezha] before any take"]);
}

/// An idle runner's notices reach a draining host within the supervisor wait:
/// the pool's supervisor answers idle at least once a wait, and the host takes
/// the notices on every answer.
#[test]
fn an_idle_pools_supervisor_answers_within_its_wait() {
    let Some(stderr) = stderr_of_child("an_idle_pools_supervisor_answers_within_its_wait") else {
        let ctx = veneer::CtxPtr::of(veneer::context_new(flz_core::SCHEMA_VERSION).unwrap());
        assert_eq!(
            veneer::pool_supervise(ctx).ok(),
            Some(FLZ_POOL_STOP),
            "no pool is open"
        );
        let policy = veneer::pool_open(ctx, 1).expect("the pool opens");
        assert_eq!(
            (policy[0], policy[2]),
            (1, 250),
            "the base and the poll timeout"
        );
        assert_eq!(veneer::take_notices(), []);
        notice(
            Level::Warn,
            "flz_core::control_unit",
            "Connection attempt 2 failed",
        );

        let asked = std::time::Instant::now();
        assert_eq!(veneer::pool_supervise(ctx).ok(), Some(FLZ_POOL_IDLE));
        assert!(asked.elapsed() < std::time::Duration::from_secs(2));
        let taken = veneer::take_notices();
        assert_eq!(taken.len(), 1, "{taken:?}");
        assert_eq!(taken[0].text, "Connection attempt 2 failed");
        veneer::context_free(ctx);
        return;
    };
    assert_eq!(stderr, Vec::<String>::new());
}

/// A context holds one pool: `flz_core_pool_left` names none, so a second
/// open replacing a live pool would hand its seats to the wrong governor.
#[test]
fn a_second_pool_open_on_one_context_is_refused() {
    let Some(stderr) = stderr_of_child("a_second_pool_open_on_one_context_is_refused") else {
        let ctx = veneer::CtxPtr::of(veneer::context_new(flz_core::SCHEMA_VERSION).unwrap());
        veneer::pool_open(ctx, 1).expect("the first open");
        let refused = veneer::pool_open(ctx, 1).expect_err("a second open is refused");
        let message = refused.into_message();
        assert!(
            message.contains("runtime.runner_already_running"),
            "{message}"
        );
        veneer::context_free(ctx);
        return;
    };
    assert_eq!(stderr, Vec::<String>::new());
}

/// A local run's end stops its context's pool: the core runs on, so the
/// workers that leave after it are not replaced, and nothing says they were.
#[test]
fn a_stopped_local_run_stops_its_pool() {
    let Some(stderr) = stderr_of_child("a_stopped_local_run_stops_its_pool") else {
        let ctx = veneer::CtxPtr::of(veneer::context_new(flz_core::SCHEMA_VERSION).unwrap());
        let class = br#"{"factory_id": "abi_local_pool", "sdk_lang": 1, "sdk_version": "0.1.0",
            "channels": [{"name": "in", "kind": "entry", "chains": [[{"link": "into", "tasklet": 0}]]}],
            "tasklets": [{"name": "t"}]}"#;
        veneer::declare_class(ctx, class).expect("the class declares");
        veneer::local_start(ctx, c"abi_local_pool", c"abi-local-pool-1").expect("the run starts");
        veneer::pool_open(ctx, 1).expect("the pool opens");

        veneer::local_stop(ctx);
        veneer::pool_left(ctx).expect("the base worker leaves");
        assert_eq!(veneer::pool_supervise(ctx).ok(), Some(FLZ_POOL_STOP));
        let taken = veneer::take_notices();
        assert!(
            taken
                .iter()
                .all(|n| !n.text.contains("replaced a poll worker")),
            "{taken:?}"
        );
        veneer::context_free(ctx);
        return;
    };
    assert!(
        stderr.iter().all(|l| !l.contains("replaced a poll worker")),
        "{stderr:?}"
    );
}
