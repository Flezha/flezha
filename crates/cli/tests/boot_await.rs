//! `flz boot await` waits for a controller it cannot reach yet.
//!
//! Right after `flz controller start --mode daemon` the controller's socket is
//! not serving yet, and a request is refused or left unaccepted. The verb's
//! whole job is to wait, so that answer is "not yet", inside the caller's
//! timeout, while a controller that answered its own verdict has already spent
//! that timeout and is not asked again.
//!
//! The fixture is a socket that refuses its first N requests and then
//! answers, recording each, so the proof is the count and the exit code,
//! never a clock.

mod common;

use std::process::{Output, Stdio};
use std::time::{Duration, Instant};

use common::{Reply, Shim};
use serde_json::Value;

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

/// Longer than any budget below, so a verb that never gives up fails the test
/// instead of hanging it.
const HANG_GUARD: Duration = Duration::from_secs(30);

const UNREACHABLE_FOR_EVER: usize = usize::MAX;

struct Run {
    code: Option<i32>,
    stdout: String,
    stderr: String,
    calls: Vec<Value>,
}

/// Refused for the first `unreachable` requests, then `answer` with exit
/// `code`: on stdout when that is 0, on stderr otherwise, which is where a
/// controller's verdict lands.
fn await_boot(timeout_secs: &str, unreachable: usize, answer: &str, code: i32) -> Run {
    let answer = format!("{answer}\n");
    let shim = Shim::new(FLZ_BIN, "#!/bin/sh\nexit 1\n").serving(move |n, _| {
        if n < unreachable {
            return Reply::Refuse;
        }
        let (stdout, stderr) = match code {
            0 => (answer.clone(), String::new()),
            _ => (String::new(), answer.clone()),
        };
        Reply::Answer {
            stdout,
            stderr,
            code,
        }
    });

    let mut child = shim
        .cmd()
        .args(["boot", "await", "--timeout", timeout_secs])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("flz runs");

    let started = Instant::now();
    while child.try_wait().expect("wait").is_none() {
        if started.elapsed() > HANG_GUARD {
            let _ = child.kill();
            panic!("flz boot await --timeout {timeout_secs} still running after {HANG_GUARD:?}");
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    let Output {
        status,
        stdout,
        stderr,
    } = child.wait_with_output().expect("output");
    Run {
        code: status.code(),
        stdout: String::from_utf8_lossy(&stdout).into_owned(),
        stderr: String::from_utf8_lossy(&stderr).into_owned(),
        calls: shim.requests(),
    }
}

/// The milliseconds a `Controller.boot_await` request asks the controller for:
/// the one number in its arguments.
fn budget_ms(call: &Value) -> u64 {
    assert_eq!(
        call["name"], "Controller.boot_await",
        "not a boot_await call: {call}"
    );
    call["args"][0].as_u64().expect("one number")
}

/// Three unreachable answers, then the node's own verdict, which is the answer.
/// The first request carries the whole budget and every later one what is
/// left of it, and the unreachable answers are not printed while it waits.
fn an_unreachable_node_is_waited_for_and_its_verdict_is_the_answer() {
    let run = await_boot("30", 3, "boot await: ready", 0);

    assert_eq!(
        run.code,
        Some(0),
        "stderr: {} calls: {:?}",
        run.stderr,
        run.calls
    );
    assert!(run.stdout.contains("boot await: ready"), "{}", run.stdout);
    assert_eq!(run.stderr, "", "a wait prints nothing while it waits");
    assert_eq!(run.calls.len(), 4, "{:?}", run.calls);

    let budgets: Vec<u64> = run.calls.iter().map(budget_ms).collect();
    assert_eq!(budgets[0], 30_000);
    assert!(
        budgets.windows(2).all(|pair| pair[1] < pair[0]),
        "each retry asks for what is left, never more: {budgets:?}"
    );
}

/// A controller that never answers is given up on at the timeout, after
/// retrying, as `controller.unreachable` with its class's exit code and a
/// message naming what a refused connection can mean, since it cannot tell
/// them apart.
fn a_node_that_stays_unreachable_is_given_up_on_at_the_timeout() {
    let run = await_boot("1", UNREACHABLE_FOR_EVER, "unused", 0);

    assert_eq!(run.code, Some(3), "stderr: {}", run.stderr);
    assert!(
        run.stderr.contains("(controller.unreachable)"),
        "{}",
        run.stderr
    );
    assert!(
        run.calls.len() >= 2,
        "retried, not refused at once: {:?}",
        run.calls
    );
    for cause in [
        "not up",
        "stopped after it was reached",
        "another socket directory",
        "FLEZHA_SOCKET_FILES_PATH",
    ] {
        assert!(
            run.stderr.contains(cause),
            "names {cause:?}: {}",
            run.stderr
        );
    }
}

/// A zero timeout is the image health check: one evaluation, reachable or not.
fn a_zero_timeout_asks_once_even_when_unreachable() {
    let run = await_boot("0", UNREACHABLE_FOR_EVER, "unused", 0);

    assert_eq!(run.code, Some(3), "stderr: {}", run.stderr);
    assert_eq!(run.calls.len(), 1, "{:?}", run.calls);
    assert_eq!(budget_ms(&run.calls[0]), 0);
}

/// A node that answered `recovering` already waited out the whole budget on
/// its own clock; asking again would wait twice as long as the caller said.
/// Its refusal still reaches the caller's stderr, with its own exit code.
fn a_reached_node_that_is_not_ready_is_asked_once() {
    let verdict = "boot await: recovering; unmet: hubs_recovered";
    let run = await_boot("30", 0, verdict, 1);

    assert_eq!(run.code, Some(1));
    assert_eq!(run.calls.len(), 1, "{:?}", run.calls);
    assert_eq!(run.stderr.trim_end(), verdict);
}

/// One `#[test]`: the fixture execs a *copy* of the binary, and a copy written
/// while a sibling thread forks is `ExecutableFileBusy` in the child that
/// inherited the write handle.
#[test]
fn boot_await_waits_only_for_a_node_it_cannot_reach() {
    an_unreachable_node_is_waited_for_and_its_verdict_is_the_answer();
    a_node_that_stays_unreachable_is_given_up_on_at_the_timeout();
    a_zero_timeout_asks_once_even_when_unreachable();
    a_reached_node_that_is_not_ready_is_asked_once();
}
