//! `flz boot await` waits for a controller it cannot reach yet.
//!
//! Right after `flz controller start --mode daemon` the node's distribution is
//! not up, and `flzvm rpc` answers `--rpc-eval : RPC failed with reason
//! :noconnection`, exit 1. The verb's whole job is to wait, so that answer is
//! "not yet", inside the caller's timeout, while a node that answered its
//! own verdict has already spent that timeout and is not asked again.
//!
//! The fixture is a `flzvm` that is unreachable for its first N calls and then
//! answers, counting the calls, so the proof is the count and the exit code,
//! never a clock.

mod common;

use std::fs;
use std::path::Path;
use std::process::{Output, Stdio};
use std::time::{Duration, Instant};

use common::Shim;
use tempfile::TempDir;

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

/// Unreachable for the first `$FLZ_TEST_UNREACHABLE` calls, then
/// `$FLZ_TEST_ANSWER` with exit `$FLZ_TEST_ANSWER_CODE`: on stdout when that is
/// 0, on stderr otherwise, which is where a controller's verdict lands. Every
/// call appends its expression and its stdin's arguments line to
/// `$FLZ_TEST_CALLS`, as one line.
const FLAKY_NODE: &str = r#"#!/bin/sh
read -r arguments
printf '%s %s\n' "$2" "$arguments" >> "$FLZ_TEST_CALLS"
n=$(wc -l < "$FLZ_TEST_CALLS")
if [ "$n" -le "$FLZ_TEST_UNREACHABLE" ]; then
  printf '%s\n' '--rpc-eval : RPC failed with reason :noconnection' >&2
  exit 1
fi
if [ "$FLZ_TEST_ANSWER_CODE" -eq 0 ]; then
  printf '%s\n' "$FLZ_TEST_ANSWER"
else
  printf '%s\n' "$FLZ_TEST_ANSWER" >&2
fi
exit "$FLZ_TEST_ANSWER_CODE"
"#;

/// Longer than any budget below, so a verb that never gives up fails the test
/// instead of hanging it.
const HANG_GUARD: Duration = Duration::from_secs(30);

const UNREACHABLE_FOR_EVER: u32 = u32::MAX;

struct Run {
    code: Option<i32>,
    stdout: String,
    stderr: String,
    calls: Vec<String>,
}

fn await_boot(shim: &Shim, timeout_secs: &str, unreachable: u32, answer: &str, code: i32) -> Run {
    let dir = TempDir::new().expect("tempdir");
    let calls = dir.path().join("calls");
    fs::write(&calls, "").expect("calls file");

    let mut child = shim
        .cmd()
        .args(["boot", "await", "--timeout", timeout_secs])
        .env("FLZ_TEST_CALLS", &calls)
        .env("FLZ_TEST_UNREACHABLE", unreachable.to_string())
        .env("FLZ_TEST_ANSWER", answer)
        .env("FLZ_TEST_ANSWER_CODE", code.to_string())
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
        calls: read_calls(&calls),
    }
}

fn read_calls(path: &Path) -> Vec<String> {
    fs::read_to_string(path)
        .expect("calls")
        .lines()
        .map(str::to_string)
        .collect()
}

/// The milliseconds a `Controller.boot_await` door call asks the node for: the
/// one number in its arguments line, which follows the expression.
fn budget_ms(call: &str) -> u64 {
    let (expression, arguments) = call.rsplit_once(' ').expect("expression and arguments");
    assert!(
        expression
            .starts_with(r#"arguments = IO.read(:line); Flezha.Service.Cli.call("Controller.boot_await", arguments, "#),
        "not a boot_await call: {call}"
    );
    let [ms]: [u64; 1] = serde_json::from_str(arguments).expect("one number");
    ms
}

/// Three unreachable answers, then the node's own verdict, which is the answer.
/// The first request carries the whole budget and every later one what is
/// left of it, and the unreachable answers are not printed while it waits.
fn an_unreachable_node_is_waited_for_and_its_verdict_is_the_answer(shim: &Shim) {
    let run = await_boot(shim, "30", 3, "boot await: ready", 0);

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

    let budgets: Vec<u64> = run.calls.iter().map(|c| budget_ms(c)).collect();
    assert_eq!(budgets[0], 30_000);
    assert!(
        budgets.windows(2).all(|pair| pair[1] < pair[0]),
        "each retry asks for what is left, never more: {budgets:?}"
    );
}

/// A node that never answers is given up on at the timeout, after retrying,
/// as `controller.unreachable` with its class's exit code and a message naming
/// what `:noconnection` can mean, since the reason itself cannot tell them
/// apart.
fn a_node_that_stays_unreachable_is_given_up_on_at_the_timeout(shim: &Shim) {
    let run = await_boot(shim, "1", UNREACHABLE_FOR_EVER, "unused", 0);

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
        "cookie",
        "FLEZHA_COOKIE_FILE",
        "node name",
    ] {
        assert!(
            run.stderr.contains(cause),
            "names {cause:?}: {}",
            run.stderr
        );
    }
}

/// A zero timeout is the image health check: one evaluation, reachable or not.
fn a_zero_timeout_asks_once_even_when_unreachable(shim: &Shim) {
    let run = await_boot(shim, "0", UNREACHABLE_FOR_EVER, "unused", 0);

    assert_eq!(run.code, Some(3), "stderr: {}", run.stderr);
    assert_eq!(run.calls.len(), 1, "{:?}", run.calls);
    assert_eq!(budget_ms(&run.calls[0]), 0);
}

/// A node that answered `recovering` already waited out the whole budget on
/// its own clock; asking again would wait twice as long as the caller said.
/// Its refusal still reaches the caller's stderr, with its own exit code.
fn a_reached_node_that_is_not_ready_is_asked_once(shim: &Shim) {
    let verdict = "boot await: recovering; unmet: hubs_recovered";
    let run = await_boot(shim, "30", 0, verdict, 1);

    assert_eq!(run.code, Some(1));
    assert_eq!(run.calls.len(), 1, "{:?}", run.calls);
    assert_eq!(run.stderr.trim_end(), verdict);
}

/// One `#[test]`, one fixture: the fixture execs a *copy* of the binary, and a
/// copy written while a sibling thread forks is `ExecutableFileBusy` in the
/// child that inherited the write handle.
#[test]
fn boot_await_waits_only_for_a_node_it_cannot_reach() {
    let shim = Shim::new(FLZ_BIN, FLAKY_NODE);

    an_unreachable_node_is_waited_for_and_its_verdict_is_the_answer(&shim);
    a_node_that_stays_unreachable_is_given_up_on_at_the_timeout(&shim);
    a_zero_timeout_asks_once_even_when_unreachable(&shim);
    a_reached_node_that_is_not_ready_is_asked_once(&shim);
}
