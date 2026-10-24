//! What `flz` exits with when the controller refuses, when it cannot be
//! reached or does not accept the request, and when the `flzvm` a local verb
//! starts refuses or is not there at all.
//!
//! A verb that answers 0 after a refusal is reporting a success it did not
//! earn, and a script cannot tell it from a success (KI-79). A release that was
//! never built is an ordinary caller condition and takes an ordinary refusal
//! naming the path, not a Rust panic (KB-277). Every other failure's code is
//! the class of the error the controller rendered.

mod common;

use std::time::{Duration, Instant};

use common::{Reply, Shim};

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

/// Exit 7 is chosen because no `flz` rule produces it from anything but a
/// code the controller or a child answered.
const REFUSED: &str = "#!/bin/sh\nprintf 'flzvm: refused\\n' >&2\nexit 7\n";

fn code(shim: &Shim, args: &[&str]) -> i32 {
    shim.run(args)
        .status
        .code()
        .expect("flz exits rather than being signalled")
}

/// One rule for every verb: the controller, or the `flzvm` a local verb
/// starts, rendered the failure and exited with its class's code, so `flz`
/// passes that code through and says nothing. One verb of each way the seam
/// settles a request is here: a door call, a wait (`boot await`) and a stop
/// that reads its marker. The first five are the verbs of KI-79, which once
/// exited 0 after a refusal.
fn every_verb_passes_the_refusals_code_through() -> Vec<String> {
    let cli = Shim::new(FLZ_BIN, REFUSED).serving(|_, _| Reply::Answer {
        stdout: String::new(),
        stderr: "error: refused (x.refused)\n".into(),
        code: 7,
    });
    let verbs: &[&[&str]] = &[
        &["relay", "stop", "op1"],
        &["relay", "rewind", "op1", "--to-iteration", "3"],
        &["relay", "replay", "a/b/c", "--from", "5"],
        &["relay", "state", "op1"],
        &["controller", "start"],
        &["relay", "send", "op1", "--text", "hi"],
        &["relay", "call", "op1", "--text", "hi"],
        &["relay", "list"],
        &["boot", "status"],
        &["boot", "await", "--timeout", "0"],
        &["controller", "status"],
        &["controller", "stop"],
        &["audit", "list"],
        &["dlq", "list"],
    ];

    verbs
        .iter()
        .filter_map(|args| {
            let got = code(&cli, args);
            (got != 7).then(|| format!("flz {}: exit {} (want 7)", args.join(" "), got))
        })
        .collect()
}

/// With nothing to spawn, every local verb refuses in the one shape: the code
/// `cli.flzvm_not_runnable`, the path it looked for, exit 1 for its class, and
/// no backtrace (KB-277). A verb that reaches a controller needs no `flzvm`.
fn a_missing_flzvm_is_refused_rather_than_panicked() -> Vec<String> {
    let cli = Shim::without_flzvm(FLZ_BIN).serving(|_, _| Reply::ok());
    let looked_for = cli.flzvm_path().display().to_string();
    let mut wrong = Vec::new();

    let cases: &[&[&str]] = &[
        &["controller", "start"],
        &["plugin", "install", "./p.fpz"],
        &["plugin", "uninstall", "p"],
        &["plugin", "sync"],
    ];

    for args in cases {
        let out = cli.run(args);
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();
        if out.status.code() != Some(1)
            || stderr.contains("panicked")
            || !stderr.contains(&looked_for)
            || !stderr.contains("(cli.flzvm_not_runnable)")
        {
            wrong.push(format!(
                "flz {}: exit {:?} (want 1), stderr {:?}",
                args.join(" "),
                out.status.code(),
                stderr.trim()
            ));
        }
    }

    if code(&cli, &["dlq", "list", "--token", "t"]) != 0 {
        wrong.push("flz dlq list needed a flzvm".into());
    }
    wrong
}

/// No socket, and a connection that ends without an exit code, are both
/// `controller.unreachable`, exit 3, naming the socket, in the form the caller
/// asked for.
fn an_unreachable_controller_is_its_own_class() -> Vec<String> {
    let absent = Shim::new(FLZ_BIN, REFUSED);
    let gone = Shim::new(FLZ_BIN, REFUSED).serving(|_, _| Reply::Hangup);
    let mut wrong = Vec::new();

    for cli in [&absent, &gone] {
        let text = cli.run(&["namespace", "list"]);
        let stderr = String::from_utf8_lossy(&text.stderr).to_string();
        if text.status.code() != Some(3)
            || !stderr.contains("(controller.unreachable)")
            || !stderr.contains(&cli.socket_path())
        {
            wrong.push(format!(
                "namespace list, unreachable: exit {:?}, stderr {:?}",
                text.status.code(),
                stderr
            ));
        }

        let json = cli.run(&["namespace", "list", "--json"]);
        let line = String::from_utf8_lossy(&json.stderr).trim().to_string();
        let parsed: serde_json::Value = serde_json::from_str(&line).unwrap_or_default();
        if json.status.code() != Some(3) || parsed["error"]["code"] != "controller.unreachable" {
            wrong.push(format!(
                "namespace list --json, unreachable: exit {:?}, stderr {:?}",
                json.status.code(),
                line
            ));
        }
    }

    wrong
}

/// A controller that reads the request and never accepts it, as one from
/// before the socket protocol does, is given up on after the acceptance window
/// as `cli.protocol_unsupported`, exit 2, naming the protocol sent.
fn a_controller_that_never_accepts_is_a_protocol_mismatch() -> Vec<String> {
    let cli = Shim::new(FLZ_BIN, REFUSED).serving(|_, _| Reply::Silent);
    let started = Instant::now();
    let out = cli.run(&["namespace", "list"]);
    let waited = started.elapsed();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();

    let right = out.status.code() == Some(2)
        && stderr.contains("(cli.protocol_unsupported)")
        && stderr.contains("speaks flz request protocol none")
        && waited >= Duration::from_secs(4);
    (!right)
        .then(|| {
            format!(
                "namespace list, never accepted: exit {:?} after {waited:?}, stderr {stderr:?}",
                out.status.code()
            )
        })
        .into_iter()
        .collect()
}

/// An attached start returns when the controller has STOPPED: `flzvm start`
/// runs the node in the foreground, so the success line under it announced a
/// start at the moment the controller went away. Only a daemon mode returns on
/// a launch, and only a daemon mode has a start to report.
fn only_a_daemon_start_announces_a_start() -> Vec<String> {
    let cli = Shim::new(FLZ_BIN, "#!/bin/sh\nexit 0\n");

    let cases: &[(&[&str], bool)] = &[
        (&["controller", "start", "--mode", "daemon"], true),
        (&["controller", "start", "--mode", "daemon_iex"], true),
        (&["controller", "start"], false),
        (&["controller", "start", "--mode", "attached"], false),
        (&["controller", "start", "--mode", "attached_iex"], false),
    ];

    cases
        .iter()
        .filter_map(|(args, want)| {
            let out = cli.run(args);
            let said = String::from_utf8_lossy(&out.stdout).contains("Controller started");

            (said != *want).then(|| {
                format!(
                    "flz {}: announced a start = {} (want {})",
                    args.join(" "),
                    said,
                    want
                )
            })
        })
        .collect()
}

/// `flz relay call` exits by what the controller answered: 0 with the answer
/// on stdout, else the class of the failure it rendered (`call.timeout` 7,
/// `relay.not_found` 5, `call.target_unkeyed` 2).
fn a_call_exits_by_its_answers_class() -> Vec<String> {
    [(0, "hello\n"), (7, ""), (5, ""), (2, "")]
        .iter()
        .filter_map(|&(code, answer)| {
            let cli = Shim::new(FLZ_BIN, REFUSED).serving(move |_, _| Reply::Answer {
                stdout: answer.into(),
                stderr: String::new(),
                code,
            });
            let out = cli.run(&["relay", "call", "op1", "-t", "hi"]);
            let stdout = String::from_utf8_lossy(&out.stdout).to_string();
            (out.status.code() != Some(code) || stdout != answer).then(|| {
                format!(
                    "flz relay call, answered exit {code}: exit {:?}, stdout {stdout:?}",
                    out.status.code()
                )
            })
        })
        .collect()
}

/// One `#[test]`, several phases: the fixture execs a *copy* of the binary, and a
/// copy written while a sibling thread forks is `ExecutableFileBusy` in the
/// child that inherited the write handle. One thread, one copy, no race.
#[test]
fn every_verb_answers_a_refusal_with_the_code_it_promises() {
    let mut wrong = every_verb_passes_the_refusals_code_through();
    wrong.extend(a_missing_flzvm_is_refused_rather_than_panicked());
    wrong.extend(an_unreachable_controller_is_its_own_class());
    wrong.extend(a_controller_that_never_accepts_is_a_protocol_mismatch());
    wrong.extend(only_a_daemon_start_announces_a_start());
    wrong.extend(a_call_exits_by_its_answers_class());

    assert!(
        wrong.is_empty(),
        "flz answered a refusal wrongly:\n{}",
        wrong.join("\n")
    );
}
