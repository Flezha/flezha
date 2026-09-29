//! What `flz` exits with when the `flzvm` it spawns refuses, and when there is
//! no `flzvm` to spawn at all.
//!
//! Every verb here delegates to the sibling `flzvm` binary. A verb that
//! answers 0 after that spawn failed is reporting a success it did not earn,
//! and a script cannot tell it from a success (KI-79). A release that was
//! never built is an ordinary caller condition and takes an ordinary
//! refusal naming the path, not a Rust panic (KB-277). Every other failure's
//! code is the class of the error the controller rendered.

mod common;

use common::Shim;

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

/// A `flz` whose `flzvm` always refuses. Exit 7 is chosen because no `flz`
/// rule produces it from anything but a propagated child code.
struct Refusing {
    shim: Shim,
}

impl Refusing {
    fn new() -> Self {
        Refusing {
            shim: Shim::new(
                FLZ_BIN,
                "#!/bin/sh\nprintf 'flzvm: refused\\n' >&2\nexit 7\n",
            ),
        }
    }

    fn code(&self, args: &[&str]) -> i32 {
        self.shim
            .run(args)
            .status
            .code()
            .expect("flz exits rather than being signalled")
    }
}

/// One rule for every verb: the controller rendered the failure and exited
/// with its class's code, so `flz` passes that code through and says nothing.
/// The first five are the verbs of KI-79, which once exited 0 after a refusal.
fn every_verb_passes_the_childs_code_through(cli: &Refusing) -> Vec<String> {
    let verbs: &[&[&str]] = &[
        &["operator", "stop", "op1"],
        &["operator", "rewind", "op1", "--to-iteration", "3"],
        &["operator", "replay", "a/b/c", "--from", "5"],
        &["operator", "state", "op1"],
        &["controller", "start"],
        &["operator", "send", "op1", "--text", "hi"],
        &["operator", "call", "op1", "--text", "hi"],
        &["operator", "list"],
        &["boot", "status"],
        &["controller", "status"],
        &["audit", "list"],
        &["dlq", "list"],
    ];

    verbs
        .iter()
        .filter_map(|args| {
            let got = cli.code(args);
            (got != 7).then(|| format!("flz {}: exit {} (want 7)", args.join(" "), got))
        })
        .collect()
}

/// With nothing to spawn, every verb refuses in the one shape: the code
/// `cli.flzvm_not_runnable`, the path it looked for, exit 1 for its class, and
/// no backtrace (KB-277).
fn a_missing_flzvm_is_refused_rather_than_panicked() -> Vec<String> {
    let cli = Shim::without_flzvm(FLZ_BIN);
    let looked_for = cli.flzvm_path().display().to_string();

    let cases: &[&[&str]] = &[
        &["dlq", "list", "--token", "t"],
        &["controller", "status"],
        &["operator", "list"],
        &["audit", "list"],
        &["operator", "stop", "op1"],
    ];

    cases
        .iter()
        .filter_map(|args| {
            let out = cli.run(args);
            let stderr = String::from_utf8_lossy(&out.stderr).to_string();
            let wrong = out.status.code() != Some(1)
                || stderr.contains("panicked")
                || !stderr.contains(&looked_for)
                || !stderr.contains("(cli.flzvm_not_runnable)");

            wrong.then(|| {
                format!(
                    "flz {}: exit {:?} (want 1), stderr {:?}",
                    args.join(" "),
                    out.status.code(),
                    stderr.trim()
                )
            })
        })
        .collect()
}

/// `elixir --rpc-eval` says a node it could not reach in one line and exits 1,
/// the code an internal failure has. `flz` holds that line back and answers
/// `controller.unreachable`, exit 3, in the form the caller asked for.
fn an_unreachable_controller_is_its_own_class() -> Vec<String> {
    let cli = Shim::new(
        FLZ_BIN,
        "#!/bin/sh\necho '--rpc-eval : RPC failed with reason :noconnection' >&2\nexit 1\n",
    );
    let mut wrong = Vec::new();

    let text = cli.run(&["namespace", "list"]);
    let stderr = String::from_utf8_lossy(&text.stderr).to_string();
    if text.status.code() != Some(3)
        || stderr.contains(":noconnection")
        || !stderr.contains("(controller.unreachable)")
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

    wrong
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

/// `flz operator call` exits by what the controller answered: 0 with the answer
/// on stdout, else the class of the failure it rendered (`call.timeout` 7,
/// `operator.not_found` 5, `call.target_unkeyed` 2).
fn a_call_exits_by_its_answers_class() -> Vec<String> {
    [(0, "hello\n"), (7, ""), (5, ""), (2, "")]
        .iter()
        .filter_map(|(code, answer)| {
            let script = format!("#!/bin/sh\nprintf '{answer}'\nexit {code}\n");
            let out = Shim::new(FLZ_BIN, &script).run(&["operator", "call", "op1", "-t", "hi"]);
            let stdout = String::from_utf8_lossy(&out.stdout).to_string();
            (out.status.code() != Some(*code) || stdout != *answer).then(|| {
                format!(
                    "flz operator call, flzvm exit {code}: exit {:?}, stdout {stdout:?}",
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
fn every_verb_answers_a_refused_flzvm_with_the_code_it_promises() {
    let cli = Refusing::new();

    let mut wrong = every_verb_passes_the_childs_code_through(&cli);
    wrong.extend(a_missing_flzvm_is_refused_rather_than_panicked());
    wrong.extend(an_unreachable_controller_is_its_own_class());
    wrong.extend(only_a_daemon_start_announces_a_start());
    wrong.extend(a_call_exits_by_its_answers_class());

    assert!(
        wrong.is_empty(),
        "flz answered its sibling wrongly:\n{}",
        wrong.join("\n")
    );
}
