use std::io::{BufRead, BufReader, Write};
use std::process::{exit, Stdio};

use crate::args::{OptSpec, Reading, JSON_SWITCH, OUTPUT};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, Exit, Failure, Opt};

const MODE: OptSpec = OptSpec::choice(
    "--mode",
    "-m",
    "mode",
    &["attached", "daemon", "attached_iex", "daemon_iex"],
);
const TIMEOUT_MS: OptSpec = OptSpec::number("--timeout-ms", "--timeout-ms", "<ms>");
const NO_WAIT: OptSpec = OptSpec::switch("--no-wait", "--no-wait");
const FORCE: OptSpec = OptSpec::switch("--force", "--force");

// Exit codes match `flz controller status` / `mem-top`:
//   0 — snapshot printed
//   3 — controller socket unreachable
const FORMAT: &[OptSpec] = &[OUTPUT, JSON_SWITCH];

pub(crate) const TABLE: Command = Command {
    path: "flz controller",
    help: help::CONTROLLER,
    verbs: &[
        Verb::new("start", handle_start)
            .reading(&[MODE])
            .screen(help::CONTROLLER_START),
        Verb::new("stop", handle_stop)
            .reading(&[TIMEOUT_MS, NO_WAIT, FORCE])
            .screen(help::CONTROLLER_STOP),
        Verb::new("status", |read| {
            rpc::rendered(read, "controller_status", rpc::UNREACHABLE)
        })
        .reading(FORMAT)
        .screen(help::CONTROLLER_STATUS),
        Verb::new("mem-top", |read| {
            rpc::rendered(read, "controller_mem_top", rpc::UNREACHABLE)
        })
        .reading(FORMAT)
        .screen(help::CONTROLLER_MEM_TOP),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Missing::Refuse,
    unknown_prints_help: false,
};

/// The one verb that does not run `flzvm rpc`: it hands `flzvm` a start mode.
/// `flzvm` halts non-zero when the node refuses to boot.
const START_FAILED: Failure = Failure::new(Exit::Fixed(1))
    .saying("Unknown code returned: {}")
    .diagnosing("Failed to start program");

fn handle_start(read: &Reading) {
    let command_mode = match read.opt(MODE.long) {
        Some("daemon") => "daemon",
        Some("attached_iex") => "start_iex",
        Some("daemon_iex") => "daemon_iex",
        // The walk refused every word but these four, so the rest is
        // `attached`, written or defaulted.
        _ => "start",
    };

    println!("Starting flezha controller...");

    rpc::verdict(rpc::flzvm().arg(command_mode).status(), START_FAILED);

    println!("Controller started successfully.");
}

/// The controller could not be reached at all: there is no outcome to report,
/// only an unreachable socket. Judged by `rpc::verdict` like every other
/// child, so this verb does not decide its own status.
const UNREACHABLE_STOP: Failure = Failure::new(Exit::Fixed(3))
    .saying("error: controller not reachable ({})")
    .diagnosing("error: failed to run `flzvm rpc`");

// Exit codes (per controller-graceful-shutdown spec):
//   0 — clean shutdown
//   1 — partial drain (timeout elapsed, some Operatives still local)
//   2 — already shutting down (or coordinator not addressable)
//   3 — controller socket unreachable
fn handle_stop(read: &Reading) {
    // Built before the announcement: a refusal here means no shutdown starts,
    // and a line saying one has is the wrong thing to print above it.
    let expression = rpc::expression("controller_stop", &[], &stop_opts(read), read.token());

    eprintln!("Stopping flezha controller...");

    exit(match stop_outcome(&expression).as_deref() {
        Some("clean") | Some("forced") => 0,
        Some("partial") => 1,
        Some("already_in_progress") => 2,
        Some(other) => {
            eprintln!("error: unrecognised result marker '{}'", other);
            1
        }
        None => {
            // RPC returned 0 but no marker — treat as partial/unknown.
            eprintln!("error: no shutdown result marker in RPC output");
            1
        }
    });
}

/// Drive the shutdown and answer its result marker.
fn stop_outcome(expression: &str) -> Option<String> {
    let mut child = match rpc::flzvm()
        .arg("rpc")
        .arg(expression)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => {
            rpc::verdict(Err(e), UNREACHABLE_STOP);
            return None;
        }
    };

    let outcome = read_stop_marker(&mut child);

    // `flzvm rpc` exits non-zero when it cannot reach the controller node (the
    // most common cause being an unstarted controller — the control socket is
    // also the readiness signal). A run that printed its marker has already
    // said what happened, so only a silent one is judged by its status.
    if outcome.is_none() {
        rpc::verdict(child.wait(), UNREACHABLE_STOP);
    }

    outcome
}

/// The three options a shutdown carries; the credential joins them where
/// every request's does.
fn stop_opts(read: &Reading) -> Vec<Opt<'static>> {
    let mut opts: Vec<Opt> = Vec::new();

    if let Some(timeout_ms) = read.opt(TIMEOUT_MS.long) {
        opts.push(("timeout_ms", timeout_ms.to_string()));
    }
    if read.switch(NO_WAIT.long) {
        opts.push(("no_wait", "true".to_string()));
    }
    if read.switch(FORCE.long) {
        opts.push(("force", "true".to_string()));
    }

    opts
}

/// Stream the child's stdout through, holding back the one line that carries
/// the shutdown outcome.
fn read_stop_marker(child: &mut std::process::Child) -> Option<String> {
    let mut outcome: Option<String> = None;

    if let Some(stdout) = child.stdout.take() {
        let reader = BufReader::new(stdout);
        let stdout_handle = std::io::stdout();

        for line in reader.lines().map_while(Result::ok) {
            if let Some(rest) = line.strip_prefix("flz-controller-stop-result: ") {
                outcome = Some(rest.trim().to_string());
            } else {
                let mut h = stdout_handle.lock();
                let _ = writeln!(h, "{}", line);
            }
        }
    }

    outcome
}
