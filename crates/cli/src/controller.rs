use std::process::exit;

use flz_transport::errors::{self, FlezhaError};
use serde_json::Value;

use crate::args::{OptSpec, Reading};
use crate::dispatch::{Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, Out};

const MODE: OptSpec = OptSpec::choice(
    "--mode",
    "-m",
    "mode",
    &["attached", "daemon", "attached_iex", "daemon_iex"],
)
.doc("How the controller runs (default: attached).");
const TIMEOUT_MS: OptSpec = OptSpec::number("--timeout-ms", "--timeout-ms", "<ms>")
    .doc("Override the configured drain timeout (ms).");
const NO_WAIT: OptSpec =
    OptSpec::switch("--no-wait", "--no-wait").doc("Issue the shutdown and exit without waiting.");
const FORCE: OptSpec = OptSpec::switch("--force", "--force")
    .doc("Skip the drain and only finalise (flush WAL, close transports).");

pub(crate) const TABLE: Command = Command {
    path: "flz controller",
    title: "Controller management commands.",
    notes: None,
    verbs: &[
        Verb::new("start", handle_start)
            .reading(&[MODE])
            .about("Start the flezha controller.")
            .notes(help::CONTROLLER_START)
            .help_word(),
        Verb::new("stop", handle_stop)
            .reading(&[TIMEOUT_MS, NO_WAIT, FORCE])
            .calls(&["Controller.stop"])
            .notes(help::CONTROLLER_STOP)
            .help_word(),
        Verb::door("status")
            .calls(&["Controller.status"])
            .notes(help::CONTROLLER_STATUS)
            .help_word(),
        Verb::door("mem-top")
            .calls(&["Controller.mem_top"])
            .notes(help::CONTROLLER_MEM_TOP)
            .help_word(),
    ],
    missing: Missing::Refuse,
    unknown_prints_help: false,
};

/// The one verb that starts the release itself: it hands `flzvm` a start mode.
/// `flzvm` halts non-zero when the node refuses to boot, and that code is the
/// verb's.
fn handle_start(read: &Reading) {
    let command_mode = match read.opt(MODE.long) {
        Some("daemon") => "daemon",
        Some("attached_iex") => "start_iex",
        Some("daemon_iex") => "daemon_iex",
        // The walk refused every word but these four, so the rest is
        // `attached`, written or defaulted.
        _ => "start",
    };

    // A daemon mode returns once the node is launched; an attached one returns
    // when the controller has stopped, so it has no start left to announce.
    let detached = matches!(command_mode, "daemon" | "daemon_iex");

    println!("Starting flezha controller...");

    rpc::verdict(rpc::flzvm().arg(command_mode).status());

    if detached {
        println!("Controller started successfully.");
    }
}

/// The line the controller's stop prints last, naming how it ended: the node
/// may be gone before the call's own exit reaches `flz`, so the marker, once
/// seen, is the outcome.
const STOP_MARKER: &str = "flz-controller-stop-result: ";

/// `0` for a clean or forced stop; a partial drain exits with the class of
/// `shutdown.partial_drain`, which the controller has already rendered. With no
/// marker the call is settled like any other: a refusal exits with its own code,
/// so a stop refused for want of a token is denied rather than unreachable, and
/// a node that could not be reached is `controller.unreachable`.
fn handle_stop(read: &Reading) {
    // Built before the announcement: a refusal here means no shutdown starts,
    // and a line saying one has is the wrong thing to print above it.
    let request = rpc::door_request("Controller.stop", &[stop_opts(read)], &[], read);

    eprintln!("Stopping flezha controller...");

    let mut marker: Option<String> = None;
    let ended = rpc::send(
        &request,
        Out::Filter(&mut |line| match line.strip_prefix(STOP_MARKER) {
            Some(outcome) => {
                marker = Some(outcome.trim().to_string());
                false
            }
            None => true,
        }),
        None,
    );

    match marker.as_deref() {
        Some("clean") | Some("forced") => exit(0),
        Some("partial") => {
            exit(errors::entry("shutdown.partial_drain").map_or(1, |entry| entry.class.exit_code()))
        }
        Some(other) => rpc::fail(unconfirmed(other)),
        None => {
            rpc::settle(ended);
            rpc::fail(unconfirmed(""));
        }
    }
}

fn unconfirmed(marker: &str) -> FlezhaError {
    FlezhaError::with(
        "controller.stop_unconfirmed",
        [("marker", Value::from(marker))],
    )
}

/// The stop's options, each only when written.
fn stop_opts(read: &Reading) -> Value {
    let mut opts = serde_json::Map::new();

    if let Some(timeout_ms) = read.num::<u64>(TIMEOUT_MS.long) {
        opts.insert("timeout_ms".into(), Value::from(timeout_ms));
    }
    for (spec, key) in [(NO_WAIT, "no_wait"), (FORCE, "force")] {
        if read.switch(spec.long) {
            opts.insert(key.into(), Value::from(true));
        }
    }

    Value::Object(opts)
}
