use std::io::{BufRead, BufReader, Write};
use std::process::{exit, Stdio};

use crate::dispatch::{self, Command};
use crate::rpc::{self, Exit, Failure};
use crate::{args, help};

pub(crate) const TABLE: Command = Command {
    help: help::CONTROLLER,
    verbs: &[
        ("start", handle_start),
        ("stop", handle_stop),
        ("status", handle_status),
        ("mem-top", handle_mem_top),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Some("Error: Invalid number of arguments. See `help` for more information."),
    unknown: "Error: Invalid argument '{}'. Use 'start', 'stop', 'status', 'mem-top', or 'help'.",
    unknown_prints_help: false,
};

// Exit codes match `flz controller status` / `stop`:
//   0 — snapshot printed
//   3 — controller socket unreachable
fn handle_mem_top(args: &[String]) {
    if dispatch::subcommand_help(&args[1..], help::CONTROLLER_MEM_TOP, dispatch::HELP_VERBS) {
        return;
    }

    rpc::run(
        format!(
            "Flezha.Service.CliManager.controller_mem_top(\"{}\")",
            args::output_format(args)
        ),
        rpc::UNREACHABLE,
    );
}

fn handle_status(args: &[String]) {
    if dispatch::subcommand_help(&args[1..], help::CONTROLLER_STATUS, dispatch::HELP_VERBS) {
        return;
    }

    rpc::run(
        format!(
            "Flezha.Service.CliManager.controller_status(\"{}\")",
            args::output_format(args)
        ),
        rpc::UNREACHABLE,
    );
}

/// The one verb that does not run `flzvm rpc`: it hands `flzvm` a start mode.
/// `flzvm` halts non-zero when the node refuses to boot.
const START_FAILED: Failure = Failure::new(Exit::Fixed(1))
    .saying("Unknown code returned: {}")
    .diagnosing("Failed to start program");

fn handle_start(args: &[String]) {
    if dispatch::subcommand_help(&args[1..], help::CONTROLLER_START, dispatch::HELP_VERBS) {
        return;
    }

    let mode_arg =
        args::or_refuse(args::extract_param(&args[1..], "--mode", "-m")).unwrap_or_default();

    let command_mode = match mode_arg.as_str() {
        "attached" => "start",
        "daemon" => "daemon",
        "attached_iex" => "start_iex",
        "daemon_iex" => "daemon_iex",
        "" => "start",
        _ => {
            eprintln!("Error: Invalid mode '{}'. Use 'attached', 'daemon', 'attached_iex', or 'daemon_iex'.", mode_arg);
            exit(1);
        }
    };

    println!("Starting flezha controller...");

    rpc::verdict(rpc::flzvm().arg(command_mode).status(), START_FAILED);

    println!("Controller started successfully.");
}

// Exit codes (per controller-graceful-shutdown spec):
//   0 — clean shutdown
//   1 — partial drain (timeout elapsed, some Operatives still local)
//   2 — already shutting down (or coordinator not addressable)
//   3 — controller socket unreachable
fn handle_stop(args: &[String]) {
    if dispatch::subcommand_help(&args[1..], help::CONTROLLER_STOP, dispatch::HELP_VERBS) {
        return;
    }

    // Built before the announcement: a refusal here means no shutdown starts,
    // and a line saying one has is the wrong thing to print above it.
    let opts_kw = stop_opts(&args[1..]);

    eprintln!("Stopping flezha controller...");

    exit(match stop_outcome(&opts_kw).as_deref() {
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

/// Drive the shutdown and answer its result marker. Exits 3 rather than
/// returning when the controller could not be reached at all: there is no
/// outcome to report then, only an unreachable socket.
fn stop_outcome(opts_kw: &str) -> Option<String> {
    let mut child = match rpc::flzvm()
        .arg("rpc")
        .arg(format!(
            "Flezha.Service.CliManager.controller_stop({})",
            opts_kw
        ))
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: failed to spawn `{} rpc`: {}", rpc::flzvm_path(), e);
            exit(3);
        }
    };

    let (outcome, saw_any_output) = read_stop_marker(&mut child);

    let status = match child.wait() {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "error: failed to wait on `{} rpc`: {}",
                rpc::flzvm_path(),
                e
            );
            exit(3);
        }
    };

    // `flzvm rpc` exits non-zero when it cannot reach the controller
    // node (the most common cause being an unstarted controller —
    // the control socket is also the readiness signal).
    if !status.success() && outcome.is_none() {
        if !saw_any_output {
            eprintln!(
                "error: controller not reachable (rpc exit {:?})",
                status.code()
            );
        }
        exit(3);
    }

    outcome
}

/// The shutdown's keyword list. The identity token rides it like any other
/// gated verb's, spelled out here because this one carries three more.
fn stop_opts(args: &[String]) -> String {
    let mut opts: Vec<String> = Vec::new();

    if let Some(timeout_ms) =
        args::or_refuse(args::number::<u64>(args, "--timeout-ms", "--timeout-ms"))
    {
        opts.push(format!("timeout_ms: {}", timeout_ms));
    }
    if args.iter().any(|a| a == "--no-wait") {
        opts.push("no_wait: true".to_string());
    }
    if args.iter().any(|a| a == "--force") {
        opts.push("force: true".to_string());
    }
    if let Some(token) = rpc::auth_token(args) {
        opts.push(format!("token: \"{}\"", rpc::escape_rpc_arg(&token)));
    }

    if opts.is_empty() {
        "[]".to_string()
    } else {
        format!("[{}]", opts.join(", "))
    }
}

/// Stream the child's stdout through, holding back the one line that carries
/// the shutdown outcome. Answers `(outcome, saw_any_output)`.
fn read_stop_marker(child: &mut std::process::Child) -> (Option<String>, bool) {
    let mut outcome: Option<String> = None;
    let mut saw_any_output = false;

    if let Some(stdout) = child.stdout.take() {
        let reader = BufReader::new(stdout);
        let stdout_handle = std::io::stdout();

        for line in reader.lines().map_while(Result::ok) {
            saw_any_output = true;
            if let Some(rest) = line.strip_prefix("flz-controller-stop-result: ") {
                outcome = Some(rest.trim().to_string());
            } else {
                let mut h = stdout_handle.lock();
                let _ = writeln!(h, "{}", line);
            }
        }
    }

    (outcome, saw_any_output)
}
