use std::io::{BufRead, BufReader, Write};
use std::process::{exit, Command, Stdio};

use crate::utils::{extract_param, get_flezha_command, get_parameter};

pub fn handle_controller_command(args: &[String]) {
    if args.is_empty() {
        eprintln!("Error: Invalid number of arguments. See `help` for more information.");
        exit(1);
    }

    match args[0].as_str() {
        "start" => handle_start(args),
        "stop" => handle_stop(args),
        "status" => handle_status(args),
        "mem-top" => handle_mem_top(args),
        "help" | "--help" | "-h" => print_controller_help(),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'start', 'stop', 'status', 'mem-top', or 'help'.",
                args[0]
            );
            exit(1);
        }
    }
}

// Exit codes match `flz controller status` / `stop`:
//   0 — snapshot printed
//   3 — controller socket unreachable
fn handle_mem_top(args: &[String]) {
    for arg in &args[1..] {
        if arg == "--help" || arg == "-h" || arg == "help" {
            print_mem_top_help();
            return;
        }
    }

    let format = if args.iter().any(|a| a == "--json" || a == "-o=json") {
        "json"
    } else {
        "text"
    };

    let command = get_flezha_command();
    let rpc_command = format!(
        "Flezha.Service.CliManager.controller_mem_top(\"{}\")",
        format
    );

    let status = Command::new(command)
        .arg("rpc")
        .arg(rpc_command)
        .status()
        .expect("Failed to execute command");

    if !status.success() {
        exit(3);
    }
}

fn handle_start(args: &[String]) {
    for arg in &args[1..] {
        if arg == "--help" || arg == "-h" || arg == "help" {
            print_start_help();
            return;
        }
    }

    if args.is_empty() {
        eprintln!("Error: Invalid number of arguments. Usage: flz controller start [--mode {{attached|daemon|attached_iex|daemon_iex}}]");
        exit(1);
    }

    let mode_arg = get_parameter(&args[1..], "--mode", "-m");

    let command_mode = match mode_arg.as_str() {
        "attached" => "start".to_string(),
        "daemon" => "daemon".to_string(),
        "attached_iex" => "start_iex".to_string(),
        "daemon_iex" => "daemon_iex".to_string(),
        "" => "start".to_string(),
        _ => {
            eprintln!("Error: Invalid mode '{}'. Use 'attached', 'daemon', 'attached_iex', or 'daemon_iex'.", mode_arg);
            exit(1);
        }
    };

    let command = get_flezha_command();

    println!("Starting flezha controller...");

    let status = Command::new(command).arg(command_mode).status();

    match status {
        Ok(status) if status.success() => println!("Controller started successfully."),
        Ok(status) => eprintln!("Unknown code returned: {}", status),
        Err(e) => eprintln!("Failed to start program: {}", e),
    }
}

// Exit codes (per controller-graceful-shutdown spec):
//   0 — clean shutdown
//   1 — partial drain (timeout elapsed, some Operatives still local)
//   2 — already shutting down (or coordinator not addressable)
//   3 — controller socket unreachable
fn handle_stop(args: &[String]) {
    for arg in &args[1..] {
        if arg == "--help" || arg == "-h" || arg == "help" {
            print_stop_help();
            return;
        }
    }

    let timeout_ms = extract_param(&args[1..], "--timeout-ms", "--timeout-ms");
    let no_wait = args.iter().any(|a| a == "--no-wait");
    let force = args.iter().any(|a| a == "--force");

    let mut opts: Vec<String> = Vec::new();
    if let Some(t) = timeout_ms {
        match t.parse::<u64>() {
            Ok(n) => opts.push(format!("timeout_ms: {}", n)),
            Err(_) => {
                eprintln!("Error: --timeout-ms requires an integer (milliseconds)");
                exit(1);
            }
        }
    }
    if no_wait {
        opts.push("no_wait: true".to_string());
    }
    if force {
        opts.push("force: true".to_string());
    }
    if let Some(token) = crate::utils::auth_token(&args[1..]) {
        opts.push(format!(
            "token: \"{}\"",
            crate::utils::escape_rpc_arg(&token)
        ));
    }

    let opts_kw = if opts.is_empty() {
        "[]".to_string()
    } else {
        format!("[{}]", opts.join(", "))
    };

    let rpc_command = format!("Flezha.Service.CliManager.controller_stop({})", opts_kw);

    eprintln!("Stopping flezha controller...");

    let command = get_flezha_command();
    let mut child = match Command::new(&command)
        .arg("rpc")
        .arg(&rpc_command)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: failed to spawn `{} rpc`: {}", command, e);
            exit(3);
        }
    };

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

    let status = match child.wait() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: failed to wait on `{} rpc`: {}", command, e);
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

    match outcome.as_deref() {
        Some("clean") | Some("forced") => exit(0),
        Some("partial") => exit(1),
        Some("already_in_progress") => exit(2),
        Some(other) => {
            eprintln!("error: unrecognised result marker '{}'", other);
            exit(1);
        }
        None => {
            // RPC returned 0 but no marker — treat as partial/unknown.
            eprintln!("error: no shutdown result marker in RPC output");
            exit(1);
        }
    }
}

fn handle_status(args: &[String]) {
    for arg in &args[1..] {
        if arg == "--help" || arg == "-h" || arg == "help" {
            print_status_help();
            return;
        }
    }

    let format = if args.iter().any(|a| a == "--json" || a == "-o=json") {
        "json"
    } else {
        "text"
    };

    let command = get_flezha_command();
    let rpc_command = format!(
        "Flezha.Service.CliManager.controller_status(\"{}\")",
        format
    );

    let status = Command::new(command)
        .arg("rpc")
        .arg(rpc_command)
        .status()
        .expect("Failed to execute command");

    if !status.success() {
        exit(3);
    }
}

fn print_controller_help() {
    println!("flz controller - Controller management commands");
    println!();
    println!("USAGE:");
    println!("    flz controller <COMMAND>");
    println!();
    println!("COMMANDS:");
    println!("    start    Start the flezha controller");
    println!("    stop     Stop the flezha controller (graceful)");
    println!("    status   Print controller status (incl. graceful-shutdown state)");
    println!("    mem-top  Snapshot top-memory processes / ETS / DETS / allocators");
    println!("    help     Show this help message");
    println!();
    println!("Use 'flz controller <COMMAND> --help' for more information on a specific command.");
}

fn print_mem_top_help() {
    println!("flz controller mem-top - Snapshot Controller memory hot spots");
    println!();
    println!("USAGE:");
    println!("    flz controller mem-top [--json]");
    println!();
    println!("OUTPUT (default text format):");
    println!("    Top-20 processes by :erlang.process_info(self, :memory),");
    println!("    sorted descending. Each row carries name / registered name /");
    println!("    initial call / :message_queue_len / :heap_size / :total_heap_size.");
    println!();
    println!("    ETS tables sorted by :ets.info(tab, :memory) (bytes).");
    println!("    DETS files under app_data/process_hub/ sorted by file size.");
    println!("    :recon_alloc.memory(:allocated_types) allocator breakdown.");
    println!();
    println!("FLAGS:");
    println!("    --json    Emit a single JSON document for machine consumption.");
}

fn print_start_help() {
    println!("flz controller start - Start the flezha controller");
    println!();
    println!("USAGE:");
    println!("    flz controller start [OPTIONS]");
    println!();
    println!("OPTIONS:");
    println!("    --mode <MODE>     Controller start mode");
    println!("    -m <MODE>         Short form of --mode");
    println!();
    println!("AVAILABLE MODES:");
    println!("    attached          Start in attached mode (default)");
    println!("    daemon            Start as daemon");
    println!("    attached_iex      Start in attached IEx mode");
    println!("    daemon_iex        Start as daemon with IEx");
    println!();
    println!("EXAMPLES:");
    println!("    flz controller start");
    println!("    flz controller start --mode daemon");
    println!("    flz controller start -m attached_iex");
}

fn print_stop_help() {
    println!("flz controller stop - Stop the flezha controller gracefully");
    println!();
    println!("USAGE:");
    println!("    flz controller stop [OPTIONS]");
    println!();
    println!("DESCRIPTION:");
    println!("    Drives Flezha.Shutdown.Coordinator over the running controller's");
    println!("    CLI socket (works for release, mix, and iex run modes). Streams");
    println!("    drain progress to stderr and prints a final outcome line on stdout.");
    println!();
    println!("OPTIONS:");
    println!("    --timeout-ms <N>  Override the configured drain timeout (ms)");
    println!("    --no-wait         Issue the shutdown and exit without waiting");
    println!("    --force           Skip drain — finalise (flush WAL + close transports) only");
    println!();
    println!("EXIT CODES:");
    println!("    0   Clean shutdown (or `--force` finalised cleanly)");
    println!("    1   Partial drain (timeout elapsed; some Operatives still local)");
    println!("    2   Already shutting down (the in-flight outcome is reported)");
    println!("    3   Controller not reachable on the CLI socket");
}

fn print_status_help() {
    println!("flz controller status - Print controller status");
    println!();
    println!("USAGE:");
    println!("    flz controller status [--json]");
    println!();
    println!("OUTPUT FIELDS:");
    println!("    shutdown_in_progress       True while quiesce/drain/finalise is running");
    println!("    shutdown_phase             :quiesce | :drain | :finalise | nil");
    println!(
        "    signal_handler_status      :ok | :degraded (degraded if :os.set_signal/2 unavailable)"
    );
    println!("    graceful_shutdown_enabled  Configured kill-switch state");
    println!("    graceful_shutdown_timeout_ms  Configured drain timeout");
}
