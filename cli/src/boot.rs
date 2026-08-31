use std::process::exit;

use crate::dispatch::{self, Command};
use crate::{help, rpc};

pub(crate) const TABLE: Command = Command {
    help: help::BOOT,
    verbs: &[
        ("status", handle_status),
        ("await", handle_await),
        ("import-legacy-snapshot", handle_import),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Some("Error: Invalid number of arguments. See `flz boot help`."),
    unknown:
        "Error: Invalid argument '{}'. Use 'status', 'await', 'import-legacy-snapshot', or 'help'.",
    unknown_prints_help: false,
};

fn handle_status(_args: &[String]) {
    rpc::run(
        "Flezha.Service.CliManager.boot_status()".to_string(),
        rpc::PROPAGATE,
    );
}

fn handle_await(args: &[String]) {
    let mut timeout_ms: u64 = 60_000;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--timeout" => {
                if i + 1 >= args.len() {
                    eprintln!("Error: --timeout requires a value (seconds)");
                    exit(1);
                }
                let secs: u64 = args[i + 1].parse().unwrap_or_else(|_| {
                    eprintln!("Error: invalid --timeout value '{}'", args[i + 1]);
                    exit(1);
                });
                timeout_ms = secs.saturating_mul(1000);
                i += 2;
            }
            other => {
                eprintln!("Error: unexpected argument '{}'", other);
                exit(1);
            }
        }
    }

    rpc::run(
        format!("Flezha.Service.CliManager.boot_await({})", timeout_ms),
        rpc::PROPAGATE,
    );
}

fn handle_import(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Usage: flz boot import-legacy-snapshot <file> [--force]");
        exit(1);
    }

    let path = &args[1];
    let force = args.iter().any(|a| a == "--force");

    rpc::run(
        format!(
            "Flezha.Service.CliManager.boot_import_legacy_snapshot(\"{}\", {})",
            path, force
        ),
        rpc::PROPAGATE,
    );
}
