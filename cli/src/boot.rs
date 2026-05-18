use std::process::{exit, Command};

use crate::utils::get_flezha_command;

pub fn handle_boot_command(args: &[String]) {
    if args.is_empty() {
        eprintln!("Error: Invalid number of arguments. See `flz boot help`.");
        exit(1);
    }

    match args[0].as_str() {
        "status" => handle_status(args),
        "await" => handle_await(args),
        "import-legacy-snapshot" => handle_import(args),
        "help" | "--help" | "-h" => print_boot_help(),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'status', 'await', 'import-legacy-snapshot', or 'help'.",
                args[0]
            );
            exit(1);
        }
    }
}

fn handle_status(_args: &[String]) {
    let command = get_flezha_command();
    let rpc_command = "Flezha.Service.CliManager.boot_status()".to_string();

    let status = Command::new(command)
        .arg("rpc")
        .arg(rpc_command)
        .status()
        .expect("Failed to execute command");

    if !status.success() {
        exit(status.code().unwrap_or(1));
    }
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

    let command = get_flezha_command();
    let rpc_command = format!("Flezha.Service.CliManager.boot_await({})", timeout_ms);

    let status = Command::new(command)
        .arg("rpc")
        .arg(rpc_command)
        .status()
        .expect("Failed to execute command");

    if !status.success() {
        exit(status.code().unwrap_or(1));
    }
}

fn handle_import(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Usage: flz boot import-legacy-snapshot <file> [--force]");
        exit(1);
    }

    let path = &args[1];
    let force = args.iter().any(|a| a == "--force");

    let command = get_flezha_command();
    let rpc_command = format!(
        "Flezha.Service.CliManager.boot_import_legacy_snapshot(\"{}\", {})",
        path, force
    );

    let status = Command::new(command)
        .arg("rpc")
        .arg(rpc_command)
        .status()
        .expect("Failed to execute command");

    if !status.success() {
        exit(status.code().unwrap_or(1));
    }
}

fn print_boot_help() {
    println!("flz boot - Boot/recovery commands");
    println!();
    println!("USAGE:");
    println!("    flz boot <COMMAND>");
    println!();
    println!("COMMANDS:");
    println!(
        "    status                    Print per-namespace recovery state and registry counts"
    );
    println!("    await [--timeout SECS]    Block until every namespace hub reports :normal");
    println!("    import-legacy-snapshot <file> [--force]");
    println!("                              Import a legacy `.flz` snapshot into ProcessHub's persistent registry");
    println!("    help                      Show this help message");
}
