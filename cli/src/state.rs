use std::process::{exit, Command};

use crate::utils::{get_flezha_command, get_output_format};

pub fn handle_state_command(args: &[String]) {
    if args.is_empty() {
        eprintln!("Error: Invalid number of arguments. See `flz state help`.");
        exit(1);
    }

    match args[0].as_str() {
        "list" => handle_list(args),
        "at" => handle_at(args),
        "rotate" => handle_rotate(args),
        "retention" => handle_retention(args),
        "help" | "--help" | "-h" => print_state_help(),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'list', 'at', 'rotate', 'retention', or 'help'.",
                args[0]
            );
            exit(1);
        }
    }
}

fn handle_list(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Usage: flz state list <relay_class> [--output text|json]");
        exit(1);
    }

    let relay_class = &args[1];
    let output_format = get_output_format(&args[2..]);
    let command = get_flezha_command();

    let rpc_command = format!(
        "Flezha.Service.CliManager.state_list(\"{}\", \"{}\")",
        relay_class, output_format,
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

fn handle_at(args: &[String]) {
    if args.len() < 3 {
        eprintln!("Usage: flz state at <relay/shard/partition_key> <seq | iso8601-timestamp> [--output text|json]");
        exit(1);
    }

    let path = &args[1];
    let target = &args[2];
    let output_format = get_output_format(&args[3..]);
    let command = get_flezha_command();

    let rpc_command = format!(
        "Flezha.Service.CliManager.state_at(\"{}\", \"{}\", \"{}\")",
        path, target, output_format,
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

fn handle_rotate(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Usage: flz state rotate <relay/shard/partition_key>");
        exit(1);
    }

    let path = &args[1];
    let command = get_flezha_command();

    let rpc_command = format!("Flezha.Service.CliManager.state_rotate(\"{}\")", path,);

    let status = Command::new(command)
        .arg("rpc")
        .arg(rpc_command)
        .status()
        .expect("Failed to execute command");

    if !status.success() {
        exit(status.code().unwrap_or(1));
    }
}

fn handle_retention(args: &[String]) {
    if args.len() < 3 {
        eprintln!("Usage: flz state retention <relay_class> <hours>");
        exit(1);
    }

    let relay_class = &args[1];
    let hours = &args[2];
    let command = get_flezha_command();

    let rpc_command = format!(
        "Flezha.Service.CliManager.state_retention(\"{}\", \"{}\")",
        relay_class, hours,
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

fn print_state_help() {
    println!("flz state - Per-Partition state inspection and time-travel queries");
    println!();
    println!("USAGE:");
    println!("    flz state <SUBCOMMAND>");
    println!();
    println!("SUBCOMMANDS:");
    println!("    list <relay>                          List Partitions under a Relay class");
    println!("    at <relay/shard/key> <seq|ts>         Query state at a point in time");
    println!("    rotate <relay/shard/key>              Force a compaction snapshot");
    println!("    retention <relay> <hours>             Set time-travel retention");
    println!();
    println!("EXAMPLES:");
    println!("    flz state list MarketRelay");
    println!("    flz state at MarketRelay/OhlcShard/M1 12345");
    println!("    flz state at MarketRelay/OhlcShard/M1 2026-05-01T12:00:00Z");
    println!("    flz state rotate MarketRelay/OhlcShard/M1");
    println!("    flz state retention MarketRelay 6");
}
