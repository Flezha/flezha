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
        "delete" => handle_delete(args),
        "outbox" => handle_outbox(args),
        "help" | "--help" | "-h" => print_state_help(),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'list', 'at', 'rotate', 'retention', 'delete', 'outbox', or 'help'.",
                args[0]
            );
            exit(1);
        }
    }
}

fn handle_list(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Usage: flz state list <operative_class> [--output text|json]");
        exit(1);
    }

    let operative_class = &args[1];
    let output_format = get_output_format(&args[2..]);
    let command = get_flezha_command();

    let rpc_command = format!(
        "Flezha.Service.CliManager.state_list(\"{}\", \"{}\")",
        operative_class, output_format,
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
        eprintln!("Usage: flz state at <operative/operation/segment_key> <seq | iso8601-timestamp> [--output text|json]");
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
        eprintln!("Usage: flz state rotate <operative/operation/segment_key>");
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
        eprintln!("Usage: flz state retention <operative_class> <hours>");
        exit(1);
    }

    let operative_class = &args[1];
    let hours = &args[2];
    let command = get_flezha_command();

    let rpc_command = format!(
        "Flezha.Service.CliManager.state_retention(\"{}\", \"{}\")",
        operative_class, hours,
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

fn handle_delete(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Usage: flz state delete <operative/operation/segment_key>");
        exit(1);
    }

    let path = &args[1];
    let command = get_flezha_command();

    let rpc_command = format!("Flezha.Service.CliManager.state_delete(\"{}\")", path,);

    let status = Command::new(command)
        .arg("rpc")
        .arg(rpc_command)
        .status()
        .expect("Failed to execute command");

    if !status.success() {
        exit(status.code().unwrap_or(1));
    }
}

fn handle_outbox(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Usage: flz state outbox <operative/operation/segment_key>");
        exit(1);
    }

    let path = &args[1];
    let command = get_flezha_command();

    let rpc_command = format!("Flezha.Service.CliManager.state_outbox(\"{}\")", path,);

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
    println!("flz state - Per-Segment state inspection and time-travel queries");
    println!();
    println!("USAGE:");
    println!("    flz state <SUBCOMMAND>");
    println!();
    println!("SUBCOMMANDS:");
    println!(
        "    list <operative>                          List Segments under an Operative class"
    );
    println!("    at <operative/operation/key> <seq|ts>         Query state at a point in time");
    println!("    rotate <operative/operation/key>              Force a compaction snapshot");
    println!("    retention <operative> <hours>             Set time-travel retention");
    println!(
        "    delete <operative/operation/key>              Hard-delete a Segment's persisted state"
    );
    println!(
        "    outbox <operative/operation/key>              Effectively-once outbox status (lag, cursor, watermark)"
    );
    println!();
    println!("EXAMPLES:");
    println!("    flz state list MarketOperative");
    println!("    flz state at MarketOperative/OhlcOperation/M1 12345");
    println!("    flz state at MarketOperative/OhlcOperation/M1 2026-05-01T12:00:00Z");
    println!("    flz state rotate MarketOperative/OhlcOperation/M1");
    println!("    flz state retention MarketOperative 6");
    println!("    flz state delete MarketOperative/OhlcOperation/M1");
    println!("    flz state outbox MarketOperative/OhlcOperation/M1");
}
