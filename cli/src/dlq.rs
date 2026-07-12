use std::process::{exit, Command};

use crate::utils::get_flezha_command;

pub fn handle_dlq_command(args: &[String]) {
    if args.is_empty() {
        eprintln!("Error: Invalid number of arguments. See `flz dlq help`.");
        exit(1);
    }

    match args[0].as_str() {
        "list" => handle_list(args),
        "show" => handle_show(args),
        "replay" => handle_replay(args),
        "drop" => handle_drop(args),
        "help" | "--help" | "-h" => print_dlq_help(),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'list', 'show', 'replay', 'drop', or 'help'.",
                args[0]
            );
            exit(1);
        }
    }
}

fn rpc(expr: String) {
    let command = get_flezha_command();
    let status = Command::new(command)
        .arg("rpc")
        .arg(expr)
        .status()
        .expect("Failed to execute command");

    if !status.success() {
        exit(status.code().unwrap_or(1));
    }
}

fn handle_list(args: &[String]) {
    let operative_class = args.get(1).map(String::as_str).unwrap_or("");
    rpc(format!(
        "Flezha.Service.CliManager.dlq_list(\"{}\")",
        operative_class
    ));
}

fn entry_ref_arg(args: &[String], usage: &str) -> String {
    match args.get(1) {
        Some(entry_ref) => entry_ref.clone(),
        None => {
            eprintln!("Usage: {}", usage);
            exit(1);
        }
    }
}

fn handle_show(args: &[String]) {
    let entry_ref = entry_ref_arg(args, "flz dlq show <operative/operation/entry-id>");
    rpc(format!(
        "Flezha.Service.CliManager.dlq_show(\"{}\")",
        entry_ref
    ));
}

fn handle_replay(args: &[String]) {
    let entry_ref = entry_ref_arg(args, "flz dlq replay <operative/operation/entry-id>");
    rpc(format!(
        "Flezha.Service.CliManager.dlq_replay(\"{}\")",
        entry_ref
    ));
}

fn handle_drop(args: &[String]) {
    let entry_ref = entry_ref_arg(args, "flz dlq drop <operative/operation/entry-id>");
    rpc(format!(
        "Flezha.Service.CliManager.dlq_drop(\"{}\")",
        entry_ref
    ));
}

fn print_dlq_help() {
    println!("Dead-letter queue: parked poison payloads (retry budget exhausted).");
    println!();
    println!("Usage:");
    println!(
        "  flz dlq list [operative]                       Parked entries, nearest-to-expiry first"
    );
    println!("  flz dlq show <operative/operation/entry-id>    Entry metadata + payload preview");
    println!("  flz dlq replay <operative/operation/entry-id>  Re-dispatch through the normal input path");
    println!("  flz dlq drop <operative/operation/entry-id>    Discard the entry (audited)");
}
