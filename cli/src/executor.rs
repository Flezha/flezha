use std::process::{exit, Command};

use crate::utils::{get_flezha_command, get_namespace, get_output_format};

pub fn handle_executor_command(args: &[String]) {
    if args.is_empty() {
        eprintln!("Error: Invalid number of arguments. See help for more information.");
        exit(1);
    }

    match args[0].as_str() {
        "list" => handle_list(args),
        "help" | "--help" | "-h" => print_executor_help(),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'list' or 'help'.",
                args[0]
            );
            exit(1);
        }
    }
}

fn handle_list(args: &[String]) {
    let namespace = get_namespace(&args[1..]);
    let output_format = get_output_format(&args[1..]);

    let command = get_flezha_command();
    let rpc_command = format!(
        "Flezha.Service.CliManager.list_executors(\"{}\", \"{}\")",
        namespace, output_format
    );

    // Only show progress message for text output
    if output_format == "text" {
        let display_namespace = if namespace.is_empty() {
            "default"
        } else {
            &namespace
        };
        println!(
            "Fetching executors for namespace '{}'...",
            display_namespace
        );
    }

    let status = Command::new(command)
        .arg("rpc")
        .arg(rpc_command)
        .status()
        .expect("Failed to execute command");

    if !status.success() {
        eprintln!("Error: Command execution failed.");
        exit(1);
    }
}

fn print_executor_help() {
    println!("flz executor - Compute engine management commands");
    println!();
    println!("USAGE:");
    println!("    flz executor <COMMAND>");
    println!();
    println!("COMMANDS:");
    println!("    list    List compute engines");
    println!("    help    Show this help message");
    println!();
    println!("Use 'flz executor <COMMAND> --help' for more information on a specific command.");
}
