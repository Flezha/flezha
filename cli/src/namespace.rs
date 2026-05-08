use std::process::{exit, Command};

use crate::utils::{get_flezha_command, get_output_format};

pub fn handle_namespace_command(args: &[String]) {
    if args.is_empty() {
        eprintln!("Error: Invalid number of arguments. See help for more information.");
        exit(1);
    }

    match args[0].as_str() {
        "list" => handle_list(args),
        "help" | "--help" | "-h" => print_namespace_help(),
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
    let output_format = get_output_format(&args[1..]);

    let command = get_flezha_command();
    let rpc_command = format!(
        "Flezha.Service.CliManager.list_namespaces(\"{}\")",
        output_format
    );

    // Only show progress message for text output
    if output_format == "text" {
        println!("Fetching namespaces...");
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

fn print_namespace_help() {
    println!("flz namespace - Namespace management commands");
    println!();
    println!("USAGE:");
    println!("    flz namespace <COMMAND>");
    println!();
    println!("COMMANDS:");
    println!("    list    List namespaces");
    println!("    help    Show this help message");
    println!();
    println!("Use 'flz namespace <COMMAND> --help' for more information on a specific command.");
}
