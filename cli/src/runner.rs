use std::process::{exit, Command};

use flz_transport::transport::config::{get_transport_type, TransportType};
use flz_transport::utility::path_helper;

use crate::utils::{get_flezha_command, get_namespace, get_output_format};

pub fn handle_runner_command(args: &[String]) {
    if args.is_empty() {
        eprintln!("Error: Invalid number of arguments. See help for more information.");
        exit(1);
    }

    match args[0].as_str() {
        "list" => handle_list(args),
        "check" => handle_check(),
        "help" | "--help" | "-h" => print_runner_help(),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'list', 'check' or 'help'.",
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
        "Flezha.Service.CliManager.list_runners(\"{}\", \"{}\")",
        namespace, output_format
    );

    // Only show progress message for text output
    if output_format == "text" {
        let display_namespace = if namespace.is_empty() {
            "default"
        } else {
            &namespace
        };
        println!("Fetching runners for namespace '{}'...", display_namespace);
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

/// Where this host's controller is, and why a runner cannot reach it — the
/// same ladder and the same probe a runner walks, from a shell, with no runner
/// code, no running runner and no controller needed.
fn handle_check() {
    if get_transport_type() == TransportType::Quic {
        println!("note: FLEZHA_TRANSPORT=quic — this check covers the UDS socket directory only");
    }

    // The runner's own words, not a second rendering of them.
    match path_helper::diagnose(None) {
        Ok(diagnosis) => {
            println!("{}", diagnosis);

            if !diagnosis.reach.reachable() {
                exit(1);
            }
        }
        Err(refusal) => {
            println!("{}", refusal);
            exit(1);
        }
    }
}

fn print_runner_help() {
    println!("flz runner - Runner management commands");
    println!();
    println!("USAGE:");
    println!("    flz runner <COMMAND>");
    println!();
    println!("COMMANDS:");
    println!("    list    List runners");
    println!("    check   Report whether a runner on this host can reach a controller");
    println!("    help    Show this help message");
    println!();
    println!("Use 'flz runner <COMMAND> --help' for more information on a specific command.");
}
