use std::process::{exit, Command};

use crate::utils::{get_flezha_command, get_namespace, get_output_format};

pub fn handle_cluster_command(args: &[String]) {
    if args.is_empty() {
        print_cluster_help();
        exit(1);
    }

    match args[0].as_str() {
        "status" => handle_status(args),
        "prepare-recovery" => handle_prepare_recovery(args),
        "help" | "--help" | "-h" => print_cluster_help(),
        other => {
            eprintln!("Error: Unknown cluster subcommand '{}'", other);
            print_cluster_help();
            exit(1);
        }
    }
}

fn handle_status(args: &[String]) {
    let namespace = get_namespace(&args[1..]);
    let output_format = get_output_format(&args[1..]);

    let command = get_flezha_command();
    let rpc_command = format!(
        "Flezha.Service.CliManager.cluster_status(\"{}\", \"{}\")",
        namespace, output_format
    );

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

fn handle_prepare_recovery(args: &[String]) {
    let namespace = get_namespace(&args[1..]);

    let command = get_flezha_command();
    let rpc_command = format!(
        "Flezha.Service.CliManager.cluster_prepare_recovery(\"{}\")",
        namespace
    );

    let status = Command::new(command)
        .arg("rpc")
        .arg(rpc_command)
        .status()
        .expect("Failed to execute command");

    // The Elixir side ends with `System.halt(0|1|2)` depending on
    // whether every node ack'd, a partial result was returned, or the
    // upstream call errored. Propagate that code verbatim — the
    // operator-runbook treats 0/1/2 as the canonical signal.
    if !status.success() {
        exit(status.code().unwrap_or(1));
    }
}

fn print_cluster_help() {
    println!("flz cluster - Controller cluster status");
    println!();
    println!("USAGE:");
    println!("    flz cluster <SUBCOMMAND>");
    println!();
    println!("SUBCOMMANDS:");
    println!(
        "    status              Show nodes, leader, per-node relay counts, relay health, recovery state"
    );
    println!(
        "    prepare-recovery    Arm a planned cluster restart by deleting the recovery marker on every node"
    );
    println!("    help                Show this help message");
    println!();
    println!("OPTIONS:");
    println!("    -n, --namespace <NS>   Namespace (default: \"default\")");
    println!("    -o, --output <FMT>     Output format: text (default) or json");
    println!();
    println!("EXAMPLES:");
    println!("    flz cluster status");
    println!("    flz cluster status --namespace prod --output json");
    println!("    flz cluster prepare-recovery");
}
