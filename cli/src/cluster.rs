use std::process::{exit, Command};

use crate::utils::{get_flezha_command, get_namespace, get_output_format};

pub fn handle_cluster_command(args: &[String]) {
    if args.is_empty() {
        print_cluster_help();
        exit(1);
    }

    match args[0].as_str() {
        "status" => handle_status(args),
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

fn print_cluster_help() {
    println!("flz cluster - Controller cluster status");
    println!();
    println!("USAGE:");
    println!("    flz cluster <SUBCOMMAND>");
    println!();
    println!("SUBCOMMANDS:");
    println!(
        "    status              Show nodes, leader, per-node operative counts, operative health, recovery state"
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
}
