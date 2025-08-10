mod agent;
mod computation;
mod namespace;
mod operator;
mod plugin;
mod utils;

use std::env;
use std::process::exit;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        print_help();
        exit(1);
    }

    match args[1].as_str() {
        "agent" => agent::handle_agent_command(&args[2..]),
        "computation" => computation::handle_computation_command(&args[2..]),
        "namespace" => namespace::handle_namespace_command(&args[2..]),
        "operator" => operator::handle_operator_command(&args[2..]),
        "plugin" => plugin::handle_plugin_command(&args[2..]),
        "help" | "--help" | "-h" => print_help(),
        _ => {
            eprintln!("Error: Unknown command '{}'", args[1]);
            print_help();
            exit(1);
        }
    }
}

fn print_help() {
    println!("flz - Flezha CLI");
    println!();
    println!("USAGE:");
    println!("    flz <SUBCOMMAND>");
    println!();
    println!("SUBCOMMANDS:");
    println!("    agent        Agent management commands");
    println!("      start      Start the flezha agent");
    println!("      stop       Stop the flezha agent");
    println!();
    println!("    computation  Computation management commands");
    println!("      start      Start a computation");
    println!("      stop       Stop a computation");
    println!("      state      Get computation state");
    println!("      list       List computations");
    println!("      info       Get computation info");
    println!("      io_listen  Listen to computation I/O");
    println!();
    println!("    namespace    Namespace management commands");
    println!("      list       List namespaces");
    println!();
    println!("    operator     Operator management commands");
    println!("      list       List operators");
    println!();
    println!("    plugin       Plugin management commands");
    println!("      enable     Enable a plugin");
    println!("      disable    Disable a plugin");
    println!();
    println!("    help         Show this help message");
    println!();
    println!("EXAMPLES:");
    println!("    flz agent start");
    println!("    flz computation start my_comp my_factory --namespace prod");
    println!("    flz plugin enable kafka_producer provider1 Xhosts=localhost:9092");
    println!("    flz namespace list --output json");
}