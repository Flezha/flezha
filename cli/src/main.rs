mod agent;
mod compute_engine;
mod namespace;
mod plugin;
mod processor;
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
        "processor" => processor::handle_processor_command(&args[2..]),
        "namespace" => namespace::handle_namespace_command(&args[2..]),
        "compute-engine" => compute_engine::handle_compute_engine_command(&args[2..]),
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
    println!("    flz <COMMAND>");
    println!();
    println!("COMMANDS:");
    println!("    agent          Agent management commands");
    println!("    processor      Processor management commands");
    println!("    namespace      Namespace management commands");
    println!("    compute-engine Compute engine management commands");
    println!("    plugin         Plugin management commands");
    println!("    help           Show this help message");
    println!();
    println!("Use 'flz <COMMAND> help' for more information on a specific command.");
    println!();
    println!("EXAMPLES:");
    println!("    flz agent start");
    println!("    flz processor start my_comp my_factory --namespace prod");
    println!("    flz plugin enable kafka_producer provider1 Xhosts=localhost:9092");
    println!("    flz namespace list --output json");
}
