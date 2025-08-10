mod agent;
mod namespace;
mod operator;
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
    println!("    processor  Processor management commands");
    println!("      start      Start a processor");
    println!("      stop       Stop a processor");
    println!("      state      Get processor state");
    println!("      list       List processors");
    println!("      info       Get processor info");
    println!("      io_listen  Listen to processor I/O");
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
    println!("    flz processor start my_comp my_factory --namespace prod");
    println!("    flz plugin enable kafka_producer provider1 Xhosts=localhost:9092");
    println!("    flz namespace list --output json");
}
