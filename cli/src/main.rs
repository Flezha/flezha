mod audit;
mod boot;
mod cluster;
mod controller;
mod dlq;
mod namespace;
mod observe;
mod operative;
mod plugin;
mod reset;
mod runner;
mod state;
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
        "controller" => controller::handle_controller_command(&args[2..]),
        "cluster" => cluster::handle_cluster_command(&args[2..]),
        "operative" => operative::handle_operative_command(&args[2..]),
        "namespace" => namespace::handle_namespace_command(&args[2..]),
        "runner" => runner::handle_runner_command(&args[2..]),
        "plugin" => plugin::handle_plugin_command(&args[2..]),
        "boot" => boot::handle_boot_command(&args[2..]),
        "state" => state::handle_state_command(&args[2..]),
        "reset" => reset::handle_reset_command(&args[2..]),
        "observe" => observe::handle_observe_command(&args[2..]),
        "audit" => audit::handle_audit_command(&args[2..]),
        "dlq" => dlq::handle_dlq_command(&args[2..]),
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
    println!("    controller     Controller management commands");
    println!("    cluster        Controller cluster status (nodes, leader, operative health)");
    println!("    operative      Operative management commands");
    println!("    namespace      Namespace management commands");
    println!("    runner       Runner management commands");
    println!("    plugin         Plugin management commands");
    println!("    boot           Boot/recovery status, await, and legacy snapshot import");
    println!("    state          Per-Segment state queries (time-travel, retention, list)");
    println!("    reset          Clear Controller durable runtime data on disk (controller must be stopped)");
    println!("    observe        Observability foundation inspection (sources, sinks, tail)");
    println!("    audit          Durable audit log inspection (list, read, tail, stats, rotate)");
    println!("    dlq            Dead-letter queue: inspect, replay, or drop parked payloads");
    println!("    help           Show this help message");
    println!();
    println!("Use 'flz <COMMAND> help' for more information on a specific command.");
    println!();
    println!("EXAMPLES:");
    println!("    flz controller start");
    println!("    flz operative start my_operative my_factory --namespace prod");
    println!("    flz plugin install /path/to/my_plugin-1.0.0.fpz");
    println!("    flz plugin list --installed");
    println!("    flz plugin enable kafka_producer provider1 Xhosts=localhost:9092");
    println!("    flz namespace list --output json");
    println!("    flz boot status");
    println!("    flz boot await --timeout 60");
}
