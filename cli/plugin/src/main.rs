use shared_lib::{get_flezha_command, get_namespace, extract_x_flags, extract_opts_with_default_group, serialize_flags};
use std::env;
use std::process::{exit, Command};

fn main() {
    let args: Vec<String> = env::args().collect();

    match args[1].as_str() {
        "enable" => handle_enable(args),
        "disable" => handle_disable(args),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'enable' or 'disable' only.",
                args[1]
            );
            exit(1);
        }
    }
}

fn handle_enable(args: Vec<String>) {
    let example_usage = format!(
        "Usage: {} enable <plugin_id> <provider_id> [Xkey=value] [--option value]\nExample: {} enable computation_kafka_producer my_provider Xhosts=kafka:9092 Xtopic=my_topic --group mygroup",
        args[0], args[0]
    );

    if args.len() < 4 {
        eprintln!(
            "{}",
            format!("Error: Invalid number of arguments.\n {}", example_usage)
        );
        exit(1);
    }

    let plugin_id = &args[2];
    let provider_id = &args[3];
    let command = get_flezha_command();

    // Extract X-prefixed flags (plugin settings)
    let x_flags = extract_x_flags(&args[4..]);
    let x_flags_json = serialize_flags(&x_flags);
    
    // Extract opts flags (CLI options with default group)
    let opts_flags = extract_opts_with_default_group(&args[4..]);
    let opts_json = serialize_flags(&opts_flags);

    let rpc_command = format!(
        "Flezha.Service.CliManager.enable_plugin(\"{}\", \"{}\", \"{}\", \"{}\")",
        plugin_id,
        provider_id,
        x_flags_json.replace("\"", "\\\""),
        opts_json.replace("\"", "\\\"")
    );

    println!("Enabling plugin...");

    // Attempt to execute the external command.
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

fn handle_disable(args: Vec<String>) {
    let example_usage = format!(
        "Usage: {} disable <computation_id> <plugin_id> [--group <group_id> | -g <group_id>]",
        args[0]
    );

    if args.len() < 4 {
        eprintln!(
            "{}",
            format!("Error: Invalid number of arguments.\n {}", example_usage)
        );
        exit(1);
    }

    let computation_id = &args[2];
    let plugin_id = &args[3];
    let group_id = get_namespace(&args[4..]);
    let command = get_flezha_command();

    let rpc_command = format!(
        "Flezha.Service.CliManager.disable_plugin(\"{}\", \"{}\", \"{}\")",
        group_id, computation_id, plugin_id
    );

    println!("Disabling plugin...");

    // Attempt to execute the external command.
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
