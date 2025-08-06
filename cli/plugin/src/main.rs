use shared_lib::{get_flezha_command, get_namespace, extract_flags_with_prefix, serialize_flags};
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
        "Usage: {} enable <plugin_id> [--Xkey=value]\nExample: {} enable computation_kafka_producer --Xinstance_id=kafka1 --Xgroup=default --Xcomputation_id=comp1 --Xtrigger=on_output --Xhosts=kafka:9092 --Xtopic=my_topic",
        args[0], args[0]
    );

    if args.len() < 3 {
        eprintln!(
            "{}",
            format!("Error: Invalid number of arguments.\n {}", example_usage)
        );
        exit(1);
    }

    let plugin_id = &args[2];
    let command = get_flezha_command();

    // Extract all X-prefixed flags 
    let all_flags = extract_flags_with_prefix(&args[3..], "X");
    
    // Separate type flags (IOBindingPlugin fields) from provider flags
    let type_field_names = ["instance_id", "group", "computation_id", "trigger", "plugin_group", "weight"];
    let (type_flags, provider_flags): (Vec<_>, Vec<_>) = all_flags.into_iter()
        .partition(|(key, _)| type_field_names.contains(&key.as_str()));
    
    let type_flags_json = serialize_flags(&type_flags);
    let provider_flags_json = serialize_flags(&provider_flags);

    let rpc_command = format!(
        "Flezha.Service.CliManager.enable_plugin_with_flags(\"{}\", \"{}\", \"{}\")",
        plugin_id,
        type_flags_json.replace("\"", "\\\""),
        provider_flags_json.replace("\"", "\\\"")
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
