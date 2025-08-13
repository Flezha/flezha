use crate::utils::{
    extract_opts_with_default_namespace, extract_x_flags, get_flezha_command, serialize_flags,
};
use std::process::{exit, Command};

pub fn handle_plugin_command(args: &[String]) {
    if args.is_empty() {
        eprintln!("Error: No plugin command specified.");
        exit(1);
    }

    match args[0].as_str() {
        "enable" => handle_enable(args),
        "disable" => handle_disable(args),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'enable' or 'disable' only.",
                args[0]
            );
            exit(1);
        }
    }
}

fn handle_enable(args: &[String]) {
    let example_usage = "Usage: flz plugin enable <plugin_id> <provider_id> [Xkey=value] [--option value]\nExample: flz plugin enable processor_kafka_producer my_provider Xhosts=kafka:9092 Xtopic=my_topic --namespace mynamespace";

    if args.len() < 3 {
        eprintln!("Error: Invalid number of arguments.\n {}", example_usage);
        exit(1);
    }

    let plugin_id = &args[1];
    let provider_id = &args[2];
    let command = get_flezha_command();

    // Extract X-prefixed flags (plugin settings)
    let x_flags = extract_x_flags(&args[3..]);
    let x_flags_json = serialize_flags(&x_flags);

    // Extract opts flags (CLI options with default namespace)
    let opts_flags = extract_opts_with_default_namespace(&args[3..]);
    let opts_json = serialize_flags(&opts_flags);

    let rpc_command = format!(
        "Flezha.Service.CliManager.enable_plugin(\"{}\", \"{}\", {}, {})",
        plugin_id,
        provider_id,
        serde_json::to_string(&x_flags_json).unwrap(),
        serde_json::to_string(&opts_json).unwrap()
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

fn handle_disable(args: &[String]) {
    let example_usage = "Usage: flz plugin disable <plugin_id>";

    if args.len() < 2 {
        eprintln!("Error: Invalid number of arguments.\n {}", example_usage);
        exit(1);
    }

    let plugin_id = &args[1];
    let command = get_flezha_command();

    let rpc_command = format!(
        "Flezha.Service.CliManager.disable_plugin(\"{}\")",
        plugin_id
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
