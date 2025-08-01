use shared_lib::{get_flezha_command, get_json, get_namespace};
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
        "Usage: {} enable <plugin_id> [--settings <json> | -s <json>]",
        args[0]
    );

    if args.len() < 3 {
        eprintln!(
            "{}",
            format!("Error: Invalid number of arguments.\n {}", example_usage)
        );
        exit(1);
    }

    let plugin_id = &args[2];
    let plugin_settings = get_json(&args[3..], "--settings", "-s");
    let command = get_flezha_command();

    // Clean the JSON string - remove newlines and extra whitespace that cause parsing issues
    let cleaned_settings = plugin_settings
        .replace("\n", " ")
        .replace("\r", " ")
        .split_whitespace()
        .collect::<Vec<&str>>()
        .join(" ");

    // Remove trailing commas that make JSON invalid
    let fixed_settings = cleaned_settings.replace(", }", " }").replace(",}", "}");

    let escaped_settings = fixed_settings.replace("\"", "\\\"");

    let rpc_command = format!(
        "Flezha.Service.CliManager.enable_plugin(\"{}\", \"{}\")",
        plugin_id, escaped_settings
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
