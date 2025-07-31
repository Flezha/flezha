use shared_lib::{extract_param, get_flezha_command, get_json, get_namespace};
use std::collections::HashMap;
use std::env;
use std::process::{exit, Command};

fn main() {
    let args: Vec<String> = env::args().collect();

    // Check if at least one argument is provided
    if args.len() < 2 {
        eprintln!("Error: Invalid number of arguments. See help for more information.");
        exit(1);
    }

    match args[1].as_str() {
        "start" => handle_start(args),
        "stop" => handle_stop(args),
        "state" => handle_state(args),
        "list" => handle_list(args),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'start', 'stop', 'state', or 'list' only.",
                args[1]
            );
            exit(1);
        }
    }
}

fn handle_start(args: Vec<String>) {
    let example_usage = format!("Usage: {} start <computation_id> <factory_id> [--group=<group_id> |-g=<group_id>] [--init_values=<json> |-iv=<json>]", args[0]);

    if args.len() < 3 {
        eprintln!(
            "{}",
            format!("Error: Invalid number of arguments.\n {}", example_usage)
        );
        exit(1);
    }

    let computation_id = &args[2];
    let factory_id = &args[3];
    let init_values = get_json(&args[4..], "--init_values=", "-iv=");

    let mut opts: HashMap<String, String> = HashMap::new();

    let spt = extract_param(&args[4..], "--snapshot-interval", "-si");
    if spt.is_some() {
        opts.insert("snapshot_interval".to_string(), spt.unwrap());
    }

    let opts_json = serde_json::to_string(&opts).unwrap();
    let command = get_flezha_command();
    let group_id = get_namespace(&args[4..]);

    let escaped_init_values = init_values.replace("\"", "\\\"");
    let escaped_opts_json = opts_json.replace("\"", "\\\"");

    let rpc_command = format!(
        "Flezha.Service.CliManager.computation_start(\"{}\", \"{}\", \"{}\", \"{}\", \"{}\")",
        group_id, computation_id, factory_id, escaped_init_values, escaped_opts_json,
    );

    println!("Starting computation...");

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

fn handle_stop(args: Vec<String>) {
    if args.len() < 1 {
        eprintln!(
            "Error: Invalid number of arguments. Usage: {} stop <computation_id>",
            args[0]
        );
        exit(1);
    }

    let computation_id = &args[2];
    let group_id = get_namespace(&args[3..]);

    let rpc_command = format!(
        "Flezha.Service.CliManager.computation_stop(\"{}\", \"{}\")",
        group_id, computation_id,
    );

    let command = get_flezha_command();

    println!("Stopping computation...");

    // Attempt to execute the external command.
    let status = Command::new(command).arg("rpc").arg(rpc_command).status();

    match status {
        Ok(status) if status.success() => println!("Computation stopped successfully."),
        Ok(_) => eprintln!("Unknown status returned."),
        Err(e) => eprintln!("Failed to stop computation: {}", e),
    }
}

fn handle_state(args: Vec<String>) {
    if args.len() < 1 {
        eprintln!(
            "Error: Invalid number of arguments. Usage: {} state <computation_id>",
            args[0]
        );
        exit(1);
    }

    let computation_id = &args[2];
    let group_id = get_namespace(&args[3..]);

    let rpc_command = format!(
        "Flezha.Service.CliManager.computation_state(\"{}\", \"{}\")",
        group_id, computation_id,
    );

    let command = get_flezha_command();

    let status = Command::new(command).arg("rpc").arg(rpc_command).status();

    match status {
        Ok(status) if status.success() => {}
        Ok(_) => eprintln!("Unknown status returned."),
        Err(e) => eprintln!("Failed to get computation state: {}", e),
    }
}

fn handle_list(args: Vec<String>) {
    let group_id = get_namespace(&args[2..]);

    let command = get_flezha_command();
    let rpc_command = format!(
        "Flezha.Service.CliManager.list_computations(\"{}\")",
        group_id
    );

    let display_group = if group_id.is_empty() {
        "default"
    } else {
        &group_id
    };
    println!("Fetching computations for group '{}'...", display_group);

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
