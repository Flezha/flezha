use crate::utils::{extract_param, get_flezha_command, get_json, get_namespace, get_output_format};
use std::collections::HashMap;
use std::process::{exit, Command};

pub fn handle_computation_command(args: &[String]) {
    if args.is_empty() {
        eprintln!("Error: Invalid number of arguments. See help for more information.");
        exit(1);
    }

    match args[0].as_str() {
        "start" => handle_start(args),
        "stop" => handle_stop(args),
        "state" => handle_state(args),
        "list" => handle_list(args),
        "info" => handle_info(args),
        "io_listen" => handle_io_listen(args),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'start', 'stop', 'state', 'list', 'info', or 'io_listen' only.",
                args[0]
            );
            exit(1);
        }
    }
}

fn handle_start(args: &[String]) {
    let example_usage = "Usage: flz computation start <computation_id> <factory_id> [--namespace <namespace> | -n <namespace>] [--init_values <json> | -iv <json>]";

    if args.len() < 3 {
        eprintln!("Error: Invalid number of arguments.\n {}", example_usage);
        exit(1);
    }

    let computation_id = &args[1];
    let factory_id = &args[2];
    let init_values = get_json(&args[3..], "--init_values", "-iv");

    let mut opts: HashMap<String, String> = HashMap::new();

    let spt = extract_param(&args[3..], "--snapshot-interval", "-si");
    if spt.is_some() {
        opts.insert("snapshot_interval".to_string(), spt.unwrap());
    }

    let opts_json = serde_json::to_string(&opts).unwrap();
    let command = get_flezha_command();
    let namespace = get_namespace(&args[3..]);

    let escaped_init_values = init_values.replace("\"", "\\\"");
    let escaped_opts_json = opts_json.replace("\"", "\\\"");

    let rpc_command = format!(
        "Flezha.Service.CliManager.computation_start(\"{}\", \"{}\", \"{}\", \"{}\", \"{}\")",
        namespace, computation_id, factory_id, escaped_init_values, escaped_opts_json,
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

fn handle_stop(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Error: Invalid number of arguments. Usage: flz computation stop <computation_id>");
        exit(1);
    }

    let computation_id = &args[1];
    let namespace = get_namespace(&args[2..]);

    let rpc_command = format!(
        "Flezha.Service.CliManager.computation_stop(\"{}\", \"{}\")",
        namespace, computation_id,
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

fn handle_state(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Error: Invalid number of arguments. Usage: flz computation state <computation_id>");
        exit(1);
    }

    let computation_id = &args[1];
    let namespace = get_namespace(&args[2..]);

    let rpc_command = format!(
        "Flezha.Service.CliManager.computation_state(\"{}\", \"{}\")",
        namespace, computation_id,
    );

    let command = get_flezha_command();

    let status = Command::new(command).arg("rpc").arg(rpc_command).status();

    match status {
        Ok(status) if status.success() => {}
        Ok(_) => eprintln!("Unknown status returned."),
        Err(e) => eprintln!("Failed to get computation state: {}", e),
    }
}

fn handle_list(args: &[String]) {
    let namespace = get_namespace(&args[1..]);
    let output_format = get_output_format(&args[1..]);

    let command = get_flezha_command();
    let rpc_command = format!(
        "Flezha.Service.CliManager.list_computations(\"{}\", \"{}\")",
        namespace, output_format
    );

    // Only show progress message for text output
    if output_format == "text" {
        let display_namespace = if namespace.is_empty() {
            "default"
        } else {
            &namespace
        };
        println!(
            "Fetching computations for namespace '{}'...",
            display_namespace
        );
    }

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

fn handle_info(args: &[String]) {
    let namespace = get_namespace(&args[1..]);
    let output_format = get_output_format(&args[1..]);

    let command = get_flezha_command();
    let rpc_command = format!(
        "Flezha.Service.CliManager.computation_info(\"{}\", \"{}\")",
        namespace, output_format
    );

    // Only show progress message for text output
    if output_format == "text" {
        let display_namespace = if namespace.is_empty() {
            "default"
        } else {
            &namespace
        };
        println!(
            "Fetching computation information for namespace '{}'...",
            display_namespace
        );
    }

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

fn handle_io_listen(args: &[String]) {
    let example_usage = "Usage: flz computation io_listen input|output <computation_id> [--namespace <namespace> | -n <namespace>]";

    if args.len() < 3 {
        eprintln!("Error: Invalid number of arguments.\n {}", example_usage);
        exit(1);
    }

    let io_type = &args[1];
    let computation_id = &args[2];

    // Validate io_type
    if io_type != "input" && io_type != "output" {
        eprintln!(
            "Error: Invalid IO type '{}'. Use 'input' or 'output' only.",
            io_type
        );
        exit(1);
    }

    let namespace = get_namespace(&args[3..]);
    let command = get_flezha_command();

    let rpc_command = format!(
        "Flezha.Service.CliManager.computation_listen(\"{}\", \"{}\", \"{}\")",
        namespace, computation_id, io_type
    );

    println!(
        "Starting IO listener for computation '{}' ({} mode)...",
        computation_id, io_type
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