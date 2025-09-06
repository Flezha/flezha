use crate::utils::{extract_param, get_flezha_command, get_json, get_namespace, get_output_format};
use std::collections::HashMap;
use std::process::{exit, Command};

pub fn handle_processor_command(args: &[String]) {
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
        "help" | "--help" | "-h" => print_processor_help(),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'start', 'stop', 'state', 'list', 'info', 'io_listen', or 'help'.",
                args[0]
            );
            exit(1);
        }
    }
}

fn handle_start(args: &[String]) {
    let example_usage = "Usage: flz processor start <processor_id> <factory_id> [--namespace <namespace> | -n <namespace>] [--init_values <json> | -iv <json>]";

    if args.len() < 3 {
        eprintln!("Error: Invalid number of arguments.\n {}", example_usage);
        exit(1);
    }

    let processor_id = &args[1];
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
        "Flezha.Service.CliManager.processor_start(\"{}\", \"{}\", \"{}\", \"{}\", \"{}\")",
        namespace, processor_id, factory_id, escaped_init_values, escaped_opts_json,
    );

    println!("Starting processor...");

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
        eprintln!("Error: Invalid number of arguments. Usage: flz processor stop <processor_id>");
        exit(1);
    }

    let processor_id = &args[1];
    let namespace = get_namespace(&args[2..]);

    let rpc_command = format!(
        "Flezha.Service.CliManager.processor_stop(\"{}\", \"{}\")",
        namespace, processor_id,
    );

    let command = get_flezha_command();

    println!("Stopping processor...");

    // Attempt to execute the external command.
    let status = Command::new(command).arg("rpc").arg(rpc_command).status();

    match status {
        Ok(status) if status.success() => println!("Processor stopped successfully."),
        Ok(_) => eprintln!("Unknown status returned."),
        Err(e) => eprintln!("Failed to stop processor: {}", e),
    }
}

fn handle_state(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Error: Invalid number of arguments. Usage: flz processor state <processor_id>");
        exit(1);
    }

    let processor_id = &args[1];
    let namespace = get_namespace(&args[2..]);

    let rpc_command = format!(
        "Flezha.Service.CliManager.processor_state(\"{}\", \"{}\")",
        namespace, processor_id,
    );

    let command = get_flezha_command();

    let status = Command::new(command).arg("rpc").arg(rpc_command).status();

    match status {
        Ok(status) if status.success() => {}
        Ok(_) => eprintln!("Unknown status returned."),
        Err(e) => eprintln!("Failed to get processor state: {}", e),
    }
}

fn handle_list(args: &[String]) {
    let namespace = get_namespace(&args[1..]);
    let output_format = get_output_format(&args[1..]);

    let command = get_flezha_command();
    let rpc_command = format!(
        "Flezha.Service.CliManager.list_processors(\"{}\", \"{}\")",
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
            "Fetching processors for namespace '{}'...",
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
        "Flezha.Service.CliManager.processor_info(\"{}\", \"{}\")",
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
            "Fetching processor information for namespace '{}'...",
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
    let example_usage = "Usage: flz processor io_listen input|output <processor_id> [--namespace <namespace> | -n <namespace>]";

    if args.len() < 3 {
        eprintln!("Error: Invalid number of arguments.\n {}", example_usage);
        exit(1);
    }

    let io_type = &args[1];
    let processor_id = &args[2];

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
        "Flezha.Service.CliManager.processor_listen(\"{}\", \"{}\", \"{}\")",
        namespace, processor_id, io_type
    );

    println!(
        "Starting IO listener for processor '{}' ({} mode)...",
        processor_id, io_type
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

fn print_processor_help() {
    println!("flz processor - Processor management commands");
    println!();
    println!("USAGE:");
    println!("    flz processor <COMMAND>");
    println!();
    println!("COMMANDS:");
    println!("    start       Start a processor");
    println!("    stop        Stop a processor");
    println!("    state       Get processor state");
    println!("    list        List processors");
    println!("    info        Get processor info");
    println!("    io_listen   Listen to processor I/O");
    println!("    help        Show this help message");
    println!();
    println!("Use 'flz processor <COMMAND> --help' for more information on a specific command.");
}
