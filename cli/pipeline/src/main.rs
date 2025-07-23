use std::collections::HashMap;
use std::env;
use std::process::{Command, exit};
use shared_lib::{extract_param, get_flezha_command, get_json, get_namespace};

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
        _ => {
            eprintln!("Error: Invalid argument '{}'. Use 'start' or 'stop' only.", args[1]);
            exit(1);
        }
    }
}

fn handle_start(args: Vec<String>) {
    let example_usage = format!("Usage: {} start <pipeline_id> <factory_id> [--group=<group_id> |-g=<group_id>] [--init_values=<json> |-iv=<json>]", args[0]);

    if args.len() < 3 {
        eprintln!("{}", format!("Error: Invalid number of arguments.\n {}",  example_usage));
        exit(1);
    }

    let pipeline_id = &args[2];
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

    let rpc_command = format!(
        "Flezha.Service.RpcCookbook.pipeline_start(\"{}\", \"{}\", \"{}\", \"{}\", \"{}\")", 
        group_id,
        pipeline_id,
        factory_id,
        init_values,
        opts_json,
    );

    println!("Starting pipeline...");

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
        eprintln!("Error: Invalid number of arguments. Usage: {} stop <pipeline_id>", args[0]);
        exit(1);
    }

    let pipeline_id = &args[2];
    let group_id = get_namespace(&args[3..]);

    let rpc_command = format!(
        "Flezha.Service.RpcCookbook.pipeline_stop(\"{}\", \"{}\")", 
        group_id,
        pipeline_id,
    );

    let command = get_flezha_command();

    println!("Stopping pipeline...");

    // Attempt to execute the external command.
    let status = Command::new(command)
        .arg("rpc")
        .arg(rpc_command)
        .status();

    match status {
        Ok(status) if status.success() => println!("Computation stopped successfully."),
        Ok(_) => eprintln!("Unknown status returned."),
        Err(e) => eprintln!("Failed to stop pipeline: {}", e),
    }
}

fn handle_state(args: Vec<String>) {
    if args.len() < 1 {
        eprintln!("Error: Invalid number of arguments. Usage: {} state <pipeline_id>", args[0]);
        exit(1);
    }

    let pipeline_id = &args[2];
    let group_id = get_namespace(&args[3..]);

    let rpc_command = format!(
        "Flezha.Service.RpcCookbook.pipeline_state(\"{}\", \"{}\")", 
        group_id,
        pipeline_id,
    );

    let command = get_flezha_command();

    let status = Command::new(command)
        .arg("rpc")
        .arg(rpc_command)
        .status();

    match status {
        Ok(status) if status.success() => {},
        Ok(_) => eprintln!("Unknown status returned."),
        Err(e) => eprintln!("Failed to get pipeline state: {}", e),
    }
}
