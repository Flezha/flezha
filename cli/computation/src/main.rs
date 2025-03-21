use std::env;
use std::process::{Command, exit};
use shared_lib::{get_namespace, get_json, get_aflecha_command};

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
    if args.len() < 3 {
        eprintln!("Error: Invalid number of arguments. Usage: {} start <pipeline_id> <factory_id> [--operator=<operator_id> |-o=<operator_id>]", args[0]);
        exit(1);
    }

    let pipeline_id = &args[2];
    let factory_id = &args[3];
    let variables = get_json(&args[4..], "--variables=", "-v=");

    let command = get_aflecha_command();
    let operator_id = get_namespace(&args[4..]);

    let rpc_command = format!(
        "Aflecha.Service.RpcCookbook.worker_start(\"{}\", \"{}\", \"{}\", \"{}\")", 
        operator_id,
        pipeline_id,
        factory_id,
        variables
    );

    println!("Starting worker...");

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

fn handle_stop(args: Vec<String>) {
    if args.len() < 1 {
        eprintln!("Error: Invalid number of arguments. Usage: {} stop <pipeline_id>", args[0]);
        exit(1);
    }

    let pipeline_id = &args[2];
    let operator_id = get_namespace(&args[3..]);

    let rpc_command = format!(
        "Aflecha.Service.RpcCookbook.worker_stop(\"{}\", \"{}\")", 
        operator_id,
        pipeline_id,
    );

    let command = get_aflecha_command();

    println!("Stopping worker...");

    // Attempt to execute the external command.
    let status = Command::new(command)
        .arg("rpc")
        .arg(rpc_command)
        .status();

    match status {
        Ok(status) if status.success() => println!("Worker stopped successfully."),
        Ok(_) => eprintln!("Unknown status returned."),
        Err(e) => eprintln!("Failed to stop worker: {}", e),
    }
}

fn handle_state(_args: Vec<String>) {

}
