use std::env;
use std::process::{Command, exit};
use shared_lib::{get_operator_id, get_json, get_aflecha_command};

fn main() {
    let args: Vec<String> = env::args().collect();

    match args[1].as_str() {
        "add" => handle_add(args),
        "rem" => handle_rem(args),
        _ => {
            eprintln!("Error: Invalid argument '{}'. Use 'add' or 'rem' only.", args[1]);
            exit(1);
        }
    }
}

fn handle_add(args: Vec<String>) {
    if args.len() < 4 {
        eprintln!("Error: Invalid number of arguments. Usage: {} add <input|output> <computation_id> [--parameters=<json> |-p=<json>] [--operator=<operator_id> |-o=<operator_id>]", args[0]);
        exit(1);
    }

    let io_mode = &args[2];
    let computation_id = &args[3];
    let params = get_json(&args[4..], "--parameters=", "-p=");

    match params.as_str() {
        "" => {
            eprintln!("Error: Missing parameters. Usage: {} add <input|output> [--parameters=<json> |-p=<json>] [--operator=<operator_id> |-o=<operator_id>]", args[0]);
            exit(1);
        },
        _ => {}
    };

    let command = get_aflecha_command();
    let operator_id = get_operator_id(&args[5..]);
    
    let rpc_command = format!(
        "Aflecha.Service.RpcCookbook.add_io(\"{}\", \"{}\",\"{}\", \"{}\")",
        operator_id,
        computation_id,
        params, 
        io_mode
    );

    println!("Adding io...");

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

fn handle_rem(_args: Vec<String>) {
    // TODO: implement
}