use std::env;
use std::process::{Command, exit};
use shared_lib::{get_namespace, get_json, get_aflecha_command, get_parameter};

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
        eprintln!("Error: Invalid number of arguments. Usage: {} add <input|output> <worker_id> [--parameters=<json> |-p=<json>] [--operator=<operator_id> |-o=<operator_id>]", args[0]);
        exit(1);
    }

    let io_mode = &args[2];
    let worker_id = &args[3];
    let params = get_json(&args[4..], "--parameters=", "-p=");

    match params.as_str() {
        "" => {
            eprintln!("Error: Missing parameters. Usage: {} add <input|output> [--parameters=<json> |-p=<json>] [--operator=<operator_id> |-o=<operator_id>]", args[0]);
            exit(1);
        },
        _ => {}
    };

    let command = get_aflecha_command();
    let operator_id = get_namespace(&args[5..]);
    let opts: String = get_opts(&args[4..]);
    
    let rpc_command = format!(
        "Aflecha.Service.RpcCookbook.add_io(\"{}\", \"{}\",\"{}\", :{}, {})",
        operator_id,
        worker_id,
        params, 
        io_mode,
        opts
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


fn get_opts(slice: &[String]) -> String {
    let io_type = get_parameter(slice, "--io_type=", "-i=");

    let mut opts = String::from("[");

    if !io_type.is_empty() {
        opts = format!("handling_type: :{}", io_type);
    }

    opts.push_str("]");

    return opts
}