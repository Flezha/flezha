use std::env;
use std::process::{Command, exit};
use shared_lib::{get_operator_id, get_json, get_aflecha_command};

fn main() {
    let args: Vec<String> = env::args().collect();

    // Check if at least one argument is provided
    if args.len() < 2 {
        eprintln!("Error: Invalid number of arguments. See help for more information.");
        exit(1);
    }

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
    if args.len() < 3 {
        eprintln!("Error: Invalid number of arguments. Usage: {} add <input|output> [--parameters=<json> |-p=<json>] [--operator=<operator_id> |-o=<operator_id>]", args[0]);
        exit(1);
    }

    let io_mode = &args[2];
    let params = get_json(&args[3..], "--parameters=", "-p=");

    match params.as_str() {
        "" => {
            eprintln!("Error: Missing parameters. Usage: {} add <input|output> [--parameters=<json> |-p=<json>] [--operator=<operator_id> |-o=<operator_id>]", args[0]);
            exit(1);
        },
        _ => {}
    };

    let command = get_aflecha_command();
    let operator_id = get_operator_id(&args[4..]);
    let mut rpc_command_params = "".to_string();

    if operator_id.as_str() != "" {
        rpc_command_params = format!("\"{}\", ", operator_id);
    }

    let rpc_command = format!(
        "{{c_id, io_struct}} = Aflecha.Service.Payloader.extract_io_settings(\"{}\"); Aflecha.add_{}(\"{}\", c_id, io_struct))",
        params, 
        io_mode, 
        rpc_command_params
    );

    println!("{}", rpc_command);

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