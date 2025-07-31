use shared_lib::get_flezha_command;
use std::env;
use std::process::{Command, exit};

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Error: Invalid number of arguments. See help for more information.");
        exit(1);
    }

    match args[1].as_str() {
        "list" => handle_list(),
        _ => {
            eprintln!("Error: Invalid argument '{}'. Use 'list' only.", args[1]);
            exit(1);
        }
    }
}

fn handle_list() {
    let command = get_flezha_command();
    let rpc_command = "Flezha.Service.CliManager.list_groups()";

    println!("Fetching groups...");

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
