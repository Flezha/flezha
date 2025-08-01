use shared_lib::{get_flezha_command, get_output_format};
use std::env;
use std::process::{Command, exit};

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Error: Invalid number of arguments. See help for more information.");
        exit(1);
    }

    match args[1].as_str() {
        "list" => handle_list(args),
        _ => {
            eprintln!("Error: Invalid argument '{}'. Use 'list' only.", args[1]);
            exit(1);
        }
    }
}

fn handle_list(args: Vec<String>) {
    let output_format = get_output_format(&args[2..]);

    let command = get_flezha_command();
    let rpc_command = format!(
        "Flezha.Service.CliManager.list_groups(\"{}\")",
        output_format
    );

    // Only show progress message for text output
    if output_format == "text" {
        println!("Fetching groups...");
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
