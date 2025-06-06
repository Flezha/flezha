use std::env;
use std::process::{Command, exit};
use shared_lib::{get_json, get_flezha_command};

fn main() {
    let args: Vec<String> = env::args().collect();

    match args[1].as_str() {
        "enable" => handle_enable(args),
        "disable" => handle_disable(args),
        _ => {
            eprintln!("Error: Invalid argument '{}'. Use 'enable' or 'disable' only.", args[1]);
            exit(1);
        }
    }
}

fn handle_enable(args: Vec<String>) {
    let example_usage = format!("Usage: {} enable <plugin_id> [--settings=<json> |-s=<json>]", args[0]);

    if args.len() < 3 {
        eprintln!("{}", format!("Error: Invalid number of arguments.\n {}",  example_usage));
        exit(1);
    }

    let plugin_id = &args[2];
    let pluggin_settings = get_json(&args[3..], "--settings=", "-s=");
    let command = get_flezha_command();
    
    let rpc_command = format!(
        "Flezha.Service.RpcCookbook.enable_plugin(\"{}\", \"{}\")",
        plugin_id,
        pluggin_settings
    );

    println!("Enabling plugin...");

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

fn handle_disable(_args: Vec<String>) {
    // TODO: implement
}
