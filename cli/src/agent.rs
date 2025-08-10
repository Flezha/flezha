use crate::utils::get_flezha_command;
use std::process::{exit, Command};

pub fn handle_agent_command(args: &[String]) {
    if args.is_empty() {
        eprintln!("Error: Invalid number of arguments. See `help` for more information.");
        exit(1);
    }

    match args[0].as_str() {
        "start" => handle_start(args),
        "stop" => handle_stop(args),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'start' or 'stop' only.",
                args[0]
            );
            exit(1);
        }
    }
}

fn handle_start(args: &[String]) {
    if args.len() < 1 {
        eprintln!("Error: Invalid number of arguments. Usage: flz agent start [--mode={{attached|daemon|attached_iex|daemon_iex}} | -m=]");
        exit(1);
    }

    let mut mode_arg = String::from("");

    // Process the arguments to find the mode parameter
    for arg in &args[1..] {
        if arg.starts_with("--mode=") {
            mode_arg = arg[7..].to_string();
        } else if arg.starts_with("-m=") {
            mode_arg = arg[3..].to_string();
        }
    }

    // Determine the command argument based on the mode
    let command_mode = match mode_arg.as_str() {
        "attached" => "start".to_string(),
        "daemon" => "daemon".to_string(),
        "attached_iex" => "start_iex".to_string(),
        "daemon_iex" => "daemon_iex".to_string(),
        "" => "start".to_string(),
        _ => {
            eprintln!("Error: Invalid mode '{}'. Use 'attached', 'daemon', 'attached_iex', or 'daemon_iex'.", mode_arg);
            exit(1);
        }
    };

    let command = get_flezha_command();

    println!("Starting flezha agent...");

    // Attempt to execute the external command.
    let status = Command::new(command).arg(command_mode).status();

    match status {
        Ok(status) if status.success() => println!("Agent started successfully."),
        Ok(status) => eprintln!("Unknown code returned: {}", status),
        Err(e) => eprintln!("Failed to start program: {}", e),
    }
}

fn handle_stop(_args: &[String]) {
    let command = get_flezha_command();

    println!("Stopping flezha agent...");

    // Attempt to execute the external command.
    let status = Command::new(command).arg("stop").status();

    match status {
        Ok(status) if status.success() => println!("Program stopped successfully."),
        Ok(status) => eprintln!("Unknown code returned: {}", status),
        Err(e) => eprintln!("Failed to stop program: {}", e),
    }
}