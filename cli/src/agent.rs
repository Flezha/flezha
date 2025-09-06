use crate::utils::{get_flezha_command, get_parameter};
use std::process::{exit, Command};

pub fn handle_agent_command(args: &[String]) {
    if args.is_empty() {
        eprintln!("Error: Invalid number of arguments. See `help` for more information.");
        exit(1);
    }

    match args[0].as_str() {
        "start" => handle_start(args),
        "stop" => handle_stop(args),
        "help" | "--help" | "-h" => print_agent_help(),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'start', 'stop', or 'help'.",
                args[0]
            );
            exit(1);
        }
    }
}

fn handle_start(args: &[String]) {
    // Check for help flags first
    for arg in &args[1..] {
        if arg == "--help" || arg == "-h" || arg == "help" {
            print_start_help();
            return;
        }
    }

    if args.len() < 1 {
        eprintln!("Error: Invalid number of arguments. Usage: flz agent start [--mode {{attached|daemon|attached_iex|daemon_iex}}]");
        exit(1);
    }

    let mode_arg = get_parameter(&args[1..], "--mode", "-m");

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

fn handle_stop(args: &[String]) {
    // Check for help flags first
    for arg in &args[1..] {
        if arg == "--help" || arg == "-h" || arg == "help" {
            print_stop_help();
            return;
        }
    }

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

fn print_agent_help() {
    println!("flz agent - Agent management commands");
    println!();
    println!("USAGE:");
    println!("    flz agent <COMMAND>");
    println!();
    println!("COMMANDS:");
    println!("    start    Start the flezha agent");
    println!("    stop     Stop the flezha agent");
    println!("    help     Show this help message");
    println!();
    println!("Use 'flz agent <COMMAND> --help' for more information on a specific command.");
}

fn print_start_help() {
    println!("flz agent start - Start the flezha agent");
    println!();
    println!("USAGE:");
    println!("    flz agent start [OPTIONS]");
    println!();
    println!("OPTIONS:");
    println!("    --mode <MODE>     Agent start mode");
    println!("    -m <MODE>         Short form of --mode");
    println!();
    println!("AVAILABLE MODES:");
    println!("    attached          Start in attached mode (default)");
    println!("    daemon            Start as daemon");
    println!("    attached_iex      Start in attached IEx mode");
    println!("    daemon_iex        Start as daemon with IEx");
    println!();
    println!("EXAMPLES:");
    println!("    flz agent start");
    println!("    flz agent start --mode daemon");
    println!("    flz agent start -m attached_iex");
}

fn print_stop_help() {
    println!("flz agent stop - Stop the flezha agent");
    println!();
    println!("USAGE:");
    println!("    flz agent stop");
    println!();
    println!("DESCRIPTION:");
    println!("    Stops the currently running flezha agent.");
}
