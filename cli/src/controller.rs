use crate::utils::{get_flezha_command, get_parameter};
use std::process::{exit, Command};

pub fn handle_controller_command(args: &[String]) {
    if args.is_empty() {
        eprintln!("Error: Invalid number of arguments. See `help` for more information.");
        exit(1);
    }

    match args[0].as_str() {
        "start" => handle_start(args),
        "stop" => handle_stop(args),
        "help" | "--help" | "-h" => print_controller_help(),
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
    for arg in &args[1..] {
        if arg == "--help" || arg == "-h" || arg == "help" {
            print_start_help();
            return;
        }
    }

    if args.is_empty() {
        eprintln!("Error: Invalid number of arguments. Usage: flz controller start [--mode {{attached|daemon|attached_iex|daemon_iex}}]");
        exit(1);
    }

    let mode_arg = get_parameter(&args[1..], "--mode", "-m");

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

    println!("Starting flezha controller...");

    let status = Command::new(command).arg(command_mode).status();

    match status {
        Ok(status) if status.success() => println!("Controller started successfully."),
        Ok(status) => eprintln!("Unknown code returned: {}", status),
        Err(e) => eprintln!("Failed to start program: {}", e),
    }
}

fn handle_stop(args: &[String]) {
    for arg in &args[1..] {
        if arg == "--help" || arg == "-h" || arg == "help" {
            print_stop_help();
            return;
        }
    }

    let command = get_flezha_command();

    println!("Stopping flezha controller...");

    let status = Command::new(command).arg("stop").status();

    match status {
        Ok(status) if status.success() => println!("Program stopped successfully."),
        Ok(status) => eprintln!("Unknown code returned: {}", status),
        Err(e) => eprintln!("Failed to stop program: {}", e),
    }
}

fn print_controller_help() {
    println!("flz controller - Controller management commands");
    println!();
    println!("USAGE:");
    println!("    flz controller <COMMAND>");
    println!();
    println!("COMMANDS:");
    println!("    start    Start the flezha controller");
    println!("    stop     Stop the flezha controller");
    println!("    help     Show this help message");
    println!();
    println!("Use 'flz controller <COMMAND> --help' for more information on a specific command.");
}

fn print_start_help() {
    println!("flz controller start - Start the flezha controller");
    println!();
    println!("USAGE:");
    println!("    flz controller start [OPTIONS]");
    println!();
    println!("OPTIONS:");
    println!("    --mode <MODE>     Controller start mode");
    println!("    -m <MODE>         Short form of --mode");
    println!();
    println!("AVAILABLE MODES:");
    println!("    attached          Start in attached mode (default)");
    println!("    daemon            Start as daemon");
    println!("    attached_iex      Start in attached IEx mode");
    println!("    daemon_iex        Start as daemon with IEx");
    println!();
    println!("EXAMPLES:");
    println!("    flz controller start");
    println!("    flz controller start --mode daemon");
    println!("    flz controller start -m attached_iex");
}

fn print_stop_help() {
    println!("flz controller stop - Stop the flezha controller");
    println!();
    println!("USAGE:");
    println!("    flz controller stop");
    println!();
    println!("DESCRIPTION:");
    println!("    Stops the currently running flezha controller.");
}
