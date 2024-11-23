use std::env;
use std::process::{Command, exit};

fn main() {
    let args: Vec<String> = env::args().collect();

    // Check if at least one argument is provided
    if args.len() < 2 {
        eprintln!("Error: Invalid number of arguments. Usage: {} <start|stop> [--mode={{attached|daemon|attached_iex|daemon_iex}} | -m=]", args[0]);
        exit(1);
    }

    // Determine the extension based on the operating system
    let extension = if cfg!(target_os = "windows") { ".bat" } else { "" };
    let mut mode_arg = String::from("");

    // Process the arguments to find the mode parameter
    for arg in &args[2..] {
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

    let command = format!("aflecha{}", extension);

    match args[1].as_str() {
        "start" => {
            println!("Starting aflecha agent...");

            // Attempt to execute the external command.
            let status = Command::new(command)
                .arg(command_mode)
                .status();

            match status {
                Ok(status) if status.success() => println!("Program started successfully."),
                Ok(status) => eprintln!("Program exited with code: {}", status),
                Err(e) => eprintln!("Failed to start program: {}", e),
            }
        }
        "stop" => {
            println!("Stopping aflecha agent...");

            // Attempt to execute the external command.
            let status = Command::new(command).status();

            match status {
                Ok(status) if status.success() => println!("Program stopped successfully."),
                Ok(status) => eprintln!("Program exited with code: {}", status),
                Err(e) => eprintln!("Failed to stop program: {}", e),
            }
        }
        _ => {
            eprintln!("Error: Invalid argument '{}'. Use 'start' or 'stop' only.", args[1]);
            exit(1);
        }
    }
}
