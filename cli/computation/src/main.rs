use std::env;
use std::process::{Command, exit};

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
        eprintln!("Error: Invalid number of arguments. Usage: {} add <computation_id> <factory_id> [--operator=<operator_id> |-o=<operator_id>]", args[0]);
        exit(1);
    }

    let computation_id = &args[2];
    let factory_id = &args[3];
    let operator_id = get_operator_id(&args[4..]);
    let computation_struct = format!("%Aflecha.Computation.Builder{{id: \"{}\", factory_id: \"{}\"}}", computation_id, factory_id);
    let command = get_aflecha_command();

    let rpc_command = match operator_id.as_str() {
        "" => format!("Aflecha.add_computation({})", computation_struct),
        _ => format!("Aflecha.add_computation({}, {})", operator_id, computation_struct)
    };

    println!("Adding computation...");

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

fn handle_rem(args: Vec<String>) {
    if args.len() < 1 {
        eprintln!("Error: Invalid number of arguments. Usage: {} rem <computation_id>", args[0]);
        exit(1);
    }

    let computation_id = &args[2];
    let operator_id = get_operator_id(&args[3..]);

    let rpc_command = match operator_id.as_str() {
        "" => format!("Aflecha.rem_computation({})", computation_id),
        _ => format!("Aflecha.rem_computation({}, {})", operator_id, computation_id)
    };

    let command = get_aflecha_command();

    println!("Removing computation...");

    // Attempt to execute the external command.
    let status = Command::new(command)
        .arg("rpc")
        .arg(rpc_command)
        .status();

    match status {
        Ok(status) if status.success() => println!("Computation removed successfully."),
        Ok(_) => eprintln!("Unknown status returned."),
        Err(e) => eprintln!("Failed to remove computation: {}", e),
    }
}

fn get_aflecha_command() -> String {
    let extension = get_extension();
    let current_exe = env::current_exe().expect("Failed to get current executable path");
    let current_dir = current_exe.parent().expect("Failed to get current directory");
    
    current_dir
        .join(format!("aflecha{}", extension))
        .to_str()
        .expect("Failed to convert path to string")
        .to_string()
}

fn get_extension() -> String {
    if cfg!(target_os = "windows") {
        return ".bat".to_string();
    } else {
        return "".to_string();
    }
}

fn get_operator_id(slice: &[String]) -> String {
    let mut operator_id = String::from("");

    // Process the arguments to find the operator_id
    for arg in slice {
        if arg.starts_with("--operator=") {
            operator_id = arg[10..].to_string();
        } else if arg.starts_with("-o=") {
            operator_id = arg[3..].to_string();
        }
    }

    return operator_id;
}
