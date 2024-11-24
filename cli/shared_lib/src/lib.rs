use std::env;

pub fn get_aflecha_command() -> String {
    let extension = get_extension();
    let current_exe = env::current_exe().expect("Failed to get current executable path");
    let current_dir = current_exe.parent().expect("Failed to get current directory");
    
    current_dir
        .join(format!("aflecha{}", extension))
        .to_str()
        .expect("Failed to convert path to string")
        .to_string()
}

pub fn get_extension() -> String {
    if cfg!(target_os = "windows") {
        return ".bat".to_string();
    } else {
        return "".to_string();
    }
}

pub fn get_operator_id(slice: &[String]) -> String {
    let mut operator_id = String::from("");

    // Process the arguments to find the operator_id
    for arg in slice {
        if arg.starts_with("--operator=") {
            operator_id = arg[10..].to_string();
        } else if arg.starts_with("-o=") {
            operator_id = arg[3..].to_string();
        }
    }

    operator_id
}

pub fn get_variables(slice: &[String]) -> String {
    let mut variables = String::from("");

    // Process the arguments to find the variables
    for arg in slice {
        if arg.starts_with("--variables=") {
            variables = arg[12..].to_string();
        } else if arg.starts_with("-v=") {
            variables = arg[3..].to_string();
        }
    }

    variables.replace("\"", "\\\"")
}