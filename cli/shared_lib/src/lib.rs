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

pub fn get_namespace(slice: &[String]) -> String {
    get_parameter(slice, "--group=", "-g=")
}

pub fn get_parameter(slice: &[String], long_key: &str, short_key: &str) -> String {
    let mut group_id = String::from("");

    // Process the arguments to find the group_id
    for arg in slice {
        if arg.starts_with(long_key) {
            group_id = arg[10..].to_string();
        } else if arg.starts_with(short_key) {
            group_id = arg[3..].to_string();
        }
    }

    group_id
}

pub fn get_json(slice: &[String], long_key: &str, short_key: &str) -> String {
    let mut json = String::from("");

    for arg in slice {
        if arg.starts_with(long_key) {
            json = arg[long_key.len()..].to_string();
        } else if arg.starts_with(short_key) {
            json = arg[short_key.len()..].to_string();
        }
    }

    if json.is_empty() {
        return "{}".to_string();
    }

    json.replace("\"", "\\\"")
}