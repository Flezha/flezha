use std::env;

pub fn get_flezha_command() -> String {
    let extension = get_extension();
    let current_exe = env::current_exe().expect("Failed to get current executable path");
    let current_dir = current_exe.parent().expect("Failed to get current directory");
    
    current_dir
        .join(format!("flezha{}", extension))
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
    let mut param = String::from("");

    // Process the arguments to find the group_id
    for arg in slice {
        if arg.starts_with(long_key) {
            param = arg[10..].to_string();
        } else if arg.starts_with(short_key) {
            param = arg[3..].to_string();
        }
    }

    param
}

pub fn extract_param(args: &[String], long_key: &str, short_key: &str) -> Option<String> {
    for arg in args {
        if let Some(value) = arg.strip_prefix(&format!("{}=", long_key)) {
            return Some(value.to_string());
        }
        if let Some(value) = arg.strip_prefix(&format!("{}=", short_key)) {
            return Some(value.to_string());
        }
    }
    
    None
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

    json
}