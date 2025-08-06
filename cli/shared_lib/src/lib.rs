use std::env;

pub fn get_flezha_command() -> String {
    let extension = get_extension();
    let current_exe = env::current_exe().expect("Failed to get current executable path");
    let current_dir = current_exe
        .parent()
        .expect("Failed to get current directory");

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
    get_parameter(slice, "--group", "-g")
}

pub fn get_parameter(slice: &[String], long_key: &str, short_key: &str) -> String {
    let mut param = String::from("");

    // Process the arguments to find the parameter
    for (i, arg) in slice.iter().enumerate() {
        // Handle --option=value format
        if let Some(value) = arg.strip_prefix(&format!("{}=", long_key)) {
            param = value.to_string();
            break;
        }
        // Handle -o=value format
        if let Some(value) = arg.strip_prefix(&format!("{}=", short_key)) {
            param = value.to_string();
            break;
        }
        // Handle --option value format (space-separated)
        if arg == long_key && i + 1 < slice.len() {
            param = slice[i + 1].clone();
            break;
        }
        // Handle -o value format (space-separated)
        if arg == short_key && i + 1 < slice.len() {
            param = slice[i + 1].clone();
            break;
        }
    }

    param
}

pub fn extract_param(args: &[String], long_key: &str, short_key: &str) -> Option<String> {
    for (i, arg) in args.iter().enumerate() {
        // Handle --option=value format
        if let Some(value) = arg.strip_prefix(&format!("{}=", long_key)) {
            return Some(value.to_string());
        }
        // Handle -o=value format
        if let Some(value) = arg.strip_prefix(&format!("{}=", short_key)) {
            return Some(value.to_string());
        }
        // Handle --option value format (space-separated)
        if arg == long_key && i + 1 < args.len() {
            return Some(args[i + 1].clone());
        }
        // Handle -o value format (space-separated)
        if arg == short_key && i + 1 < args.len() {
            return Some(args[i + 1].clone());
        }
    }

    None
}

pub fn get_json(slice: &[String], long_key: &str, short_key: &str) -> String {
    let mut json = String::from("");

    for (i, arg) in slice.iter().enumerate() {
        // Handle --option=value format
        if let Some(value) = arg.strip_prefix(&format!("{}=", long_key)) {
            json = value.to_string();
            break;
        }
        // Handle -o=value format
        if let Some(value) = arg.strip_prefix(&format!("{}=", short_key)) {
            json = value.to_string();
            break;
        }
        // Handle --option value format (space-separated)
        if arg == long_key && i + 1 < slice.len() {
            json = slice[i + 1].clone();
            break;
        }
        // Handle -o value format (space-separated)
        if arg == short_key && i + 1 < slice.len() {
            json = slice[i + 1].clone();
            break;
        }
    }

    if json.is_empty() {
        return "{}".to_string();
    }

    json
}

pub fn get_output_format(slice: &[String]) -> String {
    let format = get_parameter(slice, "--output", "-o");

    // Default to "text" if not specified or invalid
    match format.as_str() {
        "json" => "json".to_string(),
        "text" => "text".to_string(),
        "" => "text".to_string(), // Default when not provided
        _ => "text".to_string(),  // Default for invalid values
    }
}

pub fn get_weight(slice: &[String]) -> Option<i32> {
    let weight_str = get_parameter(slice, "--weight", "-w");
    
    if weight_str.is_empty() {
        return None;
    }
    
    match weight_str.parse::<i32>() {
        Ok(weight) => Some(weight),
        Err(_) => None,
    }
}

pub fn extract_flags_with_prefix(args: &[String], prefix: &str) -> Vec<(String, String)> {
    let mut flags = Vec::new();
    
    for (i, arg) in args.iter().enumerate() {
        // Handle --Tkey=value or --Pkey=value format
        if let Some(rest) = arg.strip_prefix(&format!("--{}", prefix)) {
            if let Some(eq_pos) = rest.find('=') {
                let key = &rest[..eq_pos];
                let value = &rest[eq_pos + 1..];
                flags.push((key.to_string(), strip_quotes(value)));
            }
        }
        // Handle --Tkey value or --Pkey value format (space-separated)
        else if arg.starts_with(&format!("--{}", prefix)) && i + 1 < args.len() {
            if let Some(key) = arg.strip_prefix(&format!("--{}", prefix)) {
                if !key.contains('=') {
                    flags.push((key.to_string(), strip_quotes(&args[i + 1])));
                }
            }
        }
    }
    
    flags
}

fn strip_quotes(value: &str) -> String {
    let trimmed = value.trim();
    if (trimmed.starts_with('"') && trimmed.ends_with('"')) ||
       (trimmed.starts_with('\'') && trimmed.ends_with('\'')) {
        trimmed[1..trimmed.len()-1].to_string()
    } else {
        value.to_string()
    }
}

pub fn serialize_flags(flags: &[(String, String)]) -> String {
    if flags.is_empty() {
        return "{}".to_string();
    }
    
    let json_pairs: Vec<String> = flags.iter()
        .map(|(key, value)| {
            // Check if value looks like JSON (starts with { or [)
            if value.trim_start().starts_with('{') || value.trim_start().starts_with('[') {
                format!("\"{}\": {}", key, value)
            } else {
                format!("\"{}\": \"{}\"", key, value)
            }
        })
        .collect();
    
    format!("{{{}}}", json_pairs.join(", "))
}
