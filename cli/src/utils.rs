use std::env;

pub fn get_flezha_command() -> String {
    let extension = get_extension();
    let current_exe = env::current_exe().expect("Failed to get current executable path");
    let current_dir = current_exe
        .parent()
        .expect("Failed to get current directory");

    current_dir
        .join(format!("flzvm{}", extension))
        .to_str()
        .expect("Failed to convert path to string")
        .to_string()
}

pub fn get_extension() -> String {
    if cfg!(target_os = "windows") {
        ".bat".to_string()
    } else {
        String::new()
    }
}

pub fn get_namespace(slice: &[String]) -> String {
    get_parameter(slice, "--namespace", "-n")
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

fn strip_quotes(value: &str) -> String {
    let trimmed = value.trim();
    if (trimmed.starts_with('"') && trimmed.ends_with('"'))
        || (trimmed.starts_with('\'') && trimmed.ends_with('\''))
    {
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        value.to_string()
    }
}

pub fn serialize_flags(flags: &[(String, String)]) -> String {
    if flags.is_empty() {
        return "{}".to_string();
    }

    let json_pairs: Vec<String> = flags
        .iter()
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

pub fn extract_x_flags(args: &[String]) -> Vec<(String, String)> {
    let mut flags = Vec::new();
    let mut i = 0;

    while i < args.len() {
        let arg = &args[i];

        // Handle --X prefixed arguments
        if let Some(rest) = arg.strip_prefix("--X") {
            // Handle --Xkey=value format
            if let Some(eq_pos) = rest.find('=') {
                let key = &rest[..eq_pos];
                let value = &rest[eq_pos + 1..];
                flags.push((key.to_string(), strip_quotes(value)));
            }
            // Handle --Xkey value format (space-separated)
            else if i + 1 < args.len() {
                let key = rest;
                flags.push((key.to_string(), strip_quotes(&args[i + 1])));
                i += 1; // Skip the next argument as it's the value
            }
        }

        i += 1;
    }

    flags
}

// Extract repeatable `--extension=NAME` flags and their sub-options
// `--NAME.key=value`. Returns `Vec<(name, Vec<(key, value)>)>` in declaration
// order. Does not consume values that are parsed elsewhere.
pub fn extract_extension_flags(args: &[String]) -> Vec<(String, Vec<(String, String)>)> {
    let mut names: Vec<String> = Vec::new();
    let mut sub_opts: std::collections::HashMap<String, Vec<(String, String)>> =
        std::collections::HashMap::new();
    let mut i = 0;

    while i < args.len() {
        let arg = &args[i];

        if let Some(rest) = arg.strip_prefix("--extension=") {
            let name = strip_quotes(rest);
            if !names.contains(&name) {
                names.push(name.clone());
                sub_opts.entry(name).or_default();
            }
        } else if arg == "--extension" && i + 1 < args.len() {
            let name = strip_quotes(&args[i + 1]);
            if !names.contains(&name) {
                names.push(name.clone());
                sub_opts.entry(name).or_default();
            }
            i += 1;
        } else if let Some(rest) = arg.strip_prefix("--") {
            if let Some(dot_pos) = rest.find('.') {
                let (prefix, remainder) = rest.split_at(dot_pos);
                let remainder = &remainder[1..]; // drop the '.'

                if names.contains(&prefix.to_string()) {
                    if let Some(eq_pos) = remainder.find('=') {
                        let key = &remainder[..eq_pos];
                        let value = &remainder[eq_pos + 1..];
                        let entry = sub_opts.entry(prefix.to_string()).or_default();
                        entry.push((key.to_string(), strip_quotes(value)));
                    } else if i + 1 < args.len() {
                        let entry = sub_opts.entry(prefix.to_string()).or_default();
                        entry.push((remainder.to_string(), strip_quotes(&args[i + 1])));
                        i += 1;
                    }
                }
            }
        }

        i += 1;
    }

    names
        .into_iter()
        .map(|name| {
            let opts = sub_opts.remove(&name).unwrap_or_default();
            (name, opts)
        })
        .collect()
}

// Strip the `extensions` entries from a list of key=value flags so the
// legacy opts parser does not interpret them as plain CLI options.
pub fn filter_out_extension_entries(args: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 0;

    // First pass: collect the names of declared extensions so we can skip
    // their dotted options in the second pass.
    let mut extension_names: Vec<String> = Vec::new();
    let mut j = 0;
    while j < args.len() {
        let arg = &args[j];
        if let Some(rest) = arg.strip_prefix("--extension=") {
            extension_names.push(strip_quotes(rest));
        } else if arg == "--extension" && j + 1 < args.len() {
            extension_names.push(strip_quotes(&args[j + 1]));
            j += 1;
        }
        j += 1;
    }

    while i < args.len() {
        let arg = &args[i];
        let mut skip = false;

        if arg.starts_with("--extension=") || arg == "--extension" {
            skip = true;
            if arg == "--extension" && i + 1 < args.len() {
                i += 1;
            }
        } else if let Some(rest) = arg.strip_prefix("--") {
            if let Some(dot_pos) = rest.find('.') {
                let prefix = &rest[..dot_pos];
                if extension_names.iter().any(|n| n == prefix) {
                    skip = true;
                    // If this used --NAME.key value (space-separated) we'd
                    // need to eat the next token, but since the dotted form
                    // is always --NAME.key=value, no lookahead is needed.
                }
            }
        }

        if !skip {
            out.push(arg.clone());
        }

        i += 1;
    }

    out
}

pub fn extract_opts_with_default_namespace(args: &[String]) -> Vec<(String, String)> {
    let mut flags = Vec::new();
    let mut i = 0;

    while i < args.len() {
        let arg = &args[i];

        // Skip --X-prefixed flags
        if arg.starts_with("--X") {
            // If it's --Xkey value format (space-separated), skip the next argument too
            if !arg.contains('=') && i + 1 < args.len() {
                i += 1; // Skip the value argument
            }
            i += 1;
            continue;
        }

        // Handle --option=value format
        if let Some(eq_pos) = arg.find('=') {
            if arg.starts_with("--") {
                let key = &arg[2..eq_pos];
                let value = &arg[eq_pos + 1..];
                flags.push((key.to_string(), strip_quotes(value)));
            }
        }
        // Handle --option value format (space-separated)
        else if arg.starts_with("--") && i + 1 < args.len() {
            let key = &arg[2..];
            let value = &args[i + 1];
            flags.push((key.to_string(), strip_quotes(value)));
            i += 1; // Skip the next argument as it's the value
        }
        // Handle -o=value format
        else if arg.starts_with("-") && !arg.starts_with("--") && arg.contains('=') {
            if let Some(eq_pos) = arg.find('=') {
                let key_part = &arg[1..eq_pos]; // Remove the single dash
                let key = match key_part {
                    "n" => "namespace",
                    "o" => "output",
                    _ => key_part,
                };
                let value = &arg[eq_pos + 1..];
                flags.push((key.to_string(), strip_quotes(value)));
            }
        }
        // Handle -o value format (space-separated)
        else if arg.starts_with("-") && !arg.starts_with("--") && i + 1 < args.len() {
            let key = match arg.as_str() {
                "-n" => "namespace",
                "-o" => "output",
                _ => &arg[1..], // Remove the single dash
            };
            let value = &args[i + 1];
            flags.push((key.to_string(), strip_quotes(value)));
            i += 1; // Skip the next argument as it's the value
        }

        i += 1;
    }

    // Add default namespace if not specified
    if !flags.iter().any(|(key, _)| key == "namespace") {
        flags.push(("namespace".to_string(), "default".to_string()));
    }

    flags
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn extract_extension_flags_single_no_opts() {
        let args = v(&["--extension=on_input"]);
        let got = extract_extension_flags(&args);
        assert_eq!(got, vec![("on_input".to_string(), vec![])]);
    }

    #[test]
    fn extract_extension_flags_multiple_with_dotted_opts() {
        let args = v(&[
            "--extension=on_input",
            "--on_input.position=0",
            "--extension=on_interval",
            "--on_interval.interval_ms=5000",
        ]);

        let got = extract_extension_flags(&args);

        assert_eq!(
            got,
            vec![
                (
                    "on_input".to_string(),
                    vec![("position".to_string(), "0".to_string())]
                ),
                (
                    "on_interval".to_string(),
                    vec![("interval_ms".to_string(), "5000".to_string())]
                ),
            ]
        );
    }

    #[test]
    fn extract_extension_flags_ignores_dotted_opts_for_unknown_extension() {
        // `on_unknown.x=1` has no corresponding `--extension=on_unknown`, so
        // it must not be captured.
        let args = v(&["--extension=on_input", "--on_unknown.x=1"]);
        let got = extract_extension_flags(&args);
        assert_eq!(got, vec![("on_input".to_string(), vec![])]);
    }

    #[test]
    fn extract_extension_flags_space_separated_form() {
        let args = v(&["--extension", "on_input", "--extension", "on_terminate"]);

        let got = extract_extension_flags(&args);

        assert_eq!(
            got,
            vec![
                ("on_input".to_string(), vec![]),
                ("on_terminate".to_string(), vec![]),
            ]
        );
    }

    #[test]
    fn extract_opts_ignores_extension_flags() {
        let args = v(&[
            "--extension=on_input",
            "--on_input.position=0",
            "--namespace=prod",
        ]);

        let filtered = filter_out_extension_entries(&args);
        let opts = extract_opts_with_default_namespace(&filtered);

        assert!(opts.iter().any(|(k, v)| k == "namespace" && v == "prod"));
        assert!(!opts.iter().any(|(k, _)| k == "extension"));
        assert!(!opts.iter().any(|(k, _)| k.starts_with("on_input")));
    }
}
