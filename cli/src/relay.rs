use crate::utils::{extract_param, get_flezha_command, get_json, get_namespace, get_output_format};
use std::collections::HashMap;
use std::process::{exit, Command};

pub fn handle_relay_command(args: &[String]) {
    if args.is_empty() {
        eprintln!("Error: Invalid number of arguments. See help for more information.");
        exit(1);
    }

    match args[0].as_str() {
        "start" => handle_start(args),
        "stop" => handle_stop(args),
        "state" => handle_state(args),
        "list" => handle_list(args),
        "info" => handle_info(args),
        "io_listen" => handle_io_listen(args),
        "help" | "--help" | "-h" => print_relay_help(),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'start', 'stop', 'state', 'list', 'info', 'io_listen', or 'help'.",
                args[0]
            );
            exit(1);
        }
    }
}

fn handle_start(args: &[String]) {
    let example_usage = "Usage: flz relay start <relay_id> <factory_id> [--namespace <namespace> | -n <namespace>] [--init_values <json> | -iv <json>] [--plugins <json-array>]";

    if args.len() < 3 {
        eprintln!("Error: Invalid number of arguments.\n {}", example_usage);
        exit(1);
    }

    let relay_id = &args[1];
    let factory_id = &args[2];
    let init_values = get_json(&args[3..], "--init_values", "-iv");
    let plugins_raw = get_json(&args[3..], "--plugins", "--plugins");
    let plugins_json = match parse_plugins_json(&plugins_raw) {
        Ok(canonical) => canonical,
        Err(reason) => {
            eprintln!("Error: invalid --plugins JSON: {}", reason);
            exit(1);
        }
    };

    let mut opts: HashMap<String, String> = HashMap::new();

    if let Some(spt) = extract_param(&args[3..], "--snapshot-interval", "-si") {
        opts.insert("snapshot_interval".to_string(), spt);
    }

    let opts_json = serde_json::to_string(&opts).unwrap();
    let command = get_flezha_command();
    let namespace = get_namespace(&args[3..]);

    let escaped_init_values = init_values.replace("\"", "\\\"");
    let escaped_opts_json = opts_json.replace("\"", "\\\"");
    let escaped_plugins_json = plugins_json.replace("\"", "\\\"");

    let rpc_command = format!(
        "Flezha.Service.CliManager.relay_start(\"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\")",
        namespace,
        relay_id,
        factory_id,
        escaped_init_values,
        escaped_opts_json,
        escaped_plugins_json,
    );

    println!("Starting relay...");

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

/// Validate the `--plugins` JSON blob at the CLI layer so parse errors are
/// surfaced with a local exit code and message, rather than round-tripping
/// an opaque RPC failure through the controller.
///
/// Accepts:
/// * `"{}"` / empty string — treated as "no plugins attached"; returns `"[]"`.
/// * A JSON array of plugin objects. Each object MUST have `id`, `provider`
///   (non-empty strings) and a non-empty `subscriptions` array. Deeper
///   validation (provider registration lookup, subscription-opts shape)
///   happens on the Elixir side where the registry is authoritative.
fn parse_plugins_json(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "{}" {
        return Ok("[]".to_string());
    }

    let value: serde_json::Value =
        serde_json::from_str(trimmed).map_err(|e| format!("not valid JSON — {}", e))?;

    let items = value
        .as_array()
        .ok_or_else(|| "expected a JSON array of plugin objects".to_string())?;

    for (idx, item) in items.iter().enumerate() {
        let obj = item
            .as_object()
            .ok_or_else(|| format!("plugin[{}] must be a JSON object", idx))?;

        let id = obj
            .get("id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("plugin[{}] missing non-empty string field `id`", idx))?;

        obj.get("provider")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("plugin[{}] ({}) missing `provider`", idx, id))?;

        match obj.get("subscriptions") {
            Some(serde_json::Value::Array(subs)) if !subs.is_empty() => (),
            _ => {
                return Err(format!(
                    "plugin[{}] ({}) must declare a non-empty `subscriptions` array",
                    idx, id
                ));
            }
        }
    }

    Ok(serde_json::to_string(&value).unwrap_or_else(|_| trimmed.to_string()))
}

fn handle_stop(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Error: Invalid number of arguments. Usage: flz relay stop <relay_id>");
        exit(1);
    }

    let relay_id = &args[1];
    let namespace = get_namespace(&args[2..]);

    let rpc_command = format!(
        "Flezha.Service.CliManager.relay_stop(\"{}\", \"{}\")",
        namespace, relay_id,
    );

    let command = get_flezha_command();

    println!("Stopping relay...");

    // Attempt to execute the external command.
    let status = Command::new(command).arg("rpc").arg(rpc_command).status();

    match status {
        Ok(status) if status.success() => println!("Relay stopped successfully."),
        Ok(_) => eprintln!("Unknown status returned."),
        Err(e) => eprintln!("Failed to stop relay: {}", e),
    }
}

fn handle_state(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Error: Invalid number of arguments. Usage: flz relay state <relay_id>");
        exit(1);
    }

    let relay_id = &args[1];
    let namespace = get_namespace(&args[2..]);

    let rpc_command = format!(
        "Flezha.Service.CliManager.partition_state(\"{}\", \"{}\")",
        namespace, relay_id,
    );

    let command = get_flezha_command();

    let status = Command::new(command).arg("rpc").arg(rpc_command).status();

    match status {
        Ok(status) if status.success() => {}
        Ok(_) => eprintln!("Unknown status returned."),
        Err(e) => eprintln!("Failed to get relay state: {}", e),
    }
}

fn handle_list(args: &[String]) {
    let namespace = get_namespace(&args[1..]);
    let output_format = get_output_format(&args[1..]);

    let command = get_flezha_command();
    let rpc_command = format!(
        "Flezha.Service.CliManager.list_relays(\"{}\", \"{}\")",
        namespace, output_format
    );

    // Only show progress message for text output
    if output_format == "text" {
        let display_namespace = if namespace.is_empty() {
            "default"
        } else {
            &namespace
        };
        println!(
            "Fetching relays for namespace '{}'...",
            display_namespace
        );
    }

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

fn handle_info(args: &[String]) {
    let namespace = get_namespace(&args[1..]);
    let output_format = get_output_format(&args[1..]);

    let command = get_flezha_command();
    let rpc_command = format!(
        "Flezha.Service.CliManager.relay_info(\"{}\", \"{}\")",
        namespace, output_format
    );

    // Only show progress message for text output
    if output_format == "text" {
        let display_namespace = if namespace.is_empty() {
            "default"
        } else {
            &namespace
        };
        println!(
            "Fetching relay information for namespace '{}'...",
            display_namespace
        );
    }

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

fn handle_io_listen(args: &[String]) {
    let example_usage = "Usage: flz relay io_listen input|output <relay_id> [--namespace <namespace> | -n <namespace>]";

    if args.len() < 3 {
        eprintln!("Error: Invalid number of arguments.\n {}", example_usage);
        exit(1);
    }

    let io_type = &args[1];
    let relay_id = &args[2];

    // Validate io_type
    if io_type != "input" && io_type != "output" {
        eprintln!(
            "Error: Invalid IO type '{}'. Use 'input' or 'output' only.",
            io_type
        );
        exit(1);
    }

    let namespace = get_namespace(&args[3..]);
    let command = get_flezha_command();

    let rpc_command = format!(
        "Flezha.Service.CliManager.relay_listen(\"{}\", \"{}\", \"{}\")",
        namespace, relay_id, io_type
    );

    println!(
        "Starting IO listener for relay '{}' ({} mode)...",
        relay_id, io_type
    );
    println!("Press Ctrl+C to stop listening");

    // Set up signal handling to clean up on Ctrl+C
    let mut child = Command::new(command)
        .arg("rpc")
        .arg(rpc_command)
        .spawn()
        .expect("Failed to execute command");
    
    // Wait for the child process and handle signals
    let status = child.wait().expect("Failed to wait for command");

    if !status.success() {
        eprintln!("Error: Command execution failed.");
        exit(1);
    }
}

fn print_relay_help() {
    println!("flz relay - Relay management commands");
    println!();
    println!("USAGE:");
    println!("    flz relay <COMMAND>");
    println!();
    println!("COMMANDS:");
    println!("    start       Start a relay");
    println!("    stop        Stop a relay");
    println!("    state       Get relay state");
    println!("    list        List relays");
    println!("    info        Get relay info");
    println!("    io_listen   Listen to relay I/O");
    println!("    help        Show this help message");
    println!();
    println!("Use 'flz relay <COMMAND> --help' for more information on a specific command.");
}

#[cfg(test)]
mod tests {
    use super::parse_plugins_json;

    #[test]
    fn empty_input_becomes_empty_array() {
        assert_eq!(parse_plugins_json("").unwrap(), "[]");
        assert_eq!(parse_plugins_json("{}").unwrap(), "[]");
        assert_eq!(parse_plugins_json("   ").unwrap(), "[]");
    }

    #[test]
    fn valid_sibling_spec_round_trips() {
        let input = r#"[{"id":"init_state_from_java","provider":"init_state_sibling","opts":{"source_relay_id":"market-init-state","handler_key":"fetch_init_state","request_payload":{"symbol":"XBT/USD"}},"subscriptions":[["on_relay_init",{"on_error":"empty"}]]}]"#;
        let canonical = parse_plugins_json(input).unwrap();
        assert!(canonical.contains("\"init_state_from_java\""));
        assert!(canonical.contains("\"init_state_sibling\""));
    }

    #[test]
    fn malformed_json_returns_err() {
        assert!(parse_plugins_json("not json").is_err());
        assert!(parse_plugins_json("[").is_err());
    }

    #[test]
    fn non_array_returns_err() {
        let err = parse_plugins_json(r#"{"id":"x","provider":"y"}"#).unwrap_err();
        assert!(err.contains("JSON array"));
    }

    #[test]
    fn missing_id_returns_err() {
        let err =
            parse_plugins_json(r#"[{"provider":"init_state_sibling","subscriptions":[["x",{}]]}]"#)
                .unwrap_err();
        assert!(err.contains("missing non-empty string field `id`"));
    }

    #[test]
    fn empty_subscriptions_returns_err() {
        let err = parse_plugins_json(
            r#"[{"id":"p","provider":"init_state_sibling","subscriptions":[]}]"#,
        )
        .unwrap_err();
        assert!(err.contains("non-empty `subscriptions`"));
    }
}
