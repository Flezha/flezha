use std::collections::HashMap;
use std::process::{exit, Command};

use crate::utils::{
    escape_rpc_arg, extract_param, get_flezha_command, get_json, get_namespace, get_output_format,
};

pub fn handle_operative_command(args: &[String]) {
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
        "send" => handle_send(args),
        "rewind" => handle_rewind(args),
        "replay" => handle_replay(args),
        "help" | "--help" | "-h" => print_operative_help(),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'start', 'stop', 'state', 'list', 'info', 'io_listen', 'send', 'rewind', 'replay', or 'help'.",
                args[0]
            );
            exit(1);
        }
    }
}

fn handle_start(args: &[String]) {
    let example_usage = "Usage: flz operative start <operative_id> <factory_id> [--kind <unbounded|bounded>] [--namespace <namespace> | -n <namespace>] [--init_values <json> | -iv <json>] [--plugins <json-array>]";

    if args.len() < 3 {
        eprintln!("Error: Invalid number of arguments.\n {}", example_usage);
        exit(1);
    }

    let operative_id = &args[1];
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

    let kind =
        extract_param(&args[3..], "--kind", "--kind").unwrap_or_else(|| "unbounded".to_string());

    let mut opts: HashMap<String, String> = HashMap::new();

    if let Some(spt) = extract_param(&args[3..], "--snapshot-interval", "-si") {
        opts.insert("snapshot_interval".to_string(), spt);
    }

    let opts_json = serde_json::to_string(&opts).unwrap();
    let command = get_flezha_command();
    let namespace = get_namespace(&args[3..]);

    let escaped_init_values = escape_rpc_arg(&init_values);
    let escaped_opts_json = escape_rpc_arg(&opts_json);
    let escaped_plugins_json = escape_rpc_arg(&plugins_json);

    let rpc_command = format!(
        "Flezha.Service.CliManager.operative_start(\"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\")",
        namespace,
        operative_id,
        factory_id,
        escaped_init_values,
        escaped_opts_json,
        escaped_plugins_json,
        kind,
    );

    println!("Starting operative...");

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
pub(crate) fn parse_plugins_json(raw: &str) -> Result<String, String> {
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
        eprintln!("Error: Invalid number of arguments. Usage: flz operative stop <operative_id>");
        exit(1);
    }

    let operative_id = &args[1];
    let namespace = get_namespace(&args[2..]);

    let rpc_command = format!(
        "Flezha.Service.CliManager.operative_stop(\"{}\", \"{}\")",
        namespace, operative_id,
    );

    let command = get_flezha_command();

    println!("Stopping operative...");

    // Attempt to execute the external command.
    let status = Command::new(command).arg("rpc").arg(rpc_command).status();

    match status {
        Ok(status) if status.success() => println!("Operative stopped successfully."),
        Ok(_) => eprintln!("Unknown status returned."),
        Err(e) => eprintln!("Failed to stop operative: {}", e),
    }
}

/// `flz operative send <operative_id> --text <payload> [--channel <ch>]` —
/// inject a payload into an operative's input stream (the SDK-facing
/// `send_io` route). Blocks for the confirming verdict so a parked or absent
/// target is reported rather than silently dropped.
fn handle_send(args: &[String]) {
    if args.len() < 2 {
        eprintln!(
            "Error: Invalid number of arguments. \
             Usage: flz operative send <operative_id> --text <payload> [--channel <ch>] [-n <namespace>]"
        );
        exit(1);
    }

    let operative_id = &args[1];
    let namespace = get_namespace(&args[2..]);

    let text = match extract_param(&args[2..], "--text", "-t") {
        Some(value) => value,
        None => {
            eprintln!("Error: --text <payload> is required.");
            exit(1);
        }
    };

    let channel = extract_param(&args[2..], "--channel", "-c").unwrap_or_default();

    // Escape embedded quotes so the payload/channel survive being wrapped in
    // quotes inside the Elixir RPC expression (same as handle_start).
    let escaped_text = escape_rpc_arg(&text);
    let escaped_channel = escape_rpc_arg(&channel);

    let rpc_command = format!(
        "Flezha.Service.CliManager.operative_send(\"{}\", \"{}\", \"{}\", \"{}\")",
        namespace, operative_id, escaped_text, escaped_channel,
    );

    let status = Command::new(get_flezha_command())
        .arg("rpc")
        .arg(rpc_command)
        .status();

    match status {
        Ok(status) if status.success() => {}
        // operative_send halts non-zero on error verdicts (not_found / not_ready / refused).
        Ok(_) => exit(1),
        Err(e) => {
            eprintln!("Failed to send input to operative: {}", e);
            exit(1);
        }
    }
}

/// `flz operative rewind <instance> --to-iteration N` — take a bounded loop back
/// to a committed iteration: the ones above it are compensated in reverse, state
/// returns to that boundary, and the epoch is bumped so the re-run's output
/// supersedes the unwound run's at a guarded sink.
fn handle_rewind(args: &[String]) {
    if args.len() < 2 {
        eprintln!(
            "Error: Invalid number of arguments. \
             Usage: flz operative rewind <operative_id> --to-iteration <N>"
        );
        exit(1);
    }

    let operative_id = &args[1];
    let namespace = get_namespace(&args[2..]);

    let to_iter = match extract_param(&args[2..], "--to-iteration", "-i") {
        Some(value) => match value.parse::<u32>() {
            Ok(n) => n,
            Err(_) => {
                eprintln!("Error: --to-iteration must be a non-negative integer, got '{value}'");
                exit(1);
            }
        },
        None => {
            eprintln!("Error: --to-iteration <N> is required.");
            exit(1);
        }
    };

    let rpc_command = format!(
        "Flezha.Service.CliManager.operative_rewind(\"{}\", \"{}\", {})",
        namespace, operative_id, to_iter,
    );

    let status = Command::new(get_flezha_command())
        .arg("rpc")
        .arg(rpc_command)
        .status();

    match status {
        Ok(status) if status.success() => {}
        Ok(_) => eprintln!("Unknown status returned."),
        Err(e) => eprintln!("Failed to rewind operative: {}", e),
    }
}

/// `flz operative replay <operative/operation/key> --from <seq|ts>` — re-run a
/// Segment's retained inputs from a point in the past through the current code,
/// republishing under a bumped epoch so a guarded sink overwrites the stale rows
/// by natural key. "Deploy a fix, heal the recent past."
///
/// The same transaction a late input triggers automatically; here the operator
/// picks the baseline instead of a late event.
fn handle_replay(args: &[String]) {
    if args.len() < 2 {
        eprintln!(
            "Error: Invalid number of arguments. \
             Usage: flz operative replay <operative/operation/key> --from <seq|timestamp>"
        );
        exit(1);
    }

    let segment = &args[1];
    let namespace = get_namespace(&args[2..]);

    let from = match extract_param(&args[2..], "--from", "-f") {
        Some(value) => value,
        None => {
            eprintln!(
                "Error: --from <seq|timestamp> is required. \
                 Pass a WAL seq, or epoch-millis / RFC3339 for a point in time."
            );
            exit(1);
        }
    };

    let rpc_command = format!(
        "Flezha.Service.CliManager.operative_replay(\"{}\", \"{}\", \"{}\")",
        namespace, segment, from,
    );

    let status = Command::new(get_flezha_command())
        .arg("rpc")
        .arg(rpc_command)
        .status();

    match status {
        Ok(status) if status.success() => {}
        Ok(_) => eprintln!("Unknown status returned."),
        Err(e) => eprintln!("Failed to replay segment: {}", e),
    }
}

fn handle_state(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Error: Invalid number of arguments. Usage: flz operative state <operative_id>");
        exit(1);
    }

    let operative_id = &args[1];
    let namespace = get_namespace(&args[2..]);

    let rpc_command = format!(
        "Flezha.Service.CliManager.segment_state(\"{}\", \"{}\")",
        namespace, operative_id,
    );

    let command = get_flezha_command();

    let status = Command::new(command).arg("rpc").arg(rpc_command).status();

    match status {
        Ok(status) if status.success() => {}
        Ok(_) => eprintln!("Unknown status returned."),
        Err(e) => eprintln!("Failed to get operative state: {}", e),
    }
}

fn handle_list(args: &[String]) {
    let namespace = get_namespace(&args[1..]);
    let output_format = get_output_format(&args[1..]);

    let command = get_flezha_command();
    let rpc_command = format!(
        "Flezha.Service.CliManager.list_operatives(\"{}\", \"{}\")",
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
            "Fetching operatives for namespace '{}'...",
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
        "Flezha.Service.CliManager.operative_info(\"{}\", \"{}\")",
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
            "Fetching operative information for namespace '{}'...",
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
    let example_usage = "Usage: flz operative io_listen input|output <operative_id> [--namespace <namespace> | -n <namespace>]";

    if args.len() < 3 {
        eprintln!("Error: Invalid number of arguments.\n {}", example_usage);
        exit(1);
    }

    let io_type = &args[1];
    let operative_id = &args[2];

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
        "Flezha.Service.CliManager.operative_listen(\"{}\", \"{}\", \"{}\")",
        namespace, operative_id, io_type
    );

    println!(
        "Starting IO listener for operative '{}' ({} mode)...",
        operative_id, io_type
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

fn print_operative_help() {
    println!("flz operative - Operative management commands");
    println!();
    println!("USAGE:");
    println!("    flz operative <COMMAND>");
    println!();
    println!("COMMANDS:");
    println!("    start       Start an operative");
    println!("    stop        Stop an operative");
    println!("    state       Get operative state");
    println!("    list        List operatives");
    println!("    info        Get operative info");
    println!("    io_listen   Listen to operative I/O");
    println!("    send        Send a payload to an operative's input stream");
    println!("    rewind      Rewind a bounded loop to a committed iteration");
    println!("    replay      Re-run a Segment's retained inputs from a point in the past");
    println!("    help        Show this help message");
    println!();
    println!("EXAMPLES:");
    println!("    flz operative send orchestrator --text '{{\"task\":\"refactor\"}}'");
    println!("    flz operative rewind chat-1 --to-iteration 3");
    println!("    flz operative replay candles/aggregate/BTC --from 1720000000000");
    println!();
    println!("Use 'flz operative <COMMAND> --help' for more information on a specific command.");
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
        let input = r#"[{"id":"init_state_from_java","provider":"init_state_sibling","opts":{"source_operative_id":"market-init-state","handler_key":"fetch_init_state","request_payload":{"symbol":"XBT/USD"}},"subscriptions":[["on_operative_init",{"on_error":"empty"}]]}]"#;
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
