use std::process::exit;

use crate::dispatch::{self, Command};
use crate::rpc::{self, escape_rpc_arg};
use crate::{args, help};

pub(crate) const TABLE: Command = Command {
    help: help::OPERATIVE,
    verbs: &[
        ("start", handle_start),
        ("scale", handle_scale),
        ("stop", handle_stop),
        ("state", handle_state),
        ("list", handle_list),
        ("info", handle_info),
        ("io_listen", handle_io_listen),
        ("send", handle_send),
        ("rewind", handle_rewind),
        ("replay", handle_replay),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Some("Error: Invalid number of arguments. See help for more information."),
    unknown: "Error: Invalid argument '{}'. Use 'start', 'scale', 'stop', 'state', 'list', 'info', 'io_listen', 'send', 'rewind', 'replay', or 'help'.",
    unknown_prints_help: false,
};

/// The JSON opts object `operative_start` takes: the flags that are neither
/// the operative's identity nor its init values. Ordered, because the same
/// invocation has to build the same request every run.
fn start_opts(args: &[String]) -> String {
    let mut opts: Vec<(String, String)> = Vec::new();

    for (long_key, short_key, name) in [
        ("--snapshot-interval", "-si", "snapshot_interval"),
        ("--partitions", "--partitions", "partitions"),
    ] {
        if let Some(count) = args::or_refuse(args::number::<u64>(args, long_key, short_key)) {
            opts.push((name.to_string(), count.to_string()));
        }
    }

    args::serialize_flags(&opts)
}

fn handle_start(args: &[String]) {
    let example_usage = "Usage: flz operative start <operative_id> <factory_id> [--kind <stream|workflow>] [--namespace <namespace> | -n <namespace>] [--init_values <json> | -iv <json>] [--plugins <json-array>] [--partitions <N>]";

    if args.len() < 3 {
        eprintln!("Error: Invalid number of arguments.\n {}", example_usage);
        exit(1);
    }

    let rest = &args[3..];
    let plugins_json = match parse_plugins_json(&args::json(rest, "--plugins", "--plugins")) {
        Ok(canonical) => canonical,
        Err(reason) => {
            eprintln!("Error: invalid --plugins JSON: {}", reason);
            exit(1);
        }
    };

    let rpc_command = format!(
        "Flezha.Service.CliManager.operative_start(\"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\")",
        args::namespace(rest),
        &args[1],
        &args[2],
        escape_rpc_arg(&args::json(rest, "--init_values", "-iv")),
        escape_rpc_arg(&start_opts(rest)),
        escape_rpc_arg(&plugins_json),
        args::or_refuse(args::extract_param(rest, "--kind", "--kind"))
            .unwrap_or_else(|| "stream".to_string()),
    );

    println!("Starting operative...");

    rpc::run(rpc_command, rpc::REFUSED);
}

/// `flz operative scale <group> <partitions>` — rescale a partition group.
/// The group is quiesced, every Entity whose key group changes owner is moved,
/// and the new member set is started; a refusal changes nothing.
fn handle_scale(args: &[String]) {
    let example_usage = "Usage: flz operative scale <group> <partitions> [--namespace <namespace> | -n <namespace>] [--timeout <ms>]";

    if args.len() < 3 {
        eprintln!("Error: Invalid number of arguments.\n {}", example_usage);
        exit(1);
    }

    let group_id = &args[1];
    let partitions = args::or_refuse(args::read_number::<u64>("<partitions>", &args[2]));

    let mut opts: Vec<(String, String)> = Vec::new();
    if let Some(timeout_ms) = args::or_refuse(args::number::<u64>(&args[3..], "--timeout", "-t")) {
        opts.push(("timeout_ms".to_string(), timeout_ms.to_string()));
    }

    let rpc_command = format!(
        "Flezha.Service.CliManager.operative_scale(\"{}\", \"{}\", \"{}\", \"{}\")",
        args::namespace(&args[3..]),
        group_id,
        partitions,
        escape_rpc_arg(&args::serialize_flags(&opts)),
    );

    println!("Scaling operative group {} to {}...", group_id, partitions);

    rpc::run(rpc_command, rpc::REFUSED);
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

    let rpc_command = format!(
        "Flezha.Service.CliManager.operative_stop(\"{}\", \"{}\", {})",
        args::namespace(&args[2..]),
        &args[1],
        rpc::token_opts_fragment(&args[2..]),
    );

    println!("Stopping operative...");

    // operative_stop halts non-zero on error verdicts (not_found / refused).
    rpc::run(
        rpc_command,
        rpc::UNKNOWN_STATUS.diagnosing("Failed to stop operative"),
    );

    println!("Operative stopped successfully.");
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

    let rest = &args[2..];

    let text = match args::or_refuse(args::extract_param(rest, "--text", "-t")) {
        Some(value) => value,
        None => {
            eprintln!("Error: --text <payload> is required.");
            exit(1);
        }
    };

    // Escape embedded quotes so the payload/channel survive being wrapped in
    // quotes inside the Elixir RPC expression (same as handle_start).
    let rpc_command = format!(
        "Flezha.Service.CliManager.operative_send(\"{}\", \"{}\", \"{}\", \"{}\")",
        args::namespace(rest),
        &args[1],
        escape_rpc_arg(&text),
        escape_rpc_arg(
            &args::or_refuse(args::extract_param(rest, "--channel", "-c")).unwrap_or_default(),
        ),
    );

    // operative_send halts non-zero on error verdicts (not_found / not_ready / refused).
    rpc::run(
        rpc_command,
        rpc::QUIET.diagnosing("Failed to send input to operative"),
    );
}

/// `flz operative rewind <instance> --to-iteration N` — take a workflow loop back
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

    let to_iter = match args::or_refuse(args::number::<u32>(&args[2..], "--to-iteration", "-i")) {
        Some(iteration) => iteration,
        None => {
            eprintln!("Error: --to-iteration <N> is required.");
            exit(1);
        }
    };

    // operative_rewind halts non-zero on error verdicts (not_found / refused).
    rpc::run(
        format!(
            "Flezha.Service.CliManager.operative_rewind(\"{}\", \"{}\", {})",
            args::namespace(&args[2..]),
            &args[1],
            to_iter,
        ),
        rpc::UNKNOWN_STATUS.diagnosing("Failed to rewind operative"),
    );
}

/// `flz operative replay <operative/procedure/key> --from <seq|ts>` — re-run a
/// Entity's retained inputs from a point in the past through the current code,
/// republishing under a bumped epoch so a guarded sink overwrites the stale rows
/// by natural key. "Deploy a fix, heal the recent past."
///
/// The same transaction a late input triggers automatically; here the operator
/// picks the baseline instead of a late event.
fn handle_replay(args: &[String]) {
    if args.len() < 2 {
        eprintln!(
            "Error: Invalid number of arguments. \
             Usage: flz operative replay <operative/procedure/key> --from <seq|timestamp>"
        );
        exit(1);
    }

    let from = match args::or_refuse(args::extract_param(&args[2..], "--from", "-f")) {
        Some(value) => value,
        None => {
            eprintln!(
                "Error: --from <seq|timestamp> is required. \
                 Pass a WAL seq, or epoch-millis / RFC3339 for a point in time."
            );
            exit(1);
        }
    };

    // operative_replay halts non-zero on error verdicts (not_found / refused).
    rpc::run(
        format!(
            "Flezha.Service.CliManager.operative_replay(\"{}\", \"{}\", \"{}\")",
            args::namespace(&args[2..]),
            &args[1],
            from,
        ),
        rpc::UNKNOWN_STATUS.diagnosing("Failed to replay entity"),
    );
}

fn handle_state(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Error: Invalid number of arguments. Usage: flz operative state <operative_id>");
        exit(1);
    }

    // entity_state halts non-zero on error verdicts (not_found / refused).
    rpc::run(
        format!(
            "Flezha.Service.CliManager.entity_state(\"{}\", \"{}\")",
            args::namespace(&args[2..]),
            &args[1],
        ),
        rpc::UNKNOWN_STATUS.diagnosing("Failed to get operative state"),
    );
}

fn handle_list(args: &[String]) {
    rpc::scoped_read(&args[1..], "list_operatives", Some("operatives"));
}

fn handle_info(args: &[String]) {
    rpc::scoped_read(&args[1..], "operative_info", Some("operative information"));
}

fn handle_io_listen(args: &[String]) {
    let example_usage = "Usage: flz operative io_listen input|output <operative_id> [--namespace <namespace> | -n <namespace>]";

    if args.len() < 3 {
        eprintln!("Error: Invalid number of arguments.\n {}", example_usage);
        exit(1);
    }

    let io_type = &args[1];
    let operative_id = &args[2];

    if io_type != "input" && io_type != "output" {
        eprintln!(
            "Error: Invalid IO type '{}'. Use 'input' or 'output' only.",
            io_type
        );
        exit(1);
    }

    let rpc_command = format!(
        "Flezha.Service.CliManager.operative_listen(\"{}\", \"{}\", \"{}\")",
        args::namespace(&args[3..]),
        operative_id,
        io_type
    );

    println!(
        "Starting IO listener for operative '{}' ({} mode)...",
        operative_id, io_type
    );
    println!("Press Ctrl+C to stop listening");

    rpc::run(rpc_command, rpc::REFUSED);
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
