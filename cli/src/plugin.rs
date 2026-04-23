use crate::utils::{
    extract_extension_flags, extract_opts_with_default_namespace, extract_x_flags,
    filter_out_extension_entries, get_flezha_command, serialize_flags,
};
use std::process::{exit, Command};

pub fn handle_plugin_command(args: &[String]) {
    if args.is_empty() {
        eprintln!("Error: No plugin command specified.");
        exit(1);
    }

    match args[0].as_str() {
        "enable" => handle_enable(args),
        "disable" => handle_disable(args),
        "help" => handle_help(args),
        "list" => handle_list(args),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'enable', 'disable', 'help', or 'list'.",
                args[0]
            );
            exit(1);
        }
    }
}

fn handle_enable(args: &[String]) {
    let example_usage = "Usage: flz plugin enable <plugin_id> <provider_id> [Xkey=value] [--option value]\nExample: flz plugin enable processor_kafka_producer my_provider Xhosts=kafka:9092 Xtopic=my_topic --namespace mynamespace";

    if args.len() < 3 {
        eprintln!("Error: Invalid number of arguments.\n {}", example_usage);
        exit(1);
    }

    let plugin_id = &args[1];
    let provider_id = &args[2];
    let command = get_flezha_command();

    // Extract X-prefixed flags (plugin settings)
    let x_flags = extract_x_flags(&args[3..]);
    let x_flags_json_base = serialize_flags(&x_flags);

    // Remove `--extension=...` and `--NAME.key=value` before the opts parser
    // runs so they are not misclassified as CLI options.
    let filtered_rest = filter_out_extension_entries(&args[3..]);

    // Extract opts flags (CLI options with default namespace)
    let opts_flags = extract_opts_with_default_namespace(&filtered_rest);
    let opts_json_base = serialize_flags(&opts_flags);

    // Extract repeatable `--extension=NAME` with `--NAME.key=value` sub-options.
    let extensions = extract_extension_flags(&args[3..]);

    // Embed `extensions` JSON array into plugin flags so the Elixir side sees it.
    let x_flags_json = if extensions.is_empty() {
        x_flags_json_base
    } else {
        embed_extensions_into_json(&x_flags_json_base, &extensions)
    };

    let rpc_command = format!(
        "Flezha.Service.CliManager.enable_plugin(\"{}\", \"{}\", {}, {})",
        plugin_id,
        provider_id,
        serde_json::to_string(&x_flags_json).unwrap(),
        serde_json::to_string(&opts_json_base).unwrap()
    );

    println!("Enabling plugin...");

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

fn handle_disable(args: &[String]) {
    let example_usage = "Usage: flz plugin disable <plugin_id>";

    if args.len() < 2 {
        eprintln!("Error: Invalid number of arguments.\n {}", example_usage);
        exit(1);
    }

    let plugin_id = &args[1];
    let command = get_flezha_command();

    let rpc_command = format!(
        "Flezha.Service.CliManager.disable_plugin(\"{}\")",
        plugin_id
    );

    println!("Disabling plugin...");

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

fn handle_help(args: &[String]) {
    if args.len() < 2 {
        print_plugin_help();
        return;
    }

    let provider_id = &args[1];
    let command = get_flezha_command();

    let rpc_command = format!(
        "Flezha.Service.CliManager.plugin_help(\"{}\")",
        provider_id
    );

    println!("Getting help for plugin '{}'...", provider_id);

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

fn handle_list(_args: &[String]) {
    let command = get_flezha_command();

    let rpc_command = "Flezha.Service.CliManager.list_plugins()";

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

fn print_plugin_help() {
    println!("flz plugin - Plugin management commands");
    println!();
    println!("USAGE:");
    println!("    flz plugin <COMMAND>");
    println!();
    println!("COMMANDS:");
    println!("    enable     Enable a plugin");
    println!("    disable    Disable a plugin");
    println!("    help       Show help for a specific plugin provider");
    println!("    list       List all available plugin providers");
    println!();
    println!("EXAMPLES:");
    println!("    flz plugin list");
    println!("    flz plugin help processor_websocket");
    println!("    flz plugin enable ws_id processor_websocket --Xuri=ws://localhost:4500 \\");
    println!("        --extension=on_input --on_input.position=0");
    println!("    flz plugin enable kafka_id processor_kafka_producer \\");
    println!("        --Xhosts=kafka:9092 --Xtopic=events \\");
    println!("        --extension=on_input --extension=on_interval --on_interval.interval_ms=5000");
    println!("    flz plugin disable ws_id");
}

// Serialize a `"extensions": [{"name": "...", "opts": {...}}, ...]` list into
// the plugin_flags JSON object.
fn embed_extensions_into_json(
    base_json: &str,
    extensions: &[(String, Vec<(String, String)>)],
) -> String {
    let entries: Vec<String> = extensions
        .iter()
        .map(|(name, opts)| {
            let opts_json = serialize_flags(opts);
            format!("{{\"name\":\"{}\",\"opts\":{}}}", name, opts_json)
        })
        .collect();

    let extensions_fragment = format!("\"extensions\":[{}]", entries.join(","));

    if base_json.trim() == "{}" {
        format!("{{{}}}", extensions_fragment)
    } else {
        // Insert before closing `}`
        let trimmed = base_json.trim_end_matches('}');
        if trimmed.ends_with('{') {
            format!("{}{}}}", trimmed, extensions_fragment)
        } else {
            format!("{},{}}}", trimmed, extensions_fragment)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embed_extensions_into_empty_base() {
        let got = embed_extensions_into_json(
            "{}",
            &[("on_input".to_string(), vec![])],
        );

        assert_eq!(got, "{\"extensions\":[{\"name\":\"on_input\",\"opts\":{}}]}");
    }

    #[test]
    fn embed_extensions_into_existing_json() {
        let got = embed_extensions_into_json(
            "{\"uri\": \"ws://x\"}",
            &[(
                "on_interval".to_string(),
                vec![("interval_ms".to_string(), "5000".to_string())],
            )],
        );

        assert!(got.starts_with("{\"uri\": \"ws://x\""));
        assert!(got.contains("\"extensions\":[{\"name\":\"on_interval\",\"opts\":{\"interval_ms\": \"5000\"}}]"));
        assert!(got.ends_with('}'));
    }

    #[test]
    fn embed_multiple_extensions_keeps_order() {
        let got = embed_extensions_into_json(
            "{}",
            &[
                ("on_input".to_string(), vec![("position".to_string(), "0".to_string())]),
                ("on_terminate".to_string(), vec![]),
            ],
        );

        let input_pos = got.find("on_input").expect("has on_input");
        let term_pos = got.find("on_terminate").expect("has on_terminate");
        assert!(input_pos < term_pos, "on_input must precede on_terminate in serialized output");
    }
}
