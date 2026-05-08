use std::process::{exit, Command};

use crate::utils::{
    extract_extension_flags, extract_opts_with_default_namespace, extract_x_flags,
    filter_out_extension_entries, get_flezha_command, serialize_flags,
};

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
        "install" => handle_install(args),
        "upgrade" => handle_upgrade(args),
        "uninstall" => handle_uninstall(args),
        "info" => handle_info(args),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'install', 'upgrade', 'uninstall', \
                 'info', 'enable', 'disable', 'help', or 'list'.",
                args[0]
            );
            exit(1);
        }
    }
}

fn handle_install(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Usage: flz plugin install <path-to-.fpz>");
        exit(1);
    }

    let path = &args[1];
    let command = get_flezha_command();
    let rpc_command = format!(
        "Flezha.Service.CliManager.install_plugin(\"{}\", \"{{}}\")",
        path.replace('"', "\\\"")
    );

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

fn handle_upgrade(args: &[String]) {
    if args.len() < 3 {
        eprintln!("Usage: flz plugin upgrade <plugin_id> <path-to-.fpz>");
        exit(1);
    }

    let plugin_id = &args[1];
    let path = &args[2];
    let command = get_flezha_command();
    let rpc_command = format!(
        "Flezha.Service.CliManager.upgrade_plugin(\"{}\", \"{}\", \"{{}}\")",
        plugin_id,
        path.replace('"', "\\\"")
    );

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

fn handle_uninstall(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Usage: flz plugin uninstall <plugin_id>");
        exit(1);
    }

    let plugin_id = &args[1];
    let command = get_flezha_command();
    let rpc_command = format!(
        "Flezha.Service.CliManager.uninstall_plugin(\"{}\", \"{{}}\")",
        plugin_id
    );

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
    if args.len() < 2 {
        eprintln!("Usage: flz plugin info <plugin_id>");
        exit(1);
    }

    let plugin_id = &args[1];
    let command = get_flezha_command();
    let rpc_command = format!(
        "Flezha.Service.CliManager.installed_plugin_info(\"{}\")",
        plugin_id
    );

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

// Check whether an id is tracked by the installer. Returns true/false based
// on the stdout output of the RPC call.
fn is_installed_plugin(plugin_id: &str) -> bool {
    let command = get_flezha_command();
    let rpc_command = format!(
        "Flezha.Service.CliManager.installed_plugin?(\"{}\") |> IO.puts()",
        plugin_id
    );

    let output = Command::new(command)
        .arg("rpc")
        .arg(rpc_command)
        .output()
        .ok();

    match output {
        Some(o) if o.status.success() => {
            let stdout = String::from_utf8_lossy(&o.stdout);
            stdout.trim().ends_with("true")
        }
        _ => false,
    }
}

fn handle_enable(args: &[String]) {
    let example_usage = "Usage: flz plugin enable <plugin_id> [<provider_id>] [Xkey=value] [--option value]\nExample: flz plugin enable relay_kafka_producer my_provider Xhosts=kafka:9092 Xtopic=my_topic --namespace mynamespace";

    if args.len() < 2 {
        eprintln!("Error: Invalid number of arguments.\n {}", example_usage);
        exit(1);
    }

    let plugin_id = &args[1];

    // Dispatch installed plugins through the Installer service.
    if is_installed_plugin(plugin_id) {
        let command = get_flezha_command();
        let rpc_command = format!(
            "Flezha.Service.CliManager.installer_enable_plugin(\"{}\", \"{{}}\")",
            plugin_id
        );

        println!("Enabling installed plugin {}...", plugin_id);

        let status = Command::new(command)
            .arg("rpc")
            .arg(rpc_command)
            .status()
            .expect("Failed to execute command");

        if !status.success() {
            eprintln!("Error: Command execution failed.");
            exit(1);
        }

        return;
    }

    if args.len() < 3 {
        eprintln!("Error: Invalid number of arguments.\n {}", example_usage);
        exit(1);
    }

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

    // Dispatch installed plugins through the Installer service; legacy
    // plugins keep their existing code path.
    let rpc_command = if is_installed_plugin(plugin_id) {
        format!(
            "Flezha.Service.CliManager.installer_disable_plugin(\"{}\", \"{{}}\")",
            plugin_id
        )
    } else {
        format!(
            "Flezha.Service.CliManager.disable_plugin(\"{}\")",
            plugin_id
        )
    };

    println!("Disabling plugin...");

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

    let rpc_command = format!("Flezha.Service.CliManager.plugin_help(\"{}\")", provider_id);

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

fn handle_list(args: &[String]) {
    let command = get_flezha_command();
    let want_json = args.iter().any(|a| a == "--json");
    let scope = args
        .iter()
        .find(|a| a.as_str() == "--installed" || a.as_str() == "--providers");

    let rpc_command = match scope.map(|s| s.as_str()) {
        Some("--installed") => {
            if want_json {
                "Flezha.Service.CliManager.list_installed_plugins(\"json\")".to_string()
            } else {
                "Flezha.Service.CliManager.list_installed_plugins(\"text\")".to_string()
            }
        }
        _ => {
            // Default: provider registry (legacy behaviour). `--json` also
            // works for the installed list via the explicit --installed
            // switch, so no surprise for users of the old command.
            if want_json {
                "Flezha.Service.CliManager.list_installed_plugins(\"json\")".to_string()
            } else {
                "Flezha.Service.CliManager.list_plugins()".to_string()
            }
        }
    };

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
    println!("    install    Install a plugin from a .fpz package");
    println!("    upgrade    Upgrade an installed plugin to a new version");
    println!("    uninstall  Remove an installed plugin");
    println!("    enable     Enable a plugin (installed package or legacy provider)");
    println!("    disable    Disable a plugin");
    println!("    list       List plugins: `--installed` (catalog) or providers (default)");
    println!("    info       Show details for an installed plugin");
    println!("    help       Show help for a specific plugin provider");
    println!();
    println!("EXAMPLES:");
    println!("    flz plugin install /path/to/kafka_ingest-1.0.0.fpz");
    println!("    flz plugin list --installed");
    println!("    flz plugin list --installed --json");
    println!("    flz plugin info kafka_ingest");
    println!("    flz plugin enable kafka_ingest");
    println!("    flz plugin upgrade kafka_ingest /path/to/kafka_ingest-1.1.0.fpz");
    println!("    flz plugin uninstall kafka_ingest");
    println!("    # Legacy provider enable (compile-time providers):");
    println!("    flz plugin enable ws_id relay_websocket --Xuri=ws://localhost:4500 \\");
    println!("        --extension=on_input --on_input.position=0");
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
        let got = embed_extensions_into_json("{}", &[("on_input".to_string(), vec![])]);

        assert_eq!(
            got,
            "{\"extensions\":[{\"name\":\"on_input\",\"opts\":{}}]}"
        );
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
        assert!(got.contains(
            "\"extensions\":[{\"name\":\"on_interval\",\"opts\":{\"interval_ms\": \"5000\"}}]"
        ));
        assert!(got.ends_with('}'));
    }

    #[test]
    fn embed_multiple_extensions_keeps_order() {
        let got = embed_extensions_into_json(
            "{}",
            &[
                (
                    "on_input".to_string(),
                    vec![("position".to_string(), "0".to_string())],
                ),
                ("on_terminate".to_string(), vec![]),
            ],
        );

        let input_pos = got.find("on_input").expect("has on_input");
        let term_pos = got.find("on_terminate").expect("has on_terminate");
        assert!(
            input_pos < term_pos,
            "on_input must precede on_terminate in serialized output"
        );
    }
}
