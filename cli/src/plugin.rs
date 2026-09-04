use std::process::exit;

use crate::args::{
    self, extract_extension_flags, extract_opts, extract_x_flags, filter_out_extension_entries,
    serialize_flags,
};
use crate::dispatch::Command;
use crate::help;
use crate::rpc::{self, text};

pub(crate) const TABLE: Command = Command {
    help: help::PLUGIN,
    verbs: &[
        ("enable", handle_enable),
        ("disable", handle_disable),
        // `help` is a verb here, not a spelling of the screen: with a provider
        // id it asks the controller for that provider's own help.
        ("help", handle_help),
        ("list", handle_list),
        ("install", handle_install),
        ("upgrade", handle_upgrade),
        ("uninstall", handle_uninstall),
        ("info", handle_info),
    ],
    help_verbs: &[],
    missing: Some("Error: No plugin command specified."),
    unknown: "Error: Invalid argument '{}'. Use 'install', 'upgrade', 'uninstall', \
              'info', 'enable', 'disable', 'help', or 'list'.",
    unknown_prints_help: false,
};

fn handle_install(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Usage: flz plugin install <path-to-.fpz>");
        exit(1);
    }

    rpc::run(
        "install_plugin",
        &[text(&args[1])],
        &[],
        &args[2..],
        rpc::REFUSED,
    );
}

fn handle_upgrade(args: &[String]) {
    if args.len() < 3 {
        eprintln!("Usage: flz plugin upgrade <plugin_id> <path-to-.fpz>");
        exit(1);
    }

    rpc::run(
        "upgrade_plugin",
        &[text(&args[1]), text(&args[2])],
        &[],
        &args[3..],
        rpc::REFUSED,
    );
}

fn handle_uninstall(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Usage: flz plugin uninstall <plugin_id>");
        exit(1);
    }

    rpc::run(
        "uninstall_plugin",
        &[text(&args[1])],
        &[],
        &args[2..],
        rpc::REFUSED,
    );
}

fn handle_info(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Usage: flz plugin info <plugin_id>");
        exit(1);
    }

    rpc::run(
        "installed_plugin_info",
        &[text(&args[1])],
        &[],
        &args[2..],
        rpc::REFUSED,
    );
}

// Check whether an id is tracked by the installer. Returns true/false based
// on the stdout output of the RPC call; the probe carries the caller's token
// like every other request, because the read is a gated one.
fn is_installed_plugin(plugin_id: &str, args: &[String]) -> bool {
    let output = rpc::flzvm()
        .arg("rpc")
        .arg(format!(
            "{} |> IO.puts()",
            rpc::expression("installed_plugin?", &[text(plugin_id)], &[], args)
        ))
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

/// What `flz plugin enable` prints on a bad invocation. A constant because it
/// is one of the two texts that teach the bare `Xkey=value` spelling, and
/// `args.rs` asserts the parser reads every setting it advertises.
pub(crate) const ENABLE_USAGE: &str = "Usage: flz plugin enable <plugin_id> [<provider_id>] [Xkey=value] [--option value]\nExample: flz plugin enable operative_kafka_producer my_provider Xhosts=kafka:9092 Xtopic=my_topic --namespace mynamespace";

fn handle_enable(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Error: Invalid number of arguments.\n {}", ENABLE_USAGE);
        exit(1);
    }

    let plugin_id = &args[1];

    // Dispatch installed plugins through the Installer service.
    if is_installed_plugin(plugin_id, &args[2..]) {
        println!("Enabling installed plugin {}...", plugin_id);

        rpc::run(
            "installer_enable_plugin",
            &[text(plugin_id)],
            &[],
            &args[2..],
            rpc::REFUSED,
        );

        return;
    }

    if args.len() < 3 {
        eprintln!("Error: Invalid number of arguments.\n {}", ENABLE_USAGE);
        exit(1);
    }

    println!("Enabling plugin...");

    rpc::run(
        "enable_plugin",
        &[
            text(plugin_id),
            text(&args[2]),
            text(&plugin_flags_json(&args[3..])),
            text(&opts_json(&args[3..])),
        ],
        &[],
        &args[3..],
        rpc::REFUSED,
    );
}

/// The plugin's own settings: the `--X` flags, with any declared
/// `--extension=NAME` sub-options embedded so the Elixir side sees them.
fn plugin_flags_json(args: &[String]) -> String {
    let base = serialize_flags(&extract_x_flags(args));
    let extensions = extract_extension_flags(args);

    if extensions.is_empty() {
        base
    } else {
        embed_extensions_into_json(&base, &extensions)
    }
}

/// The plain CLI options. The extension entries are removed first, so the
/// option parser does not read them as options of their own, and the
/// credential rides the request's options list rather than this document.
fn opts_json(args: &[String]) -> String {
    let opts: Vec<(String, String)> = extract_opts(&filter_out_extension_entries(args))
        .into_iter()
        .filter(|(key, _)| key != "token")
        .collect();

    serialize_flags(&opts)
}

fn handle_disable(args: &[String]) {
    let example_usage = "Usage: flz plugin disable <plugin_id>";

    if args.len() < 2 {
        eprintln!("Error: Invalid number of arguments.\n {}", example_usage);
        exit(1);
    }

    let plugin_id = &args[1];

    // Dispatch installed plugins through the Installer service; legacy
    // plugins keep their existing code path.
    let installed = is_installed_plugin(plugin_id, &args[2..]);

    println!("Disabling plugin...");

    if installed {
        rpc::run(
            "installer_disable_plugin",
            &[text(plugin_id)],
            &[],
            &args[2..],
            rpc::REFUSED,
        );
    } else {
        rpc::run(
            "disable_plugin",
            &[text(plugin_id)],
            &[],
            &args[2..],
            rpc::REFUSED,
        );
    }
}

fn handle_help(args: &[String]) {
    if args.len() < 2 {
        help::render(help::PLUGIN);
        return;
    }

    let provider_id = &args[1];

    println!("Getting help for plugin '{}'...", provider_id);

    rpc::run(
        "plugin_help",
        &[text(provider_id)],
        &[],
        &args[2..],
        rpc::REFUSED,
    );
}

/// Two collections, and two flags that must not do each other's job:
/// `--installed` chooses which list, the output format chooses how it is
/// rendered. The provider registry has one rendering, so asking it for JSON is
/// refused rather than silently answered with the other collection.
fn handle_list(args: &[String]) {
    let format = args::output_format(args);
    let installed = args.iter().any(|a| a == "--installed");

    if !installed && format == "json" {
        eprintln!("Error: the provider registry is rendered as text only. Add --installed to list the installed plugins as JSON.");
        exit(1);
    }

    if installed {
        rpc::run(
            "list_installed_plugins",
            &[text(format)],
            &[],
            args,
            rpc::REFUSED,
        );
    } else {
        rpc::run("list_plugins", &[], &[], args, rpc::REFUSED);
    }
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
        return format!("{{{}}}", extensions_fragment);
    }
    // Splice before the object's OWN closing brace, not before every trailing
    // one: a JSON-valued flag written last (`--Xheaders='{"k":"v"}'`) ends the
    // payload with `}}`, and eating both leaves a body that cannot decode.
    let body = base_json
        .trim_end()
        .strip_suffix('}')
        .expect("plugin flags always serialize to a JSON object");
    if body.trim_end().ends_with('{') {
        format!("{}{}}}", body, extensions_fragment)
    } else {
        format!("{},{}}}", body, extensions_fragment)
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

    /// fails when: a JSON-valued flag written last loses its closing brace —
    /// the controller then rejects the whole enable with a decode error, which
    /// is every attach that carries `--Xheaders`.
    #[test]
    fn embed_extensions_after_a_json_valued_flag_stays_decodable() {
        let got = embed_extensions_into_json(
            "{\"url\": \"http://x\", \"headers\": {\"authorization\": \"Bearer t\"}}",
            &[("on_terminate".to_string(), vec![])],
        );

        let parsed: serde_json::Value = serde_json::from_str(&got).expect("decodable");
        assert_eq!(parsed["headers"]["authorization"], "Bearer t");
        assert_eq!(parsed["extensions"][0]["name"], "on_terminate");
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
