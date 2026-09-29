use flz_transport::errors::FlezhaError;

use crate::args::{OptSpec, Positional, Reading};
use crate::dispatch::{Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, text};
use crate::settings::{
    extract_hook_flags, extract_opts, extract_x_flags, filter_out_hook_entries, serialize_flags,
};

const PLUGIN_ID: &[Positional] = &[Positional::required("<plugin_id>")];
const SOURCE: &[Positional] = &[Positional::required("<path-or-https-url>")];
const INSTALLED: OptSpec = OptSpec::switch("--installed", "--installed");

/// Where the verbs that place packages run, in a VM of their own.
const LOCAL: &str = "Flezha.Service.Cli.Plugin";

pub(crate) const TABLE: Command = Command {
    path: "flz plugin",
    help: help::PLUGIN,
    verbs: &[
        Verb::new("enable", handle_enable)
            .taking(&[
                Positional::required("<plugin_id>"),
                Positional::required("<provider_id>"),
            ])
            .open(),
        Verb::new("disable", handle_disable).taking(PLUGIN_ID),
        // `help` is a verb here, not a spelling of the screen: with a provider
        // id it asks the controller for that provider's own help.
        Verb::new("help", handle_help).taking(&[Positional::optional("<provider_id>")]),
        Verb::new("list", handle_list).reading(&[INSTALLED]),
        Verb::new("install", |read| {
            local(read, "install", &[read.pos(SOURCE[0].name)])
        })
        .taking(SOURCE),
        Verb::new("uninstall", |read| {
            local(read, "uninstall", &[read.pos(PLUGIN_ID[0].name)])
        })
        .taking(PLUGIN_ID),
        Verb::new("sync", |read| local(read, "sync", &[])),
    ],
    help_verbs: &[],
    missing: Missing::Refuse,
    unknown_prints_help: false,
};

/// `flz plugin install`, `uninstall` and `sync` act on the plugin root, never on
/// a running controller, so they run where no controller runs yet.
fn local(read: &Reading, verb: &str, args: &[&str]) {
    let args: Vec<String> = args.iter().map(|arg| text(arg)).collect();
    rpc::eval(
        LOCAL,
        "eval",
        &[text(verb), format!("[{}]", args.join(", "))],
        read,
    );
}

fn handle_enable(read: &Reading) {
    println!("Enabling plugin...");

    rpc::run(
        "enable_plugin",
        &[
            text(read.pos(PLUGIN_ID[0].name)),
            text(read.pos("<provider_id>")),
            text(&plugin_flags_json(read.rest())),
            text(&opts_json(read.rest())),
        ],
        &[],
        read,
    );
}

/// The plugin's own settings: the `--X` flags, with any declared
/// `--hook=NAME` sub-options embedded so the Elixir side sees them.
fn plugin_flags_json(args: &[String]) -> String {
    let base = serialize_flags(&extract_x_flags(args));
    let hooks = extract_hook_flags(args);

    if hooks.is_empty() {
        base
    } else {
        embed_hooks_into_json(&base, &hooks)
    }
}

/// The plain CLI options. The hook entries are removed first, so the
/// option parser does not read them as options of their own; the credential is
/// the walk's and never reaches this document.
fn opts_json(args: &[String]) -> String {
    serialize_flags(&extract_opts(&filter_out_hook_entries(args)))
}

fn handle_disable(read: &Reading) {
    println!("Disabling plugin...");

    rpc::one_arg(read, PLUGIN_ID[0].name, "disable_plugin");
}

fn handle_help(read: &Reading) {
    let Some(provider_id) = read.maybe_pos("<provider_id>") else {
        help::render(help::PLUGIN);
        return;
    };

    println!("Getting help for plugin '{}'...", provider_id);

    rpc::run("plugin_help", &[text(provider_id)], &[], read);
}

/// Two collections, and two flags that must not do each other's job:
/// `--installed` chooses which list, the output format chooses how it is
/// rendered. The provider registry has one rendering, so asking it for JSON is
/// refused rather than silently answered with the other collection.
fn handle_list(read: &Reading) {
    let installed = read.switch(INSTALLED.long);

    if !installed && read.output_format() == "json" {
        rpc::fail(FlezhaError::with(
            "usage.text_only",
            [
                ("what", "the provider registry".into()),
                ("alternative", "flz plugin list --installed --json".into()),
            ],
        ));
    }

    if installed {
        rpc::run("list_installed_plugins", &[], &[], read);
    } else {
        rpc::run("list_plugins", &[], &[], read);
    }
}

// Serialize a `"hooks": [{"name": "...", "opts": {...}}, ...]` list into
// the plugin_flags JSON object.
fn embed_hooks_into_json(base_json: &str, hooks: &[(String, Vec<(String, String)>)]) -> String {
    let entries: Vec<String> = hooks
        .iter()
        .map(|(name, opts)| {
            let opts_json = serialize_flags(opts);
            format!("{{\"name\":\"{}\",\"opts\":{}}}", name, opts_json)
        })
        .collect();

    let hooks_fragment = format!("\"hooks\":[{}]", entries.join(","));

    if base_json.trim() == "{}" {
        return format!("{{{}}}", hooks_fragment);
    }
    // Splice before the object's OWN closing brace, not before every trailing
    // one: a JSON-valued flag written last (`--Xheaders='{"k":"v"}'`) ends the
    // payload with `}}`, and eating both leaves a body that cannot decode.
    let body = base_json
        .trim_end()
        .strip_suffix('}')
        .expect("plugin flags always serialize to a JSON object");
    if body.trim_end().ends_with('{') {
        format!("{}{}}}", body, hooks_fragment)
    } else {
        format!("{},{}}}", body, hooks_fragment)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embed_hooks_into_empty_base() {
        let got = embed_hooks_into_json("{}", &[("on_input".to_string(), vec![])]);

        assert_eq!(got, "{\"hooks\":[{\"name\":\"on_input\",\"opts\":{}}]}");
    }

    #[test]
    fn embed_hooks_into_existing_json() {
        let got = embed_hooks_into_json(
            "{\"uri\": \"ws://x\"}",
            &[(
                "on_interval".to_string(),
                vec![("interval_ms".to_string(), "5000".to_string())],
            )],
        );

        assert!(got.starts_with("{\"uri\": \"ws://x\""));
        assert!(got.contains(
            "\"hooks\":[{\"name\":\"on_interval\",\"opts\":{\"interval_ms\": \"5000\"}}]"
        ));
        assert!(got.ends_with('}'));
    }

    /// fails when: a JSON-valued flag written last loses its closing brace —
    /// the controller then rejects the whole enable with a decode error, which
    /// is every attach that carries `--Xheaders`.
    #[test]
    fn embed_hooks_after_a_json_valued_flag_stays_decodable() {
        let got = embed_hooks_into_json(
            "{\"url\": \"http://x\", \"headers\": {\"authorization\": \"Bearer t\"}}",
            &[("on_terminate".to_string(), vec![])],
        );

        let parsed: serde_json::Value = serde_json::from_str(&got).expect("decodable");
        assert_eq!(parsed["headers"]["authorization"], "Bearer t");
        assert_eq!(parsed["hooks"][0]["name"], "on_terminate");
    }

    #[test]
    fn embed_multiple_hooks_keeps_order() {
        let got = embed_hooks_into_json(
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
