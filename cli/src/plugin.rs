use serde_json::{Map, Value};

use crate::args::{OptSpec, Positional, Reading, NAMESPACE};
use crate::dispatch::{Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, text};
use crate::settings::{
    extract_hook_flags, extract_opts, extract_x_flags, filter_out_hook_entries, flags_object,
};

const PLUGIN_ID: &[Positional] = &[Positional::required("<plugin_id>")];
const NAME: &[Positional] = &[Positional::required("<name>")];
const SOURCE: &[Positional] = &[Positional::required("<path-or-https-url>")];
const INSTALLED: OptSpec =
    OptSpec::switch("--installed", "--installed").doc("List the installed packages instead.");
const PROVIDERS: OptSpec =
    OptSpec::switch("--providers", "--providers").doc("List the providers instead.");
const SINGLE: OptSpec =
    OptSpec::switch("--single", "--single").doc("Run one copy in the whole cluster.");

/// Where the verbs that place packages run, in a VM of their own.
const LOCAL: &str = "Flezha.Plugin.Installer.Command";

pub(crate) const TABLE: Command = Command {
    path: "flz plugin",
    title: "Plugin management commands.",
    notes: Some(help::PLUGIN),
    verbs: &[
        Verb::new("enable", handle_enable)
            .taking(&[
                Positional::required("<name>"),
                Positional::required("<provider_id>"),
            ])
            .reading(&[SINGLE])
            .open()
            .calls(&["PluginManager.enable"]),
        Verb::new("bind", |read| named(read, "PluginManager.bind"))
            .taking(NAME)
            .open()
            .calls(&["PluginManager.bind"]),
        Verb::new("unbind", |read| named(read, "PluginManager.unbind"))
            .taking(NAME)
            .open()
            .calls(&["PluginManager.unbind"]),
        Verb::new("update", |read| named(read, "PluginManager.update"))
            .taking(NAME)
            .open()
            .calls(&["PluginManager.update"]),
        Verb::new("disable", |read| named(read, "PluginManager.disable"))
            .taking(NAME)
            .open()
            .calls(&["PluginManager.disable"]),
        // `help` is a verb here, not a spelling of the screen: with a provider
        // id it asks the controller for that provider's own help.
        Verb::new("help", handle_help)
            .taking(&[Positional::optional("<provider_id>")])
            .calls(&["Fleet.provider"]),
        Verb::new("list", handle_list)
            .reading(&[INSTALLED, PROVIDERS, NAMESPACE])
            .refusing(&[("--installed", "--providers")])
            .calls(&[
                "PluginManager.enabled",
                "Fleet.installed_plugins",
                "Fleet.providers",
            ]),
        Verb::new("install", |read| {
            local(read, "install", &[read.pos(SOURCE[0].name)])
        })
        .taking(SOURCE)
        .about("Place a plugin package (.fpz) from a path or an https:// URL."),
        Verb::new("uninstall", |read| {
            local(read, "uninstall", &[read.pos(PLUGIN_ID[0].name)])
        })
        .taking(PLUGIN_ID)
        .about("Remove an installed plugin package."),
        Verb::new("sync", |read| local(read, "sync", &[]))
            .about("Make the plugin root hold exactly the packages FLEZHA_PLUGINS declares."),
    ],
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
    rpc::progress_line(read, "Enabling plugin...");

    let mut fields = fields(read.rest());
    fields.insert("provider".into(), Value::from(read.pos("<provider_id>")));
    if read.switch(SINGLE.long) {
        fields.insert("single".into(), Value::from(true));
    }

    rpc::door(
        "PluginManager.enable",
        &[Value::from(read.pos(NAME[0].name)), Value::Object(fields)],
        &[],
        read,
    );
}

/// The verbs that name a plugin and carry its fields: the plain options, the
/// `X<key>=<value>` settings as `opts`, and the `--hook` families as `hooks`.
/// A verb refuses a field it does not read, so each carries all of them.
fn named(read: &Reading, name: &str) {
    rpc::door(
        name,
        &[
            Value::from(read.pos(NAME[0].name)),
            Value::Object(fields(read.rest())),
        ],
        &[],
        read,
    );
}

fn fields(args: &[String]) -> Map<String, Value> {
    let Value::Object(mut fields) = flags_object(&plain_opts(args)) else {
        unreachable!("flags serialize to an object")
    };

    let settings = extract_x_flags(args);
    if !settings.is_empty() {
        fields.insert("opts".into(), flags_object(&settings));
    }

    let hooks: Vec<Value> = extract_hook_flags(args)
        .iter()
        .map(|(name, opts)| serde_json::json!({ "name": name, "opts": flags_object(opts) }))
        .collect();
    if !hooks.is_empty() {
        fields.insert("hooks".into(), Value::Array(hooks));
    }

    fields
}

/// The plain `--key value` words: every word a row does not declare, less the
/// `--hook` families and the settings.
fn plain_opts(args: &[String]) -> Vec<(String, String)> {
    extract_opts(&filter_out_hook_entries(args))
}

fn handle_help(read: &Reading) {
    let Some(provider_id) = read.maybe_pos("<provider_id>") else {
        help::render(&help::command_screen(&TABLE));
        return;
    };

    rpc::progress_line(
        read,
        &format!("Getting help for plugin '{}'...", provider_id),
    );
    rpc::door("Fleet.provider", &[Value::from(provider_id)], &[], read);
}

fn handle_list(read: &Reading) {
    if read.switch(INSTALLED.long) {
        rpc::bare(read, "Fleet.installed_plugins");
    } else if read.switch(PROVIDERS.long) {
        rpc::bare(read, "Fleet.providers");
    } else {
        rpc::door("PluginManager.enabled", &[rpc::namespace(read)], &[], read);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(words: &[&str]) -> Vec<String> {
        words.iter().map(|word| word.to_string()).collect()
    }

    /// fails when: a JSON-valued setting loses its shape, which the controller
    /// then refuses as an option of the wrong type.
    #[test]
    fn a_json_valued_setting_stays_an_object() {
        let got = fields(&words(&[
            r#"--Xheaders={"authorization":"Bearer t"}"#,
            "--hook=on_terminate",
        ]));

        assert_eq!(got["opts"]["headers"]["authorization"], "Bearer t");
        assert_eq!(got["hooks"][0]["name"], "on_terminate");
    }

    #[test]
    fn hooks_keep_the_order_they_were_written_in() {
        let got = fields(&words(&[
            "--hook=on_input",
            "--on_input.position=0",
            "--hook=on_terminate",
        ]));

        assert_eq!(got["hooks"][0]["name"], "on_input");
        assert_eq!(got["hooks"][0]["opts"]["position"], "0");
        assert_eq!(got["hooks"][1]["name"], "on_terminate");
    }

    #[test]
    fn plain_options_are_fields_and_nothing_else_is_added() {
        let got = fields(&words(&["--relay_id", "op1", "--namespace=prod"]));

        assert_eq!(got["relay_id"], "op1");
        assert_eq!(got["namespace"], "prod");
        assert!(got.get("opts").is_none() && got.get("hooks").is_none());
    }
}
