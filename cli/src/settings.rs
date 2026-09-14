//! The settings document a verb carries but does not declare.
//!
//! `flz plugin enable` and `flz namespace create` take a map whose keys belong
//! to a provider rather than to this surface: `X<key>=<value>` settings, the
//! `--hook NAME` families with their `--NAME.key` sub-options, and the plain
//! `--key value` options the controller reads beside them. The walk in
//! `args.rs` steps over them and hands them on unread; these readers are what
//! reads them.
//!
//! They take the WORDS a document was written in, never a verb's argument
//! list: what the surface itself declares is read in one place, and it is not
//! here.

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

pub(crate) fn serialize_flags(flags: &[(String, String)]) -> String {
    if flags.is_empty() {
        return "{}".to_string();
    }

    let json_pairs: Vec<String> = flags
        .iter()
        .map(|(key, value)| {
            // Inline a value as raw JSON only when it actually parses. Looking
            // merely JSON-shaped is not enough: a subscription template is a
            // JSON-shaped string carrying `{{items}}` / `{{req_id}}`
            // placeholders, which no JSON parser accepts, and inlining it
            // produced a document the controller could not decode.
            let inline = matches!(value.trim_start().chars().next(), Some('{') | Some('['))
                && serde_json::from_str::<serde_json::Value>(value).is_ok();
            if inline {
                format!("\"{}\": {}", key, value)
            } else {
                // Serialize as a JSON string so quotes and backslashes in the
                // value cannot break out of it.
                format!("\"{}\": {}", key, serde_json::Value::String(value.clone()))
            }
        })
        .collect();

    format!("{{{}}}", json_pairs.join(", "))
}

/// The repeatable plugin settings: `--X<key>=<value>`, `--X<key> <value>`, and
/// the bare `X<key>=<value>` every help screen and usage line advertises.
///
/// The `=` is what carries the bare spelling. `--X` announces itself with its
/// dashes, so it may take its value from the next word; a bare `Xhosts` has no
/// such signal and is indistinguishable from a plugin id, so only the `=` form
/// is read without dashes, and a word whose key does not begin with `X` stays
/// positional.
pub(crate) fn extract_x_flags(words: &[String]) -> Vec<(String, String)> {
    let mut flags = Vec::new();
    let mut i = 0;

    while i < words.len() {
        let arg = &words[i];

        if let Some((key, value)) = bare_setting(arg) {
            flags.push((key.to_string(), strip_quotes(value)));
        } else if let Some(rest) = arg.strip_prefix("--X") {
            if let Some((key, value)) = rest.split_once('=') {
                flags.push((key.to_string(), strip_quotes(value)));
            } else if let Some(next) = words.get(i + 1) {
                flags.push((rest.to_string(), strip_quotes(next)));
                i += 1;
            }
        }

        i += 1;
    }

    flags
}

/// `X<key>=<value>` written without its dashes, and only with a key to name:
/// `X=v` names nothing, so it is left where it stands.
pub(crate) fn bare_setting(arg: &str) -> Option<(&str, &str)> {
    let (key, value) = arg.strip_prefix('X')?.split_once('=')?;
    (!key.is_empty()).then_some((key, value))
}

/// What declares a hook, stated once: `--hook=NAME`, or `--hook NAME`
/// consuming the word after it. Answers the name and how many words it took.
fn declared_hook(words: &[String], i: usize) -> Option<(String, usize)> {
    let arg = &words[i];
    if let Some(rest) = arg.strip_prefix("--hook=") {
        return Some((strip_quotes(rest), 1));
    }
    if arg == "--hook" {
        return words.get(i + 1).map(|next| (strip_quotes(next), 2));
    }
    None
}

/// `--NAME.key[=value]` → `(NAME, "key[=value]")`.
pub(crate) fn dotted_option(arg: &str) -> Option<(&str, &str)> {
    arg.strip_prefix("--")?.split_once('.')
}

/// Repeatable `--hook=NAME` flags with their `--NAME.key=value`
/// sub-options, in declaration order. A dotted option only binds to a name
/// already declared to its left.
pub(crate) fn extract_hook_flags(words: &[String]) -> Vec<(String, Vec<(String, String)>)> {
    let mut out: Vec<(String, Vec<(String, String)>)> = Vec::new();
    let mut i = 0;

    while i < words.len() {
        if let Some((name, taken)) = declared_hook(words, i) {
            if !out.iter().any(|(known, _)| *known == name) {
                out.push((name, Vec::new()));
            }
            i += taken;
            continue;
        }

        if let Some((prefix, remainder)) = dotted_option(&words[i]) {
            if let Some((_, opts)) = out.iter_mut().find(|(name, _)| name == prefix) {
                if let Some((key, value)) = remainder.split_once('=') {
                    opts.push((key.to_string(), strip_quotes(value)));
                } else if let Some(next) = words.get(i + 1) {
                    opts.push((remainder.to_string(), strip_quotes(next)));
                    i += 1;
                }
            }
        }

        i += 1;
    }

    out
}

/// Strip the hook entries from an argument list so the option parser
/// below does not read them as plain CLI options.
pub(crate) fn filter_out_hook_entries(words: &[String]) -> Vec<String> {
    let declared = extract_hook_flags(words);
    let mut out = Vec::new();
    let mut i = 0;

    while i < words.len() {
        if let Some((_, taken)) = declared_hook(words, i) {
            i += taken;
            continue;
        }

        let arg = &words[i];
        let sub_option = dotted_option(arg)
            .is_some_and(|(prefix, _)| declared.iter().any(|(name, _)| name == prefix));
        if !sub_option {
            out.push(arg.clone());
        }

        i += 1;
    }

    out
}

/// Single-letter option aliases.
const SHORT_KEYS: &[(&str, &str)] = &[("n", "namespace"), ("o", "output")];

/// The plain words a plugin enable's two documents carry: written like an
/// option and handed on unread by the walk, because the controller reads them
/// rather than a row. `--X<key>=<value>` carries the provider's own settings.
/// Declared here, where the parser that hands them on lives, so that the
/// screens may teach them without a row claiming the word. Nothing in the walk
/// consults it: the readers are the option-row gate, which exempts these words,
/// and the test below, which proves each still reaches a document.
#[cfg(test)]
pub(crate) const SETTING_KEYS: &[&str] = &["--hook", "--namespace", "--operative_id"];

/// The plain `--key value` options of a plugin enable, with `namespace`
/// defaulted to `"default"`. Both spellings of a setting are skipped:
/// `extract_x_flags` owns them, and `option_key` refuses the bare one because
/// it carries no leading `-`.
pub(crate) fn extract_opts(words: &[String]) -> Vec<(String, String)> {
    let mut flags = Vec::new();
    let mut i = 0;

    while i < words.len() {
        let arg = &words[i];
        let paired = words.get(i + 1);

        if arg.starts_with("--X") {
            if !arg.contains('=') && paired.is_some() {
                i += 1;
            }
        } else if let Some((flag, value)) = arg.split_once('=') {
            // `--output=json` and `-o=json` are one option in two spellings, so
            // both resolve their key through the same alias table as the
            // space-separated forms below.
            if let Some(key) = option_key(flag) {
                flags.push((key, strip_quotes(value)));
            }
        } else if let (Some(key), Some(value)) = (option_key(arg), paired) {
            flags.push((key, strip_quotes(value)));
            i += 1;
        }

        i += 1;
    }

    if !flags.iter().any(|(key, _)| key == "namespace") {
        flags.push(("namespace".to_string(), "default".to_string()));
    }

    flags
}

fn option_key(arg: &str) -> Option<String> {
    if let Some(long) = arg.strip_prefix("--") {
        return Some(long.to_string());
    }
    let short = arg.strip_prefix('-')?;
    let key = SHORT_KEYS
        .iter()
        .find(|(alias, _)| *alias == short)
        .map_or(short, |(_, name)| *name);
    Some(key.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::owned as v;

    /// A subscription template is JSON-shaped but carries `{{items}}` /
    /// `{{req_id}}` placeholders. Inlining it raw produced a document the
    /// controller rejected with a Jason.DecodeError, so the plugin could never
    /// be enabled from its own documented invocation.
    #[test]
    fn json_shaped_but_unparsable_values_are_quoted_as_strings() {
        let template =
            r#"{"method":"subscribe","params":{"symbol":{{items}}},"req_id":{{req_id}}}"#;
        let out = serialize_flags(&[("subscribe_template".to_string(), template.to_string())]);
        let parsed: serde_json::Value = serde_json::from_str(&out).expect("must stay decodable");
        assert_eq!(
            parsed["subscribe_template"],
            serde_json::Value::String(template.to_string())
        );
    }

    /// Real JSON must still be inlined as JSON — `payload_metadata` and
    /// `subscription_headers` are objects the controller reads structurally.
    #[test]
    fn parsable_json_values_are_still_inlined() {
        let out = serialize_flags(&[(
            "payload_metadata".to_string(),
            r#"{"channel":"trades_in"}"#.to_string(),
        )]);
        let parsed: serde_json::Value = serde_json::from_str(&out).expect("decodable");
        assert_eq!(parsed["payload_metadata"]["channel"], "trades_in");
    }

    /// A plain value containing a quote must not break out of its own string.
    #[test]
    fn quotes_in_plain_values_are_escaped() {
        let out = serialize_flags(&[(
            "ignore_pattern".to_string(),
            r#""event":"heartbeat""#.to_string(),
        )]);
        let parsed: serde_json::Value = serde_json::from_str(&out).expect("decodable");
        assert_eq!(parsed["ignore_pattern"], r#""event":"heartbeat""#);
    }
    #[test]
    fn extract_hook_flags_single_no_opts() {
        let args = v(&["--hook=on_input"]);
        let got = extract_hook_flags(&args);
        assert_eq!(got, vec![("on_input".to_string(), vec![])]);
    }

    #[test]
    fn extract_hook_flags_multiple_with_dotted_opts() {
        let args = v(&[
            "--hook=on_input",
            "--on_input.position=0",
            "--hook=on_interval",
            "--on_interval.interval_ms=5000",
        ]);

        let got = extract_hook_flags(&args);

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
    fn extract_hook_flags_ignores_dotted_opts_for_unknown_hook() {
        // `on_unknown.x=1` has no corresponding `--hook=on_unknown`, so
        // it must not be captured.
        let args = v(&["--hook=on_input", "--on_unknown.x=1"]);
        let got = extract_hook_flags(&args);
        assert_eq!(got, vec![("on_input".to_string(), vec![])]);
    }

    #[test]
    fn extract_hook_flags_space_separated_form() {
        let args = v(&["--hook", "on_input", "--hook", "on_terminate"]);

        let got = extract_hook_flags(&args);

        assert_eq!(
            got,
            vec![
                ("on_input".to_string(), vec![]),
                ("on_terminate".to_string(), vec![]),
            ]
        );
    }

    /// Both spellings of a declaration are read through one rule, so the filter
    /// leaves neither a hook nor one of its sub-options behind.
    #[test]
    fn the_filter_leaves_no_hook_word_behind() {
        let args = v(&[
            "--hook=on_input",
            "--hook",
            "on_terminate",
            "--on_input.position=0",
        ]);

        assert_eq!(filter_out_hook_entries(&args), Vec::<String>::new());
    }
    #[test]
    fn extract_opts_ignores_hook_flags() {
        let args = v(&[
            "--hook=on_input",
            "--on_input.position=0",
            "--namespace=prod",
        ]);

        let filtered = filter_out_hook_entries(&args);
        let opts = extract_opts(&filtered);

        assert!(opts.iter().any(|(k, v)| k == "namespace" && v == "prod"));
        assert!(!opts.iter().any(|(k, _)| k == "hook"));
        assert!(!opts.iter().any(|(k, _)| k.starts_with("on_input")));
    }

    /// The option scanner of a plugin enable reads the same four forms as
    /// `extract_param`: a short flag written with `=` is the option the
    /// operator typed, and a bare `key=value` word is a positional argument.
    #[test]
    fn extract_opts_reads_all_four_forms() {
        for form in [
            v(&["--output=json"]),
            v(&["--output", "json"]),
            v(&["-o=json"]),
            v(&["-o", "json"]),
        ] {
            let opts = extract_opts(&form);
            assert!(
                opts.contains(&("output".to_string(), "json".to_string())),
                "{:?} -> {:?}",
                form,
                opts
            );
        }

        let positional = extract_opts(&v(&["topic=t"]));
        assert!(!positional.iter().any(|(key, _)| key == "topic"));
    }

    /// fails when: a plain-word settings key the screens teach reaches neither
    /// of the two documents a plugin enable sends, so an operator types a word
    /// the verb drops. No row claims these — that is what lets the settings
    /// document own them — so this is what stands in for a row's guarantee.
    #[test]
    fn every_plain_settings_key_reaches_one_of_the_two_documents() {
        let mut dropped: Vec<String> = Vec::new();

        for key in SETTING_KEYS {
            let joined = format!("{}=on_input", key);

            for form in [v(&[&joined]), v(&[key, "on_input"])] {
                let opts = extract_opts(&filter_out_hook_entries(&form));
                let hooks = extract_hook_flags(&form);
                let bare = key.trim_start_matches('-');

                let carried = hooks.iter().any(|(name, _)| name == "on_input")
                    || opts
                        .iter()
                        .any(|(read, value)| read == bare && value == "on_input");

                if !carried {
                    dropped.push(format!("{:?} -> opts {:?}, hooks {:?}", form, opts, hooks));
                }
            }
        }

        assert!(
            dropped.is_empty(),
            "{} plain settings spellings reach no document:\n{}",
            dropped.len(),
            dropped.join("\n")
        );
    }

    /// Every settings-shaped token in a piece of operator-visible text: a
    /// word spelled `X<key>=<value>`, with or without its dashes, freed of the
    /// brackets a usage line wraps it in.
    fn advertised_settings(text: &str) -> Vec<&str> {
        text.split_whitespace()
            .map(|word| word.trim_matches(|c: char| "[]`'\",".contains(c)))
            .filter(|word| {
                let bare = word.strip_prefix("--").unwrap_or(word);
                bare.starts_with('X') && bare.contains('=')
            })
            .collect()
    }

    /// The agreement, not one spelling of it: every plugin setting the CLI's
    /// own screens and usage lines print must be one the parser reads back
    /// with the key and value the text shows. An example added to a screen is
    /// covered by this the day it is written.
    #[test]
    fn every_advertised_plugin_setting_is_read_by_the_parser() {
        // The three screens are the whole surface that prints a settings
        // EXAMPLE. A usage line prints the shape (`X<key>=<value>`) because
        // it is rendered from the row, and the row declares that the verb
        // takes settings, not which keys a provider has.
        let printed = [
            ("flz help", crate::help::FLZ),
            ("flz plugin help", crate::help::PLUGIN),
            ("flz namespace help", crate::help::NAMESPACE),
        ];

        let mut walked = 0;
        let mut unread: Vec<String> = Vec::new();

        for (source, text) in printed {
            for token in advertised_settings(text) {
                walked += 1;
                let bare = token.strip_prefix("--").unwrap_or(token);
                let (key, value) = bare[1..].split_once('=').expect("filtered on '='");
                let got = extract_x_flags(&v(&[token]));

                if got != vec![(key.to_string(), value.to_string())] {
                    unread.push(format!("{}: {:?} -> {:?}", source, token, got));
                }
            }
        }

        assert!(walked >= 2, "the walk found only {} settings", walked);
        assert!(
            unread.is_empty(),
            "{} of {} advertised plugin settings are not read by the parser:\n{}",
            unread.len(),
            walked,
            unread.join("\n")
        );
    }

    /// The comma-separated channel filter the plugin pages teach is one
    /// value, in both spellings a `--X` flag takes: the comma separates names
    /// for the provider that reads them and nothing for this parser, and the
    /// document carries it as a JSON string rather than as a list.
    #[test]
    fn a_comma_separated_setting_is_carried_whole() {
        let flags = extract_x_flags(&v(&["--Xchannel=alerts,audit"]));
        assert_eq!(
            flags,
            vec![("channel".to_string(), "alerts,audit".to_string())]
        );
        assert_eq!(serialize_flags(&flags), r#"{"channel": "alerts,audit"}"#);

        assert_eq!(
            extract_x_flags(&v(&["--Xchannel_out", "alerts,audit"])),
            vec![("channel_out".to_string(), "alerts,audit".to_string())]
        );
    }

    /// The `=` is what carries the bare spelling. Without dashes there is
    /// nothing else to separate a setting from a positional word, so a bare
    /// `X<key>` names nothing and eats nothing.
    #[test]
    fn a_bare_setting_needs_its_equals_and_consumes_no_word() {
        assert!(extract_x_flags(&v(&["Xhosts", "kafka:9092"])).is_empty());
        assert!(extract_x_flags(&v(&["X=v"])).is_empty());
        assert!(extract_x_flags(&v(&["topic=t"])).is_empty());
    }

    #[test]
    fn extract_opts_aliases_short_keys_and_defaults_the_namespace() {
        let opts = extract_opts(&v(&[
            "-n",
            "prod",
            "-o",
            "json",
            "--Xhosts",
            "kafka:9092",
            "Xtopic=my_topic",
        ]));
        assert!(opts.contains(&("namespace".to_string(), "prod".to_string())));
        assert!(opts.contains(&("output".to_string(), "json".to_string())));
        // Both spellings of a setting belong to `extract_x_flags`; neither
        // reaches the plain options under any key.
        assert!(!opts
            .iter()
            .any(|(k, _)| k.starts_with('X') || k == "hosts" || k == "topic"));

        let defaulted = extract_opts(&v(&["--topic", "t"]));
        assert!(defaulted.contains(&("namespace".to_string(), "default".to_string())));
    }
}
