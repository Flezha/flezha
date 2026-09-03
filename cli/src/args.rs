//! Reading `flz`'s own arguments: one four-form scanner, and the surfaces
//! built on it.

use std::env;
use std::path::PathBuf;
use std::process::exit;
use std::str::FromStr;

/// How one word reads against one option: it carries its own value after an
/// `=`, it takes the word after it, or it is not this option at all.
pub(crate) enum Spelling<'a> {
    Attached(&'a str),
    Separated,
    Other,
}

/// One word against one option's four spellings — `--long=value`, `--long
/// value`, `-s=value`, `-s value` — named here and nowhere else. `extract_param`
/// walks this answer for the value; a reader that also refuses what it does not
/// recognise walks it to know which words the option owns.
pub(crate) fn resolve_spelling<'a>(arg: &'a str, long_key: &str, short_key: &str) -> Spelling<'a> {
    for key in [long_key, short_key] {
        if let Some(value) = arg.strip_prefix(&format!("{}=", key)) {
            return Spelling::Attached(value);
        }
    }

    if arg == long_key || arg == short_key {
        Spelling::Separated
    } else {
        Spelling::Other
    }
}

/// The four forms every `flz` option accepts, and the three answers a reader
/// needs: absent, a value, or written with nothing after it — a refusal naming
/// the flag, built here because this is the only place that knows the name. A
/// trailing `--ttl` asks for a lifetime, not for the default.
///
/// The one scanner in this crate; every reader below calls it with its own
/// default.
pub(crate) fn extract_param(
    args: &[String],
    long_key: &str,
    short_key: &str,
) -> Result<Option<String>, String> {
    for (i, arg) in args.iter().enumerate() {
        match resolve_spelling(arg, long_key, short_key) {
            Spelling::Attached(value) => return Ok(Some(value.to_string())),
            Spelling::Separated if i + 1 < args.len() => return Ok(Some(args[i + 1].clone())),
            Spelling::Separated => return Err(format!("Error: {} requires a value", long_key)),
            Spelling::Other => {}
        }
    }

    Ok(None)
}

/// The same three answers for an option whose value is a number, and the
/// second refusal that shape needs: a value this option cannot read. `ok()` on
/// the parse would map it back onto "nobody wrote it", which is the default —
/// a `--limit` nobody can read is not every record.
///
/// A range is the option's own rule and stays at its call site; only "this is
/// not a number" is written here, where the option's name is known.
pub(crate) fn number<T: FromStr>(
    args: &[String],
    long_key: &str,
    short_key: &str,
) -> Result<Option<T>, String> {
    let Some(written) = extract_param(args, long_key, short_key)? else {
        return Ok(None);
    };

    read_number(long_key, &written).map(Some)
}

/// One number, one refusal, whichever way the operator spelled it. A value
/// standing in a positional slot is the same request as one behind a flag, and
/// a verb that reads only the flag answers one typo two ways; `key` is what the
/// verb's usage line calls the argument, so the refusal names what was typed.
pub(crate) fn read_number<T: FromStr>(key: &str, written: &str) -> Result<T, String> {
    written
        .parse()
        .map_err(|_| format!("Error: invalid {} value '{}'", key, written))
}

/// A refusal the operator sees, and the exit that follows it: the one place a
/// reader in this crate turns a refusal into an exit code, for the handlers
/// that answer `()` and cannot propagate one.
pub(crate) fn or_refuse<T>(read: Result<T, String>) -> T {
    match read {
        Ok(value) => value,
        Err(refusal) => {
            eprintln!("{}", refusal);
            exit(1);
        }
    }
}

pub(crate) fn namespace(args: &[String]) -> String {
    or_refuse(extract_param(args, "--namespace", "-n")).unwrap_or_default()
}

/// The one output-format reader in this crate. `--json` is the switch spelling
/// of `--output json`; `--output` / `-o` take their value in any of the four
/// forms above. Every verb that renders reads this, so the two spellings can
/// never mean different things on different verbs.
pub(crate) fn output_format(args: &[String]) -> &'static str {
    or_refuse(parse_output_format(args))
}

/// The reader without the process exit, for `flz reset` — which answers every
/// argument refusal with its own exit code — and so the refusal is testable.
/// A value nobody recognises is refused rather than rendered as text: silently
/// answering a `--output yaml` with a human table is what a piped `jq` cannot
/// diagnose.
pub(crate) fn parse_output_format(args: &[String]) -> Result<&'static str, String> {
    // Read before the switch is answered: `--json` winning is a precedence
    // rule, not a licence to leave an unreadable `--output` unrefused.
    let written = extract_param(args, "--output", "-o")?;

    if args.iter().any(|a| a == "--json") {
        return Ok("json");
    }

    match written.as_deref() {
        None | Some("text") => Ok("text"),
        Some("json") => Ok("json"),
        Some(other) => Err(format!(
            "Error: unknown output format '{}'. Use 'text' or 'json'.",
            other
        )),
    }
}

/// A JSON-valued option, defaulting to the empty object.
pub(crate) fn json(args: &[String], long_key: &str, short_key: &str) -> String {
    match or_refuse(extract_param(args, long_key, short_key)) {
        Some(value) if !value.is_empty() => value,
        _ => "{}".to_string(),
    }
}

/// The `app_data/` root the running Controller would use: `--app-data-root`,
/// else `FLEZHA_APP_DATA_ROOT`, else `./app_data`.
pub(crate) fn app_data_root(args: &[String]) -> PathBuf {
    let env_value = env::var("FLEZHA_APP_DATA_ROOT").ok();
    or_refuse(resolve_app_data_root(args, env_value))
}

pub(crate) fn resolve_app_data_root(
    args: &[String],
    env_value: Option<String>,
) -> Result<PathBuf, String> {
    for candidate in [
        extract_param(args, "--app-data-root", "--app-data-root")?,
        env_value,
    ] {
        match candidate {
            Some(v) if !v.is_empty() => return Ok(PathBuf::from(v)),
            _ => {}
        }
    }
    Ok(PathBuf::from("./app_data"))
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
pub(crate) fn extract_x_flags(args: &[String]) -> Vec<(String, String)> {
    let mut flags = Vec::new();
    let mut i = 0;

    while i < args.len() {
        let arg = &args[i];

        if let Some((key, value)) = bare_setting(arg) {
            flags.push((key.to_string(), strip_quotes(value)));
        } else if let Some(rest) = arg.strip_prefix("--X") {
            if let Some((key, value)) = rest.split_once('=') {
                flags.push((key.to_string(), strip_quotes(value)));
            } else if let Some(next) = args.get(i + 1) {
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
fn bare_setting(arg: &str) -> Option<(&str, &str)> {
    let (key, value) = arg.strip_prefix('X')?.split_once('=')?;
    (!key.is_empty()).then_some((key, value))
}

/// What declares an extension, stated once: `--extension=NAME`, or
/// `--extension NAME` consuming the token after it.
fn declared_extension(arg: &str, next: Option<&String>) -> Option<String> {
    if let Some(rest) = arg.strip_prefix("--extension=") {
        return Some(strip_quotes(rest));
    }
    if arg == "--extension" {
        return next.map(|n| strip_quotes(n));
    }
    None
}

/// `--NAME.key[=value]` → `(NAME, "key[=value]")`.
fn dotted_option(arg: &str) -> Option<(&str, &str)> {
    arg.strip_prefix("--")?.split_once('.')
}

/// The extension names `args` declares, in declaration order.
pub(crate) fn extension_names(args: &[String]) -> Vec<String> {
    let mut names = Vec::new();
    let mut i = 0;

    while i < args.len() {
        if let Some(name) = declared_extension(&args[i], args.get(i + 1)) {
            if args[i] == "--extension" {
                i += 1;
            }
            names.push(name);
        }
        i += 1;
    }

    names
}

/// Repeatable `--extension=NAME` flags with their `--NAME.key=value`
/// sub-options, in declaration order. A dotted option only binds to a name
/// already declared to its left.
pub(crate) fn extract_extension_flags(args: &[String]) -> Vec<(String, Vec<(String, String)>)> {
    let mut out: Vec<(String, Vec<(String, String)>)> = Vec::new();
    let mut i = 0;

    while i < args.len() {
        let arg = &args[i];

        if let Some(name) = declared_extension(arg, args.get(i + 1)) {
            if arg == "--extension" {
                i += 1;
            }
            if !out.iter().any(|(known, _)| *known == name) {
                out.push((name, Vec::new()));
            }
        } else if let Some((prefix, remainder)) = dotted_option(arg) {
            if let Some((_, opts)) = out.iter_mut().find(|(name, _)| name == prefix) {
                if let Some((key, value)) = remainder.split_once('=') {
                    opts.push((key.to_string(), strip_quotes(value)));
                } else if let Some(next) = args.get(i + 1) {
                    opts.push((remainder.to_string(), strip_quotes(next)));
                    i += 1;
                }
            }
        }

        i += 1;
    }

    out
}

/// Strip the extension entries from an argument list so the option parser
/// below does not read them as plain CLI options.
pub(crate) fn filter_out_extension_entries(args: &[String]) -> Vec<String> {
    let declared = extension_names(args);
    let mut out = Vec::new();
    let mut i = 0;

    while i < args.len() {
        let arg = &args[i];

        if arg.starts_with("--extension=") || arg == "--extension" {
            // The dotted form is always `--NAME.key=value`, so only the
            // `--extension NAME` pair needs its value eaten.
            if arg == "--extension" && i + 1 < args.len() {
                i += 1;
            }
        } else if !dotted_option(arg).is_some_and(|(p, _)| declared.iter().any(|n| n == p)) {
            out.push(arg.clone());
        }

        i += 1;
    }

    out
}

/// An argument list as `env::args()` would produce it. Every test module in
/// this crate builds one.
#[cfg(test)]
pub(crate) fn owned(args: &[&str]) -> Vec<String> {
    args.iter().map(|s| s.to_string()).collect()
}

/// Single-letter option aliases.
const SHORT_KEYS: &[(&str, &str)] = &[("n", "namespace"), ("o", "output")];

/// The plain `--key value` options of a plugin enable, with `namespace`
/// defaulted to `"default"`. Both spellings of a setting are skipped:
/// `extract_x_flags` owns them, and `option_key` refuses the bare one because
/// it carries no leading `-`.
pub(crate) fn extract_opts(args: &[String]) -> Vec<(String, String)> {
    let mut flags = Vec::new();
    let mut i = 0;

    while i < args.len() {
        let arg = &args[i];
        let paired = args.get(i + 1);

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

    use super::owned as v;

    /// One scanner, four forms — and the same four for a short key.
    #[test]
    fn extract_param_reads_all_four_forms() {
        for form in [
            v(&["--namespace=prod"]),
            v(&["--namespace", "prod"]),
            v(&["-n=prod"]),
            v(&["-n", "prod"]),
        ] {
            assert_eq!(
                extract_param(&form, "--namespace", "-n")
                    .unwrap()
                    .as_deref(),
                Some("prod"),
                "{:?}",
                form
            );
        }
    }

    /// The three answers, and the two the old `Option` could not tell apart:
    /// an option nobody wrote takes the default, and one written with nothing
    /// after it is refused by name.
    #[test]
    fn an_option_written_with_no_value_is_refused_not_absent() {
        assert_eq!(extract_param(&v(&[]), "--namespace", "-n"), Ok(None));

        for form in [v(&["--namespace"]), v(&["-o", "json", "-n"])] {
            let refusal = extract_param(&form, "--namespace", "-n")
                .expect_err("a trailing flag is a refusal, not an absence");
            assert_eq!(refusal, "Error: --namespace requires a value", "{:?}", form);
        }
    }

    /// The switch spelling settles precedence, never whether the option was
    /// readable: `--json` may not carry a trailing `--output` past the refusal.
    #[test]
    fn the_json_switch_does_not_short_circuit_the_output_refusal() {
        assert_eq!(
            parse_output_format(&v(&["--json", "--output", "text"])),
            Ok("json")
        );

        let refusal = parse_output_format(&v(&["--json", "--output"])).unwrap_err();
        assert_eq!(refusal, "Error: --output requires a value");
    }

    /// The three defaults the scanner's callers add.
    #[test]
    fn the_defaulted_readers_agree_with_an_absent_flag() {
        let none: Vec<String> = v(&[]);
        assert_eq!(namespace(&none), "");
        assert_eq!(output_format(&none), "text");
        assert_eq!(output_format(&v(&["-o", "json"])), "json");
        assert_eq!(json(&none, "--init_values", "-iv"), "{}");
        assert_eq!(json(&v(&["-iv", ""]), "--init_values", "-iv"), "{}");
        assert_eq!(json(&v(&["-iv", "[1]"]), "--init_values", "-iv"), "[1]");
    }

    /// One reader, both spellings, five forms — and a value nobody recognises
    /// refused instead of rendered as text.
    #[test]
    fn both_output_format_spellings_agree_and_a_third_is_refused() {
        for form in [
            v(&["--json"]),
            v(&["--output", "json"]),
            v(&["--output=json"]),
            v(&["-o", "json"]),
            v(&["-o=json"]),
        ] {
            assert_eq!(parse_output_format(&form), Ok("json"), "{:?}", form);
        }

        assert_eq!(parse_output_format(&v(&[])), Ok("text"));
        assert_eq!(parse_output_format(&v(&["--output", "text"])), Ok("text"));

        let refusal = parse_output_format(&v(&["--output", "yaml"])).unwrap_err();
        assert!(
            refusal.contains("unknown output format 'yaml'"),
            "{}",
            refusal
        );
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

    /// Both extension scanners read `--extension` through one rule; a name the
    /// list scanner misses is a dotted option the filter would leave behind.
    #[test]
    fn both_extension_scanners_see_the_same_names() {
        let args = v(&[
            "--extension=on_input",
            "--extension",
            "on_terminate",
            "--on_input.position=0",
        ]);

        let from_flags: Vec<String> = extract_extension_flags(&args)
            .into_iter()
            .map(|(name, _)| name)
            .collect();

        assert_eq!(extension_names(&args), from_flags);
        assert_eq!(filter_out_extension_entries(&args), Vec::<String>::new());
    }

    #[test]
    fn resolve_app_data_root_prefers_flag_over_env_and_default() {
        let args = v(&["--app-data-root", "/tmp/flag-root"]);
        let got = resolve_app_data_root(&args, Some("/var/lib/env-root".to_string())).unwrap();
        assert_eq!(got, PathBuf::from("/tmp/flag-root"));
    }

    #[test]
    fn resolve_app_data_root_uses_env_when_flag_absent() {
        let args: Vec<String> = v(&[]);
        let got = resolve_app_data_root(&args, Some("/var/lib/env-root".to_string())).unwrap();
        assert_eq!(got, PathBuf::from("/var/lib/env-root"));
    }

    #[test]
    fn resolve_app_data_root_falls_back_to_default() {
        let args: Vec<String> = v(&[]);
        assert_eq!(
            resolve_app_data_root(&args, None).unwrap(),
            PathBuf::from("./app_data")
        );
        assert_eq!(
            resolve_app_data_root(&args, Some(String::new())).unwrap(),
            PathBuf::from("./app_data")
        );
    }

    #[test]
    fn resolve_app_data_root_equals_form() {
        let args = v(&["--app-data-root=/srv/flezha"]);
        let got = resolve_app_data_root(&args, None).unwrap();
        assert_eq!(got, PathBuf::from("/srv/flezha"));
    }

    #[test]
    fn extract_opts_ignores_extension_flags() {
        let args = v(&[
            "--extension=on_input",
            "--on_input.position=0",
            "--namespace=prod",
        ]);

        let filtered = filter_out_extension_entries(&args);
        let opts = extract_opts(&filtered);

        assert!(opts.iter().any(|(k, v)| k == "namespace" && v == "prod"));
        assert!(!opts.iter().any(|(k, _)| k == "extension"));
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
        let printed = [
            ("flz help", crate::help::FLZ),
            ("flz plugin help", crate::help::PLUGIN),
            ("flz namespace help", crate::help::NAMESPACE),
            ("flz plugin enable usage", crate::plugin::ENABLE_USAGE),
            ("flz namespace create usage", crate::namespace::CREATE_USAGE),
            (
                "flz namespace set-quota usage",
                crate::namespace::SET_QUOTA_USAGE,
            ),
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

        assert!(walked >= 6, "the walk found only {} settings", walked);
        assert!(
            unread.is_empty(),
            "{} of {} advertised plugin settings are not read by the parser:\n{}",
            unread.len(),
            walked,
            unread.join("\n")
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
