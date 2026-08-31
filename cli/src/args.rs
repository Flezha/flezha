//! Reading `flz`'s own arguments: one four-form scanner, and the surfaces
//! built on it.

use std::env;
use std::path::PathBuf;

/// `--long=value`, `--long value`, `-s=value` or `-s value` — the four forms
/// every `flz` option accepts. The one scanner in this crate; every other
/// reader below is a caller of it with its own default.
pub(crate) fn extract_param(args: &[String], long_key: &str, short_key: &str) -> Option<String> {
    for (i, arg) in args.iter().enumerate() {
        if let Some(value) = arg.strip_prefix(&format!("{}=", long_key)) {
            return Some(value.to_string());
        }
        if let Some(value) = arg.strip_prefix(&format!("{}=", short_key)) {
            return Some(value.to_string());
        }
        if (arg == long_key || arg == short_key) && i + 1 < args.len() {
            return Some(args[i + 1].clone());
        }
    }

    None
}

pub(crate) fn namespace(args: &[String]) -> String {
    extract_param(args, "--namespace", "-n").unwrap_or_default()
}

/// `--output` / `-o`. Anything that is not `json` is text, a value nobody
/// recognises included.
pub(crate) fn output_format(args: &[String]) -> String {
    match extract_param(args, "--output", "-o").as_deref() {
        Some("json") => "json",
        _ => "text",
    }
    .to_string()
}

/// The `--json` switch, as the verbs that take it rather than `--output`
/// spell it. Two spellings of one question live in this tree; this is the one
/// the audit, observe and controller verbs read.
pub(crate) fn json_or_text(args: &[String]) -> &'static str {
    if args.iter().any(|a| a == "--json" || a == "-o=json") {
        "json"
    } else {
        "text"
    }
}

/// A JSON-valued option, defaulting to the empty object.
pub(crate) fn json(args: &[String], long_key: &str, short_key: &str) -> String {
    match extract_param(args, long_key, short_key) {
        Some(value) if !value.is_empty() => value,
        _ => "{}".to_string(),
    }
}

/// The `app_data/` root the running Controller would use: `--app-data-root`,
/// else `FLEZHA_APP_DATA_ROOT`, else `./app_data`.
pub(crate) fn app_data_root(args: &[String]) -> PathBuf {
    resolve_app_data_root(args, env::var("FLEZHA_APP_DATA_ROOT").ok())
}

pub(crate) fn resolve_app_data_root(args: &[String], env_value: Option<String>) -> PathBuf {
    for candidate in [
        extract_param(args, "--app-data-root", "--app-data-root"),
        env_value,
    ] {
        match candidate {
            Some(v) if !v.is_empty() => return PathBuf::from(v),
            _ => {}
        }
    }
    PathBuf::from("./app_data")
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

/// The repeatable `--X<key>=<value>` / `--X<key> <value>` plugin settings.
pub(crate) fn extract_x_flags(args: &[String]) -> Vec<(String, String)> {
    let mut flags = Vec::new();
    let mut i = 0;

    while i < args.len() {
        if let Some(rest) = args[i].strip_prefix("--X") {
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
/// defaulted to `"default"`. `--X…` is skipped: `extract_x_flags` owns it.
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
        } else if let Some((key, value)) = arg.strip_prefix("--").and_then(|r| r.split_once('=')) {
            flags.push((key.to_string(), strip_quotes(value)));
        } else if !arg.contains('=') {
            if let (Some(key), Some(value)) = (option_key(arg), paired) {
                flags.push((key, strip_quotes(value)));
                i += 1;
            }
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
                extract_param(&form, "--namespace", "-n").as_deref(),
                Some("prod"),
                "{:?}",
                form
            );
        }
        assert_eq!(
            extract_param(&v(&["--namespace"]), "--namespace", "-n"),
            None
        );
    }

    /// The three defaults the scanner's callers add.
    #[test]
    fn the_defaulted_readers_agree_with_an_absent_flag() {
        let none: Vec<String> = v(&[]);
        assert_eq!(namespace(&none), "");
        assert_eq!(output_format(&none), "text");
        assert_eq!(output_format(&v(&["--output", "yaml"])), "text");
        assert_eq!(output_format(&v(&["-o", "json"])), "json");
        assert_eq!(json(&none, "--init_values", "-iv"), "{}");
        assert_eq!(json(&v(&["-iv", ""]), "--init_values", "-iv"), "{}");
        assert_eq!(json(&v(&["-iv", "[1]"]), "--init_values", "-iv"), "[1]");
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
        let got = resolve_app_data_root(&args, Some("/var/lib/env-root".to_string()));
        assert_eq!(got, PathBuf::from("/tmp/flag-root"));
    }

    #[test]
    fn resolve_app_data_root_uses_env_when_flag_absent() {
        let args: Vec<String> = v(&[]);
        let got = resolve_app_data_root(&args, Some("/var/lib/env-root".to_string()));
        assert_eq!(got, PathBuf::from("/var/lib/env-root"));
    }

    #[test]
    fn resolve_app_data_root_falls_back_to_default() {
        let args: Vec<String> = v(&[]);
        assert_eq!(
            resolve_app_data_root(&args, None),
            PathBuf::from("./app_data")
        );
        assert_eq!(
            resolve_app_data_root(&args, Some(String::new())),
            PathBuf::from("./app_data")
        );
    }

    #[test]
    fn resolve_app_data_root_equals_form() {
        let args = v(&["--app-data-root=/srv/flezha"]);
        let got = resolve_app_data_root(&args, None);
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

    #[test]
    fn extract_opts_aliases_short_keys_and_defaults_the_namespace() {
        let opts = extract_opts(&v(&["-n", "prod", "-o", "json", "--Xhosts", "kafka:9092"]));
        assert!(opts.contains(&("namespace".to_string(), "prod".to_string())));
        assert!(opts.contains(&("output".to_string(), "json".to_string())));
        assert!(!opts.iter().any(|(k, _)| k == "hosts" || k == "Xhosts"));

        let defaulted = extract_opts(&v(&["--topic", "t"]));
        assert!(defaulted.contains(&("namespace".to_string(), "default".to_string())));
    }
}
