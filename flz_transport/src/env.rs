//! The one reader of an environment knob, for every crate above this one.
//!
//! A knob's name, type and default are declared once in `Flezha.Config.Keys`;
//! what this module owns is the parse rule that declaration promises, so a Rust
//! runner answers `"0"`, `"on"` or a typo exactly as the controller and every
//! other SDK do. `test/fixtures/knob_parse_cases.txt` is that rule written
//! down, and the tests below are this runtime's reading of it.
//!
//! One rule per type:
//!
//!   * a count is positive — `"0"`, a negative number and unparsable text all
//!     read as unset, so a knob nobody meant to set never shrinks a pool to
//!     nothing;
//!   * a duration is a count of milliseconds and `0` is a real value, because
//!     zero disables three of the windows that take one;
//!   * a flag reads `1/true/yes/on` and `0/false/no/off` case-insensitively,
//!     and anything else is not a flag at all — it takes the default;
//!   * a directory is the value with its trailing slashes cut, and an empty one
//!     is no directory at all.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::RwLock;
use std::time::Duration;

/// The connection settings a host may hand to the core instead of the
/// environment, each beside the variable it stands in for. A host whose
/// runtime keeps its own copy of the environment (the BEAM does) cannot reach
/// the core through `setenv` once its threads run.
pub const CONNECT_SETTINGS: &[(&str, &str)] = &[
    ("socket_files_path", "FLEZHA_SOCKET_FILES_PATH"),
    ("transport", "FLEZHA_TRANSPORT"),
    ("quic_addresses", "FLEZHA_QUIC_ADDRESSES"),
    ("ca_cert_path", "FLEZHA_CA_CERT_PATH"),
    ("quic_client_cert", "FLEZHA_QUIC_CLIENT_CERT"),
    ("quic_client_key", "FLEZHA_QUIC_CLIENT_KEY"),
    ("namespace_secret", "FLEZHA_NAMESPACE_SECRET"),
    ("start_wait_ms", "FLEZHA_RUNNER_START_WAIT_MS"),
];

/// The settings the host handed over, keyed by variable name.
static HANDED_OVER: RwLock<Vec<(&'static str, String)>> = RwLock::new(Vec::new());

/// Replace the handed-over settings: `(variable, value)` pairs, each variable
/// one of [`CONNECT_SETTINGS`].
pub fn hand_over_connect_settings(settings: Vec<(&'static str, String)>) {
    *HANDED_OVER.write().unwrap_or_else(|e| e.into_inner()) = settings;
}

/// A connection setting: the value the host handed over, else the variable.
/// Read at connect time only, never per message.
pub fn connect_raw(key: &str) -> Option<String> {
    HANDED_OVER
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .find(|(var, _)| *var == key)
        .and_then(|(_, value)| set(value))
        .or_else(|| env_raw(key))
}

/// The variable's value, trimmed, or `None` when it is unset or empty.
pub fn env_raw(key: &str) -> Option<String> {
    std::env::var(key).ok().as_deref().and_then(set)
}

/// A value trimmed, or `None` when nothing is left: an empty setting is unset.
fn set(value: &str) -> Option<String> {
    Some(value.trim())
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

/// The variable's value, or `default` when it is unset or empty.
pub fn env_string(key: &str, default: &str) -> String {
    env_raw(key).unwrap_or_else(|| default.to_string())
}

/// The variable parsed as `T`, or `default` when it is unset or does not parse.
pub fn env_parse<T: FromStr>(key: &str, default: T) -> T {
    env_raw(key).and_then(|s| s.parse().ok()).unwrap_or(default)
}

/// A flag, with `default` for both an unset variable and a value that is not a
/// flag at all — so a call site reads the default it takes.
pub fn env_bool(key: &str, default: bool) -> bool {
    env_raw(key).and_then(|v| parse_bool(&v)).unwrap_or(default)
}

/// A count, with `0` read as unset.
pub fn env_parse_positive(key: &str, default: usize) -> usize {
    env_raw(key)
        .and_then(|s| parse_positive(&s))
        .unwrap_or(default)
}

/// A POSITIVE number of milliseconds: `0` reads as unset, because the windows
/// that take one — a keepalive, an idle timeout — have no meaning at zero.
/// A row whose zero is a real value (an input-log hold, a compaction age) is
/// read with [`parse_ms`] instead.
pub fn env_ms(key: &str, default_ms: u64) -> Duration {
    Duration::from_millis(env_parse_positive(key, default_ms as usize) as u64)
}

/// A directory, with its trailing slashes cut; an empty value is no directory.
pub fn env_dir(key: &str) -> Option<PathBuf> {
    as_dir(env_raw(key))
}

/// A connection setting read as a directory, by the [`env_dir`] rule.
pub fn connect_dir(key: &str) -> Option<PathBuf> {
    as_dir(connect_raw(key))
}

fn as_dir(raw: Option<String>) -> Option<PathBuf> {
    raw.map(|value| value.trim_end_matches('/').to_string())
        .filter(|trimmed| !trimmed.is_empty())
        .map(PathBuf::from)
}

/// A count of one or more, else `None` — the `positive_int` rule.
pub fn parse_positive(raw: &str) -> Option<usize> {
    raw.trim().parse::<usize>().ok().filter(|&n| n > 0)
}

/// A whole number of milliseconds, zero included — the `ms` rule.
pub fn parse_ms(raw: &str) -> Option<u64> {
    raw.trim().parse::<u64>().ok()
}

/// A whole number, negative included — the `int` rule.
pub fn parse_int(raw: &str) -> Option<i64> {
    raw.trim().parse::<i64>().ok()
}

/// `1/true/yes/on` and `0/false/no/off`, case-insensitively — the `bool` rule.
pub fn parse_bool(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shared fixture, read from the repository the crate lives in.
    fn cases() -> Vec<(String, String, String)> {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../test/fixtures/knob_parse_cases.txt"
        );
        std::fs::read_to_string(path)
            .expect("the shared parse fixture is part of the repository")
            .lines()
            .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
            .map(|line| {
                let parts: Vec<&str> = line.split('|').collect();
                let input = if parts[1] == "<empty>" { "" } else { parts[1] };
                (
                    parts[0].to_string(),
                    input.to_string(),
                    parts[2].to_string(),
                )
            })
            .collect()
    }

    #[test]
    fn every_case_parses_the_way_the_fixture_says() {
        let mut seen: Vec<String> = Vec::new();
        for (kind, input, expected) in cases() {
            if !seen.contains(&kind) {
                seen.push(kind.clone());
            }
            let got = match kind.as_str() {
                "positive_int" => parse_positive(&input).map(|n| n.to_string()),
                "ms" => parse_ms(&input).map(|n| n.to_string()),
                "int" => parse_int(&input).map(|n| n.to_string()),
                "bool" => parse_bool(&input).map(|b| b.to_string()),
                other => panic!("the fixture names a type this runtime has no rule for: {other}"),
            };
            let got = got.unwrap_or_else(|| "error".to_string());
            assert_eq!(got, expected, "{kind}|{input}");
        }
        seen.sort();
        assert_eq!(seen, vec!["bool", "int", "ms", "positive_int"]);
    }

    #[test]
    fn an_unset_variable_leaves_every_default_standing() {
        let absent = "FLZ_TEST_ABSENT_VARIABLE";
        assert_eq!(env_parse_positive(absent, 7), 7);
        assert_eq!(env_ms(absent, 250), Duration::from_millis(250));
        assert_eq!(env_string(absent, "fallback"), "fallback");
        assert!(!env_bool(absent, false));
        assert!(env_bool(absent, true));
        assert_eq!(env_dir(absent), None);
    }
}
