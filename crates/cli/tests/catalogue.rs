//! Every request `flz` sends a controller names a published operation, and
//! carries no more arguments than it declares nor fewer than it requires.
//!
//! The operations are `crates/cli/catalogue.json`, which `mix flezha.catalogue`
//! writes from the controller's own declarations and a test on that side
//! holds fresh. Two checks hold the CLI to it: every door name written in
//! `crates/cli/src` is an entry, and one invocation of each, run against a `flzvm`
//! that echoes what it was handed, sends an operation its verb's row names and
//! a positional count the entry accepts. The rows are read from the tables'
//! source, since a binary's internals are out of an integration test's reach.
//!
//! The other way round, every entry has a caller: a `flz` row, an SDK control
//! module or the console, so the catalogue publishes only what is called.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use common::Shim;
use serde_json::{Deserializer, Value};

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

/// A `flzvm` that prints each argument on its own line and succeeds.
const ECHOING: &str = "#!/bin/sh\nprintf '%s\\n' \"$@\"\nexit 0\n";

const DOOR: &str = "Flezha.Service.Cli.call(";

/// One invocation per operation `flz` sends.
const INVOCATIONS: &[&[&str]] = &[
    &["audit", "list"],
    &["audit", "read", "audit-current.jsonl", "--limit", "2"],
    &["audit", "rotate"],
    &["audit", "stats"],
    &["audit", "tail", "--duration-ms", "10"],
    &["boot", "await", "--timeout", "1"],
    &["boot", "status"],
    &["cluster", "status"],
    &["controller", "mem-top"],
    &["controller", "status"],
    &["controller", "stop", "--no-wait"],
    &["dlq", "drop", "a/b/c"],
    &["dlq", "show", "a/b/c"],
    &["dlq", "list"],
    &["dlq", "replay", "a/b/c"],
    &["plugin", "list", "--installed"],
    &["plugin", "help", "relay_stdout"],
    &["plugin", "list", "--providers"],
    &["runner", "list"],
    &["runner", "drop", "default_0123"],
    &["runner", "dropped"],
    &["runner", "readmit", "stable-0123"],
    &["role", "bind", "p", "app", "ns"],
    &["auth", "bootstrap", "t"],
    &["namespace", "create", "ns", "--Xmax_relays=3"],
    &["namespace", "delete", "ns"],
    &["namespace", "show", "ns"],
    &["token", "issue", "p", "ns", "app", "--ttl", "60"],
    &["role", "list"],
    &["token", "revoke", "abcdef12"],
    &["namespace", "set-quota", "ns", "--Xmax_relays=4"],
    &["token", "list"],
    &["role", "unbind", "p"],
    &["auth", "whoami"],
    &["observe", "disable", "audit_log"],
    &["observe", "enable", "audit_log"],
    &["observe", "sinks"],
    &["observe", "sources"],
    &["observe", "tail", "audit_log", "--duration-ms", "10"],
    &["relay", "call", "op", "--text", "hi"],
    &["relay", "state", "op"],
    &["relay", "info"],
    &["relay", "info", "op"],
    &["relay", "diagnose", "op"],
    &["relay", "graph", "op"],
    &["relay", "activity", "op"],
    &["relay", "list"],
    &["relay", "listen", "op"],
    &["namespace", "list"],
    &["relay", "replay", "a/b/c", "--from", "0"],
    &["relay", "send", "op", "--text", "hi"],
    &["relay", "start", "f", "--partitions", "2"],
    &["relay", "stop", "op"],
    &["plugin", "bind", "p", "--relay_id", "op"],
    &["plugin", "disable", "p"],
    &["plugin", "enable", "p", "relay_stdout"],
    &["plugin", "list"],
    &["plugin", "unbind", "p", "--relay_id", "op"],
    &["plugin", "update", "p", "Xkey=v"],
    &[
        "schedule",
        "add",
        "s",
        "--cron",
        "0 8 * * *",
        "--tz",
        "Etc/UTC",
        "--factory",
        "f",
    ],
    &["schedule", "list"],
    &["schedule", "remove", "s"],
    &["schedule", "run", "s"],
    &["state", "list", "c"],
    &["state", "delete", "a/b/c"],
    &["state", "inbox", "a/i"],
    &["state", "journal", "a/i"],
    &["state", "outbox", "a/b/c"],
    &["relay", "scale", "g", "3"],
    &["relay", "rewind", "op", "--to-iteration", "1"],
    &["state", "rotate", "a/b/c"],
    &["state", "retention", "c", "2"],
    &["state", "get", "a/b"],
    &["state", "at", "a/b/c", "0"],
];

/// Each operation's name, and the argument counts it accepts: at least its
/// required ones, at most all it declares.
fn catalogue() -> BTreeMap<String, (usize, usize)> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("catalogue.json");
    let catalogue: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();

    catalogue["operations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|op| {
            let args = op["args"].as_array().unwrap();
            let required = args.iter().filter(|arg| arg["required"] == true).count();
            (
                op["name"].as_str().unwrap().to_string(),
                (required, args.len()),
            )
        })
        .collect()
}

/// Every door name written in a `crates/cli/src` file: a string literal that is
/// exactly `<Module>.<function>`.
fn door_names_in_source() -> Vec<(String, String)> {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut found = Vec::new();

    for entry in fs::read_dir(src).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|ext| ext != "rs") {
            continue;
        }
        let text = fs::read_to_string(&path).unwrap();
        for literal in text.split('"').skip(1).step_by(2) {
            if door_name(literal) {
                let file = path.file_name().unwrap().to_string_lossy().to_string();
                found.push((file, literal.to_string()));
            }
        }
    }

    found
}

/// The operations each verb row names, by its command's path and its name.
fn row_calls() -> BTreeMap<(String, String), Vec<String>> {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut rows = BTreeMap::new();

    for entry in fs::read_dir(src).unwrap() {
        let text = fs::read_to_string(entry.unwrap().path()).unwrap_or_default();
        let Some((_, table)) = text.split_once("path: \"") else {
            continue;
        };
        let path = table.split('"').next().unwrap().to_string();
        let verbs = table.split_once("verbs: &[").unwrap().1;
        let verbs = verbs.split_once("\n    missing:").unwrap().0;

        for row in verbs.split("Verb::").skip(1) {
            let name = row.split('"').nth(1).unwrap().to_string();
            let calls = row
                .split_once(".calls(&[")
                .map(|(_, list)| list.split_once("])").unwrap().0)
                .unwrap_or_default();
            let calls = calls.split('"').skip(1).step_by(2).map(String::from);
            rows.insert((path.clone(), name), calls.collect());
        }
    }

    rows
}

/// Every operation a caller outside `flz` names: the console's manifest, as
/// `<Module>.<function>`, and the source of each SDK's control module, whose
/// paths `test/sdk_conformance/gate_test.exs` declares once.
fn other_callers() -> (BTreeSet<String>, Vec<String>) {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let read = |path: &str| fs::read_to_string(root.join(path)).unwrap();

    let console = read("docs/internal/api/consumers/flezha_ui.txt")
        .lines()
        .filter_map(|line| line.strip_prefix("Flezha.Service.")?.split('/').next())
        .map(String::from)
        .collect();

    let gate = read("test/sdk_conformance/gate_test.exs");
    let surfaces = gate.split_once("@admin_surfaces %{").unwrap().1;
    let surfaces = surfaces.split_once("\n  }").unwrap().0;
    let sdks = surfaces
        .lines()
        .filter_map(|line| line.split('"').nth(1))
        .map(read)
        .collect();

    (console, sdks)
}

fn door_name(literal: &str) -> bool {
    match literal.split_once('.') {
        Some((module, function)) => {
            module.starts_with(|c: char| c.is_ascii_uppercase())
                && module.chars().all(|c| c.is_ascii_alphabetic())
                && !function.is_empty()
                && function
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c == '_' || c == '?')
        }
        None => false,
    }
}

/// The operation and the arguments one echoed request names.
fn sent(stdout: &str) -> Option<(String, Vec<Value>)> {
    let line = stdout.lines().find(|line| line.starts_with(DOOR))?;
    let mut values = Deserializer::from_str(&line[DOOR.len()..]).into_iter::<Value>();
    let name = values.next()?.ok()?.as_str()?.to_string();
    let rest = line[DOOR.len()..][values.byte_offset()..].trim_start_matches([',', ' ']);
    let args_text = Deserializer::from_str(rest)
        .into_iter::<Value>()
        .next()?
        .ok()?;
    let args: Vec<Value> = serde_json::from_str(args_text.as_str()?).ok()?;
    Some((name, args))
}

#[test]
fn every_door_name_in_the_source_is_a_catalogue_entry() {
    let catalogue = catalogue();
    let unknown: Vec<String> = door_names_in_source()
        .into_iter()
        .filter(|(_file, name)| !catalogue.contains_key(name))
        .map(|(file, name)| format!("{} sends {}", file, name))
        .collect();

    assert!(
        unknown.is_empty(),
        "names no published operation declares:\n{}",
        unknown.join("\n")
    );
}

#[test]
fn every_request_sends_an_operation_its_row_names() {
    let rows = row_calls();
    let shim = Shim::new(FLZ_BIN, ECHOING);
    let mut wrong: Vec<String> = Vec::new();

    for argv in INVOCATIONS {
        let out = shim.run(argv);
        let Some((name, _args)) = sent(&String::from_utf8_lossy(&out.stdout)) else {
            continue;
        };
        let row = (format!("flz {}", argv[0]), argv[1].to_string());
        match rows.get(&row) {
            Some(calls) if calls.contains(&name) => {}
            calls => wrong.push(format!(
                "flz {} sends {}; its row calls {:?}",
                argv.join(" "),
                name,
                calls
            )),
        }
    }

    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn every_request_sends_a_count_of_arguments_its_operation_takes() {
    let catalogue = catalogue();
    let shim = Shim::new(FLZ_BIN, ECHOING);
    let mut wrong: Vec<String> = Vec::new();
    let mut reached = BTreeSet::new();

    for argv in INVOCATIONS {
        let out = shim.run(argv);
        let Some((name, args)) = sent(&String::from_utf8_lossy(&out.stdout)) else {
            wrong.push(format!("flz {} sent no door call", argv.join(" ")));
            continue;
        };
        reached.insert(name.clone());

        match catalogue.get(&name) {
            None => wrong.push(format!(
                "flz {} sends {}, which no entry declares",
                argv.join(" "),
                name
            )),
            Some((required, declared)) if args.len() < *required || args.len() > *declared => wrong
                .push(format!(
                    "flz {} sends {} with {} arguments; it takes {}..={}",
                    argv.join(" "),
                    name,
                    args.len(),
                    required,
                    declared
                )),
            Some(_) => {}
        }
    }

    let in_source: BTreeSet<String> = door_names_in_source()
        .into_iter()
        .map(|(_f, n)| n)
        .collect();
    let unreached: Vec<&String> = in_source.difference(&reached).collect();

    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    assert!(
        unreached.is_empty(),
        "no invocation here sends {:?}",
        unreached
    );
}

#[test]
fn every_catalogue_operation_has_a_caller() {
    let rows: BTreeSet<String> = row_calls().into_values().flatten().collect();
    let (console, sdks) = other_callers();
    assert_eq!(sdks.len(), 7, "the gate declares seven SDK control modules");

    let called = |name: &str| {
        rows.contains(name)
            || console.contains(name)
            || sdks.iter().any(|source| {
                source.contains(&format!("\"{name}\"")) || source.contains(&format!("'{name}'"))
            })
    };

    // An undo is published while the stream it undoes is.
    let uncalled: Vec<String> = catalogue()
        .into_keys()
        .filter(|name| match name.strip_suffix(".unsubscribe") {
            Some(module) => !called(&format!("{module}.tail")),
            None => !called(name),
        })
        .collect();

    assert!(
        uncalled.is_empty(),
        "no flz row, SDK control module or console call names {:?}",
        uncalled
    );
}
