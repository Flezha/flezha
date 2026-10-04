//! Every request `flz` sends a controller names a published operation, and
//! carries no more arguments than it declares nor fewer than it requires.
//!
//! The operations are `cli/catalogue.json`, which `mix flezha.catalogue`
//! writes from the controller's own declarations and a test on that side
//! holds fresh. Two checks hold the CLI to it: every door name written in
//! `cli/src` is an entry, and one invocation of each, run against a `flzvm`
//! that echoes what it was handed, sends a positional count the entry accepts.

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
    &["plugin", "help", "operator_stdout"],
    &["plugin", "list", "--providers"],
    &["runner", "list"],
    &["role", "bind", "p", "app", "ns"],
    &["auth", "bootstrap", "t"],
    &["namespace", "create", "ns", "--Xmax_operators=3"],
    &["namespace", "delete", "ns"],
    &["namespace", "show", "ns"],
    &["token", "issue", "p", "ns", "app", "--ttl", "60"],
    &["role", "list"],
    &["token", "revoke", "abcdef12"],
    &["namespace", "set-quota", "ns", "--Xmax_operators=4"],
    &["token", "list"],
    &["role", "unbind", "p"],
    &["auth", "whoami"],
    &["observe", "disable", "audit_log"],
    &["observe", "enable", "audit_log"],
    &["observe", "sinks"],
    &["observe", "sources"],
    &["observe", "tail", "audit_log", "--duration-ms", "10"],
    &["operator", "call", "op", "--text", "hi"],
    &["operator", "state", "op"],
    &["operator", "info"],
    &["operator", "list"],
    &["operator", "listen", "op"],
    &["namespace", "list"],
    &["operator", "replay", "a/b/c", "--from", "0"],
    &["operator", "send", "op", "--text", "hi"],
    &["operator", "start", "f", "--partitions", "2"],
    &["operator", "stop", "op"],
    &["plugin", "bind", "p", "--operator_id", "op"],
    &["plugin", "disable", "p"],
    &["plugin", "enable", "p", "operator_stdout"],
    &["plugin", "list"],
    &["plugin", "unbind", "p", "--operator_id", "op"],
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
    &["operator", "scale", "g", "3"],
    &["operator", "rewind", "op", "--to-iteration", "1"],
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

/// Every door name written in a `cli/src` file: a string literal that is
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
