//! The dispatcher every command shares: what a bare invocation does, which
//! spellings print the screen, and how an unrecognised subcommand is refused.
//!
//! This was two files covering two of the fifteen commands. One table covers
//! all of them, so a refusal that stops naming its subcommand — or a `{}` that
//! stops being substituted — is caught wherever it happens.

use std::process::{Command, Output};

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

/// Every command with subcommands, and the subcommands its screen documents.
/// `flz reset` is absent on purpose — it parses flags rather than dispatching a
/// verb. A refusal's own words are not listed: the dispatcher renders them from
/// the table, so this file holds no copy of them to drift.
const COMMANDS: &[(&str, &[&str])] = &[
    (
        "controller",
        &["start", "stop", "status", "mem-top", "help"],
    ),
    ("cluster", &["status", "help"]),
    (
        "relay",
        &[
            "start", "scale", "stop", "state", "list", "info", "listen", "send", "rewind",
            "replay", "help",
        ],
    ),
    (
        "namespace",
        &[
            "list",
            "create",
            "delete",
            "show",
            "set-quota",
            "set-migration",
            "help",
        ],
    ),
    (
        "runner",
        &["list", "check", "drop", "dropped", "readmit", "help"],
    ),
    (
        "plugin",
        &[
            "install",
            "uninstall",
            "sync",
            "enable",
            "disable",
            "list",
            "help",
        ],
    ),
    ("boot", &["status", "await", "help"]),
    (
        "state",
        &[
            "get",
            "list",
            "at",
            "rotate",
            "retention",
            "delete",
            "outbox",
            "journal",
            "inbox",
            "help",
        ],
    ),
    (
        "observe",
        &["sources", "sinks", "tail", "enable", "disable", "help"],
    ),
    (
        "audit",
        &["list", "read", "tail", "stats", "rotate", "help"],
    ),
    ("dlq", &["list", "show", "replay", "drop"]),
    ("schedule", &["add", "list", "remove", "run", "help"]),
    ("auth", &["bootstrap", "whoami", "rebootstrap", "help"]),
    ("token", &["issue", "revoke", "list", "help"]),
    ("role", &["bind", "unbind", "list", "help"]),
];

fn flz(args: &[&str]) -> Output {
    Command::new(FLZ_BIN).args(args).output().expect("flz runs")
}

#[test]
fn every_command_help_documents_its_subcommands() {
    let mut wrong: Vec<String> = Vec::new();

    for (command, verbs) in COMMANDS {
        let out = flz(&[command, "help"]);
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();

        if !out.status.success() {
            wrong.push(format!(
                "flz {} help: exit {:?}",
                command,
                out.status.code()
            ));
        }
        for verb in *verbs {
            if !stdout.contains(verb) {
                wrong.push(format!("flz {} help does not document '{}'", command, verb));
            }
        }
    }

    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn every_command_refuses_an_unknown_subcommand_by_name() {
    let mut wrong: Vec<String> = Vec::new();

    for (command, verbs) in COMMANDS {
        let out = flz(&[command, "bogus"]);
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();

        if out.status.success() {
            wrong.push(format!("flz {} bogus exited 0", command));
        }
        // The refusal names the word it refused and offers every verb the
        // table declares — it is rendered from that table, so a verb added to
        // it is offered without a sentence being edited.
        let named = verbs.iter().all(|verb| stderr.contains(verb));
        if !stderr.contains("'bogus'") || !stderr.contains("(usage.unknown_command)") || !named {
            wrong.push(format!("flz {} bogus said: {}", command, stderr.trim()));
        }
    }

    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn every_command_refuses_a_missing_subcommand() {
    let mut wrong: Vec<String> = Vec::new();

    for (command, _) in COMMANDS {
        if flz(&[command]).status.success() {
            wrong.push(format!("flz {} with no subcommand exited 0", command));
        }
    }

    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// `help`, `--help` and `-h` are one screen on every command. On `flz plugin`
/// `help` is a verb of its own, which prints the same screen until it is given
/// a provider id.
#[test]
fn the_help_spellings_agree_on_every_command() {
    let mut wrong: Vec<String> = Vec::new();

    for (command, _) in COMMANDS {
        let screen = String::from_utf8_lossy(&flz(&[command, "help"]).stdout).to_string();
        for spelling in ["--help", "-h"] {
            let out = flz(&[command, spelling]);
            if !out.status.success() || String::from_utf8_lossy(&out.stdout) != screen {
                wrong.push(format!(
                    "flz {} {} does not print the screen",
                    command, spelling
                ));
            }
        }
    }

    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// A refusal of the command line honours `--json` wherever it was written: one
/// JSON line on stderr, nothing on stdout, the class's exit code, and no
/// controller reached.
#[test]
fn a_command_line_refusal_honours_json() {
    let out = flz(&["relay", "start", "echo", "--nope", "--json"]);
    let line = String::from_utf8_lossy(&out.stderr);
    let doc: serde_json::Value = serde_json::from_str(line.trim()).expect("one JSON line");

    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
    assert_eq!(doc["error"]["code"], "usage.unknown_option");
    assert_eq!(doc["error"]["class"], "invalid");
    assert_eq!(doc["error"]["details"]["option"], "--nope");
}

/// fails when: a verb's `--help` prints its command's whole screen rather
/// than what describes the verb, or `-h` and `help <verb>` answer otherwise.
/// `flz plugin help <word>` names a provider, so plugin is not asked that.
#[test]
fn a_verbs_help_describes_the_verb_not_its_command() {
    let mut wrong: Vec<String> = Vec::new();

    for (command, verbs) in COMMANDS {
        for verb in verbs.iter().filter(|verb| **verb != "help") {
            let out = flz(&[command, verb, "--help"]);
            let text = String::from_utf8_lossy(&out.stdout).to_string();
            let first = text.lines().next().unwrap_or_default();

            if !out.status.success() || !first.contains(&format!("flz {} {}", command, verb)) {
                wrong.push(format!("flz {} {} --help began {:?}", command, verb, first));
            }
            if text.contains("COMMANDS:") {
                wrong.push(format!(
                    "flz {} {} --help prints its command's screen",
                    command, verb
                ));
            }

            let mut spellings = vec![vec![*command, verb, "-h"]];
            if *command != "plugin" {
                spellings.push(vec![*command, "help", verb]);
            }
            for argv in spellings {
                let other = flz(&argv);
                if !other.status.success() || String::from_utf8_lossy(&other.stdout) != text {
                    wrong.push(format!("flz {} differs from --help", argv.join(" ")));
                }
            }
        }
    }

    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
