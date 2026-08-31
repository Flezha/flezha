//! The dispatcher every command shares: what a bare invocation does, which
//! spellings print the screen, and how an unrecognised subcommand is refused.
//!
//! This was two files covering two of the fifteen commands. One table covers
//! all of them, so a refusal that stops naming its subcommand — or a `{}` that
//! stops being substituted — is caught wherever it happens.

use std::process::{Command, Output};

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

/// Every command with subcommands: its name, the subcommands its screen
/// documents, and the words its refusal uses. `flz reset` is absent on
/// purpose — it parses flags rather than dispatching a verb.
const COMMANDS: &[(&str, &[&str], &str)] = &[
    (
        "controller",
        &["start", "stop", "status", "mem-top", "help"],
        "Invalid argument",
    ),
    ("cluster", &["status", "help"], "Unknown cluster subcommand"),
    (
        "operative",
        &[
            "start",
            "scale",
            "stop",
            "state",
            "list",
            "info",
            "io_listen",
            "send",
            "rewind",
            "replay",
            "help",
        ],
        "Invalid argument",
    ),
    (
        "namespace",
        &["list", "create", "delete", "show", "set-quota", "help"],
        "Invalid argument",
    ),
    ("runner", &["list", "check", "help"], "Invalid argument"),
    (
        "plugin",
        &[
            "install",
            "upgrade",
            "uninstall",
            "enable",
            "disable",
            "list",
            "info",
            "help",
        ],
        "Invalid argument",
    ),
    (
        "boot",
        &["status", "await", "import-legacy-snapshot", "help"],
        "Invalid argument",
    ),
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
            // No "help": `flz state help` works, but its screen is the one
            // that does not list `help` among its subcommands.
        ],
        "Invalid argument",
    ),
    (
        "observe",
        &["sources", "sinks", "tail", "enable", "disable"],
        "Unknown observe subcommand",
    ),
    (
        "audit",
        &["list", "read", "tail", "stats", "rotate"],
        "Unknown audit subcommand",
    ),
    (
        "dlq",
        &["list", "show", "replay", "drop"],
        "Invalid argument",
    ),
    (
        "schedule",
        &["add", "list", "remove", "run", "help"],
        "Unknown schedule command",
    ),
    (
        "auth",
        &["bootstrap", "whoami", "rebootstrap", "help"],
        "Invalid argument",
    ),
    (
        "token",
        &["issue", "revoke", "list", "help"],
        "Invalid argument",
    ),
    (
        "role",
        &["bind", "unbind", "list", "help"],
        "Invalid argument",
    ),
];

fn flz(args: &[&str]) -> Output {
    Command::new(FLZ_BIN).args(args).output().expect("flz runs")
}

#[test]
fn every_command_help_documents_its_subcommands() {
    let mut wrong: Vec<String> = Vec::new();

    for (command, verbs, _) in COMMANDS {
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

    for (command, _, refusal) in COMMANDS {
        let out = flz(&[command, "bogus"]);
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();

        if out.status.success() {
            wrong.push(format!("flz {} bogus exited 0", command));
        }
        // The refusal names the word it refused: the dispatcher substitutes it
        // into each command's own wording.
        if !stderr.contains(refusal) || !stderr.contains("'bogus'") {
            wrong.push(format!("flz {} bogus said: {}", command, stderr.trim()));
        }
    }

    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn every_command_refuses_a_missing_subcommand() {
    let mut wrong: Vec<String> = Vec::new();

    for (command, _, _) in COMMANDS {
        if flz(&[command]).status.success() {
            wrong.push(format!("flz {} with no subcommand exited 0", command));
        }
    }

    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// `help`, `--help` and `-h` are one screen — except on `flz plugin`, whose
/// `help` is a verb of its own that asks a provider for its documentation.
#[test]
fn the_help_spellings_agree_wherever_they_are_accepted() {
    let mut wrong: Vec<String> = Vec::new();

    for (command, _, _) in COMMANDS.iter().filter(|(c, _, _)| *c != "plugin") {
        let screen = String::from_utf8_lossy(&flz(&[command, "help"]).stdout).to_string();
        for spelling in ["--help", "-h"] {
            let got = String::from_utf8_lossy(&flz(&[command, spelling]).stdout).to_string();
            if got != screen {
                wrong.push(format!(
                    "flz {} {} prints a different screen",
                    command, spelling
                ));
            }
        }
    }

    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// `flz plugin --help` has always been a refusal, not a screen: `help` there
/// takes a provider id and asks the controller about it.
#[test]
fn plugin_refuses_the_help_flags_it_never_accepted() {
    for spelling in ["--help", "-h"] {
        let out = flz(&["plugin", spelling]);
        assert!(
            !out.status.success(),
            "flz plugin {} must stay a refusal",
            spelling
        );
        assert!(String::from_utf8_lossy(&out.stderr).contains("Invalid argument"));
    }
}
