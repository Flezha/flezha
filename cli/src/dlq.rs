use std::process::exit;

use crate::dispatch::{self, Command};
use crate::rpc::{self, text};
use crate::{args, help};

pub(crate) const TABLE: Command = Command {
    help: help::DLQ,
    verbs: &[
        ("list", handle_list),
        ("show", handle_show),
        ("replay", handle_replay),
        ("drop", handle_drop),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Some("Error: Invalid number of arguments. See `flz dlq help`."),
    unknown: "Error: Invalid argument '{}'. Use 'list', 'show', 'replay', 'drop', or 'help'.",
    unknown_prints_help: false,
};

fn handle_list(args: &[String]) {
    let operative_class = args::positional(args, 1).map(String::as_str).unwrap_or("");
    rpc::run(
        "dlq_list",
        &[text(operative_class)],
        &[],
        args,
        rpc::PROPAGATE,
    );
}

fn entry_ref_arg(args: &[String], usage: &str) -> String {
    match args::positional(args, 1) {
        Some(entry_ref) => entry_ref.clone(),
        None => {
            eprintln!("Usage: {}", usage);
            exit(1);
        }
    }
}

fn handle_show(args: &[String]) {
    let entry_ref = entry_ref_arg(args, "flz dlq show <operative/procedure/entry-id>");
    rpc::run("dlq_show", &[text(&entry_ref)], &[], args, rpc::PROPAGATE);
}

fn handle_replay(args: &[String]) {
    let entry_ref = entry_ref_arg(args, "flz dlq replay <operative/procedure/entry-id>");
    rpc::run("dlq_replay", &[text(&entry_ref)], &[], args, rpc::PROPAGATE);
}

fn handle_drop(args: &[String]) {
    let entry_ref = entry_ref_arg(args, "flz dlq drop <operative/procedure/entry-id>");
    rpc::run("dlq_drop", &[text(&entry_ref)], &[], args, rpc::PROPAGATE);
}
