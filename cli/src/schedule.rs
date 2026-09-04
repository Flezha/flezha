use std::process::exit;

use crate::dispatch::{self, Command};
use crate::operative::plugins_json_or_refuse;
use crate::rpc::{self, text, Opt};
use crate::{args, help};

pub(crate) const TABLE: Command = Command {
    help: help::SCHEDULE,
    verbs: &[
        ("add", handle_add),
        ("list", handle_list),
        ("remove", handle_remove),
        ("run", handle_run),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: None,
    unknown: "Error: Unknown schedule command '{}'. Valid: add, list, remove, run, help",
    unknown_prints_help: false,
};

fn handle_list(args: &[String]) {
    rpc::run("schedule_list", &[], &[], args, rpc::PROPAGATE);
}

/// The flags travel as the options list: the three required ones first, then
/// each optional one the operator wrote, so an absent flag is absent from the
/// request and the controller applies its own default. `--plugins` is carried
/// as the text it was written as, whatever it parses as: the controller's
/// decoder reads the JSON itself, at every firing.
fn handle_add(args: &[String]) {
    let usage = "Usage: flz schedule add <name> --cron \"<expr>\" --tz <iana> --factory <factory_id> [--namespace <ns> | -n <ns>] [--init_values <json> | -iv <json>] [--plugins <json> | -p <json>] [--kind workflow]";
    let rest = &args[1..];

    let name = match args::positional(rest, 0) {
        Some(name) => name.clone(),
        None => {
            eprintln!("Error: missing schedule name.\n {}", usage);
            exit(1);
        }
    };

    let cron = require_param(rest, "--cron", usage);
    let tz = require_param(rest, "--tz", usage);
    let factory = require_param(rest, "--factory", usage);
    let flag = |long, short| args::or_refuse(args::extract_param(rest, long, short));
    let optional = [
        ("namespace", Some(args::namespace(rest))),
        ("init_values", flag("--init_values", "-iv")),
        ("kind", flag("--kind", "--kind")),
        (
            "plugins",
            flag("--plugins", "-p").map(|p| plugins_json_or_refuse(&p)),
        ),
    ];

    let mut opts: Vec<Opt> = vec![
        ("cron", text(&cron)),
        ("tz", text(&tz)),
        ("factory", text(&factory)),
    ];
    for (key, value) in &optional {
        if let Some(value) = value.as_deref().filter(|v| !v.is_empty()) {
            opts.push((*key, text(value)));
        }
    }

    rpc::run("schedule_add", &[text(&name)], &opts, rest, rpc::PROPAGATE);
}

fn handle_remove(args: &[String]) {
    named_verb("remove", args);
}

fn handle_run(args: &[String]) {
    named_verb("run", args);
}

/// `remove` and `run` take the schedule's name and the caller's token.
fn named_verb(verb: &str, args: &[String]) {
    rpc::run(
        &format!("schedule_{}", verb),
        &[text(&schedule_name(verb, args))],
        &[],
        args,
        rpc::PROPAGATE,
    );
}

fn schedule_name(verb: &str, args: &[String]) -> String {
    match args::positional(args, 1) {
        Some(name) => name.clone(),
        None => {
            eprintln!("Usage: flz schedule {} <name>", verb);
            exit(1);
        }
    }
}

fn require_param(args: &[String], key: &str, usage: &str) -> String {
    match args::or_refuse(args::extract_param(args, key, key)) {
        Some(value) if !value.is_empty() => value,
        _ => {
            eprintln!("Error: {} is required.\n {}", key, usage);
            exit(1);
        }
    }
}
