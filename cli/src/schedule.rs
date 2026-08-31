use std::process::exit;

use crate::dispatch::{self, Command};
use crate::operative::parse_plugins_json;
use crate::rpc::{self, escape_rpc_arg};
use crate::{args, help};

pub(crate) const TABLE: Command = Command {
    help: help::SCHEDULE,
    verbs: &[
        ("add", handle_add),
        ("list", handle_list),
        ("remove", |args| handle_named("remove", args)),
        ("run", |args| handle_named("run", args)),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: None,
    unknown: "Error: Unknown schedule command '{}'. Valid: add, list, remove, run, help",
    unknown_prints_help: false,
};

fn handle_list(_args: &[String]) {
    rpc::run(
        "Flezha.Service.CliManager.schedule_list()".to_string(),
        rpc::PROPAGATE,
    );
}

fn handle_add(args: &[String]) {
    let usage = "Usage: flz schedule add <name> --cron \"<expr>\" --tz <iana> --factory <factory_id> [--namespace <ns> | -n <ns>] [--init_values <json> | -iv <json>] [--plugins <json> | -p <json>] [--kind workflow]";
    let rest = &args[1..];

    let name = match rest.first() {
        Some(name) if !name.starts_with('-') => name.clone(),
        _ => {
            eprintln!("Error: missing schedule name.\n {}", usage);
            exit(1);
        }
    };

    let cron = require_param(rest, "--cron", usage);
    let tz = require_param(rest, "--tz", usage);
    let factory = require_param(rest, "--factory", usage);
    let kind =
        args::extract_param(rest, "--kind", "--kind").unwrap_or_else(|| "workflow".to_string());
    let namespace = args::namespace(rest);
    let init_values = args::json(rest, "--init_values", "-iv");
    let escaped_init_values = escape_rpc_arg(&init_values);
    let plugins = match parse_plugins_json(&args::json(rest, "--plugins", "-p")) {
        Ok(canonical) => canonical,
        Err(reason) => {
            eprintln!("Error: invalid --plugins JSON: {}", reason);
            exit(1);
        }
    };
    let escaped_plugins = escape_rpc_arg(&plugins);

    rpc::run(
        format!(
            "Flezha.Service.CliManager.schedule_add(\"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\")",
            name, cron, tz, factory, namespace, escaped_init_values, kind, escaped_plugins,
        ),
        rpc::PROPAGATE,
    );
}

fn handle_named(verb: &str, args: &[String]) {
    let name = match args.get(1) {
        Some(name) => name.clone(),
        None => {
            eprintln!("Usage: flz schedule {} <name>", verb);
            exit(1);
        }
    };

    rpc::run(
        format!("Flezha.Service.CliManager.schedule_{}(\"{}\")", verb, name),
        rpc::PROPAGATE,
    );
}

fn require_param(args: &[String], key: &str, usage: &str) -> String {
    match args::extract_param(args, key, key) {
        Some(value) if !value.is_empty() => value,
        _ => {
            eprintln!("Error: {} is required.\n {}", key, usage);
            exit(1);
        }
    }
}
