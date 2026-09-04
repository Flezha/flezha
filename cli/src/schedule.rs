use std::process::exit;

use crate::dispatch::{self, Command};
use crate::operative::parse_plugins_json;
use crate::rpc::{self, escape_rpc_arg, token_opts_fragment};
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
    rpc::run(
        format!(
            "Flezha.Service.CliManager.schedule_list({})",
            token_opts_fragment(args)
        ),
        rpc::PROPAGATE,
    );
}

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
    let kind = args::or_refuse(args::extract_param(rest, "--kind", "--kind"))
        .unwrap_or_else(|| "workflow".to_string());
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
            "Flezha.Service.CliManager.schedule_add(\"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", {})",
            name,
            cron,
            tz,
            factory,
            namespace,
            escaped_init_values,
            kind,
            escaped_plugins,
            token_opts_fragment(rest),
        ),
        rpc::PROPAGATE,
    );
}

fn handle_remove(args: &[String]) {
    rpc::run(
        format!(
            "Flezha.Service.CliManager.schedule_remove(\"{}\", {})",
            schedule_name("remove", args),
            token_opts_fragment(args)
        ),
        rpc::PROPAGATE,
    );
}

fn handle_run(args: &[String]) {
    rpc::run(
        format!(
            "Flezha.Service.CliManager.schedule_run(\"{}\")",
            schedule_name("run", args)
        ),
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
