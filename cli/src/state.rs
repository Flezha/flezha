use std::process::exit;

use crate::dispatch::{self, Command};
use crate::rpc::{self, token_opts_fragment};
use crate::{args, help};

pub(crate) const TABLE: Command = Command {
    help: help::STATE,
    verbs: &[
        ("get", |args| {
            handle_read(args, "get", "<operative/procedure[/entity_key]>", "state_get")
        }),
        ("list", |args| {
            handle_read(args, "list", "<operative_class>", "state_list")
        }),
        ("at", handle_at),
        ("rotate", |args| {
            handle_entity_verb(args, "rotate", "state_rotate")
        }),
        ("retention", handle_retention),
        ("delete", |args| {
            handle_entity_verb(args, "delete", "state_delete")
        }),
        ("outbox", |args| {
            handle_entity_verb(args, "outbox", "state_outbox")
        }),
        ("journal", |args| {
            handle_read(args, "journal", "<operative/instance>", "state_journal")
        }),
        ("inbox", |args| {
            handle_read(args, "inbox", "<operative/instance>", "state_inbox")
        }),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Some("Error: Invalid number of arguments. See `flz state help`."),
    unknown: "Error: Invalid argument '{}'. Use 'get', 'list', 'at', 'rotate', 'retention', 'delete', 'outbox', 'journal', 'inbox', or 'help'.",
    unknown_prints_help: false,
};

fn handle_at(args: &[String]) {
    if args.len() < 3 {
        eprintln!("Usage: flz state at <operative/procedure/entity_key> <seq | iso8601-timestamp> [--output text|json]");
        exit(1);
    }

    rpc::run(
        format!(
            "Flezha.Service.CliManager.state_at(\"{}\", \"{}\", \"{}\")",
            &args[1],
            &args[2],
            args::output_format(&args[3..]),
        ),
        rpc::PROPAGATE,
    );
}

fn handle_retention(args: &[String]) {
    if args.len() < 3 {
        eprintln!("Usage: flz state retention <operative_class> <hours>");
        exit(1);
    }

    rpc::run(
        format!(
            "Flezha.Service.CliManager.state_retention(\"{}\", \"{}\")",
            &args[1], &args[2],
        ),
        rpc::PROPAGATE,
    );
}

/// The Entity-scoped write verbs — `rotate`, `delete`, `outbox` — take one
/// Entity path and no output format; they differ only in the CliManager
/// function they call. The identity token (`--token` / FLEZHA_TOKEN) rides
/// the trailing opts.
fn handle_entity_verb(args: &[String], verb: &str, rpc_fun: &str) {
    if args.len() < 2 {
        eprintln!("Usage: flz state {verb} <operative/procedure/entity_key> [--token <token>]");
        exit(1);
    }

    rpc::run(
        format!(
            "Flezha.Service.CliManager.{}(\"{}\", {})",
            rpc_fun,
            &args[1],
            token_opts_fragment(&args[2..]),
        ),
        rpc::PROPAGATE,
    );
}

/// The read verbs — `list`, `journal`, `inbox` — take one scope argument plus an
/// output format; they differ only in that argument's name and the CliManager
/// function they call.
fn handle_read(args: &[String], verb: &str, scope: &str, rpc_fun: &str) {
    if args.len() < 2 {
        eprintln!("Usage: flz state {verb} {scope} [--output text|json] [--token <token>]");
        exit(1);
    }

    rpc::run(
        format!(
            "Flezha.Service.CliManager.{}(\"{}\", \"{}\", {})",
            rpc_fun,
            &args[1],
            args::output_format(&args[2..]),
            token_opts_fragment(&args[2..]),
        ),
        rpc::PROPAGATE,
    );
}
