use std::process::exit;

use crate::args::{self, extract_x_flags, serialize_flags};
use crate::dispatch::{self, Command};
use crate::help;
use crate::rpc::{self, escape_rpc_arg, token_opts_fragment};

pub(crate) const TABLE: Command = Command {
    help: help::NAMESPACE,
    verbs: &[
        ("list", handle_list),
        ("create", handle_create),
        ("delete", handle_delete),
        ("show", handle_show),
        ("set-quota", handle_set_quota),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Some("Error: Invalid number of arguments. See help for more information."),
    unknown:
        "Error: Invalid argument '{}'. Use 'list', 'create', 'delete', 'show', 'set-quota', or 'help'.",
    unknown_prints_help: false,
};

/// The only list verb whose RPC takes no namespace: the namespaces ARE the
/// scope, so `rpc::scoped_read`'s shape does not fit it.
fn handle_list(args: &[String]) {
    let output_format = args::output_format(&args[1..]);

    if output_format == "text" {
        println!("Fetching namespaces...");
    }

    rpc::run(
        format!(
            "Flezha.Service.CliManager.list_namespaces(\"{}\")",
            output_format
        ),
        rpc::REFUSED,
    );
}

// Quota overrides ride the repeatable `--X<field>=<value>` flags
// (e.g. `--Xmax_runners=4 --Xmax_state_bytes=1073741824`), serialized to the
// same JSON object shape every other `--X` surface uses.
fn quotas_json_literal(args: &[String]) -> String {
    let flags = extract_x_flags(args);
    serde_json::Value::String(serialize_flags(&flags)).to_string()
}

/// The two usage lines that advertise a quota override. Constants because
/// `args.rs` asserts the parser reads every setting the CLI prints.
pub(crate) const CREATE_USAGE: &str =
    "Usage: flz namespace create <name> [--X<quota>=<value> ...] [--token <admin-token>]";

pub(crate) const SET_QUOTA_USAGE: &str =
    "Usage: flz namespace set-quota <name> --X<quota>=<value> [...] [--token <admin-token>]";

fn handle_create(args: &[String]) {
    if args.len() < 2 {
        eprintln!("{}", CREATE_USAGE);
        exit(1);
    }

    rpc::run(
        format!(
            "Flezha.Service.CliManager.authz_render_namespace_create(\"{}\", {}, {})",
            escape_rpc_arg(&args[1]),
            quotas_json_literal(&args[2..]),
            token_opts_fragment(&args[2..]),
        ),
        rpc::PROPAGATE,
    );
}

fn handle_delete(args: &[String]) {
    rpc::one_arg_authz(
        args,
        "authz_render_namespace_delete",
        "Usage: flz namespace delete <name> [--token <admin-token>]",
    );
}

fn handle_show(args: &[String]) {
    rpc::one_arg_authz(
        args,
        "authz_render_namespace_show",
        "Usage: flz namespace show <name> [--token <token>]",
    );
}

fn handle_set_quota(args: &[String]) {
    if args.len() < 2 {
        eprintln!("{}", SET_QUOTA_USAGE);
        exit(1);
    }

    rpc::run(
        format!(
            "Flezha.Service.CliManager.authz_render_namespace_set_quota(\"{}\", {}, {})",
            escape_rpc_arg(&args[1]),
            quotas_json_literal(&args[2..]),
            token_opts_fragment(&args[2..]),
        ),
        rpc::PROPAGATE,
    );
}
