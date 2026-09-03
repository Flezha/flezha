use std::process::exit;

use crate::dispatch::{self, Command};
use crate::rpc::{self, escape_rpc_arg, token_opts_fragment};
use crate::{args, help};

pub(crate) const TABLE: Command = Command {
    help: help::TOKEN,
    verbs: &[
        ("issue", handle_issue),
        ("revoke", handle_revoke),
        ("list", handle_list),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: None,
    unknown: "Error: Invalid argument '{}'. Use 'issue', 'revoke', 'list', or 'help'.",
    unknown_prints_help: false,
};

fn handle_issue(args: &[String]) {
    if args.len() < 4 {
        eprintln!(
            "Usage: flz token issue <principal> <namespace> <viewer|operator|admin|runner> \
             [--ttl <seconds>] [--token <admin-token>]"
        );
        exit(1);
    }

    let ttl = match args::or_refuse(args::number::<u64>(&args[4..], "--ttl", "--ttl")) {
        // A range is this option's own rule; the parse is the shared reader's.
        Some(0) => {
            eprintln!("Error: --ttl requires a positive integer (seconds)");
            exit(1);
        }
        Some(seconds) => seconds,
        // Default CLI-token TTL (config `:token_ttl_default_seconds`).
        None => 86_400,
    };

    rpc::run(
        format!(
            "Flezha.Service.CliManager.authz_render_token_issue(\"{}\", \"{}\", \"{}\", {}, {})",
            escape_rpc_arg(&args[1]),
            escape_rpc_arg(&args[2]),
            escape_rpc_arg(&args[3]),
            ttl,
            token_opts_fragment(&args[4..]),
        ),
        rpc::PROPAGATE,
    );
}

fn handle_revoke(args: &[String]) {
    rpc::one_arg_authz(
        args,
        "authz_render_token_revoke",
        "Usage: flz token revoke <token-id> [--token <admin-token>]",
    );
}

fn handle_list(args: &[String]) {
    rpc::token_only(args, "authz_render_token_list");
}
