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

    let ttl = match args::extract_param(&args[4..], "--ttl", "--ttl") {
        Some(t) => match t.parse::<u64>() {
            Ok(n) if n > 0 => n.to_string(),
            _ => {
                eprintln!("Error: --ttl requires a positive integer (seconds)");
                exit(1);
            }
        },
        // Default CLI-token TTL (config `:token_ttl_default_seconds`).
        None => "86400".to_string(),
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
