use std::process::exit;

use crate::dispatch::{self, Command};
use crate::help;
use crate::rpc::{self, text};

pub(crate) const TABLE: Command = Command {
    help: help::ROLE,
    verbs: &[
        ("bind", handle_bind),
        ("unbind", handle_unbind),
        ("list", handle_list),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: None,
    unknown: "Error: Invalid argument '{}'. Use 'bind', 'unbind', 'list', or 'help'.",
    unknown_prints_help: false,
};

fn handle_bind(args: &[String]) {
    if args.len() < 4 {
        eprintln!(
            "Usage: flz role bind <principal> <viewer|operator|admin|runner> <namespace> \
             [--token <admin-token>]"
        );
        exit(1);
    }

    rpc::run(
        "authz_render_role_bind",
        &[text(&args[1]), text(&args[2]), text(&args[3])],
        &[],
        args,
        rpc::PROPAGATE,
    );
}

fn handle_unbind(args: &[String]) {
    rpc::one_arg_authz(
        args,
        "authz_render_role_unbind",
        "Usage: flz role unbind <principal> [--token <admin-token>]",
    );
}

fn handle_list(args: &[String]) {
    rpc::token_only(args, "authz_render_role_list");
}
