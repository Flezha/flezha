use std::process::exit;

use crate::args::{OptSpec, Positional, Reading};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, text};

/// How long an issued token lives. An option, not a positional: it is optional
/// and the request carries it in the options list like every other one.
const TTL: OptSpec = OptSpec::number("--ttl", "--ttl", "<seconds>");

pub(crate) const TABLE: Command = Command {
    path: "flz token",
    help: help::TOKEN,
    verbs: &[
        Verb::new("issue", handle_issue)
            .taking(&[
                Positional::required("<principal>"),
                Positional::required("<namespace>"),
                Positional::required("<viewer|operator|admin|runner>"),
            ])
            .reading(&[TTL]),
        Verb::new("revoke", |read| {
            rpc::one_arg(
                read,
                "<token-id>",
                "authz_render_token_revoke",
                rpc::PROPAGATE,
            )
        })
        .taking(&[Positional::required("<token-id>")]),
        Verb::new("list", |read| {
            rpc::token_only(read, "authz_render_token_list")
        }),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Missing::Screen,
    unknown_prints_help: false,
};

fn handle_issue(read: &Reading) {
    let ttl = match read.num::<u64>(TTL.long) {
        // A range is this option's own rule; the parse is the walk's.
        Some(0) => {
            eprintln!("Error: --ttl requires a positive integer (seconds)");
            exit(1);
        }
        Some(seconds) => seconds,
        // Default CLI-token TTL (config `:token_ttl_default_seconds`).
        None => 86_400,
    };

    rpc::run(
        "authz_render_token_issue",
        &[
            text(read.pos("<principal>")),
            text(read.pos("<namespace>")),
            text(read.pos("<viewer|operator|admin|runner>")),
        ],
        &[("ttl", ttl.to_string())],
        read.token(),
        rpc::PROPAGATE,
    );
}
