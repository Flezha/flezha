use std::process::exit;

use crate::args::{OptSpec, Positional, Reading};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, text};

/// How long an issued token lives. An option, not a positional: it is optional
/// and the request carries it in the options list like every other one. Its
/// value is a number of seconds or the word `never`, which is what the registry
/// stores for an entry that must not lapse — a runner credential is refused any
/// other reading.
const NEVER: &str = "never";
const TTL: OptSpec = OptSpec::number_or("--ttl", "--ttl", "<seconds>|never", &[NEVER]);

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
    rpc::run(
        "authz_render_token_issue",
        &[
            text(read.pos("<principal>")),
            text(read.pos("<namespace>")),
            text(read.pos("<viewer|operator|admin|runner>")),
        ],
        &[("ttl", ttl_literal(read.opt(TTL.long)))],
        read.token(),
        rpc::PROPAGATE,
    );
}

/// The `--ttl` value as the literal the store takes: `:never` for an entry that
/// must not lapse, else a positive number of seconds. The parse is the walk's;
/// the range is this option's own rule, as it always was.
fn ttl_literal(written: Option<&str>) -> String {
    match written {
        // Default CLI-token TTL (config `:token_ttl_default_seconds`).
        None => "86400".to_string(),
        Some(NEVER) => ":never".to_string(),
        Some(seconds) => match seconds.parse::<u64>() {
            Ok(0) | Err(_) => {
                eprintln!("Error: --ttl requires a positive integer (seconds) or 'never'");
                exit(1);
            }
            Ok(positive) => positive.to_string(),
        },
    }
}
