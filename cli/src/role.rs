use crate::args::{Positional, Reading};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, text};

/// The enumeration, spelled once: both verbs read the same label, so a role
/// added to it cannot reach one of them and miss the other.
const ROLE: &str = "<viewer|app|ops|admin|runner|spawner|stopper>";

pub(crate) const TABLE: Command = Command {
    path: "flz role",
    help: help::ROLE,
    verbs: &[
        Verb::new("bind", handle_bind).taking(&[
            Positional::required("<principal>"),
            Positional::required(ROLE),
            Positional::required("<namespace>"),
        ]),
        Verb::new("unbind", handle_unbind).taking(&[
            Positional::required("<principal>"),
            Positional::optional(ROLE),
        ]),
        Verb::new("list", |read| {
            rpc::token_only(read, "authz_render_role_list")
        }),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Missing::Screen,
    unknown_prints_help: false,
};

fn handle_bind(read: &Reading) {
    rpc::run(
        "authz_render_role_bind",
        &[
            text(read.pos("<principal>")),
            text(read.pos(ROLE)),
            text(read.pos("<namespace>")),
        ],
        &[],
        read.token(),
        rpc::PROPAGATE,
    );
}

/// The role is always sent, empty when none was named, so the controller reads
/// the principal, the role and the options in fixed positions.
fn handle_unbind(read: &Reading) {
    rpc::run(
        "authz_render_role_unbind",
        &[text(read.pos("<principal>")), text(read.pos(ROLE))],
        &[],
        read.token(),
        rpc::PROPAGATE,
    );
}
