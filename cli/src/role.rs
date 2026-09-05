use crate::args::{Positional, Reading};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, text};

pub(crate) const TABLE: Command = Command {
    path: "flz role",
    help: help::ROLE,
    verbs: &[
        Verb::new("bind", handle_bind).taking(&[
            Positional::required("<principal>"),
            Positional::required("<viewer|operator|admin|runner>"),
            Positional::required("<namespace>"),
        ]),
        Verb::new("unbind", |read| {
            rpc::one_arg(
                read,
                "<principal>",
                "authz_render_role_unbind",
                rpc::PROPAGATE,
            )
        })
        .taking(&[Positional::required("<principal>")]),
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
            text(read.pos("<viewer|operator|admin|runner>")),
            text(read.pos("<namespace>")),
        ],
        &[],
        read.token(),
        rpc::PROPAGATE,
    );
}
