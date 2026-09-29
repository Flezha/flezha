use crate::args::{Positional, Reading};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, text};

/// The Cell-scoped write verbs all name the same slot.
const CELL: &[Positional] = &[Positional::required("<operator/tasklet/cell_key>")];

pub(crate) const TABLE: Command = Command {
    path: "flz state",
    help: help::STATE,
    verbs: &[
        Verb::new("get", |read| scoped_verb(read, "state_get"))
            .taking(&[Positional::required("<operator/tasklet[/cell_key]>")]),
        Verb::new("list", |read| scoped_verb(read, "state_list"))
            .taking(&[Positional::required("<operator_class>")]),
        Verb::new("at", handle_at).taking(&[
            Positional::required("<operator/tasklet/cell_key>"),
            Positional::required("<seq | iso8601-timestamp>"),
        ]),
        Verb::new("rotate", |read| {
            rpc::one_arg(read, CELL[0].name, "state_rotate")
        })
        .taking(CELL),
        Verb::new("retention", handle_retention).taking(&[
            Positional::required("<operator_class>"),
            Positional::number("<hours>"),
        ]),
        Verb::new("delete", |read| {
            rpc::one_arg(read, CELL[0].name, "state_delete")
        })
        .taking(CELL),
        Verb::new("outbox", |read| {
            rpc::one_arg(read, CELL[0].name, "state_outbox")
        })
        .taking(CELL),
        Verb::new("journal", |read| scoped_verb(read, "state_journal"))
            .taking(&[Positional::required("<operator/instance>")]),
        Verb::new("inbox", |read| scoped_verb(read, "state_inbox"))
            .taking(&[Positional::required("<operator/instance>")]),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Missing::Refuse,
    unknown_prints_help: false,
};

fn handle_at(read: &Reading) {
    rpc::run(
        "state_at",
        &[
            text(read.pos(CELL[0].name)),
            text(read.pos("<seq | iso8601-timestamp>")),
        ],
        &[],
        read,
    );
}

fn handle_retention(read: &Reading) {
    rpc::run(
        "state_retention",
        &[
            text(read.pos("<operator_class>")),
            text(read.pos("<hours>")),
        ],
        &[],
        read,
    );
}

/// The read verbs — `get`, `list`, `journal`, `inbox` — take one scope argument;
/// they differ only in that argument's name and the CliManager function they
/// call.
fn scoped_verb(read: &Reading, rpc_fun: &str) {
    let scope = read.positional_at(0);

    rpc::run(rpc_fun, &[text(scope)], &[], read);
}
