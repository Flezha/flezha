use crate::args::{Positional, Reading, JSON_SWITCH, OUTPUT};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, text};

/// The Cell-scoped write verbs all name the same slot.
const CELL: &[Positional] = &[Positional::required("<operative/tasklet/cell_key>")];

pub(crate) const TABLE: Command = Command {
    path: "flz state",
    help: help::STATE,
    verbs: &[
        Verb::new("get", |read| scoped_verb(read, "state_get"))
            .taking(&[Positional::required("<operative/tasklet[/cell_key]>")])
            .reading(&[OUTPUT, JSON_SWITCH]),
        Verb::new("list", |read| scoped_verb(read, "state_list"))
            .taking(&[Positional::required("<operative_class>")])
            .reading(&[OUTPUT, JSON_SWITCH]),
        Verb::new("at", handle_at)
            .taking(&[
                Positional::required("<operative/tasklet/cell_key>"),
                Positional::required("<seq | iso8601-timestamp>"),
            ])
            .reading(&[OUTPUT, JSON_SWITCH]),
        Verb::new("rotate", |read| {
            rpc::one_arg(read, CELL[0].name, "state_rotate", rpc::PROPAGATE)
        })
        .taking(CELL),
        Verb::new("retention", handle_retention).taking(&[
            Positional::required("<operative_class>"),
            Positional::number("<hours>"),
        ]),
        Verb::new("delete", |read| {
            rpc::one_arg(read, CELL[0].name, "state_delete", rpc::PROPAGATE)
        })
        .taking(CELL),
        Verb::new("outbox", |read| {
            rpc::one_arg(read, CELL[0].name, "state_outbox", rpc::PROPAGATE)
        })
        .taking(CELL),
        Verb::new("journal", |read| scoped_verb(read, "state_journal"))
            .taking(&[Positional::required("<operative/instance>")])
            .reading(&[OUTPUT, JSON_SWITCH]),
        Verb::new("inbox", |read| scoped_verb(read, "state_inbox"))
            .taking(&[Positional::required("<operative/instance>")])
            .reading(&[OUTPUT, JSON_SWITCH]),
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
            text(read.output_format()),
        ],
        &[],
        read.token(),
        rpc::PROPAGATE,
    );
}

fn handle_retention(read: &Reading) {
    rpc::run(
        "state_retention",
        &[
            text(read.pos("<operative_class>")),
            text(read.pos("<hours>")),
        ],
        &[],
        read.token(),
        rpc::PROPAGATE,
    );
}

/// The read verbs — `get`, `list`, `journal`, `inbox` — take one scope argument
/// plus a rendering choice; they differ only in that argument's name and the
/// CliManager function they call.
fn scoped_verb(read: &Reading, rpc_fun: &str) {
    let scope = read.positional_at(0);

    rpc::run(
        rpc_fun,
        &[text(scope), text(read.output_format())],
        &[],
        read.token(),
        rpc::PROPAGATE,
    );
}
