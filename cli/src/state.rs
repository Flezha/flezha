use serde_json::Value;

use crate::args::{Positional, Reading};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::{args, help, rpc};

/// The Cell-scoped write verbs all name the same slot.
const CELL: &[Positional] = &[Positional::required("<relay/tasklet/cell_key>")];

pub(crate) const TABLE: Command = Command {
    path: "flz state",
    help: help::STATE,
    verbs: &[
        Verb::new("get", |read| scoped_verb(read, "Steps.state_get"))
            .taking(&[Positional::required("<relay/tasklet[/cell_key]>")]),
        Verb::new("list", |read| scoped_verb(read, "Steps.cells"))
            .taking(&[Positional::required("<relay_class>")]),
        Verb::new("at", handle_at).taking(&[
            Positional::required("<relay/tasklet/cell_key>"),
            Positional::required("<seq | iso8601-timestamp>"),
        ]),
        Verb::new("rotate", |read| {
            rpc::one_arg(read, CELL[0].name, "Steps.rotate")
        })
        .taking(CELL),
        Verb::new("retention", handle_retention).taking(&[
            Positional::required("<relay_class>"),
            Positional::number("<hours>"),
        ]),
        Verb::new("delete", |read| {
            rpc::one_arg(read, CELL[0].name, "Steps.delete")
        })
        .taking(CELL),
        Verb::new("outbox", |read| {
            rpc::one_arg(read, CELL[0].name, "Steps.outbox")
        })
        .taking(CELL),
        Verb::new("journal", |read| scoped_verb(read, "Steps.journal"))
            .taking(&[Positional::required("<relay/instance>")]),
        Verb::new("inbox", |read| scoped_verb(read, "Steps.inbox"))
            .taking(&[Positional::required("<relay/instance>")]),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Missing::Refuse,
    unknown_prints_help: false,
};

fn handle_at(read: &Reading) {
    rpc::door(
        "Steps.time_travel",
        &[
            Value::from(read.pos(CELL[0].name)),
            Value::from(read.pos("<seq | iso8601-timestamp>")),
        ],
        &[],
        read,
    );
}

fn handle_retention(read: &Reading) {
    let hours = args::or_refuse(args::read_number::<i64>("<hours>", read.pos("<hours>")));
    if hours < 0 {
        rpc::fail(flz_transport::errors::FlezhaError::with(
            "usage.negative",
            [
                ("option", Value::from("<hours>")),
                ("value", Value::from(hours)),
            ],
        ));
    }

    rpc::door(
        "Steps.set_retention",
        &[Value::from(read.pos("<relay_class>")), Value::from(hours)],
        &[],
        read,
    );
}

fn scoped_verb(read: &Reading, name: &str) {
    rpc::door(name, &[Value::from(read.positional_at(0))], &[], read);
}
