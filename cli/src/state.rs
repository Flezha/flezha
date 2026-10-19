use serde_json::Value;

use crate::args::{Positional, Reading};
use crate::dispatch::{Command, Missing, Verb};
use crate::{args, help, rpc};

/// The Cell-scoped write verbs all name the same slot.
const CELL: &[Positional] = &[Positional::required("<relay/tasklet/cell_key>")];

pub(crate) const TABLE: Command = Command {
    path: "flz state",
    title: "Per-Cell state inspection and time-travel queries.",
    notes: Some(help::STATE),
    verbs: &[
        Verb::new("get", rpc::one_arg)
            .taking(&[Positional::required("<relay/tasklet[/cell_key]>")])
            .calls(&["Steps.state_get"]),
        Verb::new("list", rpc::one_arg)
            .taking(&[Positional::required("<relay_class>")])
            .calls(&["Steps.cells"]),
        Verb::new("at", handle_at)
            .taking(&[
                Positional::required("<relay/tasklet/cell_key>"),
                Positional::required("<seq | iso8601-timestamp>"),
            ])
            .calls(&["Steps.time_travel"]),
        Verb::new("rotate", rpc::one_arg)
            .taking(CELL)
            .calls(&["Steps.rotate"]),
        Verb::new("retention", handle_retention)
            .taking(&[
                Positional::required("<relay_class>"),
                Positional::number("<hours>"),
            ])
            .calls(&["Steps.set_retention"]),
        Verb::new("delete", rpc::one_arg)
            .taking(CELL)
            .calls(&["Steps.delete"]),
        Verb::new("outbox", rpc::one_arg)
            .taking(CELL)
            .calls(&["Steps.outbox"]),
        Verb::new("journal", rpc::one_arg)
            .taking(&[Positional::required("<relay/instance>")])
            .calls(&["Steps.journal"]),
        Verb::new("inbox", rpc::one_arg)
            .taking(&[Positional::required("<relay/instance>")])
            .calls(&["Steps.inbox"]),
    ],
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
