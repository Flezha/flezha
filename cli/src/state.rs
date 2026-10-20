use crate::args::Positional;
use crate::dispatch::{Command, Missing, Verb};
use crate::help;

/// The Cell-scoped write verbs all name the same slot.
const CELL: &[Positional] = &[Positional::required("<relay/tasklet/cell_key>")];

pub(crate) const TABLE: Command = Command {
    path: "flz state",
    title: "Per-Cell state inspection and time-travel queries.",
    notes: Some(help::STATE),
    verbs: &[
        Verb::door("get")
            .taking(&[Positional::required("<relay/tasklet[/cell_key]>")])
            .calls(&["Steps.state_get"]),
        Verb::door("list")
            .taking(&[Positional::required("<relay_class>")])
            .calls(&["Steps.cells"]),
        Verb::door("at")
            .taking(&[
                Positional::required("<relay/tasklet/cell_key>"),
                Positional::required("<seq | iso8601-timestamp>"),
            ])
            .calls(&["Steps.time_travel"]),
        Verb::door("rotate").taking(CELL).calls(&["Steps.rotate"]),
        Verb::door("retention")
            .taking(&[
                Positional::required("<relay_class>"),
                Positional::number("<hours>"),
            ])
            .calls(&["Steps.set_retention"]),
        Verb::door("delete").taking(CELL).calls(&["Steps.delete"]),
        Verb::door("outbox").taking(CELL).calls(&["Steps.outbox"]),
        Verb::door("journal")
            .taking(&[Positional::required("<relay/instance>")])
            .calls(&["Steps.journal"]),
        Verb::door("inbox")
            .taking(&[Positional::required("<relay/instance>")])
            .calls(&["Steps.inbox"]),
    ],
    missing: Missing::Refuse,
    unknown_prints_help: false,
};
