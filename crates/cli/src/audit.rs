use serde_json::Value;

use crate::args::{Positional, Reading, DURATION_MS};
use crate::dispatch::{Arg, Command, Missing, Verb};
use crate::logs::{written_filters, BEFORE, LIMIT, NAME, NODE, SINCE, SOURCE, UNTIL};
use crate::{help, rpc};

/// `flz audit` is `flz logs` with the audit kind filled in: every verb below
/// calls a logs operation, and nothing here reads or prints a record.
fn audit(_read: &Reading) -> Option<Value> {
    Some(Value::from("audit"))
}

/// The two filters the audit verbs declare.
fn filters(read: &Reading) -> Option<Value> {
    written_filters(read, &["source", "name"])
}

pub(crate) const TABLE: Command = Command {
    path: "flz audit",
    title: "Durable audit log inspection (`flz logs ... audit`).",
    notes: None,
    verbs: &[
        Verb::door("list")
            .sending(&[Arg::Fields(&[("kind", Arg::Built(audit))])])
            .calls(&["Logs.files"])
            .help_word(),
        Verb::door("read")
            .taking(&[Positional::optional("<file>")])
            .reading(&[SOURCE, NAME, SINCE, UNTIL, LIMIT, BEFORE, NODE])
            .sending(&[Arg::Fields(&[
                ("kind", Arg::Built(audit)),
                ("file", Arg::Pos("<file>")),
                ("filters", Arg::Built(filters)),
                ("since_us", Arg::Opt(&SINCE)),
                ("until_us", Arg::Opt(&UNTIL)),
                ("limit", Arg::Opt(&LIMIT)),
                ("before", Arg::Opt(&BEFORE)),
                ("node", Arg::Opt(&NODE)),
            ])])
            .calls(&["Logs.read"])
            .notes(help::AUDIT_READ)
            .help_word(),
        Verb::door("tail")
            .reading(&[SOURCE, NAME, DURATION_MS])
            .sending(&[Arg::Fields(&[
                ("kind", Arg::Built(audit)),
                ("filters", Arg::Built(filters)),
            ])])
            .carrying(&[("duration_ms", Arg::Built(rpc::window))])
            .calls(&["Logs.tail"])
            .notes(help::LOGS_TAIL)
            .help_word(),
        Verb::door("stats")
            .sending(&[Arg::Fields(&[("kind", Arg::Built(audit))])])
            .calls(&["Logs.stats"])
            .help_word(),
        Verb::door("rotate")
            .reading(&[NODE])
            .sending(&[Arg::Fields(&[
                ("kind", Arg::Built(audit)),
                ("node", Arg::Opt(&NODE)),
            ])])
            .calls(&["Logs.rotate"])
            .help_word(),
    ],
    missing: Missing::Screen,
    unknown_prints_help: false,
};
