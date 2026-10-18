use serde_json::Value;

use crate::args::{Positional, Reading, DURATION_MS};
use crate::dispatch::{Command, Missing, Verb};
use crate::{help, rpc};

const SOURCE: &[Positional] = &[Positional::required("<source>")];

pub(crate) const TABLE: Command = Command {
    path: "flz observe",
    title: "Observability foundation inspection.",
    notes: None,
    verbs: &[
        Verb::new("sources", |read| {
            rpc::bare(read, "Observability.list_sources")
        })
        .calls(&["Observability.list_sources"])
        .notes(help::OBSERVE_SOURCES)
        .help_word(),
        Verb::new("sinks", |read| rpc::bare(read, "Observability.list_sinks"))
            .calls(&["Observability.list_sinks"])
            .notes(help::OBSERVE_SINKS)
            .help_word(),
        Verb::new("tail", handle_tail)
            .taking(SOURCE)
            .reading(&[DURATION_MS])
            .calls(&["Observability.tail"])
            .notes(help::OBSERVE_TAIL)
            .help_word(),
        Verb::new("enable", |read| {
            rpc::one_arg(read, SOURCE[0].name, "Observability.enable_source")
        })
        .taking(SOURCE)
        .calls(&["Observability.enable_source"])
        .help_word(),
        Verb::new("disable", |read| {
            rpc::one_arg(read, SOURCE[0].name, "Observability.disable_source")
        })
        .taking(SOURCE)
        .calls(&["Observability.disable_source"])
        .help_word(),
    ],
    missing: Missing::Screen,
    unknown_prints_help: false,
};

fn handle_tail(read: &Reading) {
    rpc::door(
        "Observability.tail",
        &[Value::from(read.pos(SOURCE[0].name))],
        &[rpc::window(read)],
        read,
    );
}
