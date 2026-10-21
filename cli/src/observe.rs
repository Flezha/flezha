use crate::args::{Positional, DURATION_MS};
use crate::dispatch::{Arg, Command, Missing, Verb};
use crate::{help, rpc};

const SOURCE: &[Positional] = &[Positional::required("<source>")];

pub(crate) const TABLE: Command = Command {
    path: "flz observe",
    title: "Observability foundation inspection.",
    notes: None,
    verbs: &[
        Verb::door("sources")
            .calls(&["Observability.list_sources"])
            .notes(help::OBSERVE_SOURCES)
            .help_word(),
        Verb::door("sinks")
            .calls(&["Observability.list_sinks"])
            .notes(help::OBSERVE_SINKS)
            .help_word(),
        Verb::door("tail")
            .taking(SOURCE)
            .reading(&[DURATION_MS])
            .carrying(&[("duration_ms", Arg::Built(rpc::window))])
            .calls(&["Observability.tail"])
            .notes(help::OBSERVE_TAIL)
            .help_word(),
        Verb::door("enable")
            .taking(SOURCE)
            .calls(&["Observability.enable_source"])
            .help_word(),
        Verb::door("disable")
            .taking(SOURCE)
            .calls(&["Observability.disable_source"])
            .help_word(),
    ],
    missing: Missing::Screen,
    unknown_prints_help: false,
};
