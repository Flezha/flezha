use serde_json::Value;

use crate::args::{Positional, Reading, DURATION_MS};
use crate::dispatch::{Command, Missing, Verb};
use crate::{help, rpc};

const SOURCE: &[Positional] = &[Positional::required("<source>")];

pub(crate) const TABLE: Command = Command {
    path: "flz observe",
    help: help::OBSERVE,
    verbs: &[
        Verb::new("sources", |read| {
            rpc::bare(read, "Observability.list_sources")
        })
        .screen(help::OBSERVE_SOURCES),
        Verb::new("sinks", |read| rpc::bare(read, "Observability.list_sinks"))
            .screen(help::OBSERVE_SINKS),
        Verb::new("tail", handle_tail)
            .taking(SOURCE)
            .reading(&[DURATION_MS])
            .screen(help::OBSERVE_TAIL),
        Verb::new("enable", |read| {
            rpc::one_arg(read, SOURCE[0].name, "Observability.enable_source")
        })
        .taking(SOURCE)
        .screen(help::OBSERVE_ENABLE),
        Verb::new("disable", |read| {
            rpc::one_arg(read, SOURCE[0].name, "Observability.disable_source")
        })
        .taking(SOURCE)
        .screen(help::OBSERVE_DISABLE),
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
