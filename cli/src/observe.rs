use crate::args::{Positional, Reading, DURATION_MS};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, text};

const SOURCE: &[Positional] = &[Positional::required("<source>")];

pub(crate) const TABLE: Command = Command {
    path: "flz observe",
    help: help::OBSERVE,
    verbs: &[
        Verb::new("sources", |read| {
            rpc::bare(read, "observability_render_sources")
        })
        .screen(help::OBSERVE_SOURCES),
        Verb::new("sinks", |read| {
            rpc::bare(read, "observability_render_sinks")
        })
        .screen(help::OBSERVE_SINKS),
        Verb::new("tail", handle_tail)
            .taking(SOURCE)
            .reading(&[DURATION_MS])
            .screen(help::OBSERVE_TAIL),
        Verb::new("enable", |read| {
            rpc::one_arg(read, SOURCE[0].name, "observability_render_enable")
        })
        .taking(SOURCE)
        .screen(help::OBSERVE_ENABLE),
        Verb::new("disable", |read| {
            rpc::one_arg(read, SOURCE[0].name, "observability_render_disable")
        })
        .taking(SOURCE)
        .screen(help::OBSERVE_DISABLE),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Missing::Screen,
    unknown_prints_help: false,
};

fn handle_tail(read: &Reading) {
    rpc::run(
        "observability_render_tail",
        &[text(read.pos(SOURCE[0].name))],
        &[rpc::window(read)],
        read,
    );
}
