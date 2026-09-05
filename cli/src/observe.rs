use crate::args::{OptSpec, Positional, Reading, DURATION_MS, JSON_SWITCH, OUTPUT};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, text};

const SOURCE: &[Positional] = &[Positional::required("<source>")];
const FORMAT: &[OptSpec] = &[OUTPUT, JSON_SWITCH];

pub(crate) const TABLE: Command = Command {
    path: "flz observe",
    help: help::OBSERVE,
    verbs: &[
        Verb::new("sources", |read| {
            rpc::rendered(read, "observability_render_sources", rpc::DIAGNOSED)
        })
        .reading(FORMAT)
        .screen(help::OBSERVE_SOURCES),
        Verb::new("sinks", |read| {
            rpc::rendered(read, "observability_render_sinks", rpc::DIAGNOSED)
        })
        .reading(FORMAT)
        .screen(help::OBSERVE_SINKS),
        Verb::new("tail", handle_tail)
            .taking(SOURCE)
            .reading(&[DURATION_MS, OUTPUT, JSON_SWITCH])
            .screen(help::OBSERVE_TAIL),
        Verb::new("enable", |read| {
            rpc::one_arg(
                read,
                SOURCE[0].name,
                "observability_render_enable",
                rpc::DIAGNOSED,
            )
        })
        .taking(SOURCE)
        .screen(help::OBSERVE_ENABLE),
        Verb::new("disable", |read| {
            rpc::one_arg(
                read,
                SOURCE[0].name,
                "observability_render_disable",
                rpc::DIAGNOSED,
            )
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
        &[text(read.pos(SOURCE[0].name)), text(read.output_format())],
        &[rpc::window(read)],
        read.token(),
        rpc::DIAGNOSED,
    );
}
