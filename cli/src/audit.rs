use crate::args::{OptSpec, Positional, Reading, DURATION_MS, JSON_SWITCH, OUTPUT};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, text, Opt};

/// The five filters `audit read` narrows a file with. Their long spelling is
/// their own name, and the parse and its refusal are the walk's.
const SOURCE: OptSpec = OptSpec::text("--source", "--source", "<source>");
const NAME: OptSpec = OptSpec::text("--name", "--name", "<name>");
const SINCE: OptSpec = OptSpec::number("--since", "--since", "<microseconds>");
const UNTIL: OptSpec = OptSpec::number("--until", "--until", "<microseconds>");
const LIMIT: OptSpec = OptSpec::number("--limit", "--limit", "<count>");

/// The three verbs whose whole grammar is a rendering choice.
const FORMAT: &[OptSpec] = &[OUTPUT, JSON_SWITCH];

pub(crate) const TABLE: Command = Command {
    path: "flz audit",
    help: help::AUDIT,
    verbs: &[
        Verb::new("list", |read| {
            rpc::rendered(read, "audit_log_render_list", rpc::DIAGNOSED)
        })
        .reading(FORMAT)
        .screen(help::AUDIT_LIST),
        Verb::new("read", handle_read)
            .taking(&[Positional::required("<file>")])
            .reading(&[SOURCE, NAME, SINCE, UNTIL, LIMIT, OUTPUT, JSON_SWITCH])
            .screen(help::AUDIT_READ),
        Verb::new("tail", handle_tail)
            .reading(&[DURATION_MS, OUTPUT, JSON_SWITCH])
            .screen(help::AUDIT_TAIL),
        Verb::new("stats", |read| {
            rpc::rendered(read, "audit_log_render_stats", rpc::DIAGNOSED)
        })
        .reading(FORMAT)
        .screen(help::AUDIT_STATS),
        Verb::new("rotate", |read| {
            rpc::rendered(read, "audit_log_render_rotate", rpc::DIAGNOSED)
        })
        .reading(FORMAT)
        .screen(help::AUDIT_ROTATE),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Missing::Screen,
    unknown_prints_help: false,
};

fn handle_read(read: &Reading) {
    let mut opts: Vec<Opt> = Vec::new();

    if let Some(source) = read.opt(SOURCE.long) {
        opts.push(("source", text(source)));
    }
    if let Some(name) = read.opt(NAME.long) {
        opts.push(("name", text(name)));
    }
    if let Some(since) = read.opt(SINCE.long) {
        opts.push(("since_us", since.to_string()));
    }
    if let Some(until) = read.opt(UNTIL.long) {
        opts.push(("until_us", until.to_string()));
    }
    if let Some(limit) = read.opt(LIMIT.long) {
        opts.push(("limit", limit.to_string()));
    }

    rpc::run(
        "audit_log_render_read",
        &[text(read.pos("<file>")), text(read.output_format())],
        &opts,
        read.token(),
        rpc::DIAGNOSED,
    );
}

fn handle_tail(read: &Reading) {
    rpc::run(
        "audit_log_render_tail",
        &[text(read.output_format())],
        &[rpc::window(read)],
        read.token(),
        rpc::DIAGNOSED,
    );
}
