use serde_json::{Map, Value};

use crate::args::{OptSpec, Positional, Reading, DURATION_MS};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::{help, rpc};

/// The five filters `audit read` narrows a file with. Their long spelling is
/// their own name, and the parse and its refusal are the walk's.
const SOURCE: OptSpec = OptSpec::text("--source", "--source", "<source>");
const NAME: OptSpec = OptSpec::text("--name", "--name", "<name>");
const SINCE: OptSpec = OptSpec::number("--since", "--since", "<microseconds>");
const UNTIL: OptSpec = OptSpec::number("--until", "--until", "<microseconds>");
const LIMIT: OptSpec = OptSpec::number("--limit", "--limit", "<count>");

pub(crate) const TABLE: Command = Command {
    path: "flz audit",
    help: help::AUDIT,
    verbs: &[
        Verb::new("list", |read| rpc::bare(read, "AuditLog.list_files")).screen(help::AUDIT_LIST),
        Verb::new("read", handle_read)
            .taking(&[Positional::required("<file>")])
            .reading(&[SOURCE, NAME, SINCE, UNTIL, LIMIT])
            .screen(help::AUDIT_READ),
        Verb::new("tail", handle_tail)
            .reading(&[DURATION_MS])
            .screen(help::AUDIT_TAIL),
        Verb::new("stats", |read| rpc::bare(read, "AuditLog.stats")).screen(help::AUDIT_STATS),
        Verb::new("rotate", |read| rpc::bare(read, "AuditLog.rotate")).screen(help::AUDIT_ROTATE),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Missing::Screen,
    unknown_prints_help: false,
};

fn handle_read(read: &Reading) {
    let mut params = Map::new();
    params.insert("file".into(), Value::from(read.pos("<file>")));

    for (spec, key) in [(SOURCE, "source"), (NAME, "name")] {
        if let Some(value) = read.opt(spec.long) {
            params.insert(key.into(), Value::from(value));
        }
    }
    for (spec, key) in [(SINCE, "since_us"), (UNTIL, "until_us"), (LIMIT, "limit")] {
        if let Some(value) = read.num::<i64>(spec.long) {
            params.insert(key.into(), Value::from(value));
        }
    }

    rpc::door("AuditLog.read", &[Value::Object(params)], &[], read);
}

fn handle_tail(read: &Reading) {
    rpc::door("AuditLog.tail", &[], &[rpc::window(read)], read);
}
