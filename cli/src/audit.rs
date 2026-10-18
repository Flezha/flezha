use serde_json::{Map, Value};

use crate::args::{OptSpec, Positional, Reading, DURATION_MS};
use crate::dispatch::{Command, Missing, Verb};
use crate::{help, rpc};

/// The five filters `audit read` narrows a file with. Their long spelling is
/// their own name, and the parse and its refusal are the walk's.
const SOURCE: OptSpec =
    OptSpec::text("--source", "--source", "<source>").doc("Match records whose source is this.");
const NAME: OptSpec =
    OptSpec::text("--name", "--name", "<name>").doc("Match records whose name is this.");
const SINCE: OptSpec = OptSpec::number("--since", "--since", "<microseconds>")
    .doc("Match records with ts_us at or after this (monotonic).");
const UNTIL: OptSpec = OptSpec::number("--until", "--until", "<microseconds>")
    .doc("Match records with ts_us at or before this.");
const LIMIT: OptSpec = OptSpec::number("--limit", "--limit", "<count>")
    .doc("Return at most this many records (default 1000).");

pub(crate) const TABLE: Command = Command {
    path: "flz audit",
    title: "Durable audit log inspection.",
    notes: None,
    verbs: &[
        Verb::new("list", |read| rpc::bare(read, "AuditLog.list_files"))
            .calls(&["AuditLog.list_files"])
            .help_word(),
        Verb::new("read", handle_read)
            .taking(&[Positional::required("<file>")])
            .reading(&[SOURCE, NAME, SINCE, UNTIL, LIMIT])
            .calls(&["AuditLog.read"])
            .notes(help::AUDIT_READ)
            .help_word(),
        Verb::new("tail", handle_tail)
            .reading(&[DURATION_MS])
            .calls(&["AuditLog.tail"])
            .notes(help::AUDIT_TAIL)
            .help_word(),
        Verb::new("stats", |read| rpc::bare(read, "AuditLog.stats"))
            .calls(&["AuditLog.stats"])
            .help_word(),
        Verb::new("rotate", |read| rpc::bare(read, "AuditLog.rotate"))
            .calls(&["AuditLog.rotate"])
            .help_word(),
    ],
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
