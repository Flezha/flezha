use crate::args::{OptSpec, Positional, DURATION_MS};
use crate::dispatch::{Arg, Command, Missing, Verb};
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
        Verb::door("list")
            .calls(&["AuditLog.list_files"])
            .help_word(),
        Verb::door("read")
            .taking(&[Positional::required("<file>")])
            .reading(&[SOURCE, NAME, SINCE, UNTIL, LIMIT])
            .sending(&[Arg::Fields(&[
                ("file", Arg::Pos("<file>")),
                ("source", Arg::Opt(&SOURCE)),
                ("name", Arg::Opt(&NAME)),
                ("since_us", Arg::Opt(&SINCE)),
                ("until_us", Arg::Opt(&UNTIL)),
                ("limit", Arg::Opt(&LIMIT)),
            ])])
            .calls(&["AuditLog.read"])
            .notes(help::AUDIT_READ)
            .help_word(),
        Verb::door("tail")
            .reading(&[DURATION_MS])
            .carrying(&[("duration_ms", Arg::Built(rpc::window))])
            .calls(&["AuditLog.tail"])
            .notes(help::AUDIT_TAIL)
            .help_word(),
        Verb::door("stats").calls(&["AuditLog.stats"]).help_word(),
        Verb::door("rotate").calls(&["AuditLog.rotate"]).help_word(),
    ],
    missing: Missing::Screen,
    unknown_prints_help: false,
};
