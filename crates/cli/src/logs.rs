use serde_json::Value;

use crate::args::{OptSpec, Positional, Reading, DURATION_MS, NAMESPACE};
use crate::dispatch::{Arg, Command, Missing, Verb};
use crate::{help, rpc};

// One option per filter a kind declares (`flz logs kinds` lists which kind
// takes which); the controller refuses one the kind does not declare.
const IN_NAMESPACE: OptSpec = NAMESPACE.doc("Match records of this namespace.");
const FACTORY: OptSpec =
    OptSpec::text("--factory", "--factory", "<factory_id>").doc("Match records of this factory.");
const RELAY: OptSpec =
    OptSpec::text("--relay", "--relay", "<relay_id>").doc("Match records of this relay.");
const EVENT: OptSpec = OptSpec::text("--event", "--event", "<event>")
    .doc("Match records of this event: started, ended or start_failed.");
const OUTCOME: OptSpec = OptSpec::text("--outcome", "--outcome", "<outcome>")
    .doc("Match ended records with this outcome.");
pub(crate) const SOURCE: OptSpec =
    OptSpec::text("--source", "--source", "<source>").doc("Match records whose source is this.");
pub(crate) const NAME: OptSpec =
    OptSpec::text("--name", "--name", "<name>").doc("Match records whose name is this.");

pub(crate) const SINCE: OptSpec = OptSpec::number("--since", "--since", "<microseconds>")
    .doc("Match records written at or after this wall-clock time.");
pub(crate) const UNTIL: OptSpec = OptSpec::number("--until", "--until", "<microseconds>")
    .doc("Match records written at or before this wall-clock time.");
pub(crate) const LIMIT: OptSpec = OptSpec::number("--limit", "--limit", "<count>")
    .doc("Return at most this many records (default 100, at most 1000).");
pub(crate) const BEFORE: OptSpec = OptSpec::text("--before", "--before", "<cursor>")
    .doc("Continue from the `next` cursor a previous read printed.");
pub(crate) const NODE: OptSpec = OptSpec::text("--node", "--node", "<controller>")
    .doc("Only this controller (with --file, its file; with rotate, rotate it alone).");
pub(crate) const FILE: OptSpec =
    OptSpec::text("--file", "--file", "<file>").doc("Read this one file, oldest first.");

const KIND: &[Positional] = &[Positional::required("<kind>")];

pub(crate) const READ_OPTIONS: &[OptSpec] = &[
    IN_NAMESPACE,
    FACTORY,
    RELAY,
    EVENT,
    OUTCOME,
    SOURCE,
    NAME,
    SINCE,
    UNTIL,
    LIMIT,
    BEFORE,
    NODE,
    FILE,
];

/// Every filter option, by the field it fills.
const FILTERS: &[(&str, &OptSpec)] = &[
    ("namespace", &IN_NAMESPACE),
    ("factory", &FACTORY),
    ("relay", &RELAY),
    ("event", &EVENT),
    ("outcome", &OUTCOME),
    ("source", &SOURCE),
    ("name", &NAME),
];

/// The filters written among those `fields` names, by field; `None` when none
/// was. A verb reads only the options it declares.
pub(crate) fn written_filters(read: &Reading, fields: &[&str]) -> Option<Value> {
    let written: serde_json::Map<String, Value> = FILTERS
        .iter()
        .filter(|(field, _spec)| fields.contains(field))
        .filter_map(|(field, spec)| Some((field.to_string(), Value::from(read.opt(spec.long)?))))
        .collect();
    (!written.is_empty()).then_some(Value::Object(written))
}

fn filters(read: &Reading) -> Option<Value> {
    let every: Vec<&str> = FILTERS.iter().map(|(field, _spec)| *field).collect();
    written_filters(read, &every)
}

pub(crate) const TABLE: Command = Command {
    path: "flz logs",
    title: "The controller's durable logs (the relay history and the audit log), read across the cluster.",
    notes: Some(help::LOGS),
    verbs: &[
        Verb::door("kinds").calls(&["Logs.kinds"]).help_word(),
        Verb::door("read")
            .taking(KIND)
            .reading(READ_OPTIONS)
            .sending(&[Arg::Fields(&[
                ("kind", Arg::Pos("<kind>")),
                ("filters", Arg::Built(filters)),
                ("since_us", Arg::Opt(&SINCE)),
                ("until_us", Arg::Opt(&UNTIL)),
                ("limit", Arg::Opt(&LIMIT)),
                ("before", Arg::Opt(&BEFORE)),
                ("node", Arg::Opt(&NODE)),
                ("file", Arg::Opt(&FILE)),
            ])])
            .calls(&["Logs.read"])
            .notes(help::LOGS_READ)
            .help_word(),
        Verb::door("tail")
            .taking(KIND)
            .reading(&[IN_NAMESPACE, FACTORY, RELAY, EVENT, OUTCOME, SOURCE, NAME, DURATION_MS])
            .sending(&[Arg::Fields(&[
                ("kind", Arg::Pos("<kind>")),
                ("filters", Arg::Built(filters)),
            ])])
            .carrying(&[("duration_ms", Arg::Built(rpc::window))])
            .calls(&["Logs.tail"])
            .notes(help::LOGS_TAIL)
            .help_word(),
        Verb::door("files")
            .taking(KIND)
            .sending(&[Arg::Fields(&[("kind", Arg::Pos("<kind>"))])])
            .calls(&["Logs.files"])
            .help_word(),
        Verb::door("stats")
            .taking(KIND)
            .sending(&[Arg::Fields(&[("kind", Arg::Pos("<kind>"))])])
            .calls(&["Logs.stats"])
            .help_word(),
        Verb::door("rotate")
            .taking(KIND)
            .reading(&[NODE])
            .sending(&[Arg::Fields(&[
                ("kind", Arg::Pos("<kind>")),
                ("node", Arg::Opt(&NODE)),
            ])])
            .calls(&["Logs.rotate"])
            .help_word(),
    ],
    missing: Missing::Screen,
    unknown_prints_help: false,
};
