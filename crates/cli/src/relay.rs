use std::num::NonZeroU64;

use flz_transport::errors::FlezhaError;
use serde_json::{Map, Value};

use crate::args::{
    self, OptSpec, Positional, Reading, AFTER, DURATION_MS, INIT_VALUES, NAMESPACE, SEARCH,
};
use crate::dispatch::{self, Arg, Command, Missing, Verb};
use crate::{help, rpc};

const RELAY_ID: &[Positional] = &[Positional::required("<relay_id>")];
/// The verbs that name one relay, or one of its Cells, within the namespace.
const SCOPED: &[Arg] = &[Arg::Namespace, Arg::Positionals];

const HEALTH: OptSpec = OptSpec::text("--health", "--health", "<words>")
    .doc("Show only relays of these health words, comma-separated; unhealthy names every fault.");
const FACTORY: OptSpec =
    OptSpec::text("--factory", "--factory", "<factory>").doc("Show only this factory's relays.");
const RUN: OptSpec = OptSpec::text("--run", "--run", "<kind>")
    .doc("Show only relays of this run kind: flow or steps.");
const RUNNER: OptSpec =
    OptSpec::text("--runner", "--runner", "<runner>").doc("Show only relays bound to this runner.");
/// `flz relay list`'s filter flags, by the filter key each sends.
pub(crate) const LIST_FILTERS: &[(&str, &OptSpec)] = &[
    ("health", &HEALTH),
    ("factory", &FACTORY),
    ("run", &RUN),
    ("runner", &RUNNER),
    ("text", &SEARCH),
];

const ID: OptSpec =
    OptSpec::text("--id", "-i", "<id>").doc("Id to start under (default: generated).");
const PLUGINS: OptSpec = OptSpec::text("--plugins", "--plugins", "<json-array>").doc(
    "Plugins to bind at start, enabling each named with a provider when it is not enabled yet.",
);
const PARTITIONS: OptSpec = OptSpec::number("--partitions", "--partitions", "<N>")
    .doc("Start this many partition members as one group.");
const TIMEOUT: OptSpec = OptSpec::number("--timeout", "-t", "<ms>")
    .doc("How long the group may take to quiesce, in ms.");
const PAYLOAD: OptSpec = TEXT.required();
/// `send`'s payload spelling on `start`, where a first message is optional.
pub(crate) const TEXT: OptSpec =
    OptSpec::text("--text", "-t", "<payload>").doc("The payload, sent as written.");
pub(crate) const CHANNEL: OptSpec = OptSpec::text("--channel", "-c", "<ch>");
/// `--channel` where it names the first message's channel, on a start and on
/// every firing of a schedule; `send` and `call` take its text from their
/// operation's argument.
pub(crate) const FIRST_CHANNEL: OptSpec =
    CHANNEL.doc("The entry channel the first message names (default: the first declared).");
const TO_ITERATION: OptSpec = OptSpec::number("--to-iteration", "--to-iteration", "<N>")
    .required()
    .doc("The committed iteration to rewind to.");
const FROM: OptSpec = OptSpec::text("--from", "-f", "<seq|timestamp>").required();
const INPUT: OptSpec = OptSpec::switch("--input", "--input")
    .doc("Listen to what the relay ingests, not what it emits.");
const CALLS: OptSpec = OptSpec::switch("--calls", "--calls")
    .doc("Print the calls the relay takes and how each ended.");
const TALLY: OptSpec =
    OptSpec::switch("--tally", "--tally").doc("Print how many frames left on each output channel.");
const CELLS: OptSpec = OptSpec::text("--cells", "--cells", "<tasklet>")
    .doc("A tasklet whose most recently active Cells are listed too.");
const LIMIT: OptSpec = OptSpec::number("--limit", "--limit", "<count>")
    .doc("At most this many of those Cells (default 50, at most 500).");
const AWAIT: OptSpec = OptSpec::number("--await", "--await", "<ms>")
    .doc("Wait this many milliseconds for what the relay emits, and print it.");
const KEY: OptSpec = OptSpec::text("--key", "--key", "<key>")
    .doc("The key whose member of a partition group takes the input.");
const MSG_ID: OptSpec = OptSpec::text("--msg-id", "--msg-id", "<id>")
    .doc("The message's id; a relay whose input log still holds it absorbs the repeat.");
/// The callee's channel whose output answers a call; `--channel` names the
/// entry channel.
const ANSWER_CHANNEL: OptSpec = OptSpec::text("--answer-channel", "--answer-channel", "<ch>")
    .doc("The channel whose output answers (default: the first output).");
/// One field of a call's value, coerced by the channel's schema.
const ARG: OptSpec = OptSpec::text("--arg", "--arg", "<name=value>")
    .repeated()
    .doc("One field of a JSON input, typed by the channel's schema; repeat it for each field.");
/// `call`'s deadline: no short spelling, since `-t` is `--text`.
const CALL_TIMEOUT: OptSpec = OptSpec::number("--timeout", "--timeout", "<ms>").doc(
    "How long the relay has to answer, in ms (default: the controller's call_channel_deadline_ms).",
);

pub(crate) const TABLE: Command = Command {
    path: "flz relay",
    title: "Relay management commands.",
    notes: Some(help::RELAY),
    verbs: &[
        Verb::door("start")
            .taking(&[Positional::required("<factory_id>")])
            .reading(&[
                ID,
                NAMESPACE,
                INIT_VALUES,
                TEXT,
                FIRST_CHANNEL,
                PLUGINS,
                PARTITIONS,
            ])
            .strict()
            .sending(&[
                Arg::Namespace,
                Arg::Fields(&[
                    ("factory_id", Arg::Pos("<factory_id>")),
                    ("id", Arg::Opt(&ID)),
                    ("init_values", Arg::Built(init_values)),
                    ("plugins", Arg::Opt(&PLUGINS)),
                    ("partitions", Arg::Opt(&PARTITIONS)),
                    ("text", Arg::Opt(&TEXT)),
                    ("channel", Arg::Opt(&FIRST_CHANNEL)),
                ]),
            ])
            .calls(&["Relay.start"])
            .notes(help::RELAY_START)
            .help_word(),
        Verb::new("scale", handle_scale)
            .taking(&[
                Positional::required("<group>"),
                Positional::number("<partitions>"),
            ])
            .reading(&[NAMESPACE, TIMEOUT])
            .sending(&[
                Arg::Namespace,
                Arg::Positionals,
                Arg::Fields(&[("quiesce_timeout_ms", Arg::Opt(&TIMEOUT))]),
            ])
            .calls(&["Steps.rescale"]),
        Verb::new("stop", handle_stop)
            .taking(RELAY_ID)
            .reading(&[NAMESPACE])
            .sending(SCOPED)
            .calls(&["Relay.stop"]),
        Verb::door("state")
            .taking(RELAY_ID)
            .reading(&[NAMESPACE])
            .sending(SCOPED)
            .calls(&["Relay.cell_state"]),
        Verb::new("list", |read| {
            rpc::scoped_read(read, "relays", rpc::page(read, LIST_FILTERS))
        })
        .reading(&[
            NAMESPACE,
            args::LIMIT,
            AFTER,
            HEALTH,
            FACTORY,
            RUN,
            RUNNER,
            SEARCH,
        ])
        .calls(&["Relay.list"]),
        Verb::new("info", handle_info)
            .taking(&[Positional::optional("<relay_id>")])
            .reading(&[NAMESPACE])
            .calls(&["Relay.info", "Relay.api"]),
        Verb::door("diagnose")
            .taking(RELAY_ID)
            .reading(&[NAMESPACE])
            .sending(SCOPED)
            .calls(&["Relay.diagnose"]),
        Verb::door("graph")
            .taking(RELAY_ID)
            .reading(&[NAMESPACE])
            .sending(SCOPED)
            .calls(&["Relay.graph"]),
        Verb::door("factory")
            .taking(&[Positional::required("<factory_id>")])
            .reading(&[NAMESPACE])
            .sending(SCOPED)
            .calls(&["Fleet.factory"]),
        Verb::door("activity")
            .taking(RELAY_ID)
            .reading(&[CELLS, LIMIT, NAMESPACE])
            .sending(&[
                Arg::Namespace,
                Arg::Positionals,
                Arg::Fields(&[("cells_of", Arg::Opt(&CELLS)), ("limit", Arg::Opt(&LIMIT))]),
            ])
            .calls(&["Relay.activity"]),
        Verb::door("listen")
            .taking(RELAY_ID)
            .reading(&[INPUT, CALLS, TALLY, DURATION_MS, NAMESPACE])
            .refusing(&[
                (INPUT.long, CALLS.long),
                (INPUT.long, TALLY.long),
                (CALLS.long, TALLY.long),
            ])
            .strict()
            .sending(&[
                Arg::Namespace,
                Arg::Positionals,
                Arg::Built(direction),
                Arg::Fields(&[("window_ms", Arg::Built(rpc::window))]),
            ])
            .calls(&["Relay.listen"])
            .notes(help::RELAY_LISTEN)
            .help_word(),
        Verb::door("send")
            .taking(RELAY_ID)
            .reading(&[PAYLOAD, CHANNEL, KEY, MSG_ID, AWAIT, NAMESPACE])
            .sending(&[
                Arg::Namespace,
                Arg::Positionals,
                Arg::Opt(&PAYLOAD),
                Arg::Opt(&CHANNEL),
                Arg::Fields(&[("key", Arg::Opt(&KEY)), ("msg_id", Arg::Opt(&MSG_ID))]),
            ])
            .carrying(&[("await_ms", Arg::Opt(&AWAIT))])
            .calls(&["Relay.send"]),
        Verb::new("call", handle_call)
            .taking(RELAY_ID)
            .reading(&[
                TEXT,
                ARG,
                CHANNEL,
                ANSWER_CHANNEL,
                KEY,
                CALL_TIMEOUT,
                NAMESPACE,
            ])
            .refusing(&[(TEXT.long, ARG.long)])
            .strict()
            .calls(&["Relay.call_channel"])
            .notes(help::RELAY_CALL)
            .help_word(),
        Verb::door("rewind")
            .taking(RELAY_ID)
            .reading(&[TO_ITERATION, NAMESPACE])
            .sending(&[Arg::Namespace, Arg::Positionals, Arg::Opt(&TO_ITERATION)])
            .calls(&["Steps.rewind"]),
        Verb::door("replay")
            .taking(&[Positional::required("<relay/tasklet/key>")])
            .reading(&[FROM, NAMESPACE])
            .sending(&[Arg::Namespace, Arg::Positionals, Arg::Opt(&FROM)])
            .calls(&["Relay.replay"]),
    ],
    missing: Missing::Refuse,
    unknown_prints_help: false,
};

/// The state a `start` or a `schedule add` begins with, refused here when
/// `--init-values` is not a JSON object.
pub(crate) fn init_values(read: &Reading) -> Option<Value> {
    let option = INIT_VALUES.long;
    read.opt(option)?;
    match serde_json::from_str::<Value>(read.json(option)) {
        Ok(object @ Value::Object(_)) => Some(object),
        Ok(_) => rpc::fail(FlezhaError::with(
            "usage.json_not_object",
            [("option", Value::from(option))],
        )),
        Err(e) => rpc::fail(FlezhaError::with(
            "usage.invalid_json",
            [
                ("option", Value::from(option)),
                ("reason", Value::from(e.to_string())),
            ],
        )),
    }
}

/// `flz relay scale <group> <partitions>` — rescale a partition group.
/// The group is quiesced, every Cell whose key group changes owner is moved,
/// and the new member set is started; a refusal changes nothing.
fn handle_scale(read: &Reading) {
    let group_id = read.pos("<group>");
    let partitions = args::or_refuse(args::read_number::<u64>(
        "<partitions>",
        read.pos("<partitions>"),
    ));

    rpc::progress_line(
        read,
        &format!("Scaling relay group {} to {}...", group_id, partitions),
    );
    dispatch::send(read);
}

fn handle_stop(read: &Reading) {
    rpc::progress_line(read, "Stopping relay...");
    dispatch::send(read);
    rpc::progress_line(read, "Relay stopped successfully.");
}

/// `flz relay info [<relay_id>]`: every relay of the namespace, or one
/// relay's API.
fn handle_info(read: &Reading) {
    match read.maybe_pos("<relay_id>") {
        Some(relay_id) => rpc::door(
            "Relay.api",
            &[rpc::namespace(read), Value::from(relay_id)],
            &[],
            read,
        ),
        None => rpc::scoped_read(read, "relay information", None),
    }
}

/// `flz relay call <relay_id> (--text <payload> | --arg <name=value>...)
/// [--channel <ch>] [--answer-channel <ch>] [--key <k>] [--timeout <ms>]`:
/// call one channel by the relay's API and print its answer, or `accepted`
/// for a channel that does not answer; a failure exits by its class. `--text`
/// is the payload as written; each `--arg` is one field, which the
/// controller coerces by the channel's schema.
fn handle_call(read: &Reading) {
    let mut fields = key_fields(read);
    if let Some(written) = read.opt(CALL_TIMEOUT.long) {
        let deadline_ms: NonZeroU64 =
            args::or_refuse(args::read_number(CALL_TIMEOUT.long, written));
        fields.insert("deadline_ms".into(), Value::from(deadline_ms.get()));
    }
    if let Some(channel) = read.opt(ANSWER_CHANNEL.long) {
        fields.insert("answer_channel".into(), Value::from(channel));
    }

    let (value, opts) = match read.opt(TEXT.long) {
        Some(payload) => {
            fields.insert("payload".into(), Value::from("text"));
            (Value::from(payload), vec![])
        }
        None => (named_args(read), vec![("coerce", Value::from(true))]),
    };

    rpc::door(
        "Relay.call_channel",
        &[
            rpc::namespace(read),
            relay_id(read),
            Value::from(read.opt(CHANNEL.long).unwrap_or_default()),
            value,
            Value::Object(fields),
        ],
        &opts,
        read,
    );
}

/// The `--arg` fields as written, each value still text; one `--arg` at
/// least, and each a `name=value`.
fn named_args(read: &Reading) -> Value {
    let written = read.all(ARG.long);
    if written.is_empty() {
        rpc::fail(FlezhaError::with(
            "usage.option_required",
            [("option", Value::from("--text or --arg"))],
        ));
    }
    let mut fields = Map::new();
    for arg in written {
        let Some((name, value)) = arg.split_once('=') else {
            rpc::fail(FlezhaError::with(
                "api.input_invalid",
                [
                    (
                        "channel",
                        Value::from(read.opt(CHANNEL.long).unwrap_or_default()),
                    ),
                    ("expected", Value::from("name=value")),
                    ("reason", Value::from(format!("--arg {arg} names no value"))),
                ],
            ));
        };
        fields.insert(name.to_string(), Value::from(value));
    }
    Value::Object(fields)
}

/// A partition group's member is named by `--key`; absent, no field rides.
fn key_fields(read: &Reading) -> Map<String, Value> {
    let mut fields = Map::new();
    if let Some(key) = read.opt(KEY.long) {
        fields.insert("key".into(), Value::from(key));
    }
    fields
}

fn relay_id(read: &Reading) -> Value {
    Value::from(read.pos(RELAY_ID[0].name))
}

/// What `flz relay listen` prints: every frame the relay emits, or ingests,
/// or each call it takes and how it ended, or its output counts per channel.
fn direction(read: &Reading) -> Option<Value> {
    let direction = [(INPUT, "input"), (CALLS, "calls"), (TALLY, "tally")]
        .into_iter()
        .find(|(spec, _)| read.switch(spec.long))
        .map_or("output", |(_, direction)| direction);
    Some(Value::from(direction))
}
