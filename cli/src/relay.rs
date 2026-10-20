use std::num::NonZeroU64;

use flz_transport::errors::FlezhaError;
use serde_json::{Map, Value};

use crate::args::{self, OptSpec, Positional, Reading, DURATION_MS, INIT_VALUES, NAMESPACE};
use crate::dispatch::{self, Arg, Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, Opt};

const RELAY_ID: &[Positional] = &[Positional::required("<relay_id>")];
/// The verbs that name one relay, or one of its Cells, within the namespace.
const SCOPED: &[Arg] = &[Arg::Namespace, Arg::Positionals];

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
        Verb::new("start", handle_start)
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
            .calls(&["Relay.start"])
            .notes(help::RELAY_START)
            .help_word(),
        Verb::new("scale", handle_scale)
            .taking(&[
                Positional::required("<group>"),
                Positional::number("<partitions>"),
            ])
            .reading(&[NAMESPACE, TIMEOUT])
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
        Verb::new("list", |read| rpc::scoped_read(read, "relays"))
            .reading(&[NAMESPACE])
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
        Verb::door("activity")
            .taking(RELAY_ID)
            .reading(&[CELLS, LIMIT, NAMESPACE])
            .sending(&[
                Arg::Namespace,
                Arg::Positionals,
                Arg::Fields(&[("cells_of", Arg::Opt(&CELLS)), ("limit", Arg::Opt(&LIMIT))]),
            ])
            .calls(&["Relay.activity"]),
        Verb::new("listen", handle_listen)
            .taking(RELAY_ID)
            .reading(&[INPUT, CALLS, TALLY, DURATION_MS, NAMESPACE])
            .refusing(&[
                (INPUT.long, CALLS.long),
                (INPUT.long, TALLY.long),
                (CALLS.long, TALLY.long),
            ])
            .strict()
            .calls(&["Relay.listen"])
            .notes(help::RELAY_LISTEN)
            .help_word(),
        Verb::new("send", handle_send)
            .taking(RELAY_ID)
            .reading(&[PAYLOAD, CHANNEL, KEY, MSG_ID, AWAIT, NAMESPACE])
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

/// `--id`, `--init-values`, `--plugins`, `--partitions`, `--text` and
/// `--channel` ride the relay's fields, and only when written: an absent
/// flag is absent from the request and the controller applies its default,
/// which for the id is one it generates. `--text` is the instance's first
/// message, on the entry channel `--channel` names; a channel with no text
/// names nothing and is refused. How the relay runs is the factory's,
/// announced by its runner, so no flag names it.
fn handle_start(read: &Reading) {
    let mut fields = Map::new();
    fields.insert("factory_id".into(), Value::from(read.pos("<factory_id>")));

    if let Some(id) = read.opt(ID.long) {
        fields.insert("id".into(), Value::from(id));
    }
    if read.opt(INIT_VALUES.long).is_some() {
        fields.insert("init_values".into(), json_object(read, INIT_VALUES.long));
    }
    if let Some(plugins) = read.opt(PLUGINS.long) {
        fields.insert("plugins".into(), Value::from(plugins));
    }
    if let Some(count) = read.num::<u64>(PARTITIONS.long) {
        fields.insert("partitions".into(), Value::from(count));
    }
    fields.extend(first_message(read));

    rpc::door(
        "Relay.start",
        &[rpc::namespace(read), Value::Object(fields)],
        &[],
        read,
    );
}

/// A JSON object the caller wrote behind `option`, refused here when it is
/// not one.
pub(crate) fn json_object(read: &Reading, option: &str) -> Value {
    match serde_json::from_str::<Value>(read.json(option)) {
        Ok(object @ Value::Object(_)) => object,
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

/// The first message a `start` or a `schedule add` carries: `text` and,
/// when written, `channel`; `--channel` without `--text` is refused.
pub(crate) fn first_message(read: &Reading) -> Vec<(String, Value)> {
    let mut fields = Vec::new();
    let payload = read.opt(TEXT.long);
    if let Some(payload) = payload {
        fields.push(("text".to_string(), Value::from(payload)));
    }
    if let Some(channel) = read.opt(CHANNEL.long) {
        args::or_refuse(
            payload
                .ok_or_else(|| FlezhaError::new("relay.channel_without_text", Default::default())),
        );
        fields.push(("channel".to_string(), Value::from(channel)));
    }
    fields
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

    let mut opts = Map::new();
    if let Some(timeout_ms) = read.num::<u64>(TIMEOUT.long) {
        opts.insert("quiesce_timeout_ms".into(), Value::from(timeout_ms));
    }

    rpc::progress_line(
        read,
        &format!("Scaling relay group {} to {}...", group_id, partitions),
    );

    rpc::door(
        "Steps.rescale",
        &[
            rpc::namespace(read),
            Value::from(group_id),
            Value::from(partitions),
            Value::Object(opts),
        ],
        &[],
        read,
    );
}

fn handle_stop(read: &Reading) {
    rpc::progress_line(read, "Stopping relay...");
    dispatch::send(read);
    rpc::progress_line(read, "Relay stopped successfully.");
}

/// `flz relay send <relay_id> --text <payload> [--channel <ch>]
/// [--key <k>] [--msg-id <id>] [--await <ms>]`: inject a payload into an
/// relay's input stream (the SDK-facing `send_io` route). Blocks for the
/// confirming verdict so a halted or absent target is reported rather than
/// silently dropped. `--msg-id` and `--await` ride only when written.
fn handle_send(read: &Reading) {
    let opts: Vec<Opt> = read
        .opt(AWAIT.long)
        .map(|ms| vec![("await_ms", ms.to_string())])
        .unwrap_or_default();

    let mut fields = key_fields(read);
    if let Some(id) = read.opt(MSG_ID.long) {
        fields.insert("msg_id".into(), Value::from(id));
    }
    rpc::door("Relay.send", &input(read, fields), &opts, read);
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
        None => rpc::scoped_read(read, "relay information"),
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
        None => (named_args(read), vec![("coerce", "true".to_string())]),
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

/// The arguments `send` takes: where, which relay, the payload and its
/// channel (`""` is none), then the named fields.
fn input(read: &Reading, fields: Map<String, Value>) -> Vec<Value> {
    vec![
        rpc::namespace(read),
        relay_id(read),
        Value::from(read.opt(PAYLOAD.long).unwrap_or_default()),
        Value::from(read.opt(CHANNEL.long).unwrap_or_default()),
        Value::Object(fields),
    ]
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

/// `flz relay listen <relay_id> [--input | --calls | --tally]`: print every
/// frame the relay emits, or ingests, or each call it takes and how it ended,
/// or its output counts per channel, until the window closes. Prints nothing
/// of its own, so what reaches stdout is the frames alone.
fn handle_listen(read: &Reading) {
    let direction = [(INPUT, "input"), (CALLS, "calls"), (TALLY, "tally")]
        .into_iter()
        .find(|(spec, _)| read.switch(spec.long))
        .map_or("output", |(_, direction)| direction);
    let window_ms = rpc::window_ms(read);

    rpc::door(
        "Relay.listen",
        &[
            rpc::namespace(read),
            relay_id(read),
            Value::from(direction),
            serde_json::json!({ "window_ms": window_ms }),
        ],
        &[],
        read,
    );
}
