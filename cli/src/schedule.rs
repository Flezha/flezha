use serde_json::{Map, Value};

use crate::args::{OptSpec, Positional, Reading, INIT_VALUES, NAMESPACE};
use crate::dispatch::{Command, Missing, Verb};
use crate::relay::{first_message, json_object, FIRST_CHANNEL, TEXT};
use crate::{help, rpc};

const CRON: OptSpec = OptSpec::text("--cron", "--cron", "<expr>")
    .required()
    .doc("The cron expression it fires on.");
const TZ: OptSpec = OptSpec::text("--tz", "--tz", "<iana>")
    .required()
    .doc("The IANA time zone the expression is read in.");
const FACTORY: OptSpec = OptSpec::text("--factory", "--factory", "<factory_id>")
    .required()
    .doc("The steps factory each firing starts.");
const PLUGINS: OptSpec = OptSpec::text("--plugins", "-p", "<json>").doc(
    "The same array as `relay start --plugins`; each firing binds its instance to the plugins it names.",
);

const FIRED_STATE: OptSpec =
    INIT_VALUES.doc("The state every fired instance starts with, keyed by tasklet name.");
const FIRST_MESSAGE: OptSpec =
    TEXT.doc("The first message every fired instance receives, sent as written.");

const NAME: &[Positional] = &[Positional::required("<name>")];

pub(crate) const TABLE: Command = Command {
    path: "flz schedule",
    title: "Cron-scheduled steps relay spawns.",
    notes: Some(help::SCHEDULE),
    verbs: &[
        Verb::new("add", handle_add)
            .taking(NAME)
            .reading(&[
                CRON,
                TZ,
                FACTORY,
                NAMESPACE,
                FIRED_STATE,
                FIRST_MESSAGE,
                FIRST_CHANNEL,
                PLUGINS,
            ])
            .strict()
            .calls(&["Schedule.add"]),
        Verb::door("list").calls(&["Schedule.list"]),
        Verb::door("remove")
            .taking(NAME)
            .calls(&["Schedule.remove"]),
        Verb::door("run").taking(NAME).calls(&["Schedule.run"]),
    ],
    missing: Missing::Screen,
    unknown_prints_help: false,
};

/// The flags travel as the options list: the three required ones first, then
/// each optional one the caller wrote, so an absent flag is absent from the
/// request and the controller applies its own default. `--plugins` and
/// `--text` are carried as the text they were written as, whatever they parse
/// as: the controller's decoder reads the JSON itself, at every firing, and
/// the message is stored verbatim.
fn handle_add(read: &Reading) {
    let mut fields = Map::new();

    for (key, value) in [
        ("name", Some(read.pos(NAME[0].name))),
        ("cron", read.opt(CRON.long)),
        ("timezone", read.opt(TZ.long)),
        ("factory_id", read.opt(FACTORY.long)),
        ("namespace", read.opt(NAMESPACE.long)),
        ("plugins", read.opt(PLUGINS.long)),
    ] {
        if let Some(value) = value {
            fields.insert(key.into(), Value::from(value));
        }
    }
    if read.opt(INIT_VALUES.long).is_some() {
        fields.insert("init_values".into(), json_object(read, INIT_VALUES.long));
    }
    fields.extend(first_message(read));

    rpc::door("Schedule.add", &[Value::Object(fields)], &[], read);
}
