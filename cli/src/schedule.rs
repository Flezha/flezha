use serde_json::{Map, Value};

use crate::args::{OptSpec, Positional, Reading, INIT_VALUES, NAMESPACE};
use crate::dispatch::{Command, Missing, Verb};
use crate::relay::{first_message, json_object, plugins_json_or_refuse, CHANNEL, TEXT};
use crate::{help, rpc};

const CRON: OptSpec = OptSpec::text("--cron", "--cron", "<expr>").required();
const TZ: OptSpec = OptSpec::text("--tz", "--tz", "<iana>").required();
const FACTORY: OptSpec = OptSpec::text("--factory", "--factory", "<factory_id>").required();
const PLUGINS: OptSpec = OptSpec::text("--plugins", "-p", "<json>");

const NAME: &[Positional] = &[Positional::required("<name>")];

pub(crate) const TABLE: Command = Command {
    path: "flz schedule",
    help: help::SCHEDULE,
    verbs: &[
        Verb::new("add", handle_add)
            .taking(NAME)
            .reading(&[
                CRON,
                TZ,
                FACTORY,
                NAMESPACE,
                INIT_VALUES,
                TEXT,
                CHANNEL,
                PLUGINS,
            ])
            .strict(),
        Verb::new("list", |read| rpc::bare(read, "Schedule.list")),
        Verb::new("remove", |read| {
            rpc::one_arg(read, NAME[0].name, "Schedule.remove")
        })
        .taking(NAME),
        Verb::new("run", |read| {
            rpc::one_arg(read, NAME[0].name, "Schedule.run")
        })
        .taking(NAME),
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
    ] {
        if let Some(value) = value {
            fields.insert(key.into(), Value::from(value));
        }
    }
    if read.opt(INIT_VALUES.long).is_some() {
        fields.insert("init_values".into(), json_object(read, INIT_VALUES.long));
    }
    fields.extend(first_message(read));
    if let Some(plugins) = read.opt(PLUGINS.long) {
        fields.insert(
            "plugins".into(),
            Value::from(plugins_json_or_refuse(plugins)),
        );
    }

    rpc::door("Schedule.add", &[Value::Object(fields)], &[], read);
}
