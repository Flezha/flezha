use crate::args::{OptSpec, Positional, INIT_VALUES, NAMESPACE};
use crate::dispatch::{Arg, Command, Missing, Verb};
use crate::help;
use crate::relay::{init_values, FIRST_CHANNEL, TEXT};

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
        Verb::door("add")
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
            // `--plugins` and `--text` ride as written: the controller decodes
            // the JSON at every firing, and stores the message verbatim.
            .sending(&[Arg::Fields(&[
                ("name", Arg::Pos("<name>")),
                ("cron", Arg::Opt(&CRON)),
                ("timezone", Arg::Opt(&TZ)),
                ("factory_id", Arg::Opt(&FACTORY)),
                ("namespace", Arg::Opt(&NAMESPACE)),
                ("plugins", Arg::Opt(&PLUGINS)),
                ("init_values", Arg::Built(init_values)),
                ("text", Arg::Opt(&FIRST_MESSAGE)),
                ("channel", Arg::Opt(&FIRST_CHANNEL)),
            ])])
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
