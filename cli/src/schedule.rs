use crate::args::{OptSpec, Positional, Reading, INIT_VALUES, NAMESPACE};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::help;
use crate::operator::{first_message_opts, plugins_json_or_refuse, CHANNEL, TEXT};
use crate::rpc::{self, text, Opt};

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
        Verb::new("list", |read| {
            rpc::run("schedule_list", &[], &[], read.token(), rpc::PROPAGATE)
        }),
        Verb::new("remove", |read| {
            rpc::one_arg(read, NAME[0].name, "schedule_remove", rpc::PROPAGATE)
        })
        .taking(NAME),
        Verb::new("run", |read| {
            rpc::one_arg(read, NAME[0].name, "schedule_run", rpc::PROPAGATE)
        })
        .taking(NAME),
    ],
    help_verbs: dispatch::HELP_VERBS,
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
    let plugins = read.opt(PLUGINS.long).map(plugins_json_or_refuse);

    let mut opts: Vec<Opt> = vec![
        ("cron", text(read.opt(CRON.long).unwrap_or_default())),
        ("tz", text(read.opt(TZ.long).unwrap_or_default())),
        ("factory", text(read.opt(FACTORY.long).unwrap_or_default())),
    ];

    for (key, value) in [
        ("namespace", read.opt(NAMESPACE.long)),
        ("init_values", read.opt(INIT_VALUES.long)),
    ] {
        if let Some(value) = value {
            opts.push((key, text(value)));
        }
    }
    opts.extend(first_message_opts(read));
    if let Some(plugins) = plugins.as_deref() {
        opts.push(("plugins", text(plugins)));
    }

    rpc::run(
        "schedule_add",
        &[text(read.pos(NAME[0].name))],
        &opts,
        read.token(),
        rpc::PROPAGATE,
    );
}
