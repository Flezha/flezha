use crate::args::{Positional, Reading, JSON_SWITCH, OUTPUT};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, text};
use crate::settings::{extract_x_flags, serialize_flags};

const NAME: &[Positional] = &[Positional::required("<name>")];

pub(crate) const TABLE: Command = Command {
    path: "flz namespace",
    help: help::NAMESPACE,
    verbs: &[
        Verb::new("list", handle_list).reading(&[OUTPUT, JSON_SWITCH]),
        Verb::new("create", |read| {
            quota_verb(read, "authz_render_namespace_create")
        })
        .taking(NAME)
        .open(),
        Verb::new("delete", |read| {
            rpc::one_arg(
                read,
                NAME[0].name,
                "authz_render_namespace_delete",
                rpc::PROPAGATE,
            )
        })
        .taking(NAME),
        Verb::new("show", |read| {
            rpc::one_arg(
                read,
                NAME[0].name,
                "authz_render_namespace_show",
                rpc::PROPAGATE,
            )
        })
        .taking(NAME),
        Verb::new("set-quota", |read| {
            quota_verb(read, "authz_render_namespace_set_quota")
        })
        .taking(NAME)
        .open(),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Missing::Refuse,
    unknown_prints_help: false,
};

/// The only list verb whose RPC takes no namespace: the namespaces ARE the
/// scope, so `rpc::scoped_read`'s shape does not fit it.
fn handle_list(read: &Reading) {
    let output_format = read.output_format();

    if output_format == "text" {
        println!("Fetching namespaces...");
    }

    rpc::run(
        "list_namespaces",
        &[text(output_format)],
        &[],
        read.token(),
        rpc::REFUSED,
    );
}

/// The two verbs that carry quota overrides. They ride the repeatable
/// `--X<field>=<value>` settings (e.g. `--Xmax_runners=4`), serialized to the
/// same JSON object shape every other `--X` surface uses.
fn quota_verb(read: &Reading, rpc_fun: &str) {
    rpc::run(
        rpc_fun,
        &[
            text(read.pos(NAME[0].name)),
            text(&serialize_flags(&extract_x_flags(read.rest()))),
        ],
        &[],
        read.token(),
        rpc::PROPAGATE,
    );
}
