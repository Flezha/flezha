use serde_json::{json, Value};

use crate::args::{Positional, Reading};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::settings::{extract_x_flags, flags_object};
use crate::{help, rpc};

const NAME: &[Positional] = &[Positional::required("<name>")];

pub(crate) const TABLE: Command = Command {
    path: "flz namespace",
    help: help::NAMESPACE,
    verbs: &[
        Verb::new("list", handle_list),
        Verb::new("create", handle_create).taking(NAME).open(),
        Verb::new("delete", |read| {
            rpc::one_arg(read, NAME[0].name, "Namespace.delete")
        })
        .taking(NAME),
        Verb::new("show", |read| {
            rpc::one_arg(read, NAME[0].name, "Namespace.get")
        })
        .taking(NAME),
        Verb::new("set-quota", handle_set_quota).taking(NAME).open(),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Missing::Refuse,
    unknown_prints_help: false,
};

/// The only list verb whose RPC takes no namespace: the namespaces ARE the
/// scope, so `rpc::scoped_read`'s shape does not fit it.
fn handle_list(read: &Reading) {
    rpc::progress_line(read, "Fetching namespaces...");
    rpc::bare(read, "Relay.list_namespaces");
}

fn handle_create(read: &Reading) {
    rpc::door(
        "Namespace.create",
        &[name(read), json!({ "quotas": quotas(read) })],
        &[],
        read,
    );
}

fn handle_set_quota(read: &Reading) {
    rpc::door(
        "Namespace.set_quotas",
        &[name(read), quotas(read)],
        &[],
        read,
    );
}

fn name(read: &Reading) -> Value {
    Value::from(read.pos(NAME[0].name))
}

/// The `--X<quota>=<value>` settings, by quota name.
fn quotas(read: &Reading) -> Value {
    flags_object(&extract_x_flags(read.rest()))
}
