use crate::args::{Positional, Reading};
use crate::dispatch::{self, Arg, Command, Missing, Verb};
use crate::{help, rpc};

const NAME: &[Positional] = &[Positional::required("<name>")];

pub(crate) const TABLE: Command = Command {
    path: "flz namespace",
    title: "Namespace management commands.",
    notes: Some(help::NAMESPACE),
    verbs: &[
        Verb::new("list", handle_list).calls(&["Relay.list_namespaces"]),
        Verb::door("create")
            .taking(NAME)
            .open()
            .sending(&[Arg::Positionals, Arg::Fields(&[("quotas", Arg::Settings)])])
            .calls(&["Namespace.create"]),
        Verb::door("delete")
            .taking(NAME)
            .calls(&["Namespace.delete"]),
        Verb::door("show").taking(NAME).calls(&["Namespace.get"]),
        Verb::door("set-quota")
            .taking(NAME)
            .open()
            .sending(&[Arg::Positionals, Arg::Settings])
            .calls(&["Namespace.set_quotas"]),
        Verb::door("set-migration")
            .taking(&[
                Positional::required("<name>"),
                Positional::required("<cold|hot>"),
            ])
            .calls(&["Namespace.set_migration"]),
    ],
    missing: Missing::Refuse,
    unknown_prints_help: false,
};

/// The only list verb whose RPC takes no namespace: the namespaces ARE the
/// scope, so `rpc::scoped_read`'s shape does not fit it.
fn handle_list(read: &Reading) {
    rpc::progress_line(read, "Fetching namespaces...");
    dispatch::send(read);
}
