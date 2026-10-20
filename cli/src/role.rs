use crate::args::Positional;
use crate::dispatch::{Command, Missing, Verb};
use crate::help;

/// The enumeration, spelled once: both verbs read the same label, so a role
/// added to it cannot reach one of them and miss the other.
const ROLE: &str = "<viewer|app|ops|admin|runner|spawner|stopper>";

pub(crate) const TABLE: Command = Command {
    path: "flz role",
    title: "Principal → role bindings.",
    notes: Some(help::ROLE),
    verbs: &[
        Verb::door("bind")
            .taking(&[
                Positional::required("<principal>"),
                Positional::required(ROLE),
                Positional::required("<namespace>"),
            ])
            .calls(&["Namespace.bind_role"]),
        Verb::door("unbind")
            .taking(&[
                Positional::required("<principal>"),
                Positional::optional(ROLE),
            ])
            .calls(&["Namespace.unbind_role"]),
        Verb::door("list").calls(&["Namespace.list_roles"]),
    ],
    missing: Missing::Screen,
    unknown_prints_help: false,
};
