use serde_json::Value;

use crate::args::{Positional, Reading};
use crate::dispatch::{Command, Missing, Verb};
use crate::{help, rpc};

/// The enumeration, spelled once: both verbs read the same label, so a role
/// added to it cannot reach one of them and miss the other.
const ROLE: &str = "<viewer|app|ops|admin|runner|spawner|stopper>";

pub(crate) const TABLE: Command = Command {
    path: "flz role",
    help: help::ROLE,
    verbs: &[
        Verb::new("bind", handle_bind).taking(&[
            Positional::required("<principal>"),
            Positional::required(ROLE),
            Positional::required("<namespace>"),
        ]),
        Verb::new("unbind", handle_unbind).taking(&[
            Positional::required("<principal>"),
            Positional::optional(ROLE),
        ]),
        Verb::new("list", |read| rpc::bare(read, "Namespace.list_roles")),
    ],
    missing: Missing::Screen,
    unknown_prints_help: false,
};

fn handle_bind(read: &Reading) {
    rpc::door(
        "Namespace.bind_role",
        &[
            Value::from(read.pos("<principal>")),
            Value::from(read.pos(ROLE)),
            Value::from(read.pos("<namespace>")),
        ],
        &[],
        read,
    );
}

/// An unbind with no role takes the principal's whole binding.
fn handle_unbind(read: &Reading) {
    rpc::door(
        "Namespace.unbind_role",
        &[
            Value::from(read.pos("<principal>")),
            read.maybe_pos(ROLE).map_or(Value::Null, Value::from),
        ],
        &[],
        read,
    );
}
