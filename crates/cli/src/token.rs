use crate::args::{OptSpec, Positional};
use crate::dispatch::{Arg, Command, Missing, Verb};
use crate::help;

/// How long an issued token lives. An option, not a positional: it is optional
/// and the request carries it in the options list like every other one. Its
/// value is a number of seconds or the word `never`, which is what the registry
/// stores for an entry that must not lapse — a runner credential is refused any
/// other reading.
const TTL: OptSpec = OptSpec::number_or("--ttl", "--ttl", "<seconds>|never")
    .doc("How long the token lives, in seconds, or never.");

pub(crate) const TABLE: Command = Command {
    path: "flz token",
    title: "Scoped-token management.",
    notes: Some(help::TOKEN),
    verbs: &[
        Verb::door("issue")
            .taking(&[
                Positional::required("<principal>"),
                Positional::required("<namespace>"),
                Positional::required("<viewer|ops|admin|runner>"),
            ])
            .reading(&[TTL])
            .sending(&[Arg::Positionals, Arg::Fields(&[("ttl", Arg::Opt(&TTL))])])
            .calls(&["Namespace.issue_token"]),
        Verb::door("revoke")
            .taking(&[Positional::required("<token-id>")])
            .calls(&["Namespace.revoke_token"]),
        Verb::door("list").calls(&["Namespace.tokens"]),
    ],
    missing: Missing::Screen,
    unknown_prints_help: false,
};
