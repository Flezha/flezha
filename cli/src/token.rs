use flz_transport::errors::FlezhaError;
use serde_json::{json, Value};

use crate::args::{OptSpec, Positional, Reading};
use crate::dispatch::{Command, Missing, Verb};
use crate::{help, rpc};

/// How long an issued token lives. An option, not a positional: it is optional
/// and the request carries it in the options list like every other one. Its
/// value is a number of seconds or the word `never`, which is what the registry
/// stores for an entry that must not lapse — a runner credential is refused any
/// other reading.
const NEVER: &str = "never";
const TTL: OptSpec = OptSpec::number_or("--ttl", "--ttl", "<seconds>|never", &[NEVER])
    .doc("How long the token lives, in seconds, or never.");

pub(crate) const TABLE: Command = Command {
    path: "flz token",
    title: "Scoped-token management.",
    notes: Some(help::TOKEN),
    verbs: &[
        Verb::new("issue", handle_issue)
            .taking(&[
                Positional::required("<principal>"),
                Positional::required("<namespace>"),
                Positional::required("<viewer|ops|admin|runner>"),
            ])
            .reading(&[TTL])
            .calls(&["Namespace.issue_token"]),
        Verb::new("revoke", rpc::one_arg)
            .taking(&[Positional::required("<token-id>")])
            .calls(&["Namespace.revoke_token"]),
        Verb::new("list", rpc::bare).calls(&["Namespace.tokens"]),
    ],
    missing: Missing::Screen,
    unknown_prints_help: false,
};

fn handle_issue(read: &Reading) {
    let opts = match ttl(read.opt(TTL.long)) {
        Some(ttl) => json!({ "ttl": ttl }),
        None => json!({}),
    };

    rpc::door(
        "Namespace.issue_token",
        &[
            Value::from(read.pos("<principal>")),
            Value::from(read.pos("<namespace>")),
            Value::from(read.pos("<viewer|ops|admin|runner>")),
            opts,
        ],
        &[],
        read,
    );
}

/// The lifetime written, refused here when it is not one: the controller
/// applies its own default when none was.
fn ttl(written: Option<&str>) -> Option<Value> {
    match written? {
        NEVER => Some(Value::from(NEVER)),
        seconds => match seconds.parse::<u64>() {
            Ok(0) | Err(_) => rpc::fail(FlezhaError::with(
                "authz.invalid_ttl",
                [("ttl", seconds.into())],
            )),
            Ok(positive) => Some(Value::from(positive)),
        },
    }
}
