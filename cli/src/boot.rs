use crate::args::{OptSpec, Reading};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::{help, rpc};

/// The wait, in seconds. `boot await` is one of the two verbs that refuse a
/// word they cannot act on: it blocks the operator for as long as this option
/// says, so answering a typo with the default wait would be a silence rather
/// than a wait.
const TIMEOUT: OptSpec = OptSpec::number("--timeout", "-t", "<seconds>");

pub(crate) const TABLE: Command = Command {
    path: "flz boot",
    help: help::BOOT,
    verbs: &[
        Verb::new("status", handle_status),
        Verb::new("await", handle_await)
            .reading(&[TIMEOUT])
            .strict(),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Missing::Refuse,
    unknown_prints_help: false,
};

fn handle_status(read: &Reading) {
    rpc::run("boot_status", &[], &[], read.token(), rpc::PROPAGATE);
}

const DEFAULT_TIMEOUT_MS: u64 = 60_000;

fn handle_await(read: &Reading) {
    let timeout_ms = read
        .num::<u64>(TIMEOUT.long)
        .map_or(DEFAULT_TIMEOUT_MS, |secs| secs.saturating_mul(1000));

    rpc::run(
        "boot_await",
        &[timeout_ms.to_string()],
        &[],
        read.token(),
        rpc::PROPAGATE,
    );
}
