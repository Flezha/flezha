use std::thread;
use std::time::{Duration, Instant};

use crate::args::{OptSpec, Reading};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::{help, rpc};

/// The wait, in seconds. `boot await` is one of the two verbs that refuse a
/// word they cannot act on: it blocks the caller for as long as this option
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

/// The pause between two attempts to reach a node that is not up yet. Each
/// attempt starts a VM of its own, so the pause keeps the wait from crowding
/// the boot it waits for; 200 ms is the pace the controller polls its own
/// verdict at once reached (`boot_await_poll_ms`).
const UNREACHABLE_PAUSE: Duration = Duration::from_millis(200);

/// Still unreachable when the budget ran out. `:noconnection` is all the
/// reason says, so the line names what it stands for when the controller's host
/// can be reached, with the settings that decide this shell's cookie and node name.
const NEVER_REACHED: rpc::Failure = rpc::PROPAGATE.saying(
    "boot await: unreachable (:noconnection): the controller is not up yet or stopped \
     after it was reached, or this shell's cookie (RELEASE_COOKIE, FLEZHA_COOKIE_FILE, \
     FLEZHA_STATE_DIR) or node name (FLEZHA_NODE_NAME, FLEZHA_NODE_HOST) is not the \
     controller's",
);

/// One budget, spent on reaching the node and then on the node's own wait: the
/// first request carries all of it, and each retry what is left. A reached
/// node's verdict is final: the node has already had the budget for its wait.
fn handle_await(read: &Reading) {
    let timeout_ms = read
        .num::<u64>(TIMEOUT.long)
        .map_or(DEFAULT_TIMEOUT_MS, |secs| secs.saturating_mul(1000));
    let started = Instant::now();
    let left_ms = || timeout_ms.saturating_sub(started.elapsed().as_millis() as u64);
    let mut budget_ms = timeout_ms;

    while let Err(unreachable) = rpc::run_if_reachable(
        rpc::expression("boot_await", &[budget_ms.to_string()], &[], read.token()),
        rpc::PROPAGATE,
    ) {
        let left = left_ms();
        if left == 0 {
            return rpc::verdict(Ok(unreachable), NEVER_REACHED);
        }
        thread::sleep(UNREACHABLE_PAUSE.min(Duration::from_millis(left)));
        budget_ms = left_ms();
    }
}
