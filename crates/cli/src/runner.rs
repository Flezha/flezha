use flz_transport::errors::FlezhaError;
use flz_transport::transport::config::{get_transport_type, TransportType};
use flz_transport::utility::socket_dir::Role;
use flz_transport::utility::socket_probe::diagnose;

use crate::args::{Positional, Reading, NAMESPACE};
use crate::dispatch::{Arg, Command, Missing, Verb};
use crate::rpc;

const RUNNER_ID: &[Positional] = &[Positional::required("<runner_id>")];
const STABLE_ID: &[Positional] = &[Positional::required("<stable_id>")];
/// `drop` and `readmit` name the runner within the namespace.
const SCOPED: &[Arg] = &[Arg::Namespace, Arg::Positionals];

pub(crate) const TABLE: Command = Command {
    path: "flz runner",
    title: "Runner management commands.",
    notes: None,
    verbs: &[
        Verb::new("list", |read| rpc::scoped_read(read, "runners"))
            .reading(&[NAMESPACE])
            .calls(&["Fleet.runners"]),
        Verb::new("check", handle_check)
            .about("Report whether a runner on this host can reach a controller."),
        Verb::door("drop")
            .taking(RUNNER_ID)
            .reading(&[NAMESPACE])
            .sending(SCOPED)
            .calls(&["Fleet.drop_runner"]),
        Verb::new("dropped", |read| rpc::scoped_read(read, "dropped runners"))
            .reading(&[NAMESPACE])
            .calls(&["Fleet.dropped_runners"]),
        Verb::door("readmit")
            .taking(STABLE_ID)
            .reading(&[NAMESPACE])
            .sending(SCOPED)
            .calls(&["Fleet.readmit_runner"]),
    ],
    missing: Missing::Refuse,
    unknown_prints_help: false,
};

/// Where this host's controller is, and why a runner cannot reach it — the
/// same ladder and the same probe a runner walks, from a shell, with no runner
/// code, no running runner and no controller needed.
fn handle_check(_read: &Reading) {
    if get_transport_type() == TransportType::Quic {
        println!("note: FLEZHA_TRANSPORT=quic — this check covers the UDS socket directory only");
    }

    // The runner's own words, not a second rendering of them, one per line.
    match diagnose(Role::Runner, None) {
        Ok(diagnosis) => {
            println!("{:#}", diagnosis);

            if !diagnosis.reach.reachable() {
                check_failed(diagnosis.reach.to_string());
            }
        }
        Err(refusal) => check_failed(refusal.to_string()),
    }
}

fn check_failed(reason: String) -> ! {
    rpc::fail(FlezhaError::with(
        "cli.runner_check_failed",
        [("reason", reason.into())],
    ))
}
