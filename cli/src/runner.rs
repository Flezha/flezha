use flz_transport::errors::FlezhaError;
use flz_transport::transport::config::{get_transport_type, TransportType};
use flz_transport::utility::socket_dir::Role;
use flz_transport::utility::socket_probe::diagnose;
use serde_json::Value;

use crate::args::{Positional, Reading, NAMESPACE};
use crate::dispatch::{Command, Missing, Verb};
use crate::{help, rpc};

const RUNNER_ID: &[Positional] = &[Positional::required("<runner_id>")];
const STABLE_ID: &[Positional] = &[Positional::required("<stable_id>")];

pub(crate) const TABLE: Command = Command {
    path: "flz runner",
    help: help::RUNNER,
    verbs: &[
        Verb::new("list", handle_list).reading(&[NAMESPACE]),
        Verb::new("check", handle_check),
        Verb::new("drop", handle_drop)
            .taking(RUNNER_ID)
            .reading(&[NAMESPACE]),
        Verb::new("dropped", |read| {
            rpc::scoped_read(read, "Fleet.dropped_runners", Some("dropped runners"))
        })
        .reading(&[NAMESPACE]),
        Verb::new("readmit", handle_readmit)
            .taking(STABLE_ID)
            .reading(&[NAMESPACE]),
    ],
    missing: Missing::Refuse,
    unknown_prints_help: false,
};

fn handle_list(read: &Reading) {
    rpc::scoped_read(read, "Fleet.runners", Some("runners"));
}

/// `flz runner drop <runner_id>`: close the runner's session and refuse its
/// process on every controller until a different one connects; prints the
/// drop mark.
fn handle_drop(read: &Reading) {
    rpc::door(
        "Fleet.drop_runner",
        &[
            rpc::namespace(read),
            Value::from(read.pos(RUNNER_ID[0].name)),
        ],
        &[],
        read,
    );
}

/// `flz runner readmit <stable_id>`: lift a drop mark, so the dropped process
/// is admitted on its next retry.
fn handle_readmit(read: &Reading) {
    rpc::door(
        "Fleet.readmit_runner",
        &[
            rpc::namespace(read),
            Value::from(read.pos(STABLE_ID[0].name)),
        ],
        &[],
        read,
    );
}

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
