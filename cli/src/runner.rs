use std::process::exit;

use flz_transport::transport::config::{get_transport_type, TransportType};
use flz_transport::utility::socket_dir::Role;
use flz_transport::utility::socket_probe::diagnose;

use crate::dispatch::{self, Command};
use crate::{help, rpc};

pub(crate) const TABLE: Command = Command {
    help: help::RUNNER,
    verbs: &[("list", handle_list), ("check", handle_check)],
    help_verbs: dispatch::HELP_VERBS,
    missing: Some("Error: Invalid number of arguments. See help for more information."),
    unknown: "Error: Invalid argument '{}'. Use 'list', 'check' or 'help'.",
    unknown_prints_help: false,
};

fn handle_list(args: &[String]) {
    rpc::scoped_read(&args[1..], "list_runners", Some("runners"));
}

/// Where this host's controller is, and why a runner cannot reach it — the
/// same ladder and the same probe a runner walks, from a shell, with no runner
/// code, no running runner and no controller needed.
fn handle_check(_args: &[String]) {
    if get_transport_type() == TransportType::Quic {
        println!("note: FLEZHA_TRANSPORT=quic — this check covers the UDS socket directory only");
    }

    // The runner's own words, not a second rendering of them.
    match diagnose(Role::Runner, None) {
        Ok(diagnosis) => {
            println!("{}", diagnosis);

            if !diagnosis.reach.reachable() {
                exit(1);
            }
        }
        Err(refusal) => {
            println!("{}", refusal);
            exit(1);
        }
    }
}
