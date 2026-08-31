use crate::dispatch::{self, Command};
use crate::{help, rpc};

pub(crate) const TABLE: Command = Command {
    help: help::CLUSTER,
    verbs: &[("status", handle_status)],
    help_verbs: dispatch::HELP_VERBS,
    missing: None,
    unknown: "Error: Unknown cluster subcommand '{}'",
    unknown_prints_help: true,
};

fn handle_status(args: &[String]) {
    rpc::scoped_read(&args[1..], "cluster_status", None);
}
