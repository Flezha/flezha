use crate::args::{Reading, NAMESPACE};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::{help, rpc};

pub(crate) const TABLE: Command = Command {
    path: "flz cluster",
    help: help::CLUSTER,
    verbs: &[Verb::new("status", handle_status).reading(&[NAMESPACE])],
    help_verbs: dispatch::HELP_VERBS,
    missing: Missing::Screen,
    unknown_prints_help: true,
};

fn handle_status(read: &Reading) {
    rpc::scoped_read(read, "cluster_status", None);
}
