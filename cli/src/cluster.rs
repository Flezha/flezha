use crate::args::{Reading, NAMESPACE};
use crate::dispatch::{Command, Missing, Verb};
use crate::{help, rpc};

pub(crate) const TABLE: Command = Command {
    path: "flz cluster",
    help: help::CLUSTER,
    verbs: &[Verb::new("status", handle_status).reading(&[NAMESPACE])],
    missing: Missing::Screen,
    unknown_prints_help: true,
};

fn handle_status(read: &Reading) {
    rpc::scoped_read(read, "Controller.cluster_status", None);
}
