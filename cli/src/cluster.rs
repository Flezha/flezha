use crate::args::NAMESPACE;
use crate::dispatch::{Command, Missing, Verb};
use crate::{help, rpc};

pub(crate) const TABLE: Command = Command {
    path: "flz cluster",
    title: "Controller cluster status (nodes, leader, relay health).",
    notes: Some(help::CLUSTER),
    verbs: &[Verb::new("status", |read| rpc::scoped_read(read, None))
        .reading(&[NAMESPACE])
        .calls(&["Controller.cluster_status"])],
    missing: Missing::Screen,
    unknown_prints_help: true,
};
