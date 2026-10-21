use crate::args::NAMESPACE;
use crate::dispatch::{Arg, Command, Missing, Verb};
use crate::help;

pub(crate) const TABLE: Command = Command {
    path: "flz cluster",
    title: "Controller cluster status (nodes, leader, relay health).",
    notes: Some(help::CLUSTER),
    verbs: &[Verb::door("status")
        .reading(&[NAMESPACE])
        .sending(&[Arg::Namespace])
        .calls(&["Controller.cluster_status"])],
    missing: Missing::Screen,
    unknown_prints_help: true,
};
