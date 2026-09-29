use crate::args::{Positional, Reading};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, text};

const ENTRY_REF: &[Positional] = &[Positional::required("<operator/tasklet/entry-id>")];

pub(crate) const TABLE: Command = Command {
    path: "flz dlq",
    help: help::DLQ,
    verbs: &[
        Verb::new("list", handle_list).taking(&[Positional::optional("<operator_class>")]),
        Verb::new("show", |read| {
            rpc::one_arg(read, ENTRY_REF[0].name, "dlq_show")
        })
        .taking(ENTRY_REF),
        Verb::new("replay", |read| {
            rpc::one_arg(read, ENTRY_REF[0].name, "dlq_replay")
        })
        .taking(ENTRY_REF),
        Verb::new("drop", |read| {
            rpc::one_arg(read, ENTRY_REF[0].name, "dlq_drop")
        })
        .taking(ENTRY_REF),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Missing::Refuse,
    unknown_prints_help: false,
};

fn handle_list(read: &Reading) {
    rpc::run("dlq_list", &[text(read.pos("<operator_class>"))], &[], read);
}
