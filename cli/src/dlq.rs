use serde_json::Value;

use crate::args::{Positional, Reading};
use crate::dispatch::{Command, Missing, Verb};
use crate::rpc;

const ENTRY_REF: &[Positional] = &[Positional::required("<relay/tasklet/entry-id>")];

pub(crate) const TABLE: Command = Command {
    path: "flz dlq",
    title: "Dead-letter queue: parked poison payloads (retry budget exhausted).",
    notes: None,
    verbs: &[
        Verb::new("list", handle_list)
            .taking(&[Positional::optional("<relay_class>")])
            .strict()
            .calls(&["Dlq.list"]),
        Verb::new("show", rpc::one_arg)
            .taking(ENTRY_REF)
            .strict()
            .calls(&["Dlq.get"]),
        Verb::new("replay", rpc::one_arg)
            .taking(ENTRY_REF)
            .strict()
            .calls(&["Dlq.replay"]),
        Verb::new("drop", rpc::one_arg)
            .taking(ENTRY_REF)
            .strict()
            .calls(&["Dlq.drop"]),
    ],
    missing: Missing::Refuse,
    unknown_prints_help: false,
};

fn handle_list(read: &Reading) {
    let class = read
        .maybe_pos("<relay_class>")
        .map_or(Value::Null, Value::from);
    rpc::door("Dlq.list", &[class], &[], read);
}

#[cfg(test)]
mod tests {
    use flz_transport::errors::FlezhaError;

    use super::TABLE;
    use crate::args::{owned, Reading};

    fn read_list(argv: &[&str]) -> Result<Reading, FlezhaError> {
        let list = TABLE.verbs.iter().find(|verb| verb.name == "list");
        crate::args::read(
            TABLE.path,
            list.expect("flz dlq declares list"),
            &owned(argv),
        )
    }

    /// fails when: `flz dlq list -n trading` reads `trading` as the relay whose
    /// entries to list instead of refusing the option the queue has no use for.
    #[test]
    fn a_namespace_flag_is_refused_rather_than_read_as_the_relay() {
        let err = read_list(&["-n", "trading"]).err().expect("-n is refused");
        assert_eq!(err.code, "usage.unknown_option");
        assert!(read_list(&["prices#p0", "-o", "json"]).is_ok());
    }
}
