use serde_json::Value;

use crate::args::{Positional, Reading, AFTER, LIMIT};
use crate::dispatch::{Arg, Command, Missing, Verb};
use crate::rpc;

const ENTRY_REF: &[Positional] = &[Positional::required("<relay/tasklet/entry-id>")];

pub(crate) const TABLE: Command = Command {
    path: "flz dlq",
    title: "Dead-letter queue: parked poison payloads (retry budget exhausted).",
    notes: None,
    verbs: &[
        Verb::door("list")
            .taking(&[Positional::optional("<relay_class>")])
            .reading(&[LIMIT, AFTER])
            .sending(&[Arg::Positionals, Arg::Built(page)])
            .strict()
            .calls(&["Dlq.list"]),
        Verb::door("show")
            .taking(ENTRY_REF)
            .strict()
            .calls(&["Dlq.get"]),
        Verb::door("replay")
            .taking(ENTRY_REF)
            .strict()
            .calls(&["Dlq.replay"]),
        Verb::door("drop")
            .taking(ENTRY_REF)
            .strict()
            .calls(&["Dlq.drop"]),
    ],
    missing: Missing::Refuse,
    unknown_prints_help: false,
};

/// `flz dlq list`'s page, when `--limit` or `--after` asks for one.
fn page(read: &Reading) -> Option<Value> {
    rpc::page(read, &[])
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

    /// fails when: a paging flag is not sent as the listing's `opts`, or a
    /// listing without one sends any, which would page the unpaged answer.
    #[test]
    fn the_paging_flags_are_sent_as_opts_and_only_when_written() {
        let paged = read_list(&["--limit", "5", "--after", "abc"]).expect("reads");
        assert_eq!(
            super::page(&paged),
            Some(serde_json::json!({ "limit": 5, "after": "abc" }))
        );
        let whole = read_list(&["prices#p0"]).expect("reads");
        assert_eq!(super::page(&whole), None);

        let relay = crate::relay::TABLE
            .verbs
            .iter()
            .find(|verb| verb.name == "list");
        let argv = owned(&["--limit", "2", "--health", "unhealthy", "--search", "ord"]);
        let read =
            crate::args::read("flz relay", relay.expect("relay list"), &argv).expect("reads");
        assert_eq!(
            crate::rpc::page(&read, crate::relay::LIST_FILTERS),
            Some(serde_json::json!({
                "limit": 2,
                "filter": { "health": "unhealthy", "text": "ord" }
            }))
        );
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
