//! What a call end means once its chain is read, and where it is refused.

use super::*;

fn record(steps: &str) -> HostClassDecl {
    record_of(&Row {
        refusal: "ok".into(),
        topology: "call".into(),
        steps: steps.into(),
        needs: "-".into(),
    })
}

fn codes(issues: &[TopologyIssue]) -> Vec<String> {
    issues.iter().map(ErrorModel::code).collect()
}

#[test]
fn a_call_is_a_plugin_end_whose_response_channel_is_an_entry() {
    let derived = check(record(
        "entry(in); in.into(plain); plain.named(ask); plain.key.call_factory(agent)",
    ))
    .expect("a keyed factory call registers");
    let ask = &derived.channels[1];
    assert_eq!(
        ask.kind,
        HostChannelKind::Plugin,
        "a call with no consumer is the terminal"
    );
    assert!(ask.sink.is_none(), "a call is not a sink");
    let call = ask.call.as_ref().expect("the channel ends in the call");
    assert_eq!(call.end.factory.as_deref(), Some("agent"));
    assert!(
        call.key && !call.transform,
        "the chain's key belongs to the call"
    );
    let response = &derived.channels[2];
    assert_eq!(response.name, "ask.response");
    assert_eq!(response.kind, HostChannelKind::Entry);
    assert_eq!(
        response.consumers[0].tasklet, 0,
        "the outcome feeds the calling tasklet"
    );
}

#[test]
fn a_call_closes_only_its_own_chain() {
    let derived = check(record(
        "entry(in); in.into(plain); plain.named(ask); plain.filter.call(pricing); plain.filter.into(second)",
    ))
    .expect("a filtered call beside a filtered edge registers");
    assert_eq!(derived.channels[1].kind, HostChannelKind::InterTask);
    assert!(derived.channels[1]
        .call
        .as_ref()
        .is_some_and(|c| c.transform));
}

#[test]
fn a_call_counts_as_a_plugin_end() {
    for steps in [
        "entry(in); in.into(plain); plain.named(ask); plain.call(pricing); plain.output",
        "entry(in); in.into(plain); plain.named(ask); plain.output; plain.call(pricing)",
        "entry(in); in.into(plain); plain.named(ask); plain.call(pricing); plain.call(orders)",
    ] {
        let issues = check(record(steps)).expect_err("two plugin ends");
        assert!(
            codes(&issues).contains(&"topology.channel_ends_twice".into()),
            "{steps}"
        );
    }
}

#[test]
fn an_unread_response_is_refused_by_its_call_alone() {
    let issues = check(record(
        "entry(in); in.into(plain); plain.named(ask); plain.call_unread(pricing)",
    ))
    .expect_err("nobody reads the outcome");
    assert_eq!(
        codes(&issues),
        vec!["topology.call_response_unread".to_string()]
    );
    assert!(matches!(
        &issues[0],
        TopologyIssue::CallResponseUnread { channel, response } if channel == "ask" && response == "ask.response"
    ));
}

#[test]
fn a_steps_call_registers_only_in_a_per_input_loop() {
    const OUTSIDE: &str = "topology.call_outside_per_input_loop";
    // A call is plugin-bound, so a chained loop's terminal hands nothing off.
    const NO_HANDOFF: &str = "topology.chained_loop_terminal_without_handoff";
    for (mode, refusals) in [
        ("once", &[OUTSIDE][..]),
        ("chained", &[OUTSIDE, NO_HANDOFF][..]),
        ("per_input", &[][..]),
    ] {
        let decl = record(&format!(
            r#"class.run({{"steps":{{"each":"{mode}"}}}}); entry(in); in.into(plain); plain.named(ask); plain.call(pricing)"#
        ));
        let verdict = check(decl).map_or_else(|issues| codes(&issues), |_| Vec::new());
        assert_eq!(verdict, refusals, "{mode}");
    }
}
