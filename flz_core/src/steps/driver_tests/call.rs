//! A `:per_input` pass that calls: its request leaves at the settle of the
//! stage that emitted it, stamped, and the outcome is a later message on the
//! Response channel, taken as the next iteration.

use super::*;

#[test]
fn a_per_input_pass_calls_from_its_last_stage_and_reads_the_outcome_next() {
    let (_env, _sink, rx) = turn_env();
    let id = format!("turn_call_{}", std::process::id());
    let json = turn_json(&id, false, "per_input", "keep_progress")
        .replace(
            r#"{"link":"sink","name":"out"}"#,
            r#"{"link":"call","target":"pricing","deadline_ms":30000,"response":"out.response"}"#,
        )
        .replacen(
            r#"{"name":"mid""#,
            r#"{"name":"out.response","kind":"entry","chains":[[{"link":"into","tasklet":0}]]},
               {"name":"mid""#,
            1,
        );
    crate::host::register_host_class_json(json.as_bytes()).expect("a per-input class calls");
    let topology = crate::operator::registry::get_class(&id).expect("registered");
    let mid = topology.lookup_channel_id("mid").expect("`mid`");
    let _host = turn_host(mid, Arc::default());
    let mut node = RunnerNode::new("test-ns".to_string());
    let wire = Wire::new();
    bind_turns(&mut node, &id, "turn-call-1", true, None);

    let before = crate::util::wall_now_ms();
    feed(&wire, &node, "turn-call-1", b"hello");
    let request = next_output(&wire, "turn-call-1");
    assert_eq!(request.0, b"re:hello".to_vec());
    assert_eq!(
        request.1.get("flz.call").map(String::as_str),
        Some("id:pricing")
    );
    assert_eq!(
        request.1.get("flz.call_response").map(String::as_str),
        Some("out.response")
    );
    let deadline: u64 = request.1["flz.deadline_at"].parse().expect("a millisecond");
    assert!(deadline >= before + 30_000 && deadline <= crate::util::wall_now_ms() + 30_000);

    // The outcome is an ordinary message on the Response channel: the next turn.
    let response = HashMap::from([("channel".to_string(), "out.response".to_string())]);
    node.operator_dispatch_handle().dispatch(
        "turn-call-1",
        b"outcome".to_vec(),
        &response,
        Arc::clone(&wire.stream),
    );
    assert_eq!(next_output(&wire, "turn-call-1").0, b"re:outcome".to_vec());
    assert_no_completion(&rx, "turn-call-1", Duration::from_millis(200));
    state_sink::reset_for_test();
}

fn next_output(wire: &Wire, id: &str) -> (Vec<u8>, HashMap<String, String>) {
    loop {
        let (payload_id, payload, metadata) = wire
            .frames
            .recv_timeout(Duration::from_secs(5))
            .expect("the pass emitted its request");
        if payload_id == id {
            return (payload, metadata);
        }
    }
}
