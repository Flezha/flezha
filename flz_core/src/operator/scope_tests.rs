//! A message into a stepped scope of a durable outer instance goes to the
//! controller first: the append it asks for names the outer input it came
//! from, a refused append is sent again and then keeps that input's hold, a
//! message the controller dead-lettered frees its key, and a value that
//! cannot be encoded fails its edge into the dead-letter queue. A frame's
//! `flz.scope` is read only in the shape a scope's id has.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crossbeam::channel::{unbounded, Receiver};

use super::*;
use crate::host::register_host_class_json;
use crate::pb::command_request::Command;
use crate::runner::{RunnerMessage, RunnerNode};
use crate::test_support::{capture_parks, host_class, spin_until, stream_pair};

/// A durable flow class whose entry `in` ends in the embedded per-input
/// steps class `<id>_session`, unkeyed, with room for `max_cells` scopes.
fn gateway(id: &str, max_cells: usize) -> String {
    let inner = format!("{id}_session");
    register_host_class_json(
        format!(
            r#"{{"factory_id":"{inner}","sdk_lang":1,"sdk_version":"0.1.0","embedded":true,
                 "run":{{"steps":{{"each":"per_input"}}}},"durable":{{"local":{{}}}},
                 "channels":[{{"name":"turns","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
                 "tasklets":[{{"name":"turn"}}]}}"#
        )
        .as_bytes(),
    )
    .expect("the inner class registers");
    register_host_class_json(
        format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","run":"flow",
                 "durable":{{"local":{{"sleep_after":false}}}},
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}},
                             {{"name":"answer","kind":"inter_task","producer":0,
                               "chains":[[{{"link":"into","tasklet":1}}]]}}],
                 "tasklets":[{{"name":"session","operator":"{inner}","max_cells":{max_cells}}},
                             {{"name":"reply"}}]}}"#
        )
        .as_bytes(),
    )
    .expect("the gateway registers");
    format!("{id}_inst")
}

fn bound(id: &str, max_cells: usize) -> (RunnerNode, Arc<OperatorInstance>) {
    let instance_id = gateway(id, max_cells);
    let mut node = RunnerNode::new("test-ns".to_string());
    assert!(host_class::bind(&mut node, id, &instance_id, "", 0).is_empty());
    let outer = node
        .get_operator(&instance_id)
        .expect("the gateway is bound");
    (node, outer)
}

fn env() -> (
    std::sync::MutexGuard<'static, ()>,
    std::sync::MutexGuard<'static, ()>,
    Receiver<RunnerMessage>,
) {
    let env = crate::runner::env_test_guard();
    let sink = crate::state_sink::test_guard();
    crate::state_sink::reset_for_test();
    let (tx, rx) = unbounded::<RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    (env, sink, rx)
}

/// One frame into instance `id`, at input-log `offset`.
fn feed(node: &RunnerNode, id: &str, offset: u64, metadata: &[(&str, &str)]) {
    let mut metadata: HashMap<String, String> = metadata
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    metadata.insert("flz.offset".to_string(), offset.to_string());
    node.operator_dispatch_handle()
        .dispatch(id, b"hi".to_vec(), &metadata, stream_pair().0);
}

/// The next `ScopeInput` the runner asked the controller to log.
fn next_input(rx: &Receiver<RunnerMessage>) -> (String, crate::pb::ScopeInput) {
    loop {
        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(RunnerMessage::EmitControlRequest {
                reference_id,
                command,
            }) => {
                if let Command::ScopeInput(input) = *command {
                    return (reference_id, input);
                }
            }
            Ok(_) => {}
            Err(_) => panic!("no ScopeInput was sent"),
        }
    }
}

fn refused(reference_id: &str) {
    crate::shutdown::signal_result(
        reference_id,
        Err(crate::FlezhaError::with(
            "commit.not_owner",
            [("operator", "gateway".into())],
        )),
    );
}

fn family_of(outer: &OperatorInstance) -> &Arc<ScopeFamily> {
    outer.families.iter().flatten().next().expect("one family")
}

// fails when: an append the controller refused releases its input's hold,
// so the outer instance's processed prefix passes a message no log holds.
#[test]
fn a_refused_append_is_sent_again_and_then_keeps_its_input_unconfirmed() {
    let (_env, _sink, rx) = env();
    let (node, outer) = bound(&format!("scope_refused_{}", std::process::id()), 4);
    feed(&node, &outer.instance_id, 7, &[]);

    let mut ids = Vec::new();
    for _ in 0..crate::steps::BARRIER_SENDS {
        let (reference_id, input) = next_input(&rx);
        ids.push(input.msg_id);
        refused(&reference_id);
    }

    assert_eq!(ids, vec![format!("{}@7:0", outer.instance_id); 3]);
    assert_eq!(family_of(&outer).refused.lock_recover().len(), 1);
}

// fails when: a key the controller dead-lettered stays pending, so under
// `max_cells` it blocks every new key for as long as the instance lives.
#[test]
fn a_dead_lettered_message_frees_its_key() {
    let (_env, _sink, rx) = env();
    let (_node, outer) = bound(&format!("scope_freed_{}", std::process::id()), 1);
    let family = Arc::clone(family_of(&outer));
    let carol = Some("carol".to_string());
    family.pending.lock_recover().insert(carol.clone());
    let input = crate::pb::ScopeInput {
        operator_id: outer.instance_id.to_string(),
        scope: Some(crate::pb::Scope {
            inner: family.name.to_string(),
            key: carol,
        }),
        ..Default::default()
    };
    send_input(Arc::downgrade(&family), input, None, 1);
    assert!(!family.admit(&Some("dave".to_string())), "the cap is full");

    let (reference_id, _input) = next_input(&rx);
    crate::shutdown::signal_result(&reference_id, Ok(REFUSED.to_string()));

    assert!(family.admit(&Some("dave".to_string())));
}

// fails when: a value with no bytes for the scope's log is dropped with a
// warning instead of failing its edge, whose ladder parks it.
#[test]
fn a_value_that_cannot_be_encoded_is_parked_in_the_dead_letter_queue() {
    let (_env, _sink, rx) = env();
    let parks = capture_parks();
    let (_node, outer) = bound(&format!("scope_unencodable_{}", std::process::id()), 4);
    let (_, entry) = outer
        .topology
        .first_of_kind(crate::channel::ChannelKind::Entry)
        .expect("an entry channel");
    let value: Value = Arc::new(5u32);

    crate::routing::route_to_consumers(
        &outer,
        entry,
        &value,
        &stream_pair().0,
        Some(3),
        None,
        false,
        None,
    );

    spin_until("the value is parked", || !parks.lock_recover().is_empty());
    let park = &parks.lock_recover()[0];
    assert!(
        park.body.entry.error.contains("could not be encoded"),
        "{}",
        park.body.entry.error
    );
    assert!(!matches!(
        rx.try_iter().find_map(|m| match m {
            RunnerMessage::EmitControlRequest { command, .. } => Some(*command),
            _ => None,
        }),
        Some(Command::ScopeInput(_))
    ));
}

// fails when: a producer's `flz.scope` reaches past its instance to any id
// it names, or a malformed one keeps the frame from its instance.
#[test]
fn a_frame_names_a_scope_only_in_the_shape_of_a_scope_id() {
    for good in ["/session", "/session/alice", "/session/_YQ"] {
        assert!(is_scope_suffix(good), "{good}");
    }
    for bad in [
        "",
        "session",
        "/",
        "//alice",
        "/session/",
        "/a/b/c",
        "/../../x",
    ] {
        assert!(!is_scope_suffix(bad), "{bad}");
    }

    let (_env, _sink, rx) = env();
    let (node, outer) = bound(&format!("scope_named_{}", std::process::id()), 4);
    feed(&node, &outer.instance_id, 1, &[("flz.scope", "/../../x")]);

    let (_reference_id, input) = next_input(&rx);
    assert_eq!(input.operator_id, *outer.instance_id);
}
