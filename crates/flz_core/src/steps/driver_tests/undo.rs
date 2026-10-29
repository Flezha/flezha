//! A saga's `compensated` entry the controller refuses or never acks: it goes
//! out again under its reference id, the Cell's done marker follows only an
//! ack, and an entry never journaled leaves its Cell unmarked and ends the
//! run `error` naming the tasklet, while the rest of the undo still runs.

use super::*;
use crate::pb::command_request::Command;
use crate::state_sink::COMPENSATION_DONE_PAYLOAD_ID;

/// `a` and `b` forward each message down the chain and declare an undo that
/// writes to the plugin channel `out`; `c` fails every attempt, so a message
/// unwinds `b`, then `a`.
fn saga_json(id: &str) -> String {
    let edge = |name: &str, producer: u32| {
        format!(
            r#"{{"name":"{name}","kind":"inter_task","producer":{producer},
                 "retry_max":1,"retry_backoff_ms":1,
                 "chains":[[{{"link":"into","tasklet":{}}}]]}}"#,
            producer + 1
        )
    };
    format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0",
             "durable":{{"local":{{"sleep_after":false}}}},
             "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}},
                         {},{},
                         {{"name":"out","kind":"plugin","producer":2,
                           "chains":[[{{"link":"sink","name":"out"}}]]}}],
             "tasklets":[{{"name":"a","compensate_overridden":true}},
                         {{"name":"b","compensate_overridden":true}},{{"name":"c"}}],
             "run":{{"steps":{{"each":"per_input","on_failure":"undo_all"}}}}}}"#,
        edge("m1", 0),
        edge("m2", 1)
    )
}

/// Every `compensate` succeeds, or fails with `hook_fails` when it names a reason.
fn saga_host(edges: [u32; 2], hook_fails: Option<&'static str>) -> FakeHost {
    FakeHost::answering(move |payload, applied| match payload {
        EnginePayload::Command {
            kind: schema::ActivationKind::CompensateRun { .. },
            ..
        } => match hook_fails {
            None => schema::ActivationOutcome::Work(vec![effect(schema::Verdict::Ok, b"undone")]),
            Some(reason) => schema::ActivationOutcome::Failed(schema::WorkError {
                message: reason.to_string(),
                non_retryable: true,
            }),
        },
        EnginePayload::Work { slot, items, .. } => schema::ActivationOutcome::Work(
            items
                .iter()
                .map(
                    |item| match (edges.get(slot.tasklet_id as usize), &item.item) {
                        (Some(&channel), CellItem::Input(payload)) => {
                            forwarded(channel, slot.tasklet_id + 1, &host_bytes(&payload.value))
                        }
                        _ => effect(
                            schema::Verdict::RetryableError {
                                message: "out of stock".to_string(),
                            },
                            &[],
                        ),
                    },
                )
                .collect(),
        ),
        other => answer(other, applied, &|_: &[u8]| schema::Verdict::Ok),
    })
}

/// The tasklets whose compensation-done marker was shipped, in order.
type Marks = Arc<Mutex<Vec<String>>>;

fn capture_marks() -> Marks {
    let marks = Marks::default();
    let seen = Arc::clone(&marks);
    state_sink::install_global_sink(Arc::new(move |payload_id: &str, frame: &[u8]| {
        if payload_id == COMPENSATION_DONE_PAYLOAD_ID {
            let split = crate::test_support::split_cell_frame(frame).expect("a cell frame");
            let (_, tasklet, _) =
                crate::flusher::decode_cell_identity(split.identity).expect("a cell identity");
            seen.lock().expect("marks poisoned").push(tasklet);
        }
        Ok(())
    }));
    marks
}

/// Run one message through a saga whose barriers the controller answers as
/// `answer` says (shown the markers shipped so far), to the run's end; an
/// `ack_timeout_ms` shortens each send's wait for its ack.
fn saga(
    id: &str,
    ack_timeout_ms: Option<&str>,
    answer: impl Fn(&Command, &[String]) -> Answer,
) -> (Seen, Vec<String>) {
    saga_hooked(id, ack_timeout_ms, None, answer)
}

/// [`saga`] whose `compensate` hooks fail with `hook_fails` when it names a reason.
fn saga_hooked(
    id: &str,
    ack_timeout_ms: Option<&str>,
    hook_fails: Option<&'static str>,
    answer: impl Fn(&Command, &[String]) -> Answer,
) -> (Seen, Vec<String>) {
    const ACK_TIMEOUT: &str = "FLEZHA_STEPS_ACK_TIMEOUT_MS";
    let (_env, _sink, rx) = turn_env();
    if let Some(ms) = ack_timeout_ms {
        std::env::set_var(ACK_TIMEOUT, ms);
    }
    let marks = capture_marks();
    let factory = format!("{id}_{}", std::process::id());
    crate::host::register_host_class_json(saga_json(&factory).as_bytes())
        .expect("the saga class registers");
    let topology = crate::relay::registry::get_class(&factory).expect("registered");
    let edge = |name| topology.lookup_channel_id(name).expect("an edge channel");
    let _host = saga_host([edge("m1"), edge("m2")], hook_fails);
    let mut node = RunnerNode::new("test-ns".to_string());
    bind_turns(&mut node, &factory, id, None);
    feed_at(&node.get_relay(id).expect("bound"), b"order", 0);
    let mut seen = Seen::default();
    pump_turns(
        &rx,
        id,
        &mut seen,
        |command| answer(command, &marks.lock().expect("marks poisoned")),
        |_| false,
    );
    std::env::remove_var(ACK_TIMEOUT);
    state_sink::reset_for_test();
    let marks = marks.lock().expect("marks poisoned").clone();
    (seen, marks)
}

fn compensating(command: &Command, tasklet: u32) -> bool {
    matches!(command, Command::TaskCommit(t) if t.compensating && t.task_idx == tasklet)
}

/// The undo entry sends of `tasklet`, each under the first one's reference id.
fn sends_of(seen: &Seen, tasklet: u32) -> usize {
    let refs: Vec<&String> = seen
        .undo_refs
        .iter()
        .filter(|(task, _)| *task == tasklet)
        .map(|(_, reference_id)| reference_id)
        .collect();
    assert!(
        refs.windows(2).all(|pair| pair[0] == pair[1]),
        "one reference id per undo entry: {refs:?}"
    );
    refs.len()
}

#[test]
fn a_compensation_the_controller_always_refuses_leaves_its_cell_unmarked_and_ends_error() {
    let (seen, marks) = saga("saga-refused", None, |command, _| {
        Answer::from(matches!(command, Command::TaskCommit(t) if t.compensating))
    });
    assert_eq!((sends_of(&seen, 1), sends_of(&seen, 0)), (3, 3));
    assert!(marks.is_empty(), "no undo entry was journaled: {marks:?}");
    let (status, error) = seen.done.expect("the run ended");
    assert_eq!(status, "error");
    assert!(
        error.contains("out of stock")
            && error.contains("compensation of tasklet `b` was not journaled: "),
        "{error}"
    );
    assert_eq!(
        seen.undo, "partial",
        "a refused entry leaves the rollback incomplete"
    );
}

#[test]
fn a_compensation_refused_once_is_sent_again_and_marked_after_its_ack() {
    let refused = AtomicBool::new(false);
    let (seen, marks) = saga("saga-refused-once", None, |command, marks| {
        if !compensating(command, 1) {
            return Answer::Ack;
        }
        assert!(
            marks.is_empty(),
            "`b` was marked before its entry was acked"
        );
        Answer::from(!refused.swap(true, Ordering::SeqCst))
    });
    assert_eq!((sends_of(&seen, 1), sends_of(&seen, 0)), (2, 1));
    assert_eq!(marks, ["b", "a"]);
    let (status, error) = seen.done.expect("the run ended");
    assert_eq!(status, "error");
    assert!(
        error.contains("out of stock") && !error.contains("compensation"),
        "the planned end: {error}"
    );
    assert_eq!(seen.undo, "complete", "every hook ran and was journaled");
}

#[test]
fn a_failing_hook_reports_the_rollback_partial() {
    let (seen, _marks) = saga_hooked("saga-hook-fails", None, Some("refund declined"), |_, _| {
        Answer::Ack
    });
    let (status, _error) = seen.done.expect("the run ended");
    assert_eq!((status.as_str(), seen.undo.as_str()), ("error", "partial"));
}

#[test]
fn a_compensation_never_acked_ends_error_and_the_other_cell_is_still_marked() {
    let (seen, marks) = saga("saga-unacked", Some("100"), |command, _| {
        if compensating(command, 1) {
            Answer::Ignore
        } else {
            Answer::Ack
        }
    });
    assert_eq!((sends_of(&seen, 1), sends_of(&seen, 0)), (3, 1));
    assert_eq!(marks, ["a"]);
    let (status, error) = seen.done.expect("the run ended");
    assert_eq!(status, "error");
    assert!(
        error.contains("compensation of tasklet `b` was not journaled: ")
            && error.contains("ack timed out after 3 attempt(s)"),
        "{error}"
    );
    assert_eq!(seen.undo, "partial");
}
