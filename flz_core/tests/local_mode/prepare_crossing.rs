//! The prepare crossing: an entry payload crossing to the host for its key
//! before it is dispatched, and what becomes of one that fails there or after.

use super::*;

/// `in → forward → out`, keyed on the way in, so every entry payload crosses to
/// the host for its key before it is dispatched. The shape the conformance
/// fixtures call `partition_pass` and `dynamic_cell`.
const KEYED_ENTRY: &str = r#"{
    "factory_id": "local_keyed_entry", "sdk_lang": 1, "sdk_version": "0.1.0",
    "channels": [{"name": "in",  "kind": "entry",  "chains": [[{"link": "key"}, {"link": "into", "tasklet": 0}]]},
                 {"name": "out", "kind": "plugin", "producer": 0, "keyed": true,
                                 "chains": [[{"link": "sink", "name": "out"}]]}],
    "tasklets": [{"name": "forward"}]
}"#;

/// `in → forward → out`, keyed on the way in, retried on a failed crossing.
const KEYED_ENTRY_RETRIED: &str = r#"{
    "factory_id": "local_keyed_entry_retried", "sdk_lang": 1, "sdk_version": "0.1.0",
    "channels": [{"name": "in",  "kind": "entry", "retry_max": 3, "retry_backoff_ms": 40,
                                 "chains": [[{"link": "key"}, {"link": "into", "tasklet": 0}]]},
                 {"name": "out", "kind": "plugin", "producer": 0, "keyed": true,
                                 "chains": [[{"link": "sink", "name": "out"}]]}],
    "tasklets": [{"name": "forward"}]
}"#;

/// The tasklet of both keyed classes: it releases what reaches it, under
/// the key it was dispatched with.
fn echo_keyed(_activation: &Activation, item: &WorkItem) -> ItemEffect {
    match item {
        WorkItem::Input(input) => {
            let value = prepared_value(input);
            ok_effect(vec![sink_output(
                "out",
                &String::from_utf8_lossy(&value[..1]),
                value,
            )])
        }
        WorkItem::Timer(_) => ok_effect(Vec::new()),
    }
}

/// The one value [`prepared`] answered for this item's Cell: a keyed item
/// ships no payload of its own.
fn prepared_value(input: &flz_core::schema::InputItem) -> &[u8] {
    match &input.form {
        flz_core::schema::InputForm::Prepared(values) => &values[0],
        other => panic!("a keyed item arrives prepared, got {other:?}"),
    }
}

/// Feed `fed`, settle, and return what was released, sorted.
fn keyed_run(factory_id: &str, relay_id: &str, fed: &[&str]) -> Vec<String> {
    let run = LocalRun::start(factory_id, relay_id).expect("the run starts");
    let (seen, callback) = recorder();
    run.on_output("out", callback).expect("`out` is declared");
    for payload in fed {
        run.feed("in", payload.as_bytes().to_vec())
            .expect("accepted");
    }
    run.run_until_idle(Duration::from_secs(10))
        .expect("the graph settles");
    let mut released = payloads(&seen);
    released.sort();
    released
}

#[test]
fn a_keyed_entry_class_crosses_for_every_key_and_loses_nothing() {
    let _serialized = one_run();
    declare(KEYED_ENTRY);
    // Every payload crosses to the host for its key before it is dispatched,
    // and the key is answered slowly, as a host with busy threads answers it:
    // settling waits for a crossing still at the host, whose payload is in no
    // mailbox yet. What this pins is the whole path: feed, cross, answer,
    // dispatch, release, with no value dropped between the steps.
    let _host = Host::spawn_keying(
        |payload, want_keys| {
            std::thread::sleep(Duration::from_millis(50));
            prepared(payload, want_keys)
        },
        echo_keyed,
    );

    assert_eq!(
        keyed_run("local_keyed_entry", "keyed-1", &["a:1", "b:1", "a:2"]),
        vec!["a:1", "a:2", "b:1"]
    );
}

#[test]
fn a_crossing_on_its_retry_ladder_is_still_waited_for() {
    let _serialized = one_run();
    declare(KEYED_ENTRY_RETRIED);
    // The first key asked for fails, so its payload leaves the crossing for
    // the channel's retry ladder, where it is in no queue and no mailbox
    // until a re-attempt is answered.
    let failed_once = AtomicBool::new(false);
    let _host = Host::spawn_keying(
        move |payload, want_keys| {
            if !failed_once.swap(true, Ordering::Relaxed) {
                return Err(WorkError {
                    message: "the key service is not up yet".to_string(),
                    non_retryable: false,
                });
            }
            prepared(payload, want_keys)
        },
        echo_keyed,
    );

    assert_eq!(
        keyed_run(
            "local_keyed_entry_retried",
            "keyed-retried-1",
            &["a:1", "b:1", "a:2"]
        ),
        vec!["a:1", "a:2", "b:1"]
    );
}

#[test]
fn a_payload_that_parks_is_dropped_and_the_run_settles() {
    let _serialized = one_run();
    declare(KEYED_ENTRY_RETRIED);
    // `k:bad` exhausts its crossing's retry ladder and `w:bad` fails its work
    // non-retryably. With no controller to acknowledge a park, each is logged
    // and dropped, and its Cell goes on to the next payload of its key.
    let _host = Host::spawn_keying(
        |payload, want_keys| match payload {
            b"k:bad" => Err(WorkError {
                message: "no key for this one".to_string(),
                non_retryable: false,
            }),
            _ => prepared(payload, want_keys),
        },
        |activation, item| match item {
            WorkItem::Input(input) if prepared_value(input) == b"w:bad" => ItemEffect {
                verdict: Verdict::NonRetryableError {
                    message: "poison".to_string(),
                },
                ..ok_effect(Vec::new())
            },
            _ => echo_keyed(activation, item),
        },
    );

    let fed = ["k:bad", "w:bad", "k:ok", "w:ok"];
    assert_eq!(
        keyed_run("local_keyed_entry_retried", "parked-1", &fed),
        vec!["k:ok", "w:ok"]
    );
}
