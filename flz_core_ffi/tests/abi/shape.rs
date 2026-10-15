//! The shape every binding shares, proved once against a recording pair.
//!
//! `veneer::shape` builds an activation into a `serde_json` tree and reads a
//! result back out of one, so which key carries which field is checked here
//! and each binding's own arm proves only that its language builds and reads
//! the primitives. The activation direction is checked field by field rather
//! than by a round trip: the mapping has no inverse in production — nothing
//! ever maps a host value back to an `Activation` — so an inverse written
//! here could agree with a wrong forward mapping.

use flz_core::schema;
use flz_core_ffi::veneer::shape::{self, Key};
use flz_core_ffi::veneer::Refusal;
use serde_json::{json, Map, Value};

/// A language whose containers are `serde_json`'s. A payload is an array
/// of byte numbers, so it round-trips with no encoding of its own.
pub struct Recorder;

impl shape::Sink for Recorder {
    type Value = Value;
    type Map = Map<String, Value>;
    type List = Vec<Value>;
    type Error = Refusal;

    fn map(&self) -> Result<Self::Map, Refusal> {
        Ok(Map::new())
    }

    fn set(&self, map: &mut Self::Map, key: Key, value: Value) -> Result<(), Refusal> {
        map.insert(key.spell(<Self as shape::Sink>::CAMEL).to_string(), value);
        Ok(())
    }

    fn finish_map(&self, map: Self::Map) -> Result<Value, Refusal> {
        Ok(Value::Object(map))
    }

    fn list(&self, capacity: usize) -> Result<Vec<Value>, Refusal> {
        Ok(Vec::with_capacity(capacity))
    }

    fn push(&self, list: &mut Vec<Value>, value: Value) -> Result<(), Refusal> {
        list.push(value);
        Ok(())
    }

    fn finish_list(&self, list: Vec<Value>) -> Result<Value, Refusal> {
        Ok(Value::Array(list))
    }

    fn bytes(&self, v: Vec<u8>) -> Result<Value, Refusal> {
        Ok(raw(&v))
    }

    fn text(&self, v: String) -> Result<Value, Refusal> {
        Ok(Value::String(v))
    }

    fn word(&self, v: &'static str) -> Result<Value, Refusal> {
        Ok(Value::String(v.to_string()))
    }

    fn pair(&self, key: String, value: String) -> Result<Value, Refusal> {
        Ok(json!([key, value]))
    }

    fn u64(&self, v: u64) -> Result<Value, Refusal> {
        Ok(json!(v))
    }

    fn u32(&self, v: u32) -> Result<Value, Refusal> {
        Ok(json!(v))
    }

    fn bool(&self, v: bool) -> Result<Value, Refusal> {
        Ok(Value::Bool(v))
    }
}

impl shape::Source for Recorder {
    type Value = Value;
    type Map = Map<String, Value>;
    type Error = Refusal;

    fn get(&self, map: &Self::Map, key: Key) -> Result<Option<Value>, Refusal> {
        Ok(map
            .get(key.spell(<Self as shape::Source>::CAMEL))
            .filter(|v| !v.is_null())
            .cloned())
    }

    fn map(&self, v: &Value, what: &str) -> Result<Self::Map, Refusal> {
        v.as_object()
            .cloned()
            .ok_or_else(|| Refusal::Shape(format!("{what} must be an object")))
    }

    fn text(&self, v: &Value) -> Result<String, Refusal> {
        v.as_str()
            .map(str::to_string)
            .ok_or_else(|| Refusal::Shape(format!("{v} is not a string")))
    }

    fn bytes(&self, v: &Value, what: &str) -> Result<Vec<u8>, Refusal> {
        let items = v
            .as_array()
            .ok_or_else(|| Refusal::Shape(format!("{what} must be bytes")))?;
        items
            .iter()
            .map(|b| {
                b.as_u64()
                    .map(|b| b as u8)
                    .ok_or_else(|| Refusal::Shape(format!("{what} must be bytes")))
            })
            .collect()
    }

    fn u64(&self, v: &Value, what: &str) -> Result<u64, Refusal> {
        v.as_u64()
            .ok_or_else(|| Refusal::Shape(format!("{what} must be an integer")))
    }

    fn u32(&self, v: &Value) -> Result<u32, Refusal> {
        Ok(shape::Source::u64(self, v, "a 32-bit field")? as u32)
    }

    fn bool(&self, v: &Value) -> Result<bool, Refusal> {
        v.as_bool()
            .ok_or_else(|| Refusal::Shape(format!("{v} is not a boolean")))
    }

    fn items(&self, v: &Value) -> Result<Vec<Value>, Refusal> {
        v.as_array()
            .cloned()
            .ok_or_else(|| Refusal::Shape(format!("{v} is not a list")))
    }

    fn pair(&self, v: &Value) -> Result<(String, String), Refusal> {
        match v.as_array().map(Vec::as_slice) {
            Some([key, value]) => Ok((
                shape::Source::text(self, key)?,
                shape::Source::text(self, value)?,
            )),
            _ => Err(Refusal::Shape(
                "a metadata entry must be a (key, value) pair".to_string(),
            )),
        }
    }
}

/// Build every primitive the shape asks a language for and read it back,
/// so a container a binding cannot round-trip fails here rather than at a
/// branch in that SDK. Written once and called by each arm: what differs
/// between the bindings is only these primitives.
///
/// `word` is not read back. It is write-only in production — a host
/// answers `outcome`, `verdict` and a timer `kind` as text, never as the
/// language's word type — so each arm asserts its own spelling instead.
pub fn primitives_round_trip<S, V, M>(s: &S)
where
    S: shape::Sink<Value = V, Map = M> + shape::Source<Value = V, Map = M>,
    <S as shape::Sink>::Error: std::fmt::Debug,
    <S as shape::Source>::Error: std::fmt::Debug,
{
    let mut map = shape::Sink::map(s).expect("a map");
    let put =
        |map: &mut M, key, value| shape::Sink::set(s, map, key, value).expect("the key is set");
    put(
        &mut map,
        shape::TOKEN,
        shape::Sink::u64(s, u64::MAX).expect("a u64"),
    );
    put(
        &mut map,
        shape::TASKLET,
        shape::Sink::u32(s, 7).expect("a u32"),
    );
    put(
        &mut map,
        shape::REPLAY,
        shape::Sink::bool(s, true).expect("a bool"),
    );
    put(
        &mut map,
        shape::FACTORY,
        shape::Sink::text(s, "f".to_string()).expect("text"),
    );
    put(
        &mut map,
        shape::PAYLOAD,
        shape::Sink::bytes(s, vec![0, 255, 7]).expect("bytes"),
    );
    let mut list = shape::Sink::list(s, 1).expect("a list");
    let pair = shape::Sink::pair(s, "k".to_string(), "v".to_string()).expect("a pair");
    shape::Sink::push(s, &mut list, pair).expect("the pair is pushed");
    put(
        &mut map,
        shape::METADATA,
        shape::Sink::finish_list(s, list).expect("a list"),
    );

    let want = |key| {
        shape::Source::get(s, &map, key)
            .expect("the key reads")
            .expect("the key is present")
    };
    assert_eq!(
        shape::Source::u64(s, &want(shape::TOKEN), "token").expect("a u64"),
        u64::MAX
    );
    assert_eq!(
        shape::Source::u32(s, &want(shape::TASKLET)).expect("a u32"),
        7
    );
    assert!(shape::Source::bool(s, &want(shape::REPLAY)).expect("a bool"));
    assert_eq!(
        shape::Source::text(s, &want(shape::FACTORY)).expect("text"),
        "f"
    );
    assert_eq!(
        shape::Source::bytes(s, &want(shape::PAYLOAD), "payload").expect("bytes"),
        vec![0, 255, 7]
    );
    let rows = shape::Source::items(s, &want(shape::METADATA)).expect("a list");
    assert_eq!(
        shape::Source::pair(s, &rows[0]).expect("a pair"),
        ("k".to_string(), "v".to_string())
    );
    // An absent key reads as absent, not as the language's nil.
    assert!(shape::Source::get(s, &map, shape::CELL_KEY)
        .expect("an absent key reads")
        .is_none());
}

/// The helper every arm calls, proved against the recording pair itself.
#[test]
fn the_recording_pair_round_trips_every_primitive() {
    primitives_round_trip(&Recorder);
}

/// The released-output writer, both arms. `local_next_output` is the only
/// caller and the core only ever queues a plugin release, so the edge arm is
/// unreachable in production — which is exactly why it is proved here, and
/// against the same hand-written expectation the result direction uses.
#[test]
fn a_released_output_is_written_the_way_a_recorded_one_reads() {
    let plugin = schema::Output {
        target: schema::OutputTarget::Plugin {
            channel: "totals".into(),
        },
        key: Some("the".into()),
        event_time: Some(7),
        metadata: vec![("flz.key".into(), "the".into())],
        payload: b"the 1".to_vec(),
    };
    let edge = schema::Output {
        target: schema::OutputTarget::Edge {
            channel: 2,
            consumer: 1,
        },
        ..plugin.clone()
    };
    for output in [plugin, edge] {
        assert_eq!(
            shape::sink_output(&Recorder, output.clone()).expect("a written output"),
            record_output(&output),
        );
    }
}

/// An absent optional is an absent key, not a null one: a host reads the key
/// as missing, which is what every binding's `Source` half expects.
#[test]
fn a_released_output_omits_the_optionals_it_has_not_got() {
    let written = shape::sink_output(
        &Recorder,
        schema::Output {
            target: schema::OutputTarget::Plugin {
                channel: "totals".into(),
            },
            key: None,
            event_time: None,
            metadata: vec![],
            payload: vec![],
        },
    )
    .expect("a written output");
    let map = obj(&written);
    assert!(
        !map.contains_key("key") && !map.contains_key("event_time"),
        "{map:?}"
    );
    assert_eq!(text(map, "plugin_channel"), "totals");
}

fn raw(bytes: &[u8]) -> Value {
    Value::Array(bytes.iter().map(|b| json!(b)).collect())
}

// --- reading the recorded activation ----------------------------------

fn obj(v: &Value) -> &Map<String, Value> {
    v.as_object().expect("the shape emits maps")
}

fn at<'a>(h: &'a Map<String, Value>, key: &str) -> &'a Value {
    h.get(key)
        .unwrap_or_else(|| panic!("the shape dropped `{key}`"))
}

fn num(h: &Map<String, Value>, key: &str) -> u64 {
    at(h, key).as_u64().expect("an integer field")
}

fn text(h: &Map<String, Value>, key: &str) -> String {
    at(h, key).as_str().expect("a text field").to_string()
}

fn opt_text(h: &Map<String, Value>, key: &str) -> Option<String> {
    h.get(key)
        .map(|v| v.as_str().expect("a text field").to_string())
}

fn bytes_at(h: &Map<String, Value>, key: &str) -> Vec<u8> {
    at(h, key)
        .as_array()
        .expect("a payload field")
        .iter()
        .map(|b| b.as_u64().expect("a byte") as u8)
        .collect()
}

fn rows<'a>(h: &'a Map<String, Value>, key: &str) -> &'a [Value] {
    at(h, key).as_array().expect("a list field")
}

fn meta(h: &Map<String, Value>, key: &str) -> Vec<(String, String)> {
    rows(h, key)
        .iter()
        .map(|pair| {
            let pair = pair.as_array().expect("a metadata pair is a list");
            (
                pair[0].as_str().expect("a key").to_string(),
                pair[1].as_str().expect("a value").to_string(),
            )
        })
        .collect()
}

fn assert_body_maps(recorded: &Value, body: &schema::RestoreBody) {
    let h = obj(recorded);
    assert_eq!(bytes_at(h, "bytes"), body.bytes);
    assert_eq!(
        h.get("from_version")
            .map(|v| v.as_u64().expect("a version") as u32),
        body.from_version
    );
}

fn assert_item_maps(recorded: &Value, item: &schema::WorkItem) {
    let h = obj(recorded);
    match item {
        schema::WorkItem::Input(i) => {
            assert_eq!(text(h, "kind"), "input");
            assert_eq!(num(h, "channel") as u32, i.channel);
            assert_eq!(bytes_at(h, "payload"), i.payload);
            assert_eq!(meta(h, "metadata"), i.metadata);
            assert_eq!(
                h.get("consumed_offset").map(|v| v.as_u64().unwrap()),
                i.consumed_offset
            );
            assert_eq!(at(h, "replay").as_bool(), Some(i.replay));
            assert_eq!(num(h, "now"), i.now);
        }
        schema::WorkItem::Timer(t) => {
            assert_eq!(text(h, "kind"), "timer");
            assert_eq!(text(h, "timer_kind"), t.kind.as_str());
            assert_eq!(num(h, "scheduled_at"), t.scheduled_at);
            // A timer firing's `now` IS its scheduled time, so a handler
            // reads one field for both kinds.
            assert_eq!(num(h, "now"), t.scheduled_at);
        }
    }
}

/// Assert the decoded activation reaches a host intact.
pub fn assert_activation_maps(act: &schema::Activation) {
    let recorded = shape::activation(&Recorder, act.clone()).expect("the shape builds it");
    let h = obj(&recorded);
    assert_eq!(num(h, "token"), act.token);
    assert_eq!(text(h, "relay"), act.target.relay);
    assert_eq!(num(h, "tasklet") as u32, act.target.tasklet);
    assert_eq!(opt_text(h, "cell_key"), act.target.cell_key);
    assert_eq!(num(h, "change_seq"), act.change_seq);
    assert_eq!(text(h, "factory"), act.factory);
    assert_kind_maps(h, &act.kind);
}

fn assert_kind_maps(h: &Map<String, Value>, kind: &schema::ActivationKind) {
    match kind {
        schema::ActivationKind::Work(items) => {
            assert_eq!(text(h, "kind"), "work");
            let mapped = rows(h, "items");
            assert_eq!(mapped.len(), items.len());
            for (recorded, item) in mapped.iter().zip(items) {
                assert_item_maps(recorded, item);
            }
        }
        schema::ActivationKind::RestoreFold(fold) => {
            assert_eq!(text(h, "kind"), "restore_fold");
            assert_eq!(h.contains_key("snapshot"), fold.snapshot.is_some());
            if let Some(snapshot) = &fold.snapshot {
                assert_body_maps(at(h, "snapshot"), snapshot);
            }
            let changes = rows(h, "changes");
            assert_eq!(changes.len(), fold.changes.len());
            for (recorded, body) in changes.iter().zip(&fold.changes) {
                assert_body_maps(recorded, body);
            }
        }
        schema::ActivationKind::SnapshotRequest => assert_eq!(text(h, "kind"), "snapshot_request"),
        schema::ActivationKind::LiveStateRead => assert_eq!(text(h, "kind"), "live_state_read"),
        schema::ActivationKind::ApplyState { state_json } => {
            assert_eq!(text(h, "kind"), "apply_state");
            assert_eq!(bytes_at(h, "state_json"), *state_json);
        }
        schema::ActivationKind::MigrationSerialize => {
            assert_eq!(text(h, "kind"), "migration_serialize")
        }
        schema::ActivationKind::KeyRequest(request) => {
            assert_eq!(text(h, "kind"), "key_request");
            let mapped = obj(at(h, "key_request"));
            assert_eq!(num(mapped, "channel") as u32, request.channel);
            assert_eq!(at(mapped, "want_keys").as_bool(), Some(request.want_keys));
            assert_eq!(
                at(mapped, "want_event_time").as_bool(),
                Some(request.want_event_time)
            );
            let payloads: Vec<Vec<u8>> = rows(mapped, "payloads")
                .iter()
                .map(|p| {
                    p.as_array()
                        .expect("a payload")
                        .iter()
                        .map(|b| b.as_u64().unwrap() as u8)
                        .collect()
                })
                .collect();
            assert_eq!(payloads, request.payloads);
        }
        schema::ActivationKind::CompensateRun { outputs } => {
            assert_eq!(text(h, "kind"), "compensate_run");
            let mapped: Vec<Vec<u8>> = rows(h, "outputs")
                .iter()
                .map(|o| {
                    o.as_array()
                        .expect("an output")
                        .iter()
                        .map(|b| b.as_u64().unwrap() as u8)
                        .collect()
                })
                .collect();
            assert_eq!(mapped, *outputs);
        }
        schema::ActivationKind::Leave { whole_relay } => {
            assert_eq!(text(h, "kind"), "leave");
            assert_eq!(at(h, "whole_relay").as_bool(), Some(*whole_relay));
        }
    }
}

// --- writing a result the way a host answers --------------------------

/// Build the result in the shape a host answers with, read it back
/// through `shape::result`, and assert nothing changed. The scenario then
/// completes with the mapped value, so a lossy field fails the phase that
/// depends on it and not only this equality.
pub fn through_veneer(result: &schema::ActivationResult) -> schema::ActivationResult {
    let mapped = shape::result(&Recorder, &record_result(result)).expect("the shape reads it");
    assert_eq!(&mapped, result, "the shape changed the result");
    mapped
}

fn record_result(r: &schema::ActivationResult) -> Map<String, Value> {
    let mut out = Map::new();
    out.insert("token".into(), json!(r.token));
    let mut say = |outcome: &str| out.insert("outcome".into(), json!(outcome));
    match &r.outcome {
        schema::ActivationOutcome::Work(effects) => {
            say("work");
            let effects: Vec<Value> = effects.iter().map(record_effect).collect();
            out.insert("effects".into(), json!(effects));
        }
        schema::ActivationOutcome::Restored => {
            say("restored");
        }
        schema::ActivationOutcome::Snapshot { state } => {
            say("snapshot");
            out.insert("state".into(), raw(state));
        }
        schema::ActivationOutcome::StateJson { json } => {
            say("state_json");
            out.insert("json".into(), raw(json));
        }
        schema::ActivationOutcome::StateApplied => {
            say("state_applied");
        }
        schema::ActivationOutcome::MigrationState { json } => {
            say("migration_state");
            out.insert("json".into(), raw(json));
        }
        schema::ActivationOutcome::Prepared(answers) => {
            say("prepared");
            let answers: Vec<Value> = answers.iter().map(record_prepared).collect();
            out.insert("prepared".into(), json!(answers));
        }
        schema::ActivationOutcome::Failed(e) => {
            say("failed");
            out.insert("error".into(), json!(e.message));
            out.insert("non_retryable".into(), json!(e.non_retryable));
        }
    }
    out
}

fn record_effect(e: &schema::ItemEffect) -> Value {
    let (verdict, message) = match &e.verdict {
        schema::Verdict::Ok => ("ok", Value::Null),
        schema::Verdict::RetryableError { message } => ("retryable", json!(message)),
        schema::Verdict::NonRetryableError { message } => ("non_retryable", json!(message)),
        schema::Verdict::Unreadable { message } => ("unreadable", json!(message)),
    };
    json!({
        "verdict": verdict,
        "message": message,
        "changes": e.changes.iter().map(|c| raw(c)).collect::<Vec<_>>(),
        "outputs": e.outputs.iter().map(record_output).collect::<Vec<_>>(),
        "consumed_offset": e.consumed_offset,
        "event_time": e.event_time,
        "timers": e.timers.iter().map(|t| json!({
            "kind": t.kind.as_str(),
            "scheduled_at": t.scheduled_at,
            "register": t.register,
        })).collect::<Vec<_>>(),
        "checkpoint": e.checkpoint,
        "dlq": e.dlq.as_ref().map(|d| json!({
            "channel": d.channel,
            "error": d.error,
            "attempts": d.attempts,
            "payload": d.payload.as_deref().map(raw),
        })),
        "complete": e.complete.as_deref().map(raw),
    })
}

fn record_output(o: &schema::Output) -> Value {
    let mut recorded = json!({
        "key": o.key,
        "metadata": o.metadata.iter().map(|(k, v)| json!([k, v])).collect::<Vec<_>>(),
        "payload": raw(&o.payload),
        "event_time": o.event_time,
    });
    let map = recorded.as_object_mut().expect("an output is an object");
    match &o.target {
        schema::OutputTarget::Edge { channel, consumer } => {
            map.insert("target".into(), json!("edge"));
            map.insert("channel".into(), json!(channel));
            map.insert("consumer".into(), json!(consumer));
        }
        schema::OutputTarget::Plugin { channel } => {
            map.insert("target".into(), json!("plugin"));
            map.insert("plugin_channel".into(), json!(channel));
        }
    }
    recorded
}

fn record_prepared(a: &Result<schema::PrepareAnswer, schema::WorkError>) -> Value {
    match a {
        Ok(answer) => json!({ "keys": answer.keys, "event_time": answer.event_time }),
        Err(e) => json!({ "error": e.message, "non_retryable": e.non_retryable }),
    }
}
