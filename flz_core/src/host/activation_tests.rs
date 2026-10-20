//! An edge output is the one output kind with no durable copy, so the only
//! thing that can re-produce it after a crash is the producer re-running
//! its input. That makes the order of two statements a durability
//! guarantee: fold the effect record (offset included) after the pushes
//! land, never before. The flusher drains every ~5 ms, so "before" is a
//! real window, not a theoretical one.

use std::any::Any;
use std::sync::Arc;

use super::activation::settle_item;
use super::HostPayload;
use crate::cell::{CellItem, CellPayload, CellSlot};
use crate::poll_core::ItemAttempt;
use crate::schema;
use crate::test_support::{host_class, saturate, spin_until, stream_pair};

/// Fill one consumer's single mailbox slot and claim its round, so nothing
/// drains it until the test says so.
fn saturate_slot(slot: &Arc<CellSlot>) {
    saturate(
        &slot.mailbox,
        CellItem::Input(CellPayload {
            value: Arc::new(0usize) as Arc<dyn Any + Send + Sync>,
            stream: stream_pair().0,
            consumed_offset: None,
            call: None,
            replay: false,
            forwarded: false,
            hold: None,
        }),
    );
    assert!(slot.mailbox.claim_inflight());
}

/// One input at `offset`, and the completion that emits one edge value to
/// each consumer in `consumers`.
pub(super) fn work_and_effect(offset: u64, consumers: &[u32]) -> (ItemAttempt, schema::ItemEffect) {
    let work = ItemAttempt {
        item: CellItem::Input(CellPayload {
            value: Arc::new(HostPayload {
                bytes: b"input".to_vec(),
                channel: 0,
                keys: Vec::new(),
                event_time: None,
                metadata: Vec::new(),
                form: crate::schema::InputForm::Transformed,
                entry: None,
            }) as Arc<dyn Any + Send + Sync>,
            stream: stream_pair().0,
            consumed_offset: Some(offset),
            call: None,
            replay: false,
            forwarded: false,
            hold: None,
        }),
        now: None,
        attempt: 1,
        is_final: false,
    };
    let outputs = consumers
        .iter()
        .map(|&consumer| schema::Output {
            target: schema::OutputTarget::Edge {
                channel: 1,
                consumer,
            },
            payload: b"edge".to_vec(),
            key: None,
            metadata: Vec::new(),
            event_time: None,
        })
        .collect();
    let effect = schema::ItemEffect {
        verdict: schema::Verdict::Ok,
        changes: vec![b"change".to_vec()],
        outputs,
        consumed_offset: None,
        event_time: 0,
        timers: Vec::new(),
        checkpoint: false,
        dlq: None,
        complete: None,
    };
    (work, effect)
}

/// `settle_item` behind the stamps `apply_result` puts on the item's outputs
/// first, at change seq 0.
fn settle_stamped(slot: &Arc<CellSlot>, work: ItemAttempt, mut effect: schema::ItemEffect) {
    let effects = std::slice::from_mut(&mut effect);
    super::activation::stamp_items(slot, std::slice::from_ref(&work), effects, 0);
    settle_item(slot, &work, effect, None);
}

/// Records this Cell has actually SHIPPED. `drain_to_frames` fetch-adds
/// the per-Cell seq once per drained record, so the counter advancing is
/// exactly "a record left for the controller" — the one observable that
/// means the same thing whichever mechanism carried the backpressure. A
/// parked producer folds nothing; a deferring producer folds into a buffer
/// the flusher will not drain while the push is outstanding.
fn shipped(slot: &Arc<CellSlot>) -> u64 {
    crate::flusher::CellFlusher::instance().drain_now_for_tests();
    slot.change_seq.load(std::sync::atomic::Ordering::Acquire)
}

/// Pushes this consumer has refused for lack of room — its own counter, so a
/// sibling test's blocked push cannot end the wait early.
fn blocked_on(slot: &Arc<CellSlot>) -> u64 {
    slot.mailbox
        .counters()
        .pushes_blocked_total
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// One free slot on the consumer, announced exactly as a drain announces
/// it: pop, then serve the waiter the room is owed to.
fn free_one_slot(slot: &Arc<CellSlot>) {
    assert!(slot.mailbox.pop().is_some());
    slot.waiters.serve_one();
}

#[test]
fn no_record_carries_the_offset_until_the_edge_push_lands() {
    let id = format!("edge_order_{}", std::process::id());
    let instance = host_class::instance(&id, &["in"], &[], &[1]);
    let producer = Arc::clone(&instance.cells[0][0]);
    let consumer = Arc::clone(&instance.cells[1][0]);
    saturate_slot(&consumer);
    let before = shipped(&producer);
    let blocked_before = blocked_on(&consumer);

    let (work, effect) = work_and_effect(7, &[1]);
    let resolving = {
        let producer = Arc::clone(&producer);
        std::thread::spawn(move || settle_item(&producer, &work, effect, None))
    };

    // The push has nowhere to go — it is held back — so nothing this item
    // produced is durable, including the offset that would make a replay
    // skip its input.
    spin_until("the push to be held back", || {
        blocked_on(&consumer) > blocked_before || !producer.push_holdback.lock().unwrap().is_empty()
    });
    assert_eq!(
        shipped(&producer),
        before,
        "the offset must not be durable while its edge output is undelivered"
    );
    assert_eq!(
        consumer.mailbox.len(),
        1,
        "only the saturating item is queued"
    );

    // Room appears: the push lands and the record follows it.
    free_one_slot(&consumer);
    resolving.join().expect("resolution completes");
    crate::poll_core::global().rearm_deferred_pushes(&producer);
    assert_eq!(
        consumer.mailbox.len(),
        1,
        "the edge value reached the consumer's mailbox"
    );
    assert!(shipped(&producer) > before, "the record follows the push");
    let Some(CellItem::Input(delivered)) = consumer.mailbox.pop() else {
        panic!("the edge value is an input");
    };
    let delivered = delivered.value.downcast_ref::<HostPayload>();
    assert_eq!(
        delivered.map(|p| &p.form),
        Some(&schema::InputForm::Transformed),
        "a stream producer's output crosses transformed"
    );
}

#[test]
fn a_fan_out_holds_its_record_until_every_consumer_has_the_value() {
    let id = format!("edge_fanout_{}", std::process::id());
    let instance = host_class::instance(&id, &["in"], &[], &[1, 2, 3]);
    let producer = Arc::clone(&instance.cells[0][0]);
    let full = Arc::clone(&instance.cells[3][0]);
    saturate_slot(&full);
    let before = shipped(&producer);
    let blocked_before = blocked_on(&full);

    let (work, effect) = work_and_effect(11, &[1, 2, 3]);
    let resolving = {
        let producer = Arc::clone(&producer);
        std::thread::spawn(move || settle_item(&producer, &work, effect, None))
    };

    spin_until("the fan-out push to be held back", || {
        blocked_on(&full) > blocked_before || !producer.push_holdback.lock().unwrap().is_empty()
    });
    assert_eq!(
        instance.cells[1][0].mailbox.len(),
        1,
        "the consumers with room took theirs"
    );
    assert_eq!(instance.cells[2][0].mailbox.len(), 1);
    assert_eq!(
        shipped(&producer),
        before,
        "one undelivered consumer holds the whole record"
    );

    free_one_slot(&full);
    resolving.join().expect("resolution completes");
    crate::poll_core::global().rearm_deferred_pushes(&producer);
    assert_eq!(full.mailbox.len(), 1, "the last consumer got its value");
    assert!(shipped(&producer) > before);
}

/// Outside the tier the record that carries the offset keeps the edge value
/// beside it, so a restore can deliver it again to a consumer that lost it.
#[test]
fn outside_the_tier_the_record_carrying_the_offset_keeps_the_edge_value() {
    use std::sync::Mutex;

    let _guard = crate::state_sink::test_guard();
    crate::state_sink::reset_for_test();
    let records: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
    let seen = Arc::clone(&records);
    crate::state_sink::install_global_sink(Arc::new(move |payload_id: &str, frame: &[u8]| {
        if payload_id == crate::state_sink::CELL_EFFECT_RECORD_PAYLOAD_ID {
            seen.lock().unwrap().push(frame.to_vec());
        }
        Ok(())
    }));

    let id = format!("edge_off_tier_{}", std::process::id());
    let instance = host_class::instance_of(&id, &["in"], &[], &[1], false);
    let producer = Arc::clone(&instance.cells[0][0]);
    let (work, effect) = work_and_effect(7, &[1]);
    settle_item(&producer, &work, effect, None);
    crate::flusher::CellFlusher::instance().drain_now_for_tests();

    let body = records
        .lock()
        .unwrap()
        .iter()
        .find_map(|frame| {
            let id_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
            let at = 2 + id_len + 8 + 16 + 4;
            postcard::from_bytes::<crate::effect::EffectRecordBody>(&frame[at..frame.len() - 4])
                .ok()
                .filter(|body| body.consumed_offset == Some(7))
        })
        .expect("the producer's record for offset 7");
    assert_eq!(body.edges.len(), 1);
    assert_eq!((body.edges[0].consumer, body.edges[0].origin), (1, Some(7)));
    crate::state_sink::reset_for_test();
}

/// Off the tier a host item's plugin outputs leave at settle, after its edge
/// values reached their consumers: the plugin frame's write finds the edge
/// value already queued.
#[test]
fn a_host_items_plugin_outputs_leave_after_its_edges_land() {
    use std::sync::Mutex;

    let id = format!("ship_after_edges_{}", std::process::id());
    let instance = host_class::instance_of(&id, &["in"], &[], &[1], false);
    let producer = Arc::clone(&instance.cells[0][0]);
    let consumer = Arc::clone(&instance.cells[1][0]);
    assert!(
        consumer.mailbox.claim_inflight(),
        "nothing drains the consumer"
    );
    let queued_at_write: Arc<Mutex<Vec<usize>>> = Arc::default();
    let probe: flz_transport::transport::LocalFrameSink = {
        let (seen, consumer) = (Arc::clone(&queued_at_write), Arc::clone(&consumer));
        Arc::new(move |_frame: &[u8]| {
            seen.lock().unwrap().push(consumer.mailbox.len());
            Ok(())
        })
    };
    let (mut work, mut effect) = work_and_effect(7, &[1]);
    if let CellItem::Input(payload) = &mut work.item {
        payload.stream = Arc::new(flz_transport::transport::FlezhaStream::Local(probe));
    }
    effect.outputs.push(schema::Output {
        target: schema::OutputTarget::Plugin {
            channel: "out".to_string(),
        },
        payload: b"out".to_vec(),
        key: Some("k".to_string()),
        metadata: Vec::new(),
        event_time: None,
    });
    settle_item(&producer, &work, effect, None);
    assert_eq!(
        *queued_at_write.lock().unwrap(),
        vec![1],
        "the plugin frame leaves once the edge value is in the consumer's mailbox"
    );
}

// ---- a called input's answers ------------------------------------------------

/// `p0` feeds `p1`, whose channel `ans` ends in a keyed sink, and `p2`, whose
/// channel `out` ends in a keyed sink.
fn answering_instance(id: &str, tier: bool) -> Arc<crate::relay::instance::RelayInstance> {
    let run = if tier {
        r#"{"flow":{"exactly":"effectively_once"}}"#
    } else {
        r#""flow""#
    };
    let json = format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","durable":{{"local":{{"sleep_after":false}}}},
             "run":{run},
             "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}},
                         {{"name":"mid","kind":"inter_task","producer":0,
                           "chains":[[{{"link":"into","tasklet":1}}],[{{"link":"into","tasklet":2}}]]}},
                         {{"name":"ans","kind":"plugin","producer":1,"keyed":true,
                           "chains":[[{{"link":"sink","name":"ans"}}]]}},
                         {{"name":"out","kind":"plugin","producer":2,"keyed":true,
                           "chains":[[{{"link":"sink","name":"out"}}]]}}],
             "tasklets":[{{"name":"p0"}},{{"name":"p1"}},{{"name":"p2"}}]}}"#
    );
    crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    crate::relay::instance::RelayInstance::of_class(id, &format!("{id}_inst"))
        .expect("class registered")
}

/// One input carrying `call` on `stream`, and a completion emitting to both
/// sinks, `out`'s output with a forged `flz.reply_to`.
fn answering_work(
    stream: Arc<flz_transport::transport::FlezhaStream>,
    call: Option<&str>,
) -> (ItemAttempt, schema::ItemEffect) {
    let (mut work, mut effect) = work_and_effect(3, &[]);
    if let CellItem::Input(payload) = &mut work.item {
        payload.stream = stream;
        payload.call = call.map(Arc::from);
    }
    let plugin = |channel: &str, metadata: Vec<(String, String)>| schema::Output {
        target: schema::OutputTarget::Plugin {
            channel: channel.to_string(),
        },
        payload: channel.as_bytes().to_vec(),
        key: Some("k".to_string()),
        metadata,
        event_time: None,
    };
    effect.outputs = vec![
        plugin("ans", Vec::new()),
        plugin(
            "out",
            vec![("flz.reply_to".to_string(), "forged".to_string())],
        ),
    ];
    (work, effect)
}

fn reply_to_of(metadata: &[(String, String)]) -> Option<&str> {
    metadata
        .iter()
        .find(|(key, _)| key == "flz.reply_to")
        .map(|(_, value)| value.as_str())
}

/// Off the tier the outputs leave on the item's connection at settle: every
/// sink output of a called input carries its call, the forged one replaced,
/// and a plain input's or a timer's carries no `flz.reply_to` at all.
#[test]
fn off_the_tier_every_sink_output_of_a_called_input_carries_the_call() {
    use flz_transport::service::payloader;

    let id = format!("answer_off_tier_{}", std::process::id());
    let instance = answering_instance(&id, false);
    let producer = Arc::clone(&instance.cells[0][0]);
    let (stream, controller_end) = stream_pair();
    let controller_end = flz_transport::transport::FlezhaStream::unix(controller_end);
    let mut frames = Vec::new();
    for call in [Some("w:tok"), None] {
        let (work, effect) = answering_work(Arc::clone(&stream), call);
        settle_stamped(&producer, work, effect);
        for _ in 0..2 {
            let (_, payload, metadata) =
                payloader::extract_payload(&controller_end).expect("a plugin frame");
            frames.push((payload, metadata.get("flz.reply_to").cloned()));
        }
    }
    // A timer firing answers nobody.
    let (mut work, effect) = answering_work(Arc::clone(&stream), None);
    work.item = CellItem::Timer(crate::timer::TimerFire {
        kind: crate::timer::TimerKind::ProcessingTime,
        scheduled_at: 1,
        stream: Arc::clone(&stream),
    });
    settle_stamped(&producer, work, effect);
    for _ in 0..2 {
        let (_, payload, metadata) =
            payloader::extract_payload(&controller_end).expect("a plugin frame");
        frames.push((payload, metadata.get("flz.reply_to").cloned()));
    }
    assert_eq!(
        frames,
        vec![
            (b"ans".to_vec(), Some("w:tok".to_string())),
            (b"out".to_vec(), Some("w:tok".to_string())),
            (b"ans".to_vec(), None),
            (b"out".to_vec(), None),
            (b"ans".to_vec(), None),
            (b"out".to_vec(), None),
        ]
    );
}

/// The first effect record `settle` writes under the tier that `pick` takes.
pub(super) fn recorded(
    settle: impl FnOnce(),
    pick: impl Fn(&crate::effect::EffectRecordBody) -> bool,
) -> crate::effect::EffectRecordBody {
    use std::sync::Mutex;

    let _guard = crate::state_sink::test_guard();
    crate::state_sink::reset_for_test();
    let records: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
    let seen = Arc::clone(&records);
    crate::state_sink::install_global_sink(Arc::new(move |payload_id: &str, frame: &[u8]| {
        if payload_id == crate::state_sink::CELL_EFFECT_RECORD_PAYLOAD_ID {
            seen.lock().unwrap().push(frame.to_vec());
        }
        Ok(())
    }));
    settle();
    crate::flusher::CellFlusher::instance().drain_now_for_tests();

    let body = records
        .lock()
        .unwrap()
        .iter()
        .find_map(|frame| {
            let id_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
            let at = 2 + id_len + 8 + 16 + 4;
            postcard::from_bytes::<crate::effect::EffectRecordBody>(&frame[at..frame.len() - 4])
                .ok()
                .filter(&pick)
        })
        .expect("the record asked for");
    crate::state_sink::reset_for_test();
    body
}

/// The two outputs of the effect record `settle` writes under the tier.
fn recorded_outputs(settle: impl FnOnce()) -> Vec<crate::effect::OutputRecord> {
    recorded(settle, |body| body.outputs.len() == 2).outputs
}

/// Under the tier the outputs ride the effect record with the same stamps.
#[test]
fn under_the_tier_every_recorded_sink_output_carries_the_call() {
    let id = format!("answer_tier_{}", std::process::id());
    let instance = answering_instance(&id, true);
    let producer = Arc::clone(&instance.cells[0][0]);
    let (work, effect) = answering_work(stream_pair().0, Some("w:tok"));
    let outputs = recorded_outputs(|| settle_stamped(&producer, work, effect));
    let stamps: Vec<(&str, Option<&str>)> = outputs
        .iter()
        .map(|o| (o.channel.as_str(), reply_to_of(&o.metadata)))
        .collect();
    assert_eq!(stamps, vec![("ans", Some("w:tok")), ("out", Some("w:tok"))]);
}

/// Off the tier a Cell's failed input parks on its own frame, which carries the
/// item's call so the controller can answer the caller `call.failed`.
#[test]
fn a_tier_off_park_carries_the_call_only_when_its_item_had_one() {
    let _guard = crate::state_sink::test_guard();
    crate::state_sink::reset_for_test();
    let parks = crate::test_support::capture_parks();

    let id = format!("park_off_tier_{}", std::process::id());
    let instance = answering_instance(&id, false);
    let producer = Arc::clone(&instance.cells[0][0]);
    for call in [Some("w:tok"), None] {
        let (mut work, effect) = answering_work(stream_pair().0, call);
        // An input the controller did not journal: nothing carries its park
        // in a record, so the park leaves on its own frame.
        if let CellItem::Input(payload) = &mut work.item {
            payload.consumed_offset = None;
        }
        settle_item(&producer, &work, effect, Some(("poison".to_string(), 3)));
    }

    let calls: Vec<Option<String>> = parks
        .lock()
        .expect("park capture poisoned")
        .iter()
        .map(|park| park.body.reply_to.clone())
        .collect();
    assert_eq!(calls, vec![Some("w:tok".to_string()), None]);
    crate::state_sink::reset_for_test();
}

// ---- a call end's requests ---------------------------------------------------

/// `p0` feeds `p1`, whose channel `ask` ends in a factory call answered on
/// `ask.response`, and `p2`, whose channel `out` ends in a keyed sink.
fn calling_instance(id: &str, tier: bool) -> Arc<crate::relay::instance::RelayInstance> {
    let run = if tier {
        r#"{"flow":{"exactly":"effectively_once"}}"#
    } else {
        r#""flow""#
    };
    let json = format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","durable":{{"local":{{"sleep_after":false}}}},
             "run":{run},
             "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}},
                         {{"name":"mid","kind":"inter_task","producer":0,
                           "chains":[[{{"link":"into","tasklet":1}}],[{{"link":"into","tasklet":2}}]]}},
                         {{"name":"ask","kind":"plugin","producer":1,
                           "chains":[[{{"link":"key"}},{{"link":"call","factory":"agent","deadline_ms":60000,
                                        "create":false,"channel":"turn","answer_channel":"quotes",
                                        "response":"ask.response"}}]]}},
                         {{"name":"ask.response","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}},
                         {{"name":"out","kind":"plugin","producer":2,"keyed":true,
                           "chains":[[{{"link":"sink","name":"out"}}]]}}],
             "tasklets":[{{"name":"p0"}},{{"name":"p1"}},{{"name":"p2"}}]}}"#
    );
    crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    crate::relay::instance::RelayInstance::of_class(id, &format!("{id}_inst"))
        .expect("class registered")
}

/// A completion emitting one request to the call end under the key `anna-42`
/// and one output to the sink, both carrying forged call stamps.
fn calling_work(
    stream: Arc<flz_transport::transport::FlezhaStream>,
) -> (ItemAttempt, schema::ItemEffect) {
    let (mut work, mut effect) = work_and_effect(3, &[]);
    if let CellItem::Input(payload) = &mut work.item {
        payload.stream = stream;
    }
    let forged = vec![
        ("flz.call".to_string(), "id:elsewhere".to_string()),
        ("flz.deadline_at".to_string(), "1".to_string()),
        ("flz.call_answer_channel".to_string(), "forged".to_string()),
        ("flz.reply_to".to_string(), "forged".to_string()),
    ];
    let plugin = |channel: &str| schema::Output {
        target: schema::OutputTarget::Plugin {
            channel: channel.to_string(),
        },
        payload: channel.as_bytes().to_vec(),
        key: Some("anna-42".to_string()),
        metadata: forged.clone(),
        event_time: None,
    };
    effect.outputs = vec![plugin("ask"), plugin("out")];
    (work, effect)
}

/// The call stamps a request carries, its deadline checked against the window
/// the settle ran in and then left out.
fn call_stamps(metadata: &[(String, String)], window: (u64, u64)) -> Vec<(String, String)> {
    let mut stamps: Vec<(String, String)> = metadata
        .iter()
        .filter(|(key, _)| {
            key.starts_with("flz.call") || key == "flz.key" || key == "flz.deadline_at"
        })
        .cloned()
        .collect();
    stamps.sort();
    if let Some((_, at)) = stamps.iter().find(|(key, _)| key == "flz.deadline_at") {
        let at: u64 = at.parse().expect("a millisecond");
        assert!(
            (window.0 + 60_000..=window.1 + 60_000).contains(&at),
            "the deadline is the emit's wall clock plus 60 s: {at} for {window:?}"
        );
    }
    stamps.retain(|(key, _)| key != "flz.deadline_at");
    stamps
}

fn expected_call_stamps() -> Vec<(String, String)> {
    [
        ("flz.call", "factory:agent"),
        ("flz.call_answer_channel", "quotes"),
        ("flz.call_channel", "turn"),
        ("flz.call_create", "false"),
        ("flz.call_response", "ask.response"),
        ("flz.key", "anna-42"),
    ]
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .to_vec()
}

/// The `flz.cell` and `flz.seq` stamps of each output.
fn identity_of(outputs: &[schema::Output]) -> Vec<Vec<(String, String)>> {
    let stamp = |(key, _): &&(String, String)| key == "flz.cell" || key == "flz.seq";
    let of = |o: &schema::Output| o.metadata.iter().filter(stamp).cloned().collect();
    outputs.iter().map(of).collect()
}

/// Off the tier a keyed output carries its Cell and its place after the
/// activation's change seq, a non-keyed one taking its ordinal unstamped;
/// a class that records effects leaves the stamps to the controller.
#[test]
fn a_keyed_output_carries_its_identity_off_the_tier_only() {
    let id = format!("identity_{}", std::process::id());
    let off = calling_instance(&id, false);
    let slot = &off.cells[0][0];
    let (_, effect) = calling_work(stream_pair().0);
    let (ask, out) = (effect.outputs[0].clone(), effect.outputs[1].clone());
    let mut outputs = vec![out.clone(), ask, out];
    crate::host::stamp::stamp_plugin_ends(slot, None, &mut outputs, || 5, || 0);
    let cell = ("flz.cell".to_string(), format!("{id}_inst/p0/"));
    let seq = |at: &str| ("flz.seq".to_string(), at.to_string());
    assert_eq!(cell.1, slot.dispatcher.cell_label());
    assert_eq!(
        identity_of(&outputs),
        vec![
            vec![cell.clone(), seq("5.0")],
            vec![],
            vec![cell, seq("5.2")]
        ]
    );
    let key = ("flz.key".to_string(), "anna-42".to_string());
    assert!(outputs[0].metadata.contains(&key));

    let on = calling_instance(&format!("identity_tier_{}", std::process::id()), true);
    let (_, mut effect) = calling_work(stream_pair().0);
    crate::host::stamp::stamp_plugin_ends(&on.cells[0][0], None, &mut effect.outputs, || 0, || 0);
    assert_eq!(identity_of(&effect.outputs), vec![vec![], vec![]]);
}

/// Only an output on a call end reads the wall clock; a sink's output still
/// loses the call stamps a host forged on it.
#[test]
fn only_a_call_end_reads_the_clock() {
    let instance = calling_instance(&format!("call_clock_{}", std::process::id()), false);
    let slot = &instance.cells[0][0];
    let reads = std::cell::Cell::new(0);
    let clock = || {
        reads.set(reads.get() + 1);
        0
    };
    let (_, effect) = calling_work(stream_pair().0);
    let (mut sink, mut both): (Vec<_>, Vec<_>) = (Vec::new(), effect.outputs.clone());
    sink.extend(effect.outputs.into_iter().skip(1));
    crate::host::stamp::stamp_plugin_ends(slot, None, &mut sink, || 0, clock);
    assert_eq!(reads.get(), 0, "a sink-only batch reads no clock");
    let identity_key = vec![("flz.key".to_string(), "anna-42".to_string())];
    assert_eq!(call_stamps(&sink[0].metadata, (0, 0)), identity_key);
    assert_eq!(
        reply_to_of(&sink[0].metadata),
        None,
        "the forged stamps are gone"
    );
    crate::host::stamp::stamp_plugin_ends(slot, None, &mut both, || 0, clock);
    assert_eq!(reads.get(), 1, "one read for the batch's call end");
}

/// A called input's request carries no `flz.reply_to`, the forged one gone;
/// its sink output carries the call.
#[test]
fn a_called_inputs_request_carries_no_token() {
    let instance = calling_instance(&format!("call_token_{}", std::process::id()), false);
    let (_, mut effect) = calling_work(stream_pair().0);
    let token: Arc<str> = Arc::from("w:tok");
    let slot = &instance.cells[0][0];
    crate::host::stamp::stamp_plugin_ends(slot, Some(&token), &mut effect.outputs, || 0, || 0);
    let calls: Vec<_> = effect
        .outputs
        .iter()
        .map(|o| reply_to_of(&o.metadata))
        .collect();
    assert_eq!(calls, vec![None, Some("w:tok")]);
}

/// Off the tier a request leaves on the item's connection with every stamp,
/// its key included; the sink's output carries none of them.
#[test]
fn off_the_tier_a_request_carries_the_call_stamps_and_its_key() {
    use flz_transport::service::payloader;

    let id = format!("call_off_tier_{}", std::process::id());
    let instance = calling_instance(&id, false);
    let producer = Arc::clone(&instance.cells[0][0]);
    let (stream, controller_end) = stream_pair();
    let controller_end = flz_transport::transport::FlezhaStream::unix(controller_end);
    let (work, effect) = calling_work(Arc::clone(&stream));
    let before = crate::util::wall_now_ms();
    settle_stamped(&producer, work, effect);
    let window = (before, crate::util::wall_now_ms());
    let mut frames = Vec::new();
    for _ in 0..2 {
        let (_, payload, metadata) =
            payloader::extract_payload(&controller_end).expect("a plugin frame");
        let metadata: Vec<(String, String)> = metadata.into_iter().collect();
        frames.push((payload, call_stamps(&metadata, window)));
    }
    assert_eq!(
        frames,
        vec![
            (b"ask".to_vec(), expected_call_stamps()),
            (
                b"out".to_vec(),
                vec![("flz.key".to_string(), "anna-42".to_string())]
            ),
        ]
    );
}

/// Under the tier the request rides the effect record with the same stamps,
/// so a re-drive after a crash carries its deadline and target.
#[test]
fn under_the_tier_the_recorded_request_carries_the_call_stamps() {
    let id = format!("call_tier_{}", std::process::id());
    let instance = calling_instance(&id, true);
    let producer = Arc::clone(&instance.cells[0][0]);
    let (work, effect) = calling_work(stream_pair().0);
    let before = crate::util::wall_now_ms();
    let outputs = recorded_outputs(|| settle_stamped(&producer, work, effect));
    let window = (before, crate::util::wall_now_ms());
    let stamps: Vec<(&str, Vec<(String, String)>)> = outputs
        .iter()
        .map(|o| (o.channel.as_str(), call_stamps(&o.metadata, window)))
        .collect();
    assert_eq!(
        stamps,
        vec![("ask", expected_call_stamps()), ("out", Vec::new())]
    );
    assert_eq!(outputs[0].key, "anna-42");
}

/// The heartbeat's `handouts` is what tells a runner that took a frame up
/// from a wedged one, so it moves when the work is handed out, and again when
/// a retry is re-handed, never waiting for the item to settle.
#[test]
fn the_handout_mark_moves_on_a_handout_and_on_a_retry() {
    let id = format!("handout_mark_{}", std::process::id());
    let instance = host_class::instance_of(&id, &["in"], &[], &[1], false);
    let slot = Arc::clone(&instance.cells[0][0]);
    let core = crate::poll_core::PollCore::new();
    let (work, mut failed) = work_and_effect(1, &[]);
    failed.verdict = schema::Verdict::RetryableError {
        message: "not yet".to_string(),
    };
    assert!(slot.mailbox.try_push(work.item).is_ok());
    assert!(slot.mailbox.claim_inflight());

    let handed = core.try_assemble(&slot).expect("the work is handed out");
    assert_eq!(
        core.handouts_issued(),
        1,
        "the handout itself moves the mark"
    );
    let token = handed.token;
    let outcome = schema::ActivationOutcome::Work(vec![failed]);
    crate::host::apply_result(&core, handed, schema::ActivationResult { token, outcome })
        .expect("answered");
    assert_eq!(
        core.outstanding_count(),
        0,
        "a retry waiting out its backoff holds no handout"
    );

    let crate::poll_core::Polled::Activation(retried) =
        core.poll(std::time::Duration::from_secs(5))
    else {
        panic!("the retry was never re-handed");
    };
    assert_eq!(retried.items()[0].attempt, 2, "the re-handout is the retry");
    assert_eq!(
        core.handouts_issued(),
        2,
        "the retry's re-handout moves the mark"
    );
}
