//! The routing seam's tests: a plugin output whose write failed and is held
//! for the reconnect, the entry-channel resolution — a declared name
//! delivered, an absent one defaulted, and a name the relay does not
//! declare parked under the name it asked for — and a host payload fanned
//! across several dynamic keys.

use std::io::Read;
use std::os::unix::net::UnixStream;
use std::time::Duration;

use super::*;
use crate::flusher::SendFrameFn;
use crate::relay::instance::RelayInstance;
use crate::state_sink;
use crate::test_support::{capture_parks, host_class};

const RELAY: &str = "op-plugin-emit";
const BODY: &[u8] = b"the relay's only output";

/// A stream whose peer is gone: the shape the controller leaves behind
/// when it closes the socket half of a rebound pair.
fn dead_stream() -> Arc<FlezhaStream> {
    let (near, far) = UnixStream::pair().expect("a socket pair");
    drop(far);
    Arc::new(FlezhaStream::unix(near))
}

fn sink_metadata() -> HashMap<String, String> {
    HashMap::from([("channel".to_string(), "out".to_string())])
}

/// The dispatch handle a stream reader holds over one registered instance.
fn handle_over(instance: &Arc<RelayInstance>) -> RelayDispatchHandle {
    let registry = Arc::new(crate::relay::registry::RelayRegistry::new());
    registry.register(Arc::clone(instance));
    RelayDispatchHandle { registry }
}

/// One frame off the wire into `instance`, naming `channel` when it is
/// `Some` — a `None` is the metadata a input plugin sends by default.
fn feed(instance: &Arc<RelayInstance>, channel: Option<&str>, payload: &[u8]) {
    feed_with(
        instance,
        &channel.map(channel_metadata).unwrap_or_default(),
        payload,
    );
}

/// A topology with no entry channel. No declaration this core accepts builds
/// one, so the test builds the topology itself — the shape `dispatch` would
/// be handed.
fn entry_less_instance(id: &'static str) -> Arc<RelayInstance> {
    let topology = crate::relay::topology::RelayTopology {
        sdk_lang: 1,
        channels: Vec::new(),
        name_to_id: HashMap::new(),
        tasklets: Vec::new(),
        tasklet_name_to_id: HashMap::new(),
        factory_id: id,
        description: "",
        durable: crate::relay::settings::Durable::Memory,
        max_duration: None,
        effectively_once: false,
        correction: None,
        ack_granularity: Default::default(),
        steps: None,
        api: Vec::new(),
    };
    RelayInstance::new(Arc::new(topology), Arc::from(id))
}

/// One frame carrying `metadata`, through the dispatch a stream reader makes;
/// no `channel` key is the metadata a spawn payload carries.
fn feed_with(instance: &Arc<RelayInstance>, metadata: &HashMap<String, String>, payload: &[u8]) {
    handle_over(instance).dispatch(
        &instance.instance_id,
        payload.to_vec(),
        metadata,
        dead_stream(),
    );
}

fn channel_metadata(channel: &str) -> HashMap<String, String> {
    HashMap::from([("channel".to_string(), channel.to_string())])
}

/// A frame naming `channel` at input-log offset `offset`.
fn journaled_metadata(channel: &str, offset: u64) -> HashMap<String, String> {
    HashMap::from([
        ("channel".to_string(), channel.to_string()),
        ("flz.offset".to_string(), offset.to_string()),
    ])
}

/// The messages a run not yet opened holds for its first pass: each one's
/// entry channel, input-log offset and the items addressed to its roots.
/// A queued message: its entry channel, input-log offset and root values.
type Queued = (u32, Option<u64>, Vec<Arc<dyn Any + Send + Sync>>);

fn queued_messages(instance: &RelayInstance) -> Vec<Queued> {
    let run = instance.steps.as_ref().expect("a steps instance");
    run.sheet()
        .queue
        .iter()
        .filter_map(|trigger| match trigger {
            crate::steps::instance::Trigger::Message(message) => Some(message),
            crate::steps::instance::Trigger::Firing(_) => None,
        })
        .map(|message| {
            let values = message
                .roots
                .iter()
                .filter_map(|root| match &root.item {
                    crate::cell::CellItem::Input(payload) => Some(Arc::clone(&payload.value)),
                    _ => None,
                })
                .collect();
            (message.channel, message.offset, values)
        })
        .collect()
}

/// The offset a park released, once the off-thread park has landed.
fn parked_offset(instance: &RelayInstance) -> Option<u64> {
    let run = instance.steps.as_ref().expect("a steps instance");
    let mut offset = None;
    crate::test_support::wait_until(Duration::from_secs(10), Duration::from_millis(5), || {
        offset = run.sheet().parked_offset;
        offset.is_some()
    });
    offset
}

/// A steps relay whose one entry channel parses: `poison` is refused, anything
/// else becomes its text, on a ladder of two. No host declaration builds a
/// parsed entry, so the topology is hand-built as a typed SDK builds one.
fn parsing_steps(id: &'static str) -> Arc<RelayInstance> {
    use crate::channel::{ChannelDescriptor, ConsumerEdge, OverflowPolicy, RetryPolicy};
    let wrap: crate::channel::EntryWrapFn = Arc::new(|bytes: Vec<u8>| {
        if bytes == b"poison" {
            return Err("poison".to_string());
        }
        Ok(Arc::new(String::from_utf8_lossy(&bytes).to_string()) as Arc<dyn Any + Send + Sync>)
    });
    let topology = crate::relay::topology::RelayTopology {
        sdk_lang: 1,
        channels: vec![ChannelDescriptor {
            name: Arc::from("in"),
            kind: ChannelKind::Entry,
            producer: None,
            consumers: vec![ConsumerEdge {
                tasklet: 0,
                transform: None,
                key: None,
            }],
            wrap_input: Some(wrap),
            entry_parsed: true,
            plugin_sink: None,
            sink: false,
            call: None,
            keyed: false,
            overflow_policy: OverflowPolicy::DropNothing,
            retry: RetryPolicy {
                max_attempts: 2,
                backoff_base_ms: 1,
            },
            prepare_wants: (false, false),
            codec: None,
        }],
        name_to_id: HashMap::from([("in", 0)]),
        tasklets: Vec::new(),
        tasklet_name_to_id: HashMap::new(),
        factory_id: id,
        description: "",
        durable: crate::relay::settings::Durable::Memory,
        max_duration: None,
        effectively_once: false,
        correction: None,
        ack_granularity: Default::default(),
        steps: Some(crate::steps::spec::StepsSpec {
            dag: crate::steps::spec::TaskDag::new(
                vec![0],
                vec![vec![]],
                vec![vec![]],
                [(0, vec![0])],
            ),
            on_failure: crate::steps::OnFailure::UndoAll,
            each: crate::steps::Each::Once,
        }),
        api: Vec::new(),
    };
    RelayInstance::new(Arc::new(topology), Arc::from(id))
}

#[test]
fn a_steps_frame_naming_an_undeclared_channel_parks_under_that_name() {
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    let parks = capture_parks();
    let instance = host_class::steps("steps_undeclared", &["orders"]);

    feed_with(
        &instance,
        &journaled_metadata("refunds", 4),
        b"payload-bytes",
    );

    assert_eq!(
        parked_offset(&instance),
        Some(4),
        "the run counts the parked message consumed"
    );
    let park = parks.lock().expect("park capture poisoned").remove(0);
    assert_eq!(
        park.body.origin_offset,
        Some(4),
        "parked with its durable-inbox offset"
    );
    assert_eq!(park.body.entry.channel, "refunds");
    assert!(
        park.body.entry.error.contains("orders"),
        "the error names the declared set: {}",
        park.body.entry.error
    );
    assert_eq!(
        park.body.entry.payload.as_deref(),
        Some(&b"payload-bytes"[..])
    );
    assert_eq!(park.identity.1, "_unresolved");
    assert!(queued_messages(&instance).is_empty(), "no pass was queued");
    state_sink::reset_for_test();
}

#[test]
fn a_steps_frame_naming_no_channel_enters_the_first_entry_channel() {
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    let _parks = capture_parks();
    let instance = host_class::steps("steps_default", &["first", "second"]);

    feed_with(&instance, &HashMap::new(), b"payload-bytes");

    let [(channel, offset, values)] = <[_; 1]>::try_from(queued_messages(&instance))
        .unwrap_or_else(|_| panic!("one message queued"));
    assert_eq!(channel, 0, "the first declared entry channel took it");
    assert_eq!(offset, None);
    let host = values
        .first()
        .and_then(|value| value.downcast_ref::<crate::host::HostPayload>())
        .expect("the entry wrapper's value, addressed to the root");
    assert_eq!(host.bytes, b"payload-bytes");
    assert_eq!(host.channel, 0);
    state_sink::reset_for_test();
}

#[test]
fn a_steps_frame_on_the_second_entry_channel_carries_that_channel_and_its_facts() {
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    let _parks = capture_parks();
    let instance = host_class::steps("steps_second", &["first", "second"]);
    let metadata = HashMap::from([
        ("channel".to_string(), "second".to_string()),
        ("flz.offset".to_string(), "7".to_string()),
    ]);

    feed_with(&instance, &metadata, b"payload-bytes");

    let [(channel, offset, _)] = <[_; 1]>::try_from(queued_messages(&instance))
        .unwrap_or_else(|_| panic!("one message queued"));
    assert_eq!(channel, 1, "the named channel took it, not the first");
    assert_eq!(offset, Some(7), "the input-log offset rides the message");
    state_sink::reset_for_test();
}

#[test]
fn a_steps_frame_a_parsed_entry_rejects_rides_the_ladder_and_parks_under_the_channel() {
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    let parks = capture_parks();
    let instance = parsing_steps("steps_parsed");

    feed_with(&instance, &journaled_metadata("in", 5), b"poison");

    assert_eq!(
        parked_offset(&instance),
        Some(5),
        "the run counts the parked message consumed"
    );
    let park = parks.lock().expect("park capture poisoned").remove(0);
    assert_eq!(park.body.origin_offset, Some(5));
    assert_eq!(
        park.body.entry.channel, "in",
        "parked under the entry channel"
    );
    assert_eq!(
        park.body.entry.attempts, 2,
        "the ladder climbed its two rungs"
    );
    assert_eq!(park.body.entry.payload.as_deref(), Some(&b"poison"[..]));
    assert_eq!(park.identity.1, "in");
    assert!(queued_messages(&instance).is_empty(), "no pass was queued");

    // The next message enters its chain.
    feed_with(&instance, &channel_metadata("in"), b"fine");
    assert_eq!(queued_messages(&instance).len(), 1);
    state_sink::reset_for_test();
}

fn queued(instance: &Arc<RelayInstance>) -> usize {
    instance.cells[0][0].mailbox.len()
}

/// The entry channel the one queued frame resolved to: the host wrapper
/// records the channel it was parsed for, so the mailbox itself says which
/// entry channel took the frame.
fn queued_channel(instance: &Arc<RelayInstance>) -> Option<u32> {
    match instance.cells[0][0].mailbox.pop()? {
        crate::cell::CellItem::Input(payload) => payload
            .value
            .downcast_ref::<crate::host::HostPayload>()
            .map(|host| host.channel),
        _ => None,
    }
}

#[test]
fn a_frame_naming_an_undeclared_entry_channel_is_parked_with_the_name_it_asked_for() {
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    let parks = capture_parks();
    let instance = host_class::instance("q2_undeclared", &["q2_prices"], &[], &[1]);

    feed(&instance, Some("not_q2_prices"), b"payload-bytes");

    crate::test_support::spin_until("the unresolved name to be parked", || {
        !parks.lock().expect("park capture poisoned").is_empty()
    });
    let park = parks.lock().expect("park capture poisoned").remove(0);
    let body = park.body;
    assert_eq!(
        body.entry.channel, "not_q2_prices",
        "the row carries the name the frame asked for"
    );
    assert!(
        body.entry.error.contains("q2_prices"),
        "the error names the declared set, got: {}",
        body.entry.error
    );
    assert_eq!(
        body.entry.payload.as_deref(),
        Some(&b"payload-bytes"[..]),
        "the payload bytes are kept"
    );
    assert_eq!(body.entry.attempts, 1, "the park is immediate");
    assert_eq!(
        park.identity.1, "_unresolved",
        "the identity slot the controller makes a directory is this side's own"
    );
    assert_eq!(queued(&instance), 0, "no tasklet received the frame");
    state_sink::reset_for_test();
}

#[test]
fn a_frame_with_no_channel_key_still_reaches_the_first_entry_channel() {
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    // Installed so that a park this test must NOT provoke is acked rather
    // than re-sending for the rest of the run if the code regresses.
    let _parks = capture_parks();
    let instance = host_class::instance("q2_default", &["q2_first", "q2_second"], &[], &[1]);

    feed(&instance, None, b"payload-bytes");

    // Delivery and the park are the two arms of one match, so proving the
    // frame was delivered is what proves it was not parked; reading the
    // capture here would race the park thread and pass either way.
    assert_eq!(queued(&instance), 1, "the default entry channel took it");
    assert_eq!(
        queued_channel(&instance),
        Some(0),
        "the first entry channel was the one delivered to, not the second"
    );
    state_sink::reset_for_test();
}

#[test]
fn a_frame_naming_a_declared_entry_channel_is_delivered_to_it() {
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    let _parks = capture_parks();
    let instance = host_class::instance("q2_named", &["q2_head", "q2_tail"], &[], &[1]);

    feed(&instance, Some("q2_tail"), b"payload-bytes");

    assert_eq!(queued(&instance), 1, "the named channel's consumer took it");
    assert_eq!(
        queued_channel(&instance),
        Some(1),
        "the named channel took it, not the first"
    );
    state_sink::reset_for_test();
}

/// A class with no entry channel cannot be DECLARED — `register` refuses a
/// class with no tasklet, and refuses a tasklet no entry channel reaches
/// (`UnreachableTasklet`) — but a topology can hold that shape, and
/// `dispatch` takes topologies. So the branch is driven, not argued.
#[test]
fn a_relay_declaring_no_entry_channel_parks_with_its_own_reason() {
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    let parks = capture_parks();
    let instance = entry_less_instance("q2_no_entry");

    feed(&instance, None, b"nowhere-to-go");

    crate::test_support::spin_until("the entry-less relay to park", || {
        !parks.lock().expect("park capture poisoned").is_empty()
    });
    let park = parks.lock().expect("park capture poisoned").remove(0);
    let body = park.body;
    assert_eq!(
        body.entry.error, NO_ENTRY_CHANNEL_ERROR,
        "the reason names the condition, not an unresolved name against an empty set"
    );
    assert_eq!(
        body.entry.channel, "",
        "there is no requested name to attribute to"
    );
    state_sink::reset_for_test();
}

/// A frame naming a channel the relay DOES declare, of another kind: a
/// sink is not an entry channel, so it parks exactly as an undeclared name
/// does. Before the kind-aware lookup this frame resolved, found no
/// `wrap_input`, and was dropped with a line on stderr.
#[test]
fn a_frame_naming_a_declared_channel_of_another_kind_is_parked_too() {
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    let parks = capture_parks();
    let instance = host_class::instance("q2_wrong_kind", &["q2_in"], &[], &[1]);

    // `out` is the class's plugin-bound sink; `mid` its inter-tasklet channel.
    feed(&instance, Some("out"), b"payload-bytes");

    crate::test_support::spin_until("the sink name to be parked", || {
        !parks.lock().expect("park capture poisoned").is_empty()
    });
    let park = parks.lock().expect("park capture poisoned").remove(0);
    let body = park.body;
    assert_eq!(body.entry.channel, "out");
    assert_eq!(
        body.entry.error,
        "no entry channel `out`; the relay declares: q2_in"
    );
    assert_eq!(body.entry.payload.as_deref(), Some(&b"payload-bytes"[..]));
    assert_eq!(queued(&instance), 0, "no tasklet received the frame");
    state_sink::reset_for_test();
}

#[test]
fn the_reason_for_a_relay_with_no_entry_channel_names_that_condition() {
    assert_eq!(
        unresolved_entry_error(&[], ""),
        NO_ENTRY_CHANNEL_ERROR,
        "an empty declared set is named as such, not printed as an empty list"
    );
    assert_eq!(
        unresolved_entry_error(&["config", "prices"], "quotes"),
        "no entry channel `quotes`; the relay declares: config, prices"
    );
}

#[test]
fn the_declared_set_is_the_entry_channels_in_one_order() {
    let instance = host_class::instance("q2_declared_set", &["q2_z", "q2_a"], &[], &[1]);
    assert_eq!(
        instance.topology.declared_names(ChannelKind::Entry),
        vec!["q2_a", "q2_z"],
        "only entry channels, sorted — `mid` and `out` are other kinds"
    );
}

/// The requested name is attacker-influenced and the controller turns the
/// identity's tasklet slot into a directory under the dead-letter root. A
/// name that would climb out of it must reach the row's `channel` field and
/// nothing else.
#[test]
fn a_requested_name_that_would_climb_out_of_a_directory_never_reaches_the_identity() {
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    let parks = capture_parks();
    let instance = host_class::instance("q2_traversal", &["q2_only"], &[], &[1]);

    feed(&instance, Some("../../../../etc"), b"payload-bytes");

    crate::test_support::spin_until("the climbing name to be parked", || {
        !parks.lock().expect("park capture poisoned").is_empty()
    });
    let park = parks.lock().expect("park capture poisoned").remove(0);
    assert_eq!(
        park.identity.1, "_unresolved",
        "no frame-controlled name in the slot that becomes a path component"
    );
    assert_eq!(
        park.body.entry.channel, "../../../../etc",
        "the row still records what the frame asked for"
    );
    state_sink::reset_for_test();
}

#[test]
fn a_requested_name_at_the_metadata_cap_parks_rather_than_tripping_the_identity_assertion() {
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    let parks = capture_parks();
    let instance = host_class::instance("q2_long_name", &["q2_short"], &[], &[1]);
    // The longest name a frame can carry: a metadata value is capped at
    // `MAX_METADATA_FIELD_LEN`, which is also what the identity encoder
    // asserts, so the two limits meet exactly here.
    let longest = "n".repeat(flz_transport::service::payloader::MAX_METADATA_FIELD_LEN);

    feed(&instance, Some(&longest), b"payload-bytes");

    crate::test_support::spin_until("the longest possible name to be parked", || {
        !parks.lock().expect("park capture poisoned").is_empty()
    });
    let park = parks.lock().expect("park capture poisoned").remove(0);
    let body = park.body;
    assert_eq!(
        body.entry.channel, longest,
        "the row carries the whole name"
    );
    state_sink::reset_for_test();
}

#[test]
fn a_plugin_frame_whose_write_fails_is_held_for_replay() {
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();

    send_plugin_frame(&dead_stream(), RELAY, BODY, &sink_metadata()).unwrap();

    assert_eq!(
        state_sink::ring_len_for_test(),
        1,
        "a plugin output whose write failed must be held, not dropped"
    );
    state_sink::reset_for_test();
}

#[test]
fn a_held_plugin_frame_reaches_the_stream_the_reconnect_wires() {
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();

    let metadata = sink_metadata();
    let expected = payloader::build_from_binary(RELAY, BODY, metadata.clone()).unwrap();
    send_plugin_frame(&dead_stream(), RELAY, BODY, &metadata).unwrap();

    // The reconnect: a fresh secondary, then the sink install that replays
    // the ring onto it.
    let (near, mut far) = UnixStream::pair().expect("a socket pair");
    far.set_read_timeout(Some(Duration::from_secs(2)))
        .expect("a read deadline, so a lost frame fails instead of hanging");
    state_sink::set_secondary_stream(Some(Arc::new(FlezhaStream::unix(near))));
    let sink: SendFrameFn = Arc::new(|_payload_id: &str, _frame: &[u8]| Ok(()));
    state_sink::install_global_sink(sink);

    let mut seen = vec![0u8; expected.len()];
    far.read_exact(&mut seen)
        .expect("the held frame must reach the fresh secondary");
    assert_eq!(
        seen, expected,
        "the frame is replayed exactly as it was framed"
    );
    assert_eq!(state_sink::ring_len_for_test(), 0, "the ring is drained");

    state_sink::set_secondary_stream(None);
    state_sink::reset_for_test();
}

/// A host class whose one entry is keyed into a dynamic tasklet, the shape a
/// `key_by` behind a fanning `flat_map` declares.
fn keyed_dynamic_instance(id: &str) -> Arc<RelayInstance> {
    let json = format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0",
             "channels":[{{"name":"in","kind":"entry",
                           "chains":[[{{"link":"key"}},{{"link":"into","tasklet":0}}]]}}],
             "tasklets":[{{"name":"p0"}}]}}"#
    );
    crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    RelayInstance::of_class(id, &format!("{id}_inst")).expect("class registered")
}

#[test]
fn a_host_payload_fanned_across_keys_reaches_every_keyed_cell() {
    let instance = keyed_dynamic_instance("fanned_host_keys");
    let channel = instance.topology.channel(0).expect("the entry channel");
    let value: Arc<dyn Any + Send + Sync> = Arc::new(crate::host::HostPayload {
        bytes: b"a:1,b:2".to_vec(),
        channel: 0,
        keys: vec!["A".to_string(), "B".to_string()],
        event_time: None,
        metadata: Vec::new(),
    });

    let stream = dead_stream();
    let d = Delivery {
        stream: &stream,
        consumed_offset: None,
        call: None,
        replay: false,
    };
    route_to_consumers(&instance, channel, &value, &d, None);

    let registry = instance.dynamic_registries[0]
        .as_ref()
        .expect("a dynamic consumer has a registry");
    for key in ["A", "B"] {
        let slot = registry
            .cells
            .get(key)
            .unwrap_or_else(|| panic!("the fan answered key {key}, and no Cell was minted for it"));
        assert_eq!(slot.mailbox.len(), 1, "key {key} receives the payload once");
    }
}

/// The inter-tasklet channel of a tier class, `p0` into a one-slot `p1`, with
/// both Cells.
fn tier_edge(id: &str) -> (Arc<RelayInstance>, Arc<CellSlot>, Arc<CellSlot>) {
    let instance = crate::test_support::host_class::instance(id, &["in"], &[], &[1]);
    let producer = Arc::clone(&instance.cells[0][0]);
    let consumer = Arc::clone(&instance.cells[1][0]);
    (instance, producer, consumer)
}

fn host_value(bytes: &[u8]) -> Arc<dyn Any + Send + Sync> {
    Arc::new(crate::host::HostPayload {
        bytes: bytes.to_vec(),
        channel: 1,
        keys: Vec::new(),
        event_time: None,
        metadata: Vec::new(),
    })
}

#[test]
fn a_held_destination_collects_the_addressed_item_instead_of_admitting_it() {
    let (instance, _producer, consumer) = tier_edge("held_destination");
    let channel = instance
        .topology
        .channel(1)
        .expect("the inter-tasklet channel");
    let mut held = Vec::new();
    let stream = dead_stream();
    let d = Delivery {
        stream: &stream,
        consumed_offset: None,
        call: None,
        replay: false,
    };

    let deferred = deliver_edge(
        &instance,
        channel,
        &channel.consumers[0],
        &host_value(b"staged"),
        &d,
        &mut Destination::Held(&mut held),
    )
    .expect("the edge delivers");

    assert!(!deferred);
    assert_eq!(held.len(), 1, "the item is addressed and held");
    assert!(Arc::ptr_eq(&held[0].0, &consumer), "to the consumer's Cell");
    assert!(consumer.mailbox.is_empty(), "nothing reached the mailbox");
}

#[test]
fn a_non_parking_admission_into_a_full_mailbox_defers_whatever_the_switch_says() {
    crate::admission::set_nonparking_for_tests(false);
    let (_instance, producer, consumer) = tier_edge("non_parking_admission");
    let stream = dead_stream();
    let item =
        |bytes: &[u8]| CellItem::Input(make_cell_payload(&host_value(bytes), &stream, None, false));
    crate::test_support::saturate(&consumer.mailbox, item(b"first"));

    let deferred = crate::admission::admit_from_cell(
        &producer,
        &consumer,
        item(b"second"),
        crate::channel::OverflowPolicy::DropNothing,
        true,
    );

    assert!(deferred, "the caller's non-parking form returns at once");
    assert_eq!(producer.push_holdback.lock().unwrap().len(), 1);
    assert_eq!(consumer.mailbox.len(), 1, "the full mailbox took nothing");
}

/// The call an item in `slot`'s mailbox carries.
fn queued_call(slot: &CellSlot) -> Option<String> {
    match slot.mailbox.pop().expect("an item is queued") {
        CellItem::Input(payload) => payload.call.map(|token| token.to_string()),
        CellItem::Timer(_) | CellItem::CloseInterval => panic!("an input was queued"),
    }
}

/// The token rides where the offset rides: onto the entry item, and from a
/// Cell's inter-tasklet emit onto the downstream item. An input without the
/// key, or with an empty one, carries none.
#[test]
fn an_inputs_call_rides_its_entry_item_and_every_value_derived_from_it() {
    let (instance, producer, consumer) = tier_edge("call_rides_the_edge");
    let mut called = journaled_metadata("in", 1);
    called.insert(REPLY_TO_KEY.to_string(), "w:t".to_string());
    feed_with(&instance, &called, b"called");
    assert_eq!(queued_call(&producer).as_deref(), Some("w:t"));

    feed_with(&instance, &journaled_metadata("in", 2), b"sent");
    assert_eq!(queued_call(&producer), None);
    let mut empty = journaled_metadata("in", 3);
    empty.insert(REPLY_TO_KEY.to_string(), String::new());
    feed_with(&instance, &empty, b"empty");
    assert_eq!(queued_call(&producer), None, "an empty token names no call");

    let mid = instance.topology.lookup_channel_id("mid").expect("`mid`");
    let token: Arc<str> = Arc::from("w:t");
    let stream = dead_stream();
    let d = Delivery {
        stream: &stream,
        consumed_offset: Some(1),
        call: Some(&token),
        replay: false,
    };
    deliver_host_output(&instance, mid, 1, &host_value(b"derived"), &d, &producer)
        .expect("the edge delivers");
    assert_eq!(queued_call(&consumer).as_deref(), Some("w:t"));
}

#[path = "routing_tests/park_bound.rs"]
mod park_bound;
