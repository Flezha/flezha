//! The routing seam's tests: a plugin output whose write failed and is held
//! for the reconnect, the entry-channel resolution — a declared name
//! delivered, an absent one defaulted, and a name the operative does not
//! declare parked under the name it asked for — and a host payload fanned
//! across several dynamic keys.

use std::io::Read;
use std::os::unix::net::UnixStream;
use std::time::Duration;

use super::*;
use crate::flusher::SendFrameFn;
use crate::operative::instance::StreamInstance;
use crate::state_sink;
use crate::test_support::{capture_parks, host_class};

const OPERATIVE: &str = "op-plugin-emit";
const BODY: &[u8] = b"the operative's only output";

/// A stream whose peer is gone: the shape the controller leaves behind
/// when it closes the socket half of a rebound pair.
fn dead_stream() -> Arc<FlezhaStream> {
    let (near, far) = UnixStream::pair().expect("a socket pair");
    drop(far);
    Arc::new(FlezhaStream::Unix(near))
}

fn sink_metadata() -> HashMap<String, String> {
    HashMap::from([("channel".to_string(), "out".to_string())])
}

/// The dispatch handle a stream reader holds over one registered instance.
fn handle_over(instance: &Arc<StreamInstance>) -> OperativeDispatchHandle {
    let registry = Arc::new(crate::operative::registry::OperativeRegistry::new());
    registry.register_stream(Arc::clone(instance));
    OperativeDispatchHandle { registry }
}

/// One frame off the wire into `instance`, naming `channel` when it is
/// `Some` — a `None` is the metadata a source plugin sends by default.
fn feed(instance: &Arc<StreamInstance>, channel: Option<&str>, payload: &[u8]) {
    let metadata = channel
        .map(|name| HashMap::from([("channel".to_string(), name.to_string())]))
        .unwrap_or_default();
    handle_over(instance).dispatch(
        &instance.instance_id,
        payload.to_vec(),
        &metadata,
        dead_stream(),
    );
}

/// A topology with no entry channel. No declaration this core accepts builds
/// one, so the test builds the topology itself — the shape `dispatch` would
/// be handed.
fn entry_less_instance(id: &'static str) -> Arc<StreamInstance> {
    let topology = crate::operative::topology::OperativeTopology {
        sdk_lang: 1,
        channels: Vec::new(),
        name_to_id: HashMap::new(),
        procedures: Vec::new(),
        procedure_name_to_id: HashMap::new(),
        factory_id: id,
        persistence_enabled_const: false,
        effectively_once: false,
        correction: None,
        ack_granularity: Default::default(),
    };
    StreamInstance::new(Arc::new(topology), Arc::from(id))
}

fn queued(instance: &Arc<StreamInstance>) -> usize {
    instance.entities[0][0].mailbox.len()
}

/// The entry channel the one queued frame resolved to: the host wrapper
/// records the channel it was parsed for, so the mailbox itself says which
/// entry channel took the frame.
fn queued_channel(instance: &Arc<StreamInstance>) -> Option<u32> {
    match instance.entities[0][0].mailbox.pop()? {
        crate::entity::EntityItem::Input(payload) => payload
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
    assert_eq!(queued(&instance), 0, "no procedure received the frame");
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
/// class with no procedure, and refuses a procedure no entry channel reaches
/// (`UnreachableProcedure`) — but a topology can hold that shape, and
/// `dispatch` takes topologies. So the branch is driven, not argued.
#[test]
fn an_operative_declaring_no_entry_channel_parks_with_its_own_reason() {
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    let parks = capture_parks();
    let instance = entry_less_instance("q2_no_entry");

    feed(&instance, None, b"nowhere-to-go");

    crate::test_support::spin_until("the entry-less operative to park", || {
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

/// A frame naming a channel the operative DOES declare, of another kind: a
/// sink is not an entry channel, so it parks exactly as an undeclared name
/// does. Before the kind-aware lookup this frame resolved, found no
/// `wrap_input`, and was dropped with a line on stderr.
#[test]
fn a_frame_naming_a_declared_channel_of_another_kind_is_parked_too() {
    let _guard = state_sink::test_guard();
    state_sink::reset_for_test();
    let parks = capture_parks();
    let instance = host_class::instance("q2_wrong_kind", &["q2_in"], &[], &[1]);

    // `out` is the class's plugin-bound sink; `mid` its inter-procedure channel.
    feed(&instance, Some("out"), b"payload-bytes");

    crate::test_support::spin_until("the sink name to be parked", || {
        !parks.lock().expect("park capture poisoned").is_empty()
    });
    let park = parks.lock().expect("park capture poisoned").remove(0);
    let body = park.body;
    assert_eq!(body.entry.channel, "out");
    assert_eq!(
        body.entry.error,
        "no entry channel `out`; the operative declares: q2_in"
    );
    assert_eq!(body.entry.payload.as_deref(), Some(&b"payload-bytes"[..]));
    assert_eq!(queued(&instance), 0, "no procedure received the frame");
    state_sink::reset_for_test();
}

#[test]
fn the_reason_for_an_operative_with_no_entry_channel_names_that_condition() {
    assert_eq!(
        unresolved_entry_error(&[], ""),
        NO_ENTRY_CHANNEL_ERROR,
        "an empty declared set is named as such, not printed as an empty list"
    );
    assert_eq!(
        unresolved_entry_error(&["config", "prices"], "quotes"),
        "no entry channel `quotes`; the operative declares: config, prices"
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
/// identity's procedure slot into a directory under the dead-letter root. A
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

    send_plugin_frame(&dead_stream(), OPERATIVE, BODY, &sink_metadata()).unwrap();

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
    let expected = payloader::build_from_binary(OPERATIVE, BODY, metadata.clone()).unwrap();
    send_plugin_frame(&dead_stream(), OPERATIVE, BODY, &metadata).unwrap();

    // The reconnect: a fresh secondary, then the sink install that replays
    // the ring onto it.
    let (near, mut far) = UnixStream::pair().expect("a socket pair");
    far.set_read_timeout(Some(Duration::from_secs(2)))
        .expect("a read deadline, so a lost frame fails instead of hanging");
    state_sink::set_secondary_stream(Some(Arc::new(FlezhaStream::Unix(near))));
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

/// A host class whose one entry is keyed into a dynamic procedure, the shape a
/// `key_by` behind a fanning `flat_map` declares.
fn keyed_dynamic_instance(id: &str) -> Arc<StreamInstance> {
    let json = format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0",
             "channels":[{{"name":"in","kind":"entry",
                           "chains":[[{{"link":"key"}},{{"link":"into","procedure":0}}]]}}],
             "procedures":[{{"name":"p0","entity_mode":"dynamic"}}]}}"#
    );
    crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    StreamInstance::of_class(id, &format!("{id}_inst")).expect("class registered")
}

#[test]
fn a_host_payload_fanned_across_keys_reaches_every_keyed_entity() {
    let instance = keyed_dynamic_instance("fanned_host_keys");
    let channel = instance.topology.channel(0).expect("the entry channel");
    let value: Arc<dyn Any + Send + Sync> = Arc::new(crate::host::HostPayload {
        bytes: b"a:1,b:2".to_vec(),
        channel: 0,
        keys: vec!["A".to_string(), "B".to_string()],
        event_time: None,
        metadata: Vec::new(),
    });

    route_to_consumers(
        &instance,
        channel,
        &value,
        &dead_stream(),
        None,
        false,
        None,
    );

    let registry = instance.dynamic_registries[0]
        .as_ref()
        .expect("a dynamic consumer has a registry");
    for key in ["A", "B"] {
        let slot = registry.entities.get(key).unwrap_or_else(|| {
            panic!("the fan answered key {key}, and no Entity was minted for it")
        });
        assert_eq!(slot.mailbox.len(), 1, "key {key} receives the payload once");
    }
}
