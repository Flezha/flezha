//! The routing kernel: a frame off the wire into the operative it names
//! ([`OperativeDispatchHandle::dispatch`]), a channel value to every consumer
//! edge's Entity (`route_to_consumers`), and the end-of-slice broadcast. Live
//! delivery and the controller's replay slice both travel these seams, and a
//! `ConsumerEdge`'s closures become mailbox pushes in exactly one place
//! (`deliver_edge`), so a ladder re-attempt cannot deliver differently from
//! the attempt it retries.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;

use crate::admission::{admit_from_entity, admit_from_reader};
use crate::channel::{ChannelDescriptor, ChannelKind, ConsumerEdge};
use crate::entity::{make_entity_payload, EntityItem, EntitySlot};
use crate::error::Error;
use crate::operative::instance::StreamInstance;
use crate::operative::registry::{Operative, OperativeRegistry};
use crate::operative::topology::{EntityMode, ProcedureDescriptor};
use crate::retry::{spawn_edge_ladder, spawn_entry_parse_ladder, EntryParseLadder};

/// Route one channel value to every consumer through its own edge — the one
/// seam live delivery, the replay slice and an inter-procedure emit share. A
/// closure that unwinds sends the value up the channel's ladder; the other
/// consumers are still served.
#[allow(clippy::too_many_arguments)]
pub fn route_to_consumers(
    instance: &Arc<StreamInstance>,
    channel: &ChannelDescriptor,
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    replay: bool,
    producer: Option<&Arc<EntitySlot>>,
) -> bool {
    let mut deferred = false;
    for edge in &channel.consumers {
        match deliver_edge(
            instance,
            channel,
            edge,
            value,
            stream,
            consumed_offset,
            replay,
            producer,
        ) {
            Ok(held) => deferred |= held,
            Err(e) => spawn_edge_ladder(
                instance,
                channel,
                edge,
                value,
                stream,
                consumed_offset,
                replay,
                e.to_string(),
            ),
        }
    }
    deferred
}

/// One value through one edge's transform and into the mailbox — the ONE
/// place an edge's closures become pushes, for live routing and the ladder's
/// re-attempt alike.
#[allow(clippy::too_many_arguments)]
pub(crate) fn deliver_edge(
    instance: &Arc<StreamInstance>,
    channel: &ChannelDescriptor,
    edge: &ConsumerEdge,
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    replay: bool,
    producer: Option<&Arc<EntitySlot>>,
) -> Result<bool, Error> {
    match edge.transform_value(value).map_err(Error::Routing)? {
        None => deliver_to_consumer(
            instance,
            channel,
            edge,
            value,
            stream,
            consumed_offset,
            replay,
            producer,
        ),
        Some(yielded) => yielded.into_iter().try_fold(false, |held, v| {
            deliver_to_consumer(
                instance,
                channel,
                edge,
                &v,
                stream,
                consumed_offset,
                replay,
                producer,
            )
            .map(|d| held | d)
        }),
    }
}

/// One host completion's edge value to its named consumer; the host already
/// ran the edge's transforms and key.
#[allow(clippy::too_many_arguments)]
pub(crate) fn deliver_host_output(
    instance: &Arc<StreamInstance>,
    channel_id: u32,
    consumer: u32,
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    producer: &Arc<EntitySlot>,
) -> Result<bool, Error> {
    let channel = instance
        .topology
        .channel(channel_id)
        .ok_or_else(|| Error::Routing(format!("edge output names unknown channel {channel_id}")))?;
    let edge = channel
        .consumers
        .iter()
        .find(|e| e.procedure == consumer)
        .ok_or_else(|| {
            Error::Routing(format!(
                "edge output names procedure {consumer}, not a consumer of channel `{}`",
                channel.name
            ))
        })?;
    // An inter-procedure value is never a replay member: the downstream
    // Procedure receives corrected outputs already in order.
    deliver_to_consumer(
        instance,
        channel,
        edge,
        value,
        stream,
        consumed_offset,
        false,
        Some(producer),
    )
}

/// One post-transform value to one consumer's Entity. `Err` is an unwinding
/// key closure; an absent registry or an unmapped static key is a silent
/// skip.
#[allow(clippy::too_many_arguments)]
fn deliver_to_consumer(
    instance: &StreamInstance,
    channel: &ChannelDescriptor,
    edge: &ConsumerEdge,
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    replay: bool,
    producer: Option<&Arc<EntitySlot>>,
) -> Result<bool, Error> {
    let policy = channel.overflow_policy;
    let mut deferred = false;
    // The one place the two admission forms are chosen between: a value with a
    // producing Entity withholds work from it, a value from the entry path (or
    // a replay member, which only the entry path produces) parks the reader.
    let mut admit = |consumer: &Arc<EntitySlot>| {
        let item = EntityItem::Input(make_entity_payload(value, stream, consumed_offset, replay));
        match producer {
            Some(p) if !replay => deferred |= admit_from_entity(p, consumer, item, policy),
            _ => admit_from_reader(consumer, item, policy),
        }
    };
    let consumer_procedure_id = edge.procedure;
    let consumer = match instance.topology.procedure(consumer_procedure_id) {
        Some(s) => s,
        None => return Ok(false),
    };
    match consumer.mode {
        EntityMode::Dynamic => deliver_dynamic(instance, edge, value, &mut admit)?,
        EntityMode::Single => deliver_single(instance, consumer_procedure_id, &mut admit),
        EntityMode::Static => {
            deliver_static(instance, consumer_procedure_id, consumer, value, &mut admit)
        }
    }
    // The closure's borrow of `deferred` ends at its last use above.
    Ok(deferred)
}

/// A dynamic consumer: one Entity by key, minted on first sight. A host
/// class's key rides the payload, consulted when no edge closure exists.
fn deliver_dynamic(
    instance: &StreamInstance,
    edge: &ConsumerEdge,
    value: &Arc<dyn Any + Send + Sync>,
    admit: &mut impl FnMut(&Arc<EntitySlot>),
) -> Result<(), Error> {
    let Some(registry) = instance
        .dynamic_registries
        .get(edge.procedure as usize)
        .and_then(|r| r.as_ref())
    else {
        return Ok(());
    };
    let key = match edge.key_of(value.as_ref()).map_err(Error::Routing)? {
        Some(k) => Some(k),
        None => value
            .downcast_ref::<crate::host::HostPayload>()
            .and_then(|p| p.keys.first().cloned()),
    };
    let Some(key) = key else {
        return Ok(());
    };
    // A key this member does not own means the source and the topology
    // disagree about what a key is. Refuse on the first frame — before
    // `get_or_create` mints an Entity — so a misconfigured group parks
    // its traffic instead of folding half of it into private state.
    if instance.partition.partitioned() {
        instance.partition.admit(&key)?;
    }
    if let Ok(slot) = registry.get_or_create(&key) {
        admit(&slot);
    }
    Ok(())
}

fn deliver_single(
    instance: &StreamInstance,
    procedure_id: u32,
    admit: &mut impl FnMut(&Arc<EntitySlot>),
) {
    if let Some(slot) = instance
        .entities
        .get(procedure_id as usize)
        .and_then(|p| p.first())
    {
        admit(slot);
    }
}

/// A static consumer: the declared Entities the value's keys name, empty
/// meaning all of them. A partition member leaves its out-of-range declared
/// Entities idle — not a mis-route, somebody else's entity.
fn deliver_static(
    instance: &StreamInstance,
    procedure_id: u32,
    consumer: &ProcedureDescriptor,
    value: &Arc<dyn Any + Send + Sync>,
    admit: &mut impl FnMut(&Arc<EntitySlot>),
) {
    let Some(entities) = instance.entities.get(procedure_id as usize) else {
        return;
    };
    let key_to_idx = instance.entity_key_to_idx.get(procedure_id as usize);
    let partition = instance.partition;
    let mut deliver_idx = |idx: usize| {
        if partition.partitioned()
            && !consumer
                .entity_keys
                .get(idx)
                .is_some_and(|key| partition.owns(key))
        {
            return;
        }
        if let Some(slot) = entities.get(idx) {
            admit(slot);
        }
    };
    if let Some(host_payload) = value.downcast_ref::<crate::host::HostPayload>() {
        if host_payload.keys.is_empty() {
            for idx in 0..entities.len() {
                deliver_idx(idx);
            }
        } else {
            for key in &host_payload.keys {
                if let Some(&idx) = key_to_idx.and_then(|m| m.get(key.as_str())) {
                    deliver_idx(idx);
                }
            }
        }
    } else {
        for key in (consumer.entities_for_erased)(value.as_ref()) {
            if let Some(&idx) = key_to_idx.and_then(|m| m.get(key)) {
                deliver_idx(idx);
            }
        }
    }
}

/// Close every Entity's replayed slice and wake their drains. A broadcast,
/// not a marker on the last input: an input reaches one Entity by key. On
/// the frame thread after every slice frame, so no Entity is still receiving
/// members when its slice closes.
pub fn end_replay_slice(instance: &StreamInstance) {
    for slot in instance.all_slots() {
        slot.replay_open.store(false, Ordering::Release);
        // The drain may already have run dry and exited with the slice still
        // buffered — it had no way to know more was coming. Wake it so it can
        // see the closed slice and flush it.
        if slot.mailbox.claim_inflight() {
            crate::poll_core::global().schedule(&slot);
        }
    }
}

/// One plugin output onto the stream that carried its input. A write the
/// stream refuses is held for the reconnect, never dropped.
pub fn send_plugin_frame(
    stream: &Arc<FlezhaStream>,
    instance_id: &str,
    bytes: &[u8],
    metadata: &HashMap<String, String>,
) {
    // The wire's other writer holds a frame it could not send and drives the
    // reconnect that replays it; an output is not worth less than a record.
    let frame = payloader::build_from_binary(instance_id, bytes, metadata.clone());
    crate::state_sink::write(crate::state_sink::Class::Output, Some(stream), frame);
}

/// The stream readers' handle onto the live instance registry.
#[derive(Clone)]
pub struct OperativeDispatchHandle {
    pub registry: Arc<OperativeRegistry>,
}

impl OperativeDispatchHandle {
    /// A frame off the wire into the operative it names: the entry channel
    /// resolved, the bytes wrapped, the value routed. The input-liveness
    /// signal is taken before anything can decline the frame; a replay-end
    /// broadcast defers behind a host class's staged slice members.
    pub fn dispatch(
        &self,
        instance_id: &str,
        payload: Vec<u8>,
        metadata: &HashMap<String, String>,
        stream: Arc<FlezhaStream>,
    ) {
        let Some(instance) = self.stream_instance(instance_id, &payload, metadata) else {
            return;
        };

        if metadata.contains_key("flz.replay_end") {
            crate::host::deferred_end_replay_slice(&instance);
            return;
        }

        let Some(channel_id) = resolve_entry_channel(&instance, instance_id, metadata) else {
            return;
        };
        let channel = match instance.topology.channel(channel_id) {
            Some(c) => c,
            None => return,
        };

        channel.observe_delivery();
        let (consumed_offset, replay) = frame_facts(metadata);

        let Some(typed_payload) = enter_channel(
            &instance,
            channel_id,
            channel,
            payload,
            &stream,
            consumed_offset,
            replay,
        ) else {
            return;
        };

        // The entry path has no producing Entity to withhold work from, so a
        // `DropNothing` push here parks this reader — the one park the core
        // keeps, and what carries ingress backpressure to the controller.
        route_to_consumers(
            &instance,
            channel,
            &typed_payload,
            &stream,
            consumed_offset,
            replay,
            None,
        );
    }
}

impl OperativeDispatchHandle {
    /// The stream instance a frame names; a workflow instance takes it as
    /// inbox input and answers `None`.
    fn stream_instance(
        &self,
        instance_id: &str,
        payload: &[u8],
        metadata: &HashMap<String, String>,
    ) -> Option<Arc<StreamInstance>> {
        match self.registry.get_operative(instance_id) {
            Some(unit) => match &*unit {
                Operative::Stream(ri) => Some(Arc::clone(ri)),
                Operative::Workflow(instance) => {
                    route_inbox_frame(instance, instance_id, payload.to_vec(), metadata);
                    None
                }
            },
            None => {
                eprintln!("[flezha] operative instance {} not found", instance_id);
                None
            }
        }
    }
}

/// What the controller stamped: the input-log offset and whether the frame
/// is a replayed slice member.
fn frame_facts(metadata: &HashMap<String, String>) -> (Option<u64>, bool) {
    let consumed_offset = metadata
        .get("flz.offset")
        .and_then(|s| s.parse::<u64>().ok());
    (consumed_offset, metadata.contains_key("flz.replay"))
}

/// Post-start input into the running driver's inbox with its stamped offset;
/// dropped only when the instance is not running.
fn route_inbox_frame(
    instance: &crate::workflow::instance::WorkflowInstance,
    instance_id: &str,
    payload: Vec<u8>,
    metadata: &HashMap<String, String>,
) {
    let offset = metadata
        .get(crate::workflow::INBOX_OFFSET_KEY)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    if !instance.push_inbox(payload, offset) {
        eprintln!(
            "[flezha] inbox frame for workflow unit {} dropped (not running / no inbox)",
            instance_id
        );
    }
}

/// `metadata["channel"]`, or the first declared entry channel; `None` is
/// reported.
fn resolve_entry_channel(
    instance: &StreamInstance,
    instance_id: &str,
    metadata: &HashMap<String, String>,
) -> Option<u32> {
    let topology = &instance.topology;
    match metadata.get("channel") {
        Some(name) => match topology.lookup_channel_id(name) {
            Some(id) => Some(id),
            None => {
                eprintln!(
                    "[flezha] operative {} has no channel `{}`",
                    instance_id, name
                );
                None
            }
        },
        None => {
            match topology
                .channels
                .iter()
                .position(|c| c.kind == ChannelKind::Entry)
            {
                Some(idx) => Some(idx as u32),
                None => {
                    eprintln!("[flezha] operative {} has no entry channels", instance_id);
                    None
                }
            }
        }
    }
}

/// The value a frame becomes on its entry channel, or `None` when it left
/// the reader's hands — staged for a prepare crossing, or a failing parse on
/// the ladder.
fn enter_channel(
    instance: &Arc<StreamInstance>,
    channel_id: u32,
    channel: &ChannelDescriptor,
    payload: Vec<u8>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    replay: bool,
) -> Option<Arc<dyn Any + Send + Sync>> {
    let payload = crate::host::maybe_stage_prepare(
        instance,
        channel_id,
        channel,
        payload,
        consumed_offset,
        replay,
        stream,
    )?;
    wrap_entry(
        instance,
        channel_id,
        channel,
        payload,
        stream,
        consumed_offset,
        replay,
    )
}

/// The wire bytes through `wrap_input`. A parsed entry's failure rides the
/// ladder, so its bytes are kept; a passthrough entry keeps the drop-and-log
/// path and no clone.
fn wrap_entry(
    instance: &Arc<StreamInstance>,
    channel_id: u32,
    channel: &ChannelDescriptor,
    payload: Vec<u8>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    replay: bool,
) -> Option<Arc<dyn Any + Send + Sync>> {
    let retry_bytes = channel.entry_parsed.then(|| payload.clone());
    let Some(wrap) = &channel.wrap_input else {
        eprintln!(
            "[flezha] operative {} channel `{}` has no entry wrapper",
            instance.instance_id, channel.name
        );
        return None;
    };
    match wrap(payload) {
        Ok(p) => Some(p),
        Err(e) => {
            match retry_bytes {
                Some(bytes) => spawn_entry_parse_ladder(EntryParseLadder {
                    instance: Arc::clone(instance),
                    channel_id,
                    bytes,
                    first_error: e,
                    stream: Arc::clone(stream),
                    consumed_offset,
                    replay,
                }),
                None => eprintln!(
                    "[flezha] operative {} channel `{}`: wrap_input failed: {}",
                    instance.instance_id, channel.name, e
                ),
            }
            None
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::io::Read;
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    use super::*;
    use crate::flusher::SendFrameFn;
    use crate::state_sink;

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

    #[test]
    fn a_plugin_frame_whose_write_fails_is_held_for_replay() {
        let _guard = state_sink::test_guard();
        state_sink::reset_for_test();

        send_plugin_frame(&dead_stream(), OPERATIVE, BODY, &sink_metadata());

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
        let expected = payloader::build_from_binary(OPERATIVE, BODY, metadata.clone());
        send_plugin_frame(&dead_stream(), OPERATIVE, BODY, &metadata);

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
}
