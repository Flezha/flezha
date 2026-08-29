//! The routing kernel: a frame off the wire into the operative it names
//! ([`OperativeDispatchHandle::dispatch`]), a channel value to every consumer
//! edge's Entity (`route_to_consumers`), and the end-of-slice broadcast. Live
//! delivery and the controller's replay slice both travel these seams, and a
//! `ConsumerEdge`'s closures become mailbox pushes in exactly one place
//! (`deliver_edge`), so a ladder re-attempt cannot deliver differently from
//! the attempt it retries.

use std::any::Any;
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;

use crate::admission::{admit_from_entity, admit_from_reader};
use crate::channel::{ChannelDescriptor, ChannelKind, ConsumerEdge};
use crate::entity::{make_entity_payload, EntityItem, EntitySlot};
use crate::operative::instance::StreamInstance;
use crate::operative::registry::{Operative, OperativeRegistry};
use crate::operative::topology::{EntityMode, ProcedureDescriptor};
use crate::retry::{spawn_edge_ladder, spawn_entry_parse_ladder, EntryParseLadder};

/// Route one channel value to every subscribed consumer, each through its own
/// edge's transform and key. The one seam a `ConsumerEdge`'s closures are
/// applied on: the wire-reader path (live delivery and the controller's
/// re-forwarded replay slice) and an inter-procedure emit both come through
/// here, so the two can never drift.
///
/// A closure that unwinds does not take the routing thread with it — the value
/// climbs this channel's retry→DLQ ladder, the same one a failing entry parse
/// rides, and the remaining consumers are still served.
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
                e,
            ),
        }
    }
    deferred
}

/// Put one value through one edge's transform and deliver everything it
/// yields. The ONE place a `ConsumerEdge`'s closures become mailbox pushes:
/// live routing and the retry ladder's re-attempt both call it, so a
/// re-attempt cannot deliver differently from the attempt it retries.
///
/// A transform-free edge — every pre-existing topology — takes the `None` arm
/// and pays nothing.
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
) -> Result<bool, String> {
    match edge.transform_value(value)? {
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

/// Deliver one host completion's edge value to its named consumer — the
/// single-edge form of [`route_to_consumers`] for producer-side-evaluated
/// outputs (the host already ran the edge's transforms and key).
#[allow(clippy::too_many_arguments)]
pub(crate) fn deliver_host_output(
    instance: &Arc<StreamInstance>,
    channel_id: u32,
    consumer: u32,
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    producer: &Arc<EntitySlot>,
) -> Result<bool, String> {
    let channel = instance
        .topology
        .channel(channel_id)
        .ok_or_else(|| format!("edge output names unknown channel {channel_id}"))?;
    let edge = channel
        .consumers
        .iter()
        .find(|e| e.procedure == consumer)
        .ok_or_else(|| {
            format!(
                "edge output names procedure {consumer}, not a consumer of channel `{}`",
                channel.name
            )
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

/// Deliver one post-transform value to one consumer's Entity. `Err` carries an
/// unwinding key closure up to the ladder; every other outcome — an absent
/// registry, an unmapped static key — is the pre-existing silent skip.
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
) -> Result<bool, String> {
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

/// A dynamic consumer: one Entity by key, minted on first sight. The key is
/// total for typed classes — `build_topology` refuses an unkeyed edge into a
/// dynamic consumer. A host class's key rides the payload itself (prepare
/// crossing / producer-side evaluation), so it is consulted when no edge
/// closure exists.
fn deliver_dynamic(
    instance: &StreamInstance,
    edge: &ConsumerEdge,
    value: &Arc<dyn Any + Send + Sync>,
    admit: &mut impl FnMut(&Arc<EntitySlot>),
) -> Result<(), String> {
    let Some(registry) = instance
        .dynamic_registries
        .get(edge.procedure as usize)
        .and_then(|r| r.as_ref())
    else {
        return Ok(());
    };
    let key = match edge.key_of(value.as_ref())? {
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

/// A single-mode consumer: entity 0.
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

/// A static consumer: the declared Entities the value's keys name. Host
/// fan-out keys ride the payload (prepare crossing / producer-side
/// evaluation); an empty set means every declared entity — the entities-for
/// default. A typed class answers through its `entities_for_erased`.
///
/// A statically keyed class partitions by leaving its out-of-range declared
/// Entities idle: one filter covers the named-key routes and the broadcast
/// alike. Unlike a dynamic key this is never a park — a declared entity the
/// member does not own is not a mis-route, it is somebody else's entity.
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

/// Close every Entity's replayed slice and wake their drains to run it.
///
/// A broadcast rather than a marker riding the slice's last input, because it
/// has to be: an input routes to exactly one Entity by key, so a marker on the
/// last one would reach one Entity and leave every other Entity that buffered
/// slice members holding them forever.
///
/// Arrives on the frame-handling thread after every frame of the slice, so a
/// Entity cannot still be receiving members when its slice is closed.
pub fn end_replay_slice(instance: &StreamInstance) {
    let statics = instance.entities.iter().flatten().cloned();
    let dynamics = instance.dynamic_registries.iter().flatten().flat_map(|r| {
        r.entities
            .iter()
            .map(|e| Arc::clone(e.value()))
            .collect::<Vec<_>>()
    });

    for slot in statics.chain(dynamics) {
        slot.replay_open.store(false, Ordering::Release);
        // The drain may already have run dry and exited with the slice still
        // buffered — it had no way to know more was coming. Wake it so it can
        // see the closed slice and flush it.
        if slot.mailbox.claim_inflight() {
            crate::poll_core::global().schedule(&slot);
        }
    }
}

pub fn send_plugin_frame(
    stream: &Arc<FlezhaStream>,
    instance_id: &str,
    bytes: &[u8],
    metadata: &HashMap<String, String>,
) {
    let frame = payloader::build_from_binary(instance_id, bytes, metadata.clone());
    if let Err(e) = (&**stream).write_all(&frame) {
        eprintln!(
            "[flezha] plugin emit: failed to write {} bytes for {}: {}",
            bytes.len(),
            instance_id,
            e
        );
    }
}

/// Stream-reader handle for the StreamOperative model. Cheap to clone; carries a
/// reference to the live StreamOperative-instance registry. Per-Entity drain
/// scheduling goes through the poll core.
#[derive(Clone)]
pub struct OperativeDispatchHandle {
    pub registry: Arc<OperativeRegistry>,
}

impl OperativeDispatchHandle {
    /// Dispatch a frame from the wire. Resolves the entry channel by
    /// `metadata["channel"]` (or the StreamOperative's first declared input
    /// channel as the documented default), wraps the payload bytes via
    /// the channel's `wrap_input`, and pushes the typed
    /// `Arc<dyn Any + Send + Sync>` onto every consumer Procedure's
    /// entity mailbox.
    ///
    /// The end of a replayed slice (`flz.replay_end`) is a broadcast, not an
    /// input — an input routes to one Entity by key, so a marker riding one
    /// would strand the slice every *other* Entity buffered; a host class may
    /// hold slice members in the prepare crossing, so the broadcast defers
    /// behind them. The input-liveness signal is taken before anything can
    /// decline the frame: a payload that fails to parse still proves the feed
    /// is alive, and a host class's staged payload never reaches the routing.
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
    /// The stream instance a frame names. A workflow instance takes the frame
    /// as inbox input here and answers `None`; an unknown id is reported.
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

/// What the controller stamped on a forwarded frame: the durable input-log
/// offset under the effectively-once tier (threaded to the invocation so the
/// atomic effect record carries the same offset as consumed-offset), and
/// whether the frame is a replayed slice member.
fn frame_facts(metadata: &HashMap<String, String>) -> (Option<u64>, bool) {
    let consumed_offset = metadata
        .get("flz.offset")
        .and_then(|s| s.parse::<u64>().ok());
    (consumed_offset, metadata.contains_key("flz.replay"))
}

/// Workflow inbox: route post-start input to the running driver's inbox
/// instead of dropping it. Dropped only if the instance is not running / has
/// no live inbox. A durable instance's input was journaled to its inbox
/// before this frame was forwarded; the stamped offset rides into the
/// iteration commit as the consumed offset.
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

/// The entry channel a frame names — `metadata["channel"]`, or the first
/// declared entry channel as the documented default. `None` is reported.
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

/// The typed value a frame becomes on its entry channel, or `None` when it
/// left the reader's hands: a host class whose consumers need keys / event
/// times stages the payload for the prepare crossing instead of enqueueing
/// directly, and a failing parse rides the ladder or is reported.
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

/// The wire bytes through the channel's `wrap_input`. Typed entries
/// (`sdk-graph-authoring`): a failing parse is classified like a failing
/// invocation and rides the channel's retry→DLQ ladder, so the wire bytes are
/// kept for the re-attempts and the park record. Passthrough `Vec<u8>` /
/// `String` entries keep the drop-and-log path (no clone on the hot path).
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
