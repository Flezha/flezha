//! The routing kernel: a frame off the wire into the operator it names
//! ([`OperatorDispatchHandle::dispatch`]), a channel value to every consumer
//! edge's Cell (`route_to_consumers`), and the end-of-slice broadcast. Live
//! delivery and the controller's replay slice both travel these seams, and a
//! `ConsumerEdge`'s closures become mailbox pushes in exactly one place
//! (`deliver_edge`), so a ladder re-attempt cannot deliver differently from
//! the attempt it retries.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;

use crate::admission::{admit_from_cell, admit_from_reader, nonparking_enabled};
use crate::cell::{make_cell_payload, CellItem, CellSlot};
use crate::channel::{ChannelDescriptor, ChannelId, ChannelKind, ConsumerEdge};
use crate::error::{Code, Error};
use crate::operator::instance::OperatorInstance;
use crate::operator::registry::OperatorRegistry;
use crate::operator::topology::{CellMode, ChannelLookup, OperatorTopology, TaskletDescriptor};
use crate::retry::{spawn_edge_ladder, spawn_entry_parse_ladder, EntryDeliver, EntryParseLadder};

/// What the entrance reads off an instance of either kind: the topology the
/// channel name resolves against, the id a park is filed under and the
/// count of ladders holding a payload off-thread for it.
pub trait EntryView: Send + Sync + 'static {
    fn topology(&self) -> &Arc<OperatorTopology>;
    fn instance_id(&self) -> &Arc<str>;
    fn ladders(&self) -> &AtomicUsize;

    /// The entrance parked a message instead of delivering it, durably;
    /// `offset` is the one the controller stamped on it.
    fn entry_parked(&self, _offset: Option<u64>) {}
}

impl EntryView for OperatorInstance {
    fn topology(&self) -> &Arc<OperatorTopology> {
        &self.topology
    }

    fn instance_id(&self) -> &Arc<str> {
        &self.instance_id
    }

    fn ladders(&self) -> &AtomicUsize {
        &self.ladders
    }

    /// A bounded operator counts the message consumed without a pass, so its park
    /// record releases it from the input log.
    fn entry_parked(&self, offset: Option<u64>) {
        crate::bounded::entry_parked(self, offset);
    }
}

/// Route one channel value to every consumer through its own edge — the one
/// seam live delivery, the replay slice and an inter-tasklet emit share. A
/// closure that unwinds sends the value up the channel's ladder; the other
/// consumers are still served. A bounded operator's value is not admitted here: a
/// message starts or queues a pass, a stage's output is held for its commit.
#[allow(clippy::too_many_arguments)]
pub fn route_to_consumers(
    instance: &Arc<OperatorInstance>,
    channel: &ChannelDescriptor,
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    call: Option<&Arc<str>>,
    replay: bool,
    producer: Option<&Arc<CellSlot>>,
) -> bool {
    if instance.bounded.is_some() {
        match producer {
            Some(producer) => {
                crate::bounded::hold_emit(instance, Some(channel), value, stream, producer)
            }
            None => {
                crate::bounded::message(instance, channel, value, stream, consumed_offset, call)
            }
        }
        return false;
    }
    let mut deferred = false;
    for edge in &channel.consumers {
        match deliver_edge(
            instance,
            channel,
            edge,
            value,
            stream,
            consumed_offset,
            call,
            replay,
            &mut Destination::Mailbox(producer),
        ) {
            Ok(held) => deferred |= held,
            Err(e) => spawn_edge_ladder(
                instance,
                channel,
                edge,
                value,
                stream,
                consumed_offset,
                call,
                replay,
                e.to_string(),
            ),
        }
    }
    deferred
}

/// Where a routed item goes: into its consumer's mailbox (a producing Cell
/// withholds work from itself instead of parking), or collected for a
/// bounded stage, which admits it only once its producer commits.
pub(crate) enum Destination<'a> {
    Mailbox(Option<&'a Arc<CellSlot>>),
    Held(&'a mut Vec<(Arc<CellSlot>, CellItem)>),
    /// An edge value a restore delivers again from its producer's record:
    /// admitted as the entry path admits, but not forwarded input.
    Redelivered,
}

/// One value through one edge's transform and into the mailbox — the ONE
/// place an edge's closures become pushes, for live routing and the ladder's
/// re-attempt alike.
#[allow(clippy::too_many_arguments)]
pub(crate) fn deliver_edge(
    instance: &OperatorInstance,
    channel: &ChannelDescriptor,
    edge: &ConsumerEdge,
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    call: Option<&Arc<str>>,
    replay: bool,
    to: &mut Destination<'_>,
) -> Result<bool, Error> {
    match edge.transform_value(value).map_err(Error::Routing)? {
        None => deliver_to_consumer(
            instance,
            channel,
            edge,
            value,
            stream,
            consumed_offset,
            call,
            replay,
            to,
        ),
        Some(yielded) => yielded.into_iter().try_fold(false, |held, v| {
            deliver_to_consumer(
                instance,
                channel,
                edge,
                &v,
                stream,
                consumed_offset,
                call,
                replay,
                to,
            )
            .map(|d| held | d)
        }),
    }
}

/// One host completion's edge value to its named consumer; the host already
/// ran the edge's transforms and key.
#[allow(clippy::too_many_arguments)]
pub(crate) fn deliver_host_output(
    instance: &Arc<OperatorInstance>,
    channel_id: u32,
    consumer: u32,
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    call: Option<&Arc<str>>,
    producer: &Arc<CellSlot>,
) -> Result<bool, Error> {
    let channel = instance.topology.channel(channel_id).ok_or_else(|| {
        Error::Routing(Code::UnknownChannelId.with([("channel_id", channel_id.into())]))
    })?;
    let edge = channel
        .consumers
        .iter()
        .find(|e| e.tasklet == consumer)
        .ok_or_else(|| {
            Error::Routing(Code::NotAConsumer.with([
                ("tasklet", consumer.into()),
                ("channel", channel.name.as_ref().into()),
            ]))
        })?;
    // An inter-tasklet value is never a replay member: the downstream
    // Tasklet receives corrected outputs already in order.
    deliver_to_consumer(
        instance,
        channel,
        edge,
        value,
        stream,
        consumed_offset,
        call,
        false,
        &mut Destination::Mailbox(Some(producer)),
    )
}

/// One post-transform value to one consumer's Cell. `Err` is an unwinding
/// key closure; an absent registry or an unmapped static key is a silent
/// skip.
#[allow(clippy::too_many_arguments)]
fn deliver_to_consumer(
    instance: &OperatorInstance,
    channel: &ChannelDescriptor,
    edge: &ConsumerEdge,
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    call: Option<&Arc<str>>,
    replay: bool,
    to: &mut Destination<'_>,
) -> Result<bool, Error> {
    let policy = channel.overflow_policy;
    let mut deferred = false;
    let forwarded = matches!(to, Destination::Mailbox(None));
    // The one place the two admission forms are chosen between: a value with a
    // producing Cell withholds work from it, a value from the entry path (or
    // a replay member, which only the entry path produces) parks the reader.
    let mut admit = |consumer: &Arc<CellSlot>| {
        let mut payload = make_cell_payload(value, stream, consumed_offset, replay);
        payload.forwarded = forwarded;
        payload.call = call.cloned();
        payload.hold = consumed_offset
            .zip(instance.prefix.as_ref())
            .map(|(offset, prefix)| prefix.hold(offset));
        let item = CellItem::Input(payload);
        match to {
            Destination::Held(held) => held.push((Arc::clone(consumer), item)),
            Destination::Mailbox(Some(p)) if !replay => {
                deferred |= admit_from_cell(p, consumer, item, policy, nonparking_enabled())
            }
            Destination::Mailbox(_) | Destination::Redelivered => {
                admit_from_reader(consumer, item, policy)
            }
        }
    };
    let consumer_tasklet_id = edge.tasklet;
    let consumer = match instance.topology.tasklet(consumer_tasklet_id) {
        Some(s) => s,
        None => return Ok(false),
    };
    match consumer.mode {
        CellMode::Dynamic => deliver_dynamic(instance, edge, value, &mut admit)?,
        CellMode::Single | CellMode::Static => {
            deliver_declared(instance, consumer_tasklet_id, consumer, value, &mut admit)
        }
    }
    // The closure's borrow of `deferred` ends at its last use above.
    Ok(deferred)
}

/// The declared Cells of `tasklet_id` a value reaches, with no edge between:
/// a chained loop's carry into its roots.
pub(crate) fn declared_cells_for(
    instance: &OperatorInstance,
    tasklet_id: u32,
    value: &Arc<dyn Any + Send + Sync>,
) -> Vec<Arc<CellSlot>> {
    let mut cells = Vec::new();
    if let Some(consumer) = instance.topology.tasklet(tasklet_id) {
        deliver_declared(instance, tasklet_id, consumer, value, &mut |cell| {
            cells.push(Arc::clone(cell))
        });
    }
    cells
}

/// A single or static consumer's Cells the value reaches.
fn deliver_declared(
    instance: &OperatorInstance,
    tasklet_id: u32,
    consumer: &TaskletDescriptor,
    value: &Arc<dyn Any + Send + Sync>,
    admit: &mut impl FnMut(&Arc<CellSlot>),
) {
    match consumer.mode {
        CellMode::Static => deliver_static(instance, tasklet_id, consumer, value, admit),
        _ => deliver_single(instance, tasklet_id, admit),
    }
}

/// A dynamic consumer: one Cell per key, minted on first sight. A host
/// class's keys ride the payload, consulted when no edge closure exists; a
/// payload its host fanned out answers one key per Cell it reaches.
fn deliver_dynamic(
    instance: &OperatorInstance,
    edge: &ConsumerEdge,
    value: &Arc<dyn Any + Send + Sync>,
    admit: &mut impl FnMut(&Arc<CellSlot>),
) -> Result<(), Error> {
    let Some(registry) = instance
        .dynamic_registries
        .get(edge.tasklet as usize)
        .and_then(|r| r.as_ref())
    else {
        return Ok(());
    };
    let edge_key = edge.key_of(value.as_ref()).map_err(Error::Routing)?;
    let keys = match &edge_key {
        Some(key) => std::slice::from_ref(key),
        None => value
            .downcast_ref::<crate::host::HostPayload>()
            .map_or(&[][..], |p| p.keys.as_slice()),
    };
    // A key this member does not own means the source and the topology
    // disagree about what a key is. Refuse on the first frame — before
    // `get_or_create` mints any Cell — so a misconfigured group parks
    // its traffic instead of folding part of it into private state.
    if instance.partition.partitioned() {
        for key in keys {
            instance.partition.admit(key)?;
        }
    }
    for key in keys {
        if let Ok(slot) = registry.get_or_create(key) {
            admit(&slot);
        }
    }
    Ok(())
}

fn deliver_single(
    instance: &OperatorInstance,
    tasklet_id: u32,
    admit: &mut impl FnMut(&Arc<CellSlot>),
) {
    if let Some(slot) = instance
        .cells
        .get(tasklet_id as usize)
        .and_then(|p| p.first())
    {
        admit(slot);
    }
}

/// A static consumer: the declared Cells the value's keys name, empty
/// meaning all of them. A partition member leaves its out-of-range declared
/// Cells idle — not a mis-route, somebody else's cell.
fn deliver_static(
    instance: &OperatorInstance,
    tasklet_id: u32,
    consumer: &TaskletDescriptor,
    value: &Arc<dyn Any + Send + Sync>,
    admit: &mut impl FnMut(&Arc<CellSlot>),
) {
    let Some(cells) = instance.cells.get(tasklet_id as usize) else {
        return;
    };
    let key_to_idx = instance.cell_key_to_idx.get(tasklet_id as usize);
    let partition = instance.partition;
    let mut deliver_idx = |idx: usize| {
        if partition.partitioned()
            && !consumer
                .cell_keys
                .get(idx)
                .is_some_and(|key| partition.owns(key))
        {
            return;
        }
        if let Some(slot) = cells.get(idx) {
            admit(slot);
        }
    };
    if let Some(host_payload) = value.downcast_ref::<crate::host::HostPayload>() {
        if host_payload.keys.is_empty() {
            for idx in 0..cells.len() {
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
        for key in (consumer.cells_for_erased)(value.as_ref()) {
            if let Some(&idx) = key_to_idx.and_then(|m| m.get(key)) {
                deliver_idx(idx);
            }
        }
    }
}

/// Close every Cell's replayed slice and wake their drains. A broadcast,
/// not a marker on the last input: an input reaches one Cell by key. On
/// the frame thread after every slice frame, so no Cell is still receiving
/// members when its slice closes.
pub fn end_replay_slice(instance: &OperatorInstance) {
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
///
/// A metadata field too long for its length prefix is refused instead: the
/// error names the field, and no bytes reach the wire. Retrying cannot help,
/// so the emit fails rather than being held.
pub fn send_plugin_frame(
    stream: &Arc<FlezhaStream>,
    instance_id: &str,
    bytes: &[u8],
    metadata: &HashMap<String, String>,
) -> std::io::Result<()> {
    let frame = payloader::build_from_binary(instance_id, bytes, metadata.clone())?;
    // The wire's other writer holds a frame it could not send and drives the
    // reconnect that replays it; an output is not worth less than a record.
    crate::state_sink::write(crate::state_sink::Class::Output, Some(stream), frame);
    Ok(())
}

/// The stream readers' handle onto the live instance registry.
#[derive(Clone)]
pub struct OperatorDispatchHandle {
    pub registry: Arc<OperatorRegistry>,
}

impl OperatorDispatchHandle {
    /// A frame off the wire into the operator it names: the entry channel
    /// resolved, the bytes wrapped, the value routed to the consumers — or,
    /// for a bounded operator, into the pass it starts or the queue it waits in. A
    /// name the operator does not declare is parked under that name rather
    /// than dropped or rerouted; past that, the input-liveness signal is taken
    /// before anything can decline the frame. A replay-end broadcast defers
    /// behind a host class's staged slice members.
    pub fn dispatch(
        &self,
        instance_id: &str,
        payload: Vec<u8>,
        metadata: &HashMap<String, String>,
        stream: Arc<FlezhaStream>,
    ) {
        let Some(instance) = self.registry.get(instance_id) else {
            eprintln!("[flezha] operator instance {} not found", instance_id);
            return;
        };
        route_stream_frame(&instance, payload, metadata, stream);
    }
}

fn route_stream_frame(
    instance: &Arc<OperatorInstance>,
    payload: Vec<u8>,
    metadata: &HashMap<String, String>,
    stream: Arc<FlezhaStream>,
) {
    if metadata.contains_key("flz.replay_end") {
        crate::host::deferred_end_replay_slice(instance);
        return;
    }

    let (consumed_offset, call, replay) = frame_facts(metadata);
    if let Some(edge) = metadata.get("flz.edge") {
        return redeliver_edge(instance, edge, payload, stream, consumed_offset, call);
    }
    if consumed_offset.is_some() {
        instance.owed_arrived();
    }
    let Some((channel_id, channel)) =
        entry_channel_or_park(instance, &payload, metadata, consumed_offset, call.as_ref())
    else {
        return;
    };

    let Some(typed_payload) = enter_channel(
        instance,
        channel_id,
        channel,
        payload,
        &stream,
        consumed_offset,
        call.as_ref(),
        replay,
    ) else {
        return;
    };

    // The entry path has no producing Cell to withhold work from, so a
    // `DropNothing` push here parks this reader — the one park the core
    // keeps, and what carries ingress backpressure to the controller.
    route_to_consumers(
        instance,
        channel,
        &typed_payload,
        &stream,
        consumed_offset,
        call.as_ref(),
        replay,
        None,
    );
}

/// The records of `value`, emitted on `channel_id` for the input at `origin`,
/// one per consumer edge, through the channel's codec: what a producer that
/// records effects keeps so a restore can deliver the value again. A value
/// the codec refuses is delivered live and recorded nowhere.
pub fn edge_records(
    instance: &OperatorInstance,
    channel: &ChannelDescriptor,
    channel_id: ChannelId,
    origin: Option<u64>,
    value: &Arc<dyn Any + Send + Sync>,
) -> Vec<crate::flusher::EdgeRecord> {
    channel
        .consumers
        .iter()
        .filter_map(|edge| edge_record(instance, channel_id, edge.tasklet, origin, value))
        .collect()
}

/// One consumer edge's record of `value`; see [`edge_records`].
pub(crate) fn edge_record(
    instance: &OperatorInstance,
    channel_id: ChannelId,
    consumer: u32,
    origin: Option<u64>,
    value: &Arc<dyn Any + Send + Sync>,
) -> Option<crate::flusher::EdgeRecord> {
    let codec = instance.topology.channel(channel_id)?.codec.as_ref()?;
    match (codec.encode)(value.as_ref()) {
        Ok(payload) => Some(crate::flusher::EdgeRecord {
            channel: channel_id,
            consumer,
            origin,
            payload,
        }),
        Err(error) => {
            tracing::warn!(
                target: "flezha::routing",
                instance = %instance.instance_id,
                channel = channel_id,
                "an edge value the channel codec refused is not recorded: {error}"
            );
            None
        }
    }
}

/// An edge value a restore delivers again (`flz.edge` names its channel and
/// consumer): decoded through the channel's codec and admitted to that one
/// consumer, whose floor drops it when its own records acknowledge `origin`.
fn redeliver_edge(
    instance: &Arc<OperatorInstance>,
    edge: &str,
    payload: Vec<u8>,
    stream: Arc<FlezhaStream>,
    origin: Option<u64>,
    call: Option<Arc<str>>,
) {
    let named = edge
        .split_once(':')
        .and_then(|(c, t)| Some((c.parse::<ChannelId>().ok()?, t.parse::<u32>().ok()?)));
    let Some((channel_id, consumer)) = named else {
        return;
    };
    let Some(channel) = instance.topology.channel(channel_id) else {
        return;
    };
    let (Some(codec), Some(edge)) = (
        channel.codec.as_ref(),
        channel.consumers.iter().find(|e| e.tasklet == consumer),
    ) else {
        return;
    };
    let value = match (codec.decode)(payload) {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(
                target: "flezha::routing",
                instance = %instance.instance_id,
                channel = channel_id,
                "a re-delivered edge value did not decode: {error}"
            );
            return;
        }
    };
    if let Err(error) = deliver_edge(
        instance,
        channel,
        edge,
        &value,
        &stream,
        origin,
        call.as_ref(),
        false,
        &mut Destination::Redelivered,
    ) {
        tracing::warn!(
            target: "flezha::routing",
            instance = %instance.instance_id,
            channel = channel_id,
            "a re-delivered edge value was not admitted: {error}"
        );
    }
}

/// What the controller stamped: the input-log offset `flz.offset` (an unbounded
/// or a bounded operator with durable input), the outside call waiting on the
/// input (`flz.reply_to`) and whether the frame is a replayed slice member.
fn frame_facts(metadata: &HashMap<String, String>) -> (Option<u64>, Option<Arc<str>>, bool) {
    let consumed_offset = metadata
        .get("flz.offset")
        .and_then(|s| s.parse::<u64>().ok());
    let call = metadata
        .get(CALL_KEY)
        .filter(|token| !token.is_empty())
        .map(|token| Arc::from(token.as_str()));
    (consumed_offset, call, metadata.contains_key("flz.replay"))
}

/// The metadata key a call's token rides: stamped by the controller on the
/// input, and by the runner on every output of a `response()` end.
pub const CALL_KEY: &str = "flz.reply_to";

/// The reason a frame that named no channel did not enter: there was nothing
/// to default to. Both cores report it in these words.
pub(crate) const NO_ENTRY_CHANNEL_ERROR: &str = "the operator declares no entry channel";

/// The entry channel a frame enters: the one its `channel` metadata names, or
/// the first declared entry channel when it names none. `None` when it names
/// a channel the operator does not declare — the payload is then parked under
/// that name, with the offset the controller stamped on the frame, neither
/// dropped nor routed to some other channel.
fn entry_channel_or_park<'a, V: EntryView>(
    instance: &'a Arc<V>,
    payload: &[u8],
    metadata: &HashMap<String, String>,
    consumed_offset: Option<u64>,
    call: Option<&Arc<str>>,
) -> Option<(ChannelId, &'a ChannelDescriptor)> {
    let topology = instance.topology();
    // The name the frame asked for; empty when it asked for none and there is
    // no entry channel to default to.
    let requested: &str = match metadata.get("channel") {
        Some(name) => match topology.channel_of_kind(name, ChannelKind::Entry) {
            ChannelLookup::Found(id, channel) => return Some((id, channel)),
            // A declared name of another kind — a sink, an inter-tasklet
            // channel — is no more an entry channel than one declared
            // nowhere, so both park and both name the entry set.
            ChannelLookup::WrongKind(_) | ChannelLookup::Undeclared => name,
        },
        None => match topology.first_of_kind(ChannelKind::Entry) {
            Some(first) => return Some(first),
            None => "",
        },
    };
    park_unresolved_entry(instance, requested, payload, consumed_offset, call);
    None
}

/// Why the frame did not enter: the name it asked for against the set the
/// operator declares, or that it declares none at all.
fn unresolved_entry_error(declared: &[&str], requested: &str) -> String {
    if declared.is_empty() {
        return NO_ENTRY_CHANNEL_ERROR.to_string();
    }
    format!(
        "no entry channel `{requested}`; the operator declares: {}",
        declared.join(", ")
    )
}

/// A frame naming a channel the operator does not declare: parked under the
/// name it asked for, off the reader thread as every other park is. The budget
/// is one — the same declared set answers every re-attempt — so the ladder
/// parks without waiting.
fn park_unresolved_entry<V: EntryView>(
    instance: &Arc<V>,
    requested: &str,
    payload: &[u8],
    consumed_offset: Option<u64>,
    call: Option<&Arc<str>>,
) {
    let declared = instance.topology().declared_names(ChannelKind::Entry);
    let error = unresolved_entry_error(&declared, requested);
    // A replay parks again rather than being skipped: the controller's store
    // keys an entry on its ORIGIN identity (offset, or channel and payload
    // hash) rather than on the park id, so a re-park of a frame that parked
    // before is the same row. Skipping the replay would instead lose a frame
    // that only became unresolved after the class changed.
    let parked = Arc::clone(instance);
    let requested = requested.to_string();
    let payload = payload.to_vec();
    let call = call.cloned();
    crate::retry::spawn_park_thread(instance, "flezha-unresolved-entry-park", move || {
        crate::retry::run_entrance_ladder(
            &*parked,
            &format!("entry channel `{requested}`"),
            // The budget of one, and the identity slot a frame-controlled name
            // never reaches, are the target's to decide.
            crate::retry::ParkTarget::unresolved(&requested),
            consumed_offset,
            call.as_ref(),
            Some(payload),
            error,
            // Unreachable at a budget of one, and the truth at any other:
            // the name is a fact of the frame.
            || Err("an unresolved channel name cannot resolve on a re-attempt".to_string()),
        );
    });
}

/// The value a frame becomes on its entry channel, or `None` when it left
/// the reader's hands — staged for a prepare crossing, or a failing parse on
/// the ladder.
#[allow(clippy::too_many_arguments)]
fn enter_channel(
    instance: &Arc<OperatorInstance>,
    channel_id: u32,
    channel: &ChannelDescriptor,
    payload: Vec<u8>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    call: Option<&Arc<str>>,
    replay: bool,
) -> Option<Arc<dyn Any + Send + Sync>> {
    let entry = crate::host::StagedEntry {
        bytes: payload,
        consumed_offset,
        call: call.cloned(),
        replay,
        stream: Arc::clone(stream),
        stage: None,
    };
    let payload = crate::host::stage_prepare(instance, channel_id, channel, vec![entry])
        .pop()?
        .bytes;
    // The ladder's re-attempt runs off the reader thread with no producing
    // Cell in hand, so it takes the entry form of delivery.
    let deliver = || {
        let instance = Arc::clone(instance);
        let stream = Arc::clone(stream);
        let call = call.cloned();
        Box::new(move |value: Arc<dyn Any + Send + Sync>| {
            let channel = instance
                .topology
                .channel(channel_id)
                .ok_or_else(|| format!("entry channel {channel_id} left the topology"))?;
            route_to_consumers(
                &instance,
                channel,
                &value,
                &stream,
                consumed_offset,
                call.as_ref(),
                replay,
                None,
            );
            Ok(())
        }) as EntryDeliver
    };
    wrap_entry(
        instance,
        channel_id,
        channel,
        payload,
        consumed_offset,
        call,
        deliver,
    )
}

/// The wire bytes through `wrap_input`. A parsed entry's failure rides the
/// ladder, so its bytes are kept and `deliver` is built for the re-attempts;
/// a passthrough entry keeps the drop-and-log path and no clone.
fn wrap_entry<V: EntryView>(
    instance: &Arc<V>,
    channel_id: u32,
    channel: &ChannelDescriptor,
    payload: Vec<u8>,
    consumed_offset: Option<u64>,
    call: Option<&Arc<str>>,
    deliver: impl FnOnce() -> EntryDeliver,
) -> Option<Arc<dyn Any + Send + Sync>> {
    let retry_bytes = channel.entry_parsed.then(|| payload.clone());
    let Some(wrap) = &channel.wrap_input else {
        eprintln!(
            "[flezha] operator {} channel `{}` has no entry wrapper",
            instance.instance_id(),
            channel.name
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
                    consumed_offset,
                    call: call.cloned(),
                    deliver: deliver(),
                }),
                None => eprintln!(
                    "[flezha] operator {} channel `{}`: wrap_input failed: {}",
                    instance.instance_id(),
                    channel.name,
                    e
                ),
            }
            None
        }
    }
}

#[cfg(all(test, unix))]
#[path = "routing_tests.rs"]
mod tests;
