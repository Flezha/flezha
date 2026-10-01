//! A stepped scope: an edge that ends in a steps class runs one instance of
//! that class per edge key (one on an unkeyed edge) inside the outer
//! instance. Each is an ordinary steps [`OperatorInstance`] of the inner
//! class, with its own pass sheet, Cells and deadlines, so the steps engine
//! runs it unchanged; what is its own is where its frames say they come from
//! (the outer instance and the scope) and where its exit output goes (into
//! the outer graph, after the node that embeds it).
//!
//! A scope inside a durable outer instance is bound by the controller: every
//! message is first appended to the scope's input log (`ScopeInput`), and the
//! controller binds the scope (`AddOperator` with the scope), restores its
//! Cells and forwards the message to it, as it binds a top-level steps
//! instance. Inside any other instance a scope is minted here, on its key's
//! first message.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};

use dashmap::DashMap;
use flz_transport::transport::FlezhaStream;

use crate::channel::{ChannelDescriptor, ChannelId, TaskletId};
use crate::dynamic::DynamicCellCounters;
use crate::error::Error;
use crate::host::TopologyIssue;
use crate::operator::instance::{KeyGroupRange, OperatorInstance};
use crate::operator::registry::OperatorRegistry;
use crate::operator::topology::OperatorTopology;
use crate::util::LockRecover;

type Value = Arc<dyn Any + Send + Sync>;

/// What a graph node that embeds a steps class runs: the class, the entry
/// channel its edge feeds and the tasklet whose output continues on the
/// outer edge after it.
pub struct ScopeNode {
    pub inner: Arc<OperatorTopology>,
    pub entry: ChannelId,
    pub exit: Option<TaskletId>,
    /// How a typed SDK's edge value becomes the bytes a scope's input log
    /// keeps; a host's value is bytes already.
    pub input_codec: Option<crate::channel::ChannelCodec>,
}

/// Where a scope instance sits: its outer instance, the node that embeds it,
/// that node's name and the edge key.
pub struct ScopeOf {
    pub parent: Weak<OperatorInstance>,
    pub outer_id: Arc<str>,
    pub node: TaskletId,
    pub inner: Arc<str>,
    pub key: Option<Arc<str>>,
    /// The connection the outer instance's last message for this scope came
    /// on, which its exit output rides on: a local run's own, where the
    /// runner's secondary stream does not exist.
    stream: Mutex<Option<Arc<FlezhaStream>>>,
}

impl ScopeOf {
    pub fn wire(&self) -> crate::pb::Scope {
        crate::pb::Scope {
            inner: self.inner.to_string(),
            key: self.key.as_deref().map(str::to_string),
        }
    }
}

/// One embedding node's scopes on one outer instance, by edge key.
pub struct ScopeFamily {
    node: TaskletId,
    name: Arc<str>,
    max_cells: usize,
    children: DashMap<Option<String>, Arc<OperatorInstance>>,
    /// Keys forwarded to the controller whose scope is not bound yet; they
    /// count against the cap as a bound scope does.
    pending: Mutex<HashSet<Option<String>>>,
    /// How many values each outer input sent each key in this life of the
    /// instance, until the processed prefix passes that input: a value's
    /// place among its input's names it in the scope's log, so a replay of
    /// the input appends nothing new.
    sent: Mutex<HashMap<(Option<String>, u64), u32>>,
    /// The holds of inputs whose message the controller refused to log: the
    /// processed prefix stays below them, so the instance's next bind
    /// forwards them again.
    refused: Mutex<Vec<Arc<crate::prefix::Hold>>>,
    pub counters: DynamicCellCounters,
}

/// The local id of a scope: its instance id on the runner, the instance
/// element of its Cells' identities and its directory under the outer
/// instance's. The key is encoded as the controller encodes a Cell key on
/// disk (`Flezha.State.Paths.encode_cell_key/1`).
pub fn scope_id(instance: &str, inner: &str, key: Option<&str>) -> String {
    match key {
        Some(key) => format!("{instance}/{inner}/{}", encode_key(key)),
        None => format!("{instance}/{inner}"),
    }
}

/// Whether `suffix` is what a scope's id adds to its instance's:
/// `/<inner>` or `/<inner>/<key>`, no segment empty.
pub(crate) fn is_scope_suffix(suffix: &str) -> bool {
    suffix.strip_prefix('/').is_some_and(|rest| {
        let mut segments = 0;
        rest.split('/').all(|segment| {
            segments += 1;
            !segment.is_empty()
        }) && segments <= 2
    })
}

fn encode_key(key: &str) -> String {
    let safe = !key.is_empty()
        && !key.starts_with('_')
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'));
    if safe {
        key.to_string()
    } else {
        format!("_{}", base64_url(key.as_bytes()))
    }
}

/// Unpadded URL-safe base64, the controller's `Base.url_encode64(padding: false)`.
fn base64_url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, &b)| n | (b as u32) << (16 - 8 * i));
        for i in 0..=chunk.len() {
            out.push(ALPHABET[(n >> (18 - 6 * i) & 63) as usize] as char);
        }
    }
    out
}

/// The node an edge into the class `inner` becomes inside the class `outer`,
/// as [`crate::host::judge_embedding`] judges it.
pub fn embed(
    outer: &str,
    outer_steps: bool,
    inner: &str,
    input: Option<&str>,
) -> Result<ScopeNode, TopologyIssue> {
    let (inner, entry, exit) = crate::host::judge_embedding(outer, outer_steps, inner, input)?;
    Ok(ScopeNode {
        entry,
        exit,
        inner,
        input_codec: None,
    })
}

impl ScopeFamily {
    pub fn new(node: TaskletId, name: Arc<str>, max_cells: usize) -> Self {
        Self {
            node,
            name,
            max_cells,
            children: DashMap::new(),
            pending: Mutex::new(HashSet::new()),
            sent: Mutex::new(HashMap::new()),
            refused: Mutex::new(Vec::new()),
            counters: DynamicCellCounters::default(),
        }
    }

    pub fn name(&self) -> &Arc<str> {
        &self.name
    }

    /// The scopes alive on this runner right now.
    pub fn children(&self) -> Vec<Arc<OperatorInstance>> {
        self.children
            .iter()
            .map(|e| Arc::clone(e.value()))
            .collect()
    }

    /// Whether `key` may hold a scope: it holds one, or the cap leaves room
    /// for one more. A refusal is counted as a dynamic Cell's is.
    fn admit(&self, key: &Option<String>) -> bool {
        let pending = self.pending.lock_recover();
        if self.children.contains_key(key) || pending.contains(key) {
            return true;
        }
        if self.children.len() + pending.len() >= self.max_cells {
            self.counters
                .inputs_dropped_capped_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return false;
        }
        true
    }

    /// The identity of the next value input `offset` of `outer` sends `key`.
    fn msg_id(&self, outer: &OperatorInstance, key: &Option<String>, offset: u64) -> String {
        let mut sent = self.sent.lock_recover();
        if let Some(done) = outer.prefix.as_ref().and_then(|p| p.reported()) {
            sent.retain(|(_, origin), _| *origin > done);
        }
        let count = sent.entry((key.clone(), offset)).or_insert(0);
        *count += 1;
        format!("{}@{offset}:{}", outer.instance_id, *count - 1)
    }

    /// The controller took no scope for `key`: its message went to the
    /// dead-letter queue, or its bind failed.
    pub(crate) fn not_pending(&self, key: &Option<String>) {
        self.pending.lock_recover().remove(key);
    }
}

impl OperatorInstance {
    /// The scope this instance runs as, `None` for a top-level instance.
    pub fn scope(&self) -> Option<&ScopeOf> {
        self.scope_of.get()
    }

    /// The id every frame names: the outer instance's for a scope.
    pub fn operator_id(&self) -> &str {
        self.scope()
            .map_or(&self.instance_id, |scope| &scope.outer_id)
    }

    /// The scope every frame carries, absent for a top-level instance.
    pub fn wire_scope(&self) -> Option<crate::pb::Scope> {
        self.scope().map(ScopeOf::wire)
    }

    /// The tasklet whose output leaves this scope for the outer graph.
    pub(crate) fn exit_tasklet(&self) -> Option<TaskletId> {
        let scope = self.scope()?;
        let outer = scope.parent.upgrade()?;
        outer.topology.tasklet(scope.node)?.scope.as_ref()?.exit
    }

    /// The family of the node `tasklet` when it embeds a steps class.
    pub fn family(&self, tasklet: TaskletId) -> Option<&Arc<ScopeFamily>> {
        self.families.get(tasklet as usize)?.as_ref()
    }

    /// The family embedding node `inner` names.
    fn family_named(&self, inner: &str) -> Option<&Arc<ScopeFamily>> {
        self.families.iter().flatten().find(|f| &*f.name == inner)
    }
}

/// One value on an edge into an embedding node, for each key it reaches: a
/// durable outer instance hands it to the controller, any other one to the
/// key's scope, minted on first sight.
#[allow(clippy::too_many_arguments)]
pub(crate) fn deliver(
    outer: &OperatorInstance,
    family: &Arc<ScopeFamily>,
    channel: &ChannelDescriptor,
    keys: Vec<Option<String>>,
    value: &Value,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    call: Option<&Arc<str>>,
) -> Result<(), Error> {
    let node = outer
        .topology
        .tasklet(family.node)
        .and_then(|t| t.scope.as_ref())
        .expect("a family's node embeds a class");
    for key in keys {
        if !family.admit(&key) {
            continue;
        }
        if outer.durable() {
            forward(outer, family, node, channel, key, value, consumed_offset)?;
        } else {
            let child = family
                .children
                .entry(key.clone())
                .or_insert_with(|| mint(outer, family, node, key));
            enter(&child, node, value, stream, call);
        }
    }
    Ok(())
}

/// A scope inside an instance that keeps nothing: bound here, with nothing
/// to restore.
fn mint(
    outer: &OperatorInstance,
    family: &ScopeFamily,
    node: &ScopeNode,
    key: Option<String>,
) -> Arc<OperatorInstance> {
    let id = scope_id(&outer.instance_id, &family.name, key.as_deref());
    let child = OperatorInstance::with_options(
        Arc::clone(&node.inner),
        Arc::from(id),
        Some(false),
        KeyGroupRange::UNPARTITIONED,
    );
    place(&child, outer, family, key);
    if let Some(run) = &child.steps {
        run.bind(Default::default());
    }
    crate::steps::open(&child);
    child
}

fn place(
    child: &OperatorInstance,
    outer: &OperatorInstance,
    family: &ScopeFamily,
    key: Option<String>,
) {
    let _ = child.scope_of.set(ScopeOf {
        parent: outer.me.clone(),
        outer_id: Arc::clone(&outer.instance_id),
        node: family.node,
        inner: Arc::clone(&family.name),
        key: key.map(Arc::from),
        stream: Mutex::new(None),
    });
}

/// A value into a scope's entry channel, through the channel's own wrapper
/// when the edge carries bytes, so a host's payload names the inner channel.
fn enter(
    child: &Arc<OperatorInstance>,
    node: &ScopeNode,
    value: &Value,
    stream: &Arc<FlezhaStream>,
    call: Option<&Arc<str>>,
) {
    let Some(entry) = child.topology.channel(node.entry) else {
        return;
    };
    if let Some(scope) = child.scope() {
        *scope.stream.lock_recover() = Some(Arc::clone(stream));
    }
    let entered = match (
        value.downcast_ref::<crate::host::HostPayload>(),
        &entry.wrap_input,
    ) {
        (Some(payload), Some(wrap)) => match wrap(payload.bytes.clone()) {
            Ok(wrapped) => wrapped,
            Err(_) => return,
        },
        _ => Arc::clone(value),
    };
    crate::steps::message(child, entry, &entered, stream, None, call);
}

/// The bytes of a value on the outer edge, as the scope's input log keeps
/// them: a host's payload as it is, a typed value through the codec its SDK
/// derived for the embedding node.
fn edge_bytes(
    node: &ScopeNode,
    channel: &ChannelDescriptor,
    value: &Value,
) -> Result<Vec<u8>, String> {
    if let Some(payload) = value.downcast_ref::<crate::host::HostPayload>() {
        return Ok(payload.bytes.clone());
    }
    let codec = node
        .input_codec
        .as_ref()
        .ok_or_else(|| format!("the edge on channel `{}` has no codec", channel.name))?;
    (codec.encode)(value.as_ref())
}

/// A message for a durable scope to the controller, which journals it to
/// the scope's input log and forwards it. The outer instance's input is not
/// confirmed processed until the controller logged it; a value that cannot
/// be encoded fails its edge, which parks it in the dead-letter queue.
fn forward(
    outer: &OperatorInstance,
    family: &Arc<ScopeFamily>,
    node: &ScopeNode,
    channel: &ChannelDescriptor,
    key: Option<String>,
    value: &Value,
    consumed_offset: Option<u64>,
) -> Result<(), Error> {
    let payload = edge_bytes(node, channel, value).map_err(|reason| {
        Error::Routing(crate::error::Code::ScopeInputUnencodable.with([
            ("scope", family.name.to_string().into()),
            ("reason", reason.into()),
        ]))
    })?;
    let entry = node
        .inner
        .channel(node.entry)
        .map(|c| c.name.to_string())
        .unwrap_or_default();
    let hold = consumed_offset
        .zip(outer.prefix.as_ref())
        .map(|(offset, prefix)| prefix.hold(offset));
    let msg_id = consumed_offset
        .map(|offset| family.msg_id(outer, &key, offset))
        .unwrap_or_default();
    if !family.children.contains_key(&key) {
        family.pending.lock_recover().insert(key.clone());
    }
    let input = crate::pb::ScopeInput {
        operator_id: outer.instance_id.to_string(),
        scope: Some(crate::pb::Scope {
            inner: family.name.to_string(),
            key,
        }),
        channel: entry,
        payload,
        msg_id,
    };
    send_input(Arc::downgrade(family), input, hold, 1);
    Ok(())
}

/// One send of a scope input. A refused append is sent again, up to the
/// barrier budget, and then keeps its input's hold; a message the controller
/// dead-lettered instead binds no scope for its key.
fn send_input(
    family: Weak<ScopeFamily>,
    input: crate::pb::ScopeInput,
    hold: Option<Arc<crate::prefix::Hold>>,
    sends: u32,
) {
    let reference_id = format!(
        "scope-input-{}-{}",
        input.operator_id,
        crate::util::nonce_suffix()
    );
    let resend = input.clone();
    crate::shutdown::register_pending_ack(
        reference_id.clone(),
        Box::new(move |answer| {
            let Some(family) = family.upgrade() else {
                return;
            };
            let key = resend.scope.as_ref().and_then(|s| s.key.clone());
            match answer {
                Ok(outcome) if outcome == REFUSED => family.not_pending(&key),
                Ok(_) => {}
                Err(_) if sends < crate::steps::BARRIER_SENDS => {
                    send_input(Arc::downgrade(&family), resend, hold, sends + 1)
                }
                Err(error) => {
                    tracing::warn!(target: "flezha::scope", scope = %family.name, %error, "a message into a scope was not logged; its input stays unconfirmed");
                    family.refused.lock_recover().extend(hold);
                }
            }
        }),
    );
    crate::runner::emit_control_request(
        reference_id,
        crate::pb::command_request::Command::ScopeInput(input),
    );
}

/// The outcome a `ScopeInputResponse` with `refused` set answers.
pub(crate) const REFUSED: &str = "refused";

/// A message the controller forwarded to a scope of a typed SDK: the edge
/// value's bytes in its embedding node's codec, read back as that value, with
/// the entry channel it enters. `None` for a host's payload and for a
/// top-level instance, which enter through their entry channel's own wrapper.
pub(crate) fn forwarded(child: &OperatorInstance, bytes: &[u8]) -> Option<(ChannelId, Value)> {
    let scope = child.scope()?;
    let outer = scope.parent.upgrade()?;
    let node = outer.topology.tasklet(scope.node)?.scope.as_ref()?;
    let codec = node.input_codec.as_ref()?;
    let value = (codec.decode)(bytes.to_vec()).ok()?;
    Some((node.entry, value))
}

/// A bound scope's instance joins its outer instance's family; `false` when
/// the outer instance or its node is not here.
pub fn attach(
    registry: &OperatorRegistry,
    child: &OperatorInstance,
    operator_id: &str,
    scope: &crate::pb::Scope,
) -> bool {
    let Some(outer) = registry.get(operator_id) else {
        return false;
    };
    let Some(family) = outer.family_named(&scope.inner) else {
        return false;
    };
    place(child, &outer, family, scope.key.clone());
    let child = match registry.get(&child.instance_id) {
        Some(child) => child,
        None => return false,
    };
    family.pending.lock_recover().remove(&scope.key);
    family.children.insert(scope.key.clone(), child);
    true
}

/// The controller stops `scope` of `outer`, bound or not: its key no longer
/// waits for a bind.
pub fn released(outer: Option<&OperatorInstance>, scope: &crate::pb::Scope) {
    if let Some(family) = outer.and_then(|outer| outer.family_named(&scope.inner)) {
        family.not_pending(&scope.key);
    }
}

/// A scope instance leaves its family: stopped, slept or finished.
pub fn detach(child: &OperatorInstance) {
    let Some(scope) = child.scope() else {
        return;
    };
    let Some(outer) = scope.parent.upgrade() else {
        return;
    };
    if let Some(family) = outer.family(scope.node) {
        let key = scope.key.as_deref().map(str::to_string);
        family
            .children
            .remove_if(&key, |_, held| held.seq == child.seq);
    }
}

/// A scope that ran to its end. One the controller bound is stopped by it;
/// one minted here leaves its family, so its key's next message starts a
/// new one, except a `once` scope, which stays finished.
pub(crate) fn ended(child: &OperatorInstance, each: crate::steps::Each) {
    if child.scope().is_some() && !child.durable() && each != crate::steps::Each::Once {
        detach(child);
    }
}

/// Every scope of an outer instance that stops goes with it.
pub fn stop_children(outer: &OperatorInstance, registry: &OperatorRegistry) {
    for family in outer.families.iter().flatten() {
        for entry in family.children.iter() {
            let child = entry.value();
            registry.remove(&child.instance_id);
            child.shutdown_background_threads();
            crate::steps::removed(child);
        }
        family.children.clear();
        family.pending.lock_recover().clear();
    }
}

/// A scope's exit values continue on the outer edge after the node that
/// embeds it; a host's payload is relabelled as the outer channel's, so it
/// names that channel.
pub(crate) fn exit(child: &OperatorInstance, values: Vec<Value>) {
    let Some(scope) = child.scope() else {
        return;
    };
    let Some(outer) = scope.parent.upgrade() else {
        return;
    };
    let Some((channel_id, channel)) = outer_channel(&outer, scope.node) else {
        return;
    };
    let stream = scope
        .stream
        .lock_recover()
        .clone()
        .unwrap_or_else(crate::steps::unwired_stream);
    for value in values {
        let value = match value.downcast_ref::<crate::host::HostPayload>() {
            Some(payload) => host_value(channel_id, payload.bytes.clone()),
            None => value,
        };
        crate::routing::route_to_consumers(
            &outer, channel, &value, &stream, None, None, false, None,
        );
    }
}

/// Whether a scope's exit values go through its outer instance's input log:
/// a scope that journals, inside an instance whose input is journaled (the
/// controller's `Replay.floor_reporter/1`: a kept flow instance on the
/// effectively-once tier, or one that sleeps), puts them on the commit that
/// makes them final, so none is lost with the runner before the outer graph
/// took it. Any other scope's are as durable as its instance's own input:
/// routed in memory, at most once.
pub(crate) fn exits_logged(child: &OperatorInstance) -> bool {
    child.durable()
        && child
            .scope()
            .and_then(|scope| scope.parent.upgrade())
            .is_some_and(|outer| {
                outer.durable()
                    && (outer.topology.effectively_once || outer.topology.sleep_after().is_some())
            })
}

/// The exit a commit of a logged scope carries: `values` and a completion's
/// `result`, in the bytes the channel after the embedding node reads; `None`
/// when nothing leaves.
pub(crate) fn wire_exit(
    child: &OperatorInstance,
    values: &[Value],
    result: Option<&[u8]>,
) -> Result<Option<crate::pb::ScopeExit>, String> {
    let result = result.filter(|r| !r.is_empty());
    if !exits_logged(child) || (values.is_empty() && result.is_none()) {
        return Ok(None);
    }
    let outer = child.scope().and_then(|scope| scope.parent.upgrade());
    let Some((_, channel)) = outer.as_deref().and_then(|outer| {
        child
            .scope()
            .and_then(|scope| outer_channel(outer, scope.node))
    }) else {
        return Ok(None);
    };
    let mut bytes = Vec::with_capacity(values.len() + 1);
    for value in values {
        match value.downcast_ref::<crate::host::HostPayload>() {
            Some(payload) => bytes.push(payload.bytes.clone()),
            None => {
                let codec = channel
                    .codec
                    .as_ref()
                    .ok_or_else(|| format!("the exit channel `{}` has no codec", channel.name))?;
                bytes.push((codec.encode)(value.as_ref())?);
            }
        }
    }
    bytes.extend(result.map(<[u8]>::to_vec));
    Ok(Some(crate::pb::ScopeExit {
        channel: channel.name.to_string(),
        values: bytes,
    }))
}

/// A scope's exit value the controller logged as an input of `instance` and
/// forwards (`flz.exit` names its channel): it enters the outer graph on the
/// channel after the embedding node, at its offset. One naming no such
/// channel, or that does not decode, is parked in the dead-letter queue.
pub(crate) fn exited(
    instance: &Arc<OperatorInstance>,
    channel_name: &str,
    bytes: Vec<u8>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    call: Option<&Arc<str>>,
) {
    let after = instance.families.iter().flatten().find_map(|family| {
        let (id, channel) = outer_channel(instance, family.node)?;
        (&*channel.name == channel_name).then_some((family.node, id, channel))
    });
    let Some((node, id, channel)) = after else {
        let error = format!("no channel `{channel_name}` follows an embedding node");
        return park_exit(
            instance,
            None,
            channel_name,
            bytes,
            consumed_offset,
            call,
            error,
        );
    };
    let value = if instance
        .topology
        .tasklet(node)
        .is_some_and(|t| t.host_facts.is_some())
    {
        host_value(id, bytes)
    } else {
        let decoded = match channel.codec.as_ref() {
            Some(codec) => (codec.decode)(bytes.clone()),
            None => Err(format!("the exit channel `{channel_name}` has no codec")),
        };
        match decoded {
            Ok(value) => value,
            Err(error) => {
                return park_exit(
                    instance,
                    Some(id),
                    channel_name,
                    bytes,
                    consumed_offset,
                    call,
                    error,
                )
            }
        }
    };
    crate::routing::route_to_consumers(
        instance,
        channel,
        &value,
        stream,
        consumed_offset,
        call,
        false,
        None,
    );
}

/// An exit value the outer graph cannot take, parked under its channel's
/// edge slot, or the unresolved slot when no such channel exists.
fn park_exit(
    instance: &Arc<OperatorInstance>,
    channel: Option<ChannelId>,
    channel_name: &str,
    bytes: Vec<u8>,
    consumed_offset: Option<u64>,
    call: Option<&Arc<str>>,
    error: String,
) {
    let parked = Arc::clone(instance);
    let name = channel_name.to_string();
    let call = call.cloned();
    crate::retry::spawn_park_thread(instance, "flezha-exit-park", move || {
        let target = match channel.and_then(|id| parked.topology.channel(id)) {
            Some(channel) => crate::retry::ParkTarget::edge(channel),
            None => crate::retry::ParkTarget::unresolved(&name),
        };
        crate::retry::park_exhausted(
            &format!("exit value on `{name}`"),
            target,
            &parked.instance_id,
            consumed_offset,
            call.as_ref(),
            Some(bytes),
            error,
            1,
        );
    });
}

/// The channel after the node `node` embeds, with its id.
fn outer_channel(
    outer: &OperatorInstance,
    node: TaskletId,
) -> Option<(ChannelId, &ChannelDescriptor)> {
    let id = outer.topology.tasklet(node)?.output_channel_id?;
    Some((id, outer.topology.channel(id)?))
}

fn host_value(channel: ChannelId, bytes: Vec<u8>) -> Value {
    Arc::new(crate::host::HostPayload {
        bytes,
        channel,
        keys: Vec::new(),
        event_time: None,
        metadata: Vec::new(),
    })
}

/// A completion result as a value of the channel after the embedding node;
/// nothing for an end that carried none.
pub(crate) fn result_value(child: &OperatorInstance, result: &[u8]) -> Option<Value> {
    if result.is_empty() {
        return None;
    }
    let scope = child.scope()?;
    let outer = scope.parent.upgrade()?;
    let (id, channel) = outer_channel(&outer, scope.node)?;
    if outer.topology.tasklet(scope.node)?.host_facts.is_some() {
        return Some(host_value(id, result.to_vec()));
    }
    (channel.codec.as_ref()?.decode)(result.to_vec()).ok()
}

#[cfg(test)]
#[path = "scope_tests.rs"]
mod tests;
