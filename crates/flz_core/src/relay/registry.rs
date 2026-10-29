//! The class registry (process-global, by `factory_id`) and the per-runner
//! instance registry (by `instance_id`). One of each serves both kinds: a
//! class's kind is its topology's `steps` field, and a steps instance is
//! a [`RelayInstance`] that also carries a run.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, RwLock};

use crate::cell::CellSlot;
use crate::channel::ChannelKind;
use crate::relay::instance::RelayInstance;
use crate::relay::topology::{ChannelApi, RelayTopology};

static CLASS_REGISTRY: LazyLock<RwLock<HashMap<&'static str, Arc<RelayTopology>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Re-registering a factory id replaces its topology.
pub fn register_class(factory_id: &'static str, topology: Arc<RelayTopology>) {
    CLASS_REGISTRY.write().unwrap().insert(factory_id, topology);
}

/// Idempotent on an unknown id.
pub fn unregister_class(factory_id: &str) {
    CLASS_REGISTRY.write().unwrap().remove(factory_id);
}

pub fn get_class(factory_id: &str) -> Option<Arc<RelayTopology>> {
    CLASS_REGISTRY.read().unwrap().get(factory_id).cloned()
}

pub fn class_factory_ids() -> Vec<&'static str> {
    CLASS_REGISTRY.read().unwrap().keys().copied().collect()
}

/// The announcement of every factory in `ids`, which the connect frame and
/// `UpdateFactories` both carry.
pub fn advertised(ids: &[String]) -> Vec<crate::pb::FactoryDecl> {
    ids.iter().map(|id| advertised_for_factory(id)).collect()
}

/// What a runner advertises for `factory_id`: the class's SDK language, how
/// it runs, what it is for, its API, its graph and its declarations (the
/// `AddRelay` answer's metadata). An undeclared factory answers
/// [`crate::header::SDK_LANG_UNSPECIFIED`] and an empty run, which the
/// controller refuses to place.
pub fn advertised_for_factory(factory_id: &str) -> crate::pb::FactoryDecl {
    let mut decl = crate::pb::FactoryDecl {
        id: factory_id.to_string(),
        sdk_lang: crate::header::SDK_LANG_UNSPECIFIED as u32,
        ..Default::default()
    };
    if let Some(topology) = get_class(factory_id) {
        decl.sdk_lang = topology.sdk_lang as u32;
        decl.description = topology.description.to_string();
        match &topology.steps {
            Some(spec) => {
                decl.run = "steps".to_string();
                decl.each = spec.each.as_str().to_string();
                decl.on_failure = spec.on_failure.as_str().to_string();
            }
            None => decl.run = "flow".to_string(),
        }
        decl.api = api_entries(&topology);
        decl.graph = Some(graph(&topology));
        decl.declarations = crate::command::class_metadata(&topology);
    }
    decl
}

/// The class's tasklets and channels by name.
fn graph(topology: &RelayTopology) -> crate::pb::RelayGraph {
    let steps = topology.steps.is_some();
    crate::pb::RelayGraph {
        channels: topology
            .channels
            .iter()
            .map(|channel| graph_channel(topology, channel))
            .collect(),
        tasklets: topology
            .tasklets
            .iter()
            .map(|tasklet| graph_tasklet(steps, tasklet))
            .collect(),
    }
}

/// The plugin end is read from `sink` and `call`, which every builder sets; a
/// host-built class leaves `plugin_sink` unset.
fn graph_channel(
    topology: &RelayTopology,
    channel: &crate::channel::ChannelDescriptor,
) -> crate::pb::GraphChannel {
    let tasklet_name = |id: crate::channel::TaskletId| {
        topology
            .tasklets
            .get(id as usize)
            .map(|tasklet| tasklet.name.to_string())
            .unwrap_or_default()
    };
    let call = channel.call.as_ref();
    crate::pb::GraphChannel {
        name: channel.name.to_string(),
        kind: channel.kind.as_str().to_string(),
        producer: channel.producer.map(tasklet_name).unwrap_or_default(),
        consumers: channel.consumer_ids().map(tasklet_name).collect(),
        plugin_end: match (call, channel.sink) {
            (Some(_), _) => "call",
            (None, true) => "output",
            (None, false) => "none",
        }
        .to_string(),
        call_target: call.map(|call| call.target.to_string()).unwrap_or_default(),
        overflow: channel.overflow_policy.as_str().to_string(),
        retry_max: Some(channel.retry.max_attempts),
        retry_backoff_ms: Some(channel.retry.backoff_base_ms),
        call_deadline_ms: call.map(|call| call.deadline_ms),
        call_create: call.map(|call| call.create),
    }
}

/// Only a steps class compensates, and only a dynamic tasklet announces its
/// Cell bounds.
fn graph_tasklet(
    steps: bool,
    tasklet: &crate::relay::topology::TaskletDescriptor,
) -> crate::pb::GraphTasklet {
    let dynamic = tasklet.mode == crate::relay::topology::CellMode::Dynamic;
    crate::pb::GraphTasklet {
        name: tasklet.name.to_string(),
        mode: tasklet.mode.as_str().to_string(),
        cell_keys: tasklet
            .cell_keys
            .iter()
            .map(|key| key.to_string())
            .collect(),
        mailbox_capacity: u32::try_from(tasklet.mailbox_capacity).unwrap_or(u32::MAX),
        compensates: Some(steps && tasklet.compensate_overridden),
        idle_ttl_ms: tasklet
            .idle_ttl
            .filter(|_| dynamic)
            .map(|ttl| u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)),
        max_cells: u32::try_from(tasklet.max_cells).ok().filter(|_| dynamic),
    }
}

/// One entry per entry channel a caller can address (not the channel a call
/// end's outcome comes back on), with the answer of the outputs that answer
/// reachable from it.
fn api_entries(topology: &RelayTopology) -> Vec<crate::pb::ApiEntry> {
    let responses: HashSet<&str> = topology
        .channels
        .iter()
        .filter_map(|channel| channel.call.as_ref())
        .map(|call| &*call.response)
        .collect();
    topology
        .channels
        .iter()
        .enumerate()
        .filter(|(_, c)| c.kind == ChannelKind::Entry && !responses.contains(&*c.name))
        .map(|(id, channel)| {
            let facts = api_facts(topology, id);
            let (answer_codec, answer_schema_json) = answer(topology, id).unwrap_or_default();
            crate::pb::ApiEntry {
                channel: channel.name.to_string(),
                codec: codec_name(facts).to_string(),
                schema_json: facts.schema_json.clone(),
                description: facts.description.clone(),
                answer_codec,
                answer_schema_json,
            }
        })
        .collect()
}

/// The format and schema of the outputs that answer reachable from `entry`
/// along its consumer edges: theirs when they agree, `bytes` with no schema
/// when they do not, `None` when none is reachable.
fn answer(topology: &RelayTopology, entry: usize) -> Option<(String, String)> {
    let mut seen = vec![false; topology.channels.len()];
    let mut pending = vec![entry];
    let mut answers = HashSet::new();
    while let Some(id) = pending.pop() {
        if std::mem::replace(&mut seen[id], true) {
            continue;
        }
        let channel = &topology.channels[id];
        if channel.sink {
            let facts = api_facts(topology, id);
            answers.insert((codec_name(facts), facts.schema_json.as_str()));
        }
        pending.extend(
            channel
                .consumer_ids()
                .filter_map(|tasklet| topology.tasklets.get(tasklet as usize))
                .filter_map(|tasklet| tasklet.output_channel_id)
                .map(|out| out as usize),
        );
    }
    match answers.len() {
        0 => None,
        1 => answers
            .into_iter()
            .next()
            .map(|(codec, schema)| (codec.to_string(), schema.to_string())),
        _ => Some(("bytes".to_string(), String::new())),
    }
}

fn api_facts(topology: &RelayTopology, channel: usize) -> &ChannelApi {
    static NONE: ChannelApi = ChannelApi {
        codec: "",
        schema_json: String::new(),
        description: String::new(),
    };
    topology.api.get(channel).unwrap_or(&NONE)
}

fn codec_name(facts: &ChannelApi) -> &'static str {
    if facts.codec.is_empty() {
        "json"
    } else {
        facts.codec
    }
}

pub struct RelayRegistry {
    inner: dashmap::DashMap<String, Arc<RelayInstance>>,
}

impl RelayRegistry {
    pub fn new() -> Self {
        Self {
            inner: dashmap::DashMap::new(),
        }
    }

    pub fn register(&self, instance: Arc<RelayInstance>) -> Arc<RelayInstance> {
        self.inner
            .insert(instance.instance_id.to_string(), Arc::clone(&instance));
        instance
    }

    pub fn get(&self, instance_id: &str) -> Option<Arc<RelayInstance>> {
        self.inner.get(instance_id).map(|r| Arc::clone(r.value()))
    }

    /// The Cell a wire identity names, with its instance: a keyed one is
    /// minted as [`RelayInstance::slot_by_identity`] mints it.
    pub fn find_cell(&self, cell_id: &[u8]) -> Option<(Arc<RelayInstance>, Arc<CellSlot>)> {
        let instance = self.instance_of_cell(cell_id)?;
        let slot = instance.slot_by_identity(cell_id)?;
        Some((instance, slot))
    }

    /// The live instance a wire identity names, whether or not it holds the
    /// Cell.
    pub fn instance_of_cell(&self, cell_id: &[u8]) -> Option<Arc<RelayInstance>> {
        let (instance_id, _, _) = crate::flusher::decode_cell_identity(cell_id)?;
        self.get(&instance_id)
    }

    pub fn remove(&self, instance_id: &str) -> Option<Arc<RelayInstance>> {
        self.inner.remove(instance_id).map(|(_, v)| v)
    }

    /// Live instances of `factory_id`, both kinds.
    pub fn count_by_factory(&self, factory_id: &str) -> usize {
        self.inner
            .iter()
            .filter(|r| r.value().topology.factory_id == factory_id)
            .count()
    }

    pub fn contains(&self, instance_id: &str) -> bool {
        self.inner.contains_key(instance_id)
    }

    /// Every live instance's id, both kinds, sorted.
    pub fn ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.inner.iter().map(|r| r.key().clone()).collect();
        ids.sort_unstable();
        ids
    }

    /// The flow instances. A durable steps relay is suspended through its
    /// journal and resumed at its stage, never re-seeded from a shipped state
    /// document, so the graceful-shutdown walk leaves it out.
    pub fn snapshot_stream(&self) -> HashMap<String, Arc<RelayInstance>> {
        self.inner
            .iter()
            .filter(|r| r.value().steps.is_none())
            .map(|r| (r.key().clone(), Arc::clone(r.value())))
            .collect()
    }

    pub fn clear(&self) {
        self.inner.clear();
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl Default for RelayRegistry {
    fn default() -> Self {
        Self::new()
    }
}
