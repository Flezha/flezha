//! The class registry (process-global, by `factory_id`) and the per-runner
//! instance registry (by `instance_id`). One of each serves both kinds: a
//! class's kind is its topology's `workflow` field, and a workflow instance is
//! an [`OperativeInstance`] that also carries a run.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

use crate::cell::CellSlot;
use crate::operative::instance::OperativeInstance;
use crate::operative::topology::OperativeTopology;

static CLASS_REGISTRY: LazyLock<RwLock<HashMap<&'static str, Arc<OperativeTopology>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Re-registering a factory id replaces its topology.
pub fn register_class(factory_id: &'static str, topology: Arc<OperativeTopology>) {
    CLASS_REGISTRY.write().unwrap().insert(factory_id, topology);
}

/// Idempotent on an unknown id.
pub fn unregister_class(factory_id: &str) {
    CLASS_REGISTRY.write().unwrap().remove(factory_id);
}

pub fn get_class(factory_id: &str) -> Option<Arc<OperativeTopology>> {
    CLASS_REGISTRY.read().unwrap().get(factory_id).cloned()
}

pub fn class_factory_ids() -> Vec<&'static str> {
    CLASS_REGISTRY.read().unwrap().keys().copied().collect()
}

/// What a runner advertises for `factory_id`: the class's SDK language and
/// its kind. An undeclared factory answers
/// [`crate::header::SDK_LANG_UNSPECIFIED`] and an empty kind, which the
/// controller refuses to place.
pub fn advertised_for_factory(factory_id: &str) -> (u8, &'static str) {
    get_class(factory_id)
        .map(|topology| (topology.sdk_lang, kind_of(&topology)))
        .unwrap_or((crate::header::SDK_LANG_UNSPECIFIED, ""))
}

/// The kind a class declares, as the wire names it.
pub fn kind_of(topology: &OperativeTopology) -> &'static str {
    match topology.workflow {
        Some(_) => "workflow",
        None => "stream",
    }
}

pub struct OperativeRegistry {
    inner: dashmap::DashMap<String, Arc<OperativeInstance>>,
}

impl OperativeRegistry {
    pub fn new() -> Self {
        Self {
            inner: dashmap::DashMap::new(),
        }
    }

    pub fn register(&self, instance: Arc<OperativeInstance>) -> Arc<OperativeInstance> {
        self.inner
            .insert(instance.instance_id.to_string(), Arc::clone(&instance));
        instance
    }

    pub fn get(&self, instance_id: &str) -> Option<Arc<OperativeInstance>> {
        self.inner.get(instance_id).map(|r| Arc::clone(r.value()))
    }

    /// The Cell a wire identity names, with its instance: a keyed one is
    /// minted as [`OperativeInstance::slot_by_identity`] mints it.
    pub fn find_cell(&self, cell_id: &[u8]) -> Option<(Arc<OperativeInstance>, Arc<CellSlot>)> {
        let instance = self.instance_of_cell(cell_id)?;
        let slot = instance.slot_by_identity(cell_id)?;
        Some((instance, slot))
    }

    /// The live instance a wire identity names, whether or not it holds the
    /// Cell.
    pub fn instance_of_cell(&self, cell_id: &[u8]) -> Option<Arc<OperativeInstance>> {
        let (instance_id, _, _) = crate::flusher::decode_cell_identity(cell_id)?;
        self.get(&instance_id)
    }

    pub fn remove(&self, instance_id: &str) -> Option<Arc<OperativeInstance>> {
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

    /// The stream instances. A durable workflow is suspended through its
    /// journal and resumed at its stage, never re-seeded from a shipped state
    /// document, so the graceful-shutdown walk leaves it out.
    pub fn snapshot_stream(&self) -> HashMap<String, Arc<OperativeInstance>> {
        self.inner
            .iter()
            .filter(|r| r.value().workflow.is_none())
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

impl Default for OperativeRegistry {
    fn default() -> Self {
        Self::new()
    }
}
