//! The class registry (process-global, by `factory_id`) and the per-runner
//! instance registry (by `instance_id`). One of each serves both kinds: a
//! class's kind is its topology's `steps` field, and a steps instance is
//! an [`OperatorInstance`] that also carries a run.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

use crate::cell::CellSlot;
use crate::operator::instance::OperatorInstance;
use crate::operator::topology::OperatorTopology;

static CLASS_REGISTRY: LazyLock<RwLock<HashMap<&'static str, Arc<OperatorTopology>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Re-registering a factory id replaces its topology.
pub fn register_class(factory_id: &'static str, topology: Arc<OperatorTopology>) {
    CLASS_REGISTRY.write().unwrap().insert(factory_id, topology);
}

/// Idempotent on an unknown id.
pub fn unregister_class(factory_id: &str) {
    CLASS_REGISTRY.write().unwrap().remove(factory_id);
}

pub fn get_class(factory_id: &str) -> Option<Arc<OperatorTopology>> {
    CLASS_REGISTRY.read().unwrap().get(factory_id).cloned()
}

pub fn class_factory_ids() -> Vec<&'static str> {
    CLASS_REGISTRY.read().unwrap().keys().copied().collect()
}

/// What a runner advertises for `factory_id`: the class's SDK language, how
/// it runs and what it is for. An undeclared factory answers
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
    }
    decl
}

pub struct OperatorRegistry {
    inner: dashmap::DashMap<String, Arc<OperatorInstance>>,
}

impl OperatorRegistry {
    pub fn new() -> Self {
        Self {
            inner: dashmap::DashMap::new(),
        }
    }

    pub fn register(&self, instance: Arc<OperatorInstance>) -> Arc<OperatorInstance> {
        self.inner
            .insert(instance.instance_id.to_string(), Arc::clone(&instance));
        instance
    }

    pub fn get(&self, instance_id: &str) -> Option<Arc<OperatorInstance>> {
        self.inner.get(instance_id).map(|r| Arc::clone(r.value()))
    }

    /// The Cell a wire identity names, with its instance: a keyed one is
    /// minted as [`OperatorInstance::slot_by_identity`] mints it.
    pub fn find_cell(&self, cell_id: &[u8]) -> Option<(Arc<OperatorInstance>, Arc<CellSlot>)> {
        let instance = self.instance_of_cell(cell_id)?;
        let slot = instance.slot_by_identity(cell_id)?;
        Some((instance, slot))
    }

    /// The live instance a wire identity names, whether or not it holds the
    /// Cell.
    pub fn instance_of_cell(&self, cell_id: &[u8]) -> Option<Arc<OperatorInstance>> {
        let (instance_id, _, _) = crate::flusher::decode_cell_identity(cell_id)?;
        self.get(&instance_id)
    }

    pub fn remove(&self, instance_id: &str) -> Option<Arc<OperatorInstance>> {
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

    /// The flow instances. A durable steps operator is suspended through its
    /// journal and resumed at its stage, never re-seeded from a shipped state
    /// document, so the graceful-shutdown walk leaves it out.
    pub fn snapshot_stream(&self) -> HashMap<String, Arc<OperatorInstance>> {
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

impl Default for OperatorRegistry {
    fn default() -> Self {
        Self::new()
    }
}
