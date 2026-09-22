//! The class registry (process-global, by `factory_id`) and the per-runner
//! instance registry (by `instance_id`), whose values are a sealed
//! [`Operative`] — stream or workflow — so the dispatch boundary resolves the
//! kind once per frame.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

use crate::cell::CellSlot;
use crate::operative::instance::StreamInstance;
use crate::operative::topology::OperativeTopology;
use crate::workflow::instance::WorkflowInstance;

/// A bound instance of either kind, carrying its own runtime state.
pub enum Operative {
    Stream(Arc<StreamInstance>),
    Workflow(Arc<WorkflowInstance>),
}

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
/// its kind, read from the registry the class landed in — the one lookup both
/// advertisements read. An undeclared factory answers
/// [`crate::header::SDK_LANG_UNSPECIFIED`] and an empty kind, which the
/// controller refuses to place.
pub fn advertised_for_factory(factory_id: &str) -> (u8, &'static str) {
    get_class(factory_id)
        .map(|topology| (topology.sdk_lang, "stream"))
        .or_else(|| {
            crate::workflow::registry::get_class(factory_id)
                .map(|spec| (spec.topology.sdk_lang, "workflow"))
        })
        .unwrap_or((crate::header::SDK_LANG_UNSPECIFIED, ""))
}

pub struct OperativeRegistry {
    inner: dashmap::DashMap<String, Arc<Operative>>,
}

impl OperativeRegistry {
    pub fn new() -> Self {
        Self {
            inner: dashmap::DashMap::new(),
        }
    }

    pub fn register_stream(&self, instance: Arc<StreamInstance>) -> Arc<StreamInstance> {
        let id = instance.instance_id.to_string();
        self.inner
            .insert(id, Arc::new(Operative::Stream(Arc::clone(&instance))));
        instance
    }

    pub fn register_workflow(&self, instance: Arc<WorkflowInstance>) {
        self.inner.insert(
            instance.instance_id.to_string(),
            Arc::new(Operative::Workflow(instance)),
        );
    }

    pub fn get_operative(&self, instance_id: &str) -> Option<Arc<Operative>> {
        self.inner.get(instance_id).map(|r| Arc::clone(r.value()))
    }

    /// `None` for a workflow or an absent id.
    pub fn get_stream(&self, instance_id: &str) -> Option<Arc<StreamInstance>> {
        self.inner
            .get(instance_id)
            .and_then(|r| match &**r.value() {
                Operative::Stream(ri) => Some(Arc::clone(ri)),
                Operative::Workflow(_) => None,
            })
    }

    /// `None` for a stream or an absent id.
    pub fn get_workflow(&self, instance_id: &str) -> Option<Arc<WorkflowInstance>> {
        self.inner
            .get(instance_id)
            .and_then(|r| match &**r.value() {
                Operative::Workflow(bi) => Some(Arc::clone(bi)),
                Operative::Stream(_) => None,
            })
    }

    /// A live workflow Cell slot by wire identity, with its owning instance.
    pub fn find_workflow_cell(
        &self,
        cell_id: &[u8],
    ) -> Option<(Arc<WorkflowInstance>, Arc<CellSlot>)> {
        self.inner.iter().find_map(|r| match &**r.value() {
            Operative::Workflow(bi) => bi
                .cell_by_identity(cell_id)
                .map(|slot| (Arc::clone(bi), slot)),
            Operative::Stream(_) => None,
        })
    }

    pub fn remove(&self, instance_id: &str) -> Option<Arc<Operative>> {
        self.inner.remove(instance_id).map(|(_, v)| v)
    }

    /// Live instances of `factory_id`, both kinds.
    pub fn count_by_factory(&self, factory_id: &str) -> usize {
        self.inner
            .iter()
            .filter(|r| match &**r.value() {
                Operative::Stream(ri) => ri.topology.factory_id == factory_id,
                Operative::Workflow(bi) => &*bi.factory_id == factory_id,
            })
            .count()
    }

    pub fn contains(&self, instance_id: &str) -> bool {
        self.inner.contains_key(instance_id)
    }

    /// The stream instances; a workflow carries no serializable state.
    pub fn snapshot_stream(&self) -> HashMap<String, Arc<StreamInstance>> {
        self.inner
            .iter()
            .filter_map(|r| match &**r.value() {
                Operative::Stream(ri) => Some((r.key().clone(), Arc::clone(ri))),
                Operative::Workflow(_) => None,
            })
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
