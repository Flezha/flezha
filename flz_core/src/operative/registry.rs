//! StreamOperative class + instance registries.
//!
//! `OperativeClassRegistry`: process-global; one entry per StreamOperative class. Holds
//! the immutable `Arc<OperativeTopology>` produced by `define_stream::<R>()`.
//! Looked up by controller-facing `factory_id` when `AddOperative` arrives.
//!
//! `OperativeRegistry`: per-`RunnerNode`; one entry per
//! `AddOperative` (instance_id). Reuses the DashMap pattern from the
//! pre-change `OperativeRegistry` so concurrent reader/writer access is
//! procedureed. Values are a sealed [`Operative`] — operative or workflow — so the
//! dispatch boundary resolves the variant once per frame; the `Arc` keeps
//! map values pointer-sized.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

use crate::entity::EntitySlot;
use crate::operative::instance::StreamInstance;
use crate::operative::topology::OperativeTopology;
use crate::workflow::instance::WorkflowInstance;

/// A control-plane-managed unit instance. Sealed two-variant enum: a new kind
/// must be handled at every match, and the 2-variant match keeps the operative hot
/// path branch-predictable rather than devirtualizing a single variant. Each
/// variant carries its per-instance runtime state — a workflow operative's
/// cancel/inbox/Entities live on its [`WorkflowInstance`] (no workflow-specific
/// process-global).
pub enum Operative {
    Stream(Arc<StreamInstance>),
    Workflow(Arc<WorkflowInstance>),
}

static CLASS_REGISTRY: LazyLock<RwLock<HashMap<&'static str, Arc<OperativeTopology>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Overwrite-on-insert: re-registering a factory id replaces its topology,
/// so a restarted runner generation (or a re-run inventory harvest) is
/// idempotent rather than a collision.
pub fn register_class(factory_id: &'static str, topology: Arc<OperativeTopology>) {
    CLASS_REGISTRY.write().unwrap().insert(factory_id, topology);
}

/// On-demand removal (`FlezhaRunner::remove_stream`). Idempotent on an
/// unknown id.
pub fn unregister_class(factory_id: &str) {
    CLASS_REGISTRY.write().unwrap().remove(factory_id);
}

pub fn get_class(factory_id: &str) -> Option<Arc<OperativeTopology>> {
    CLASS_REGISTRY.read().unwrap().get(factory_id).cloned()
}

pub fn class_factory_ids() -> Vec<&'static str> {
    CLASS_REGISTRY.read().unwrap().keys().copied().collect()
}

/// The SDK language to advertise for `factory_id`, from whichever class
/// registry holds it.
///
/// The one lookup every factory advertisement reads — the connect init frame
/// and `UpdateFactories` alike — so no veneer restates the byte it already
/// declared to the core, and the two frames cannot disagree.
///
/// A factory this runner declared no class for answers
/// [`crate::header::SDK_LANG_UNSPECIFIED`], which the controller refuses to
/// place rather than guessing at. That default lives here, once: a second
/// spelling at a call site is exactly how the two halves of the seam drift.
pub fn sdk_lang_for_factory(factory_id: &str) -> u8 {
    get_class(factory_id)
        .map(|topology| topology.sdk_lang)
        .or_else(|| crate::workflow::registry::get_class(factory_id).map(|spec| spec.sdk_lang))
        .unwrap_or(crate::header::SDK_LANG_UNSPECIFIED)
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

    /// Resolve the unit variant for `instance_id` — the per-frame dispatch
    /// lookup.
    pub fn get_operative(&self, instance_id: &str) -> Option<Arc<Operative>> {
        self.inner.get(instance_id).map(|r| Arc::clone(r.value()))
    }

    /// Resolve `instance_id` only when it is a operative; `None` for a workflow or
    /// an absent id.
    pub fn get_stream(&self, instance_id: &str) -> Option<Arc<StreamInstance>> {
        self.inner
            .get(instance_id)
            .and_then(|r| match &**r.value() {
                Operative::Stream(ri) => Some(Arc::clone(ri)),
                Operative::Workflow(_) => None,
            })
    }

    /// Resolve `instance_id` only when it is a workflow operative; `None` for a
    /// stream or an absent id.
    pub fn get_workflow(&self, instance_id: &str) -> Option<Arc<WorkflowInstance>> {
        self.inner
            .get(instance_id)
            .and_then(|r| match &**r.value() {
                Operative::Workflow(bi) => Some(Arc::clone(bi)),
                Operative::Stream(_) => None,
            })
    }

    /// Locate a live workflow Entity slot by its wire identity, for the inbound
    /// `RestoreInitState` handler (the stream scan does not see workflow
    /// Entities). Returns the owning instance (to record the restore against its
    /// gate) and the matched slot.
    pub fn find_workflow_entity(
        &self,
        entity_id: &[u8],
    ) -> Option<(Arc<WorkflowInstance>, Arc<EntitySlot>)> {
        self.inner.iter().find_map(|r| match &**r.value() {
            Operative::Workflow(bi) => bi
                .entity_by_identity(entity_id)
                .map(|slot| (Arc::clone(bi), slot)),
            Operative::Stream(_) => None,
        })
    }

    pub fn remove(&self, instance_id: &str) -> Option<Arc<Operative>> {
        self.inner.remove(instance_id).map(|(_, v)| v)
    }

    /// Live instances of `factory_id`, both kinds — the removal check's
    /// input. Run on the coordinator loop so it is serialized with
    /// `AddOperative` handling.
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

    /// Snapshot of the operative instances only — workflow carry no serializable
    /// state, so the shutdown snapshot and `get_operatives` view skip them.
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
