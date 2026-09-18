//! Workflow class registry: process-global, one entry per workflow class. Mirrors
//! `operative::registry`'s `OperativeClassRegistry` — keyed by the controller-facing
//! `factory_id`, looked up when `AddOperative --kind workflow` arrives. Holds the
//! frozen [`WorkflowSpec`] (its procedure topology and class facts).

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

use super::spec::WorkflowSpec;

static WORKFLOW_CLASS_REGISTRY: LazyLock<RwLock<HashMap<&'static str, Arc<WorkflowSpec>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Overwrite-on-insert: re-registering a factory id replaces its spec, so a
/// restarted runner generation is idempotent rather than a collision.
pub fn register_class(factory_id: &'static str, spec: WorkflowSpec) {
    WORKFLOW_CLASS_REGISTRY
        .write()
        .unwrap()
        .insert(factory_id, Arc::new(spec));
}

/// On-demand removal (`FlezhaRunner::remove_workflow`). Idempotent on an
/// unknown id.
pub fn unregister_class(factory_id: &str) {
    WORKFLOW_CLASS_REGISTRY.write().unwrap().remove(factory_id);
}

pub fn get_class(factory_id: &str) -> Option<Arc<WorkflowSpec>> {
    WORKFLOW_CLASS_REGISTRY
        .read()
        .unwrap()
        .get(factory_id)
        .cloned()
}

/// Factory ids of every registered workflow class. Advertised to the controller
/// on connect alongside operative factories so `AddOperative --kind workflow` validates.
pub fn workflow_factory_ids() -> Vec<&'static str> {
    WORKFLOW_CLASS_REGISTRY
        .read()
        .unwrap()
        .keys()
        .copied()
        .collect()
}
