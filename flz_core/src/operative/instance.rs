//! `StreamInstance`: per-`AddOperative` runtime state.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;

use crate::channel::BuildEntityArgs;
use crate::dynamic::{spawn_reaper, DynamicEntityRegistry};
use crate::entity::EntitySlot;
use crate::operative::topology::{EntityMode, OperativeTopology};
use crate::persistence::resolve_persistence_enabled;

/// The key-group range one partition member owns, straight off its
/// `AddOperative`.
///
/// A partitioned operative runs as N ordinary instances, each owning a
/// contiguous slice of the 256 key groups. The range is shipped explicitly
/// rather than derived from `(index, count)` so a rescale's recomputed ranges
/// need no format change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyGroupRange {
    pub index: u32,
    pub count: u32,
    pub from: u32,
    pub to: u32,
}

impl KeyGroupRange {
    /// The whole key space, guard inert — every operative that predates
    /// partitioning, and every `--partitions 1` start.
    pub const UNPARTITIONED: Self = Self {
        index: 0,
        count: 0,
        from: 0,
        to: crate::util::KEY_GROUPS - 1,
    };

    /// Whether the dispatch guard applies at all. One comparison; an
    /// unpartitioned operative pays exactly this and nothing else.
    #[inline]
    pub fn partitioned(&self) -> bool {
        self.count >= 2
    }

    /// `Err` naming the violated contract when `key` belongs to another
    /// member. A mis-routed key means the source and the topology disagree
    /// about what a key is, so the message states the rule rather than the
    /// symptom.
    pub fn admit(&self, key: &str) -> Result<(), String> {
        let group = crate::util::key_group(key);
        if group >= self.from && group <= self.to {
            return Ok(());
        }
        Err(format!(
            "key '{}' hashes to key group {}, which partition {} of {} does not own \
             (it owns groups {}..{}) — the routing item / flz.key must equal the key_by result",
            key, group, self.index, self.count, self.from, self.to
        ))
    }

    /// Whether `key` belongs here — the filtering form for a statically keyed
    /// procedure, whose out-of-range declared Entities stay idle rather than
    /// parking every broadcast.
    #[inline]
    pub fn owns(&self, key: &str) -> bool {
        !self.partitioned() || self.admit(key).is_ok()
    }
}

pub struct StreamInstance {
    pub topology: Arc<OperativeTopology>,
    pub instance_id: Arc<str>,
    /// This instance's partition identity, from `AddOperative`. Immutable
    /// start-time truth; a rebind re-ships it.
    pub partition: KeyGroupRange,
    /// Indexed by `ProcedureId`. Each entry is a Vec of entities for that
    /// Procedure. Single-mode procedures have one entry; static-mode procedures have
    /// `entity_keys().len()` entries; dynamic-mode procedures have none (their
    /// Entities live in `dynamic_registries`).
    pub entities: Vec<Vec<Arc<EntitySlot>>>,
    /// Indexed by `ProcedureId`. Maps `&'static str` entity keys to the
    /// entity index. Empty for single- and dynamic-mode procedures.
    pub entity_key_to_idx: Vec<HashMap<&'static str, usize>>,
    /// Indexed by `ProcedureId`. `Some` only for dynamic-mode procedures; holds
    /// the lazily-populated entity map + reaper-facing counters.
    pub dynamic_registries: Vec<Option<Arc<DynamicEntityRegistry>>>,
    /// Stop flag + join handles for this instance's background threads: one
    /// reaper per dynamic Procedure with a TTL. Driven by
    /// [`Self::shutdown_background_threads`] / `Drop`.
    background_stop: Arc<AtomicBool>,
    background_handles: Mutex<Vec<JoinHandle<()>>>,
    /// Per-entry-channel staging for the host prepare crossing (C-ABI
    /// classes whose consumers need keys/event times before enqueue).
    /// Lazily populated; typed classes never touch it.
    pub host_prepare: Mutex<crate::host::HostPrepareState>,
    /// Frozen-at-instance-start persistence flag. Resolved from the
    /// StreamOperative class const + runner override; propagated to every
    /// `EntitySlot`. Subsequent runner config changes do not
    /// affect this instance — only `stop_operative` + `start_operative` cycles
    /// pick up new values.
    persistence_enabled: bool,
}

impl StreamInstance {
    /// Construct a fresh `StreamInstance` with no runner persistence
    /// override and no partitioning. Equivalent to
    /// `StreamInstance::with_options(.., None, KeyGroupRange::UNPARTITIONED)`.
    pub fn new(topology: Arc<OperativeTopology>, instance_id: Arc<str>) -> Arc<Self> {
        Self::with_options(topology, instance_id, None, KeyGroupRange::UNPARTITIONED)
    }

    /// Construct a fresh `StreamInstance` with an explicit runner
    /// override for `operative_persistence.<OperativeClass>` (when `Some`, it
    /// wins over the class const) and an explicit partition range.
    pub fn with_options(
        topology: Arc<OperativeTopology>,
        instance_id: Arc<str>,
        operator_override: Option<bool>,
        partition: KeyGroupRange,
    ) -> Arc<Self> {
        let persistence_enabled =
            resolve_persistence_enabled(topology.persistence_enabled_const, operator_override);
        Arc::new_cyclic(|weak: &Weak<StreamInstance>| {
            let procedure_count = topology.procedures.len();
            let mut entities: Vec<Vec<Arc<EntitySlot>>> = Vec::with_capacity(procedure_count);
            let mut entity_key_to_idx: Vec<HashMap<&'static str, usize>> =
                Vec::with_capacity(procedure_count);
            let mut dynamic_registries: Vec<Option<Arc<DynamicEntityRegistry>>> =
                Vec::with_capacity(procedure_count);

            for (procedure_id, procedure_desc) in topology.procedures.iter().enumerate() {
                let procedure_id_u32 = procedure_id as u32;
                if procedure_desc.mode == EntityMode::Dynamic {
                    // Dynamic procedures own zero Entities at startup; the
                    // registry allocates them lazily on first key.
                    entities.push(Vec::new());
                    entity_key_to_idx.push(HashMap::new());
                    dynamic_registries.push(Some(Arc::new(DynamicEntityRegistry::new(
                        procedure_id_u32,
                        procedure_desc.max_entities,
                        Arc::clone(&topology),
                        Weak::clone(weak),
                        Arc::clone(&instance_id),
                        persistence_enabled,
                    ))));
                } else if procedure_desc.entity_keys.is_empty() {
                    let slot = (procedure_desc.build_entity)(BuildEntityArgs {
                        topology: Arc::clone(&topology),
                        instance: Weak::clone(weak),
                        instance_id: Arc::clone(&instance_id),
                        procedure_id: procedure_id_u32,
                        entity_key: None,
                        entity_index: 0,
                        persistence_enabled,
                    });
                    entities.push(vec![slot]);
                    entity_key_to_idx.push(HashMap::new());
                    dynamic_registries.push(None);
                } else {
                    let mut slots = Vec::with_capacity(procedure_desc.entity_keys.len());
                    let mut key_map = HashMap::with_capacity(procedure_desc.entity_keys.len());
                    for (idx, key) in procedure_desc.entity_keys.iter().enumerate() {
                        let slot = (procedure_desc.build_entity)(BuildEntityArgs {
                            topology: Arc::clone(&topology),
                            instance: Weak::clone(weak),
                            instance_id: Arc::clone(&instance_id),
                            procedure_id: procedure_id_u32,
                            entity_key: Some(key),
                            entity_index: idx,
                            persistence_enabled,
                        });
                        slots.push(slot);
                        key_map.insert(*key, idx);
                    }
                    entities.push(slots);
                    entity_key_to_idx.push(key_map);
                    dynamic_registries.push(None);
                }
            }

            // One reaper per dynamic Procedure that opts into a TTL. Default
            // `idle_ttl == None` spawns none — dynamic Entities then live
            // as long as the StreamOperative, like static ones.
            let background_stop = Arc::new(AtomicBool::new(false));
            let mut background_handles: Vec<JoinHandle<()>> = Vec::new();
            for (procedure_id, registry) in dynamic_registries.iter().enumerate() {
                if let (Some(registry), Some(idle_ttl)) =
                    (registry, topology.procedures[procedure_id].idle_ttl)
                {
                    background_handles.push(spawn_reaper(
                        idle_ttl,
                        Arc::clone(registry),
                        Arc::clone(&background_stop),
                    ));
                }
            }

            StreamInstance {
                host_prepare: Mutex::new(crate::host::HostPrepareState::default()),
                topology,
                instance_id,
                partition,
                entities,
                entity_key_to_idx,
                dynamic_registries,
                background_stop,
                background_handles: Mutex::new(background_handles),
                persistence_enabled,
            }
        })
    }

    /// Read the resolved persistence flag for this instance. Frozen at
    /// construction; only changes on the next stop+start cycle.
    pub fn persistence_enabled(&self) -> bool {
        self.persistence_enabled
    }

    /// Stop and join this instance's background threads (the dynamic-mode
    /// reapers). Called by `RunnerNode::stop_operative`; idempotent and a
    /// no-op for an instance that started none.
    pub fn shutdown_background_threads(&self) {
        self.signal_background_stop();
        let handles = std::mem::take(
            &mut *self
                .background_handles
                .lock()
                .unwrap_or_else(|p| p.into_inner()),
        );
        for handle in handles {
            let _ = handle.join();
        }
    }

    /// Ask every background thread to wind down.
    fn signal_background_stop(&self) {
        self.background_stop
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

impl Drop for StreamInstance {
    fn drop(&mut self) {
        // Safety net for an instance dropped without `stop_operative`: signal the
        // background threads (don't join — Drop must not block on a sweep
        // interval) so each thread and the state it holds wind down.
        self.signal_background_stop();
    }
}
