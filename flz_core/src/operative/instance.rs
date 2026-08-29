//! `StreamInstance`: per-`AddOperative` runtime state.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;

use crate::channel::BuildEntityArgs;
use crate::dynamic::{spawn_reaper, DynamicEntityRegistry};
use crate::entity::EntitySlot;
use crate::operative::topology::{EntityMode, OperativeTopology};

/// FNV-1a-128 offset basis, frozen by the cross-language `StableId` spec.
pub const FNV128_OFFSET_BASIS: u128 = 0x6c62272e07bb014262b821756295c58d;

/// The FNV-128 prime, `2^88 + 0x13b`.
const FNV128_PRIME: u128 = 0x0000000001000000000000000000013b;

/// FNV-1a-128 (frozen by spec: xor byte, multiply by the FNV 128 prime),
/// continuing from `hash` so a caller can chain several byte runs.
///
/// The one implementation behind both `StableId::derive` and [`key_group`];
/// the controller's `Flezha.Partition` and Java's `StableId` carry the same
/// constants, pinned by one shared fixture-vector set.
pub fn fnv1a128(mut hash: u128, bytes: &[u8]) -> u128 {
    for &b in bytes {
        hash ^= b as u128;
        hash = hash.wrapping_mul(FNV128_PRIME);
    }
    hash
}

/// How many key groups a partitioned operative's key space is divided into.
pub const KEY_GROUPS: u32 = 256;

/// The key group a routing key belongs to: `fnv1a128(key) mod 256`. The
/// controller assigns each partition a contiguous range of these and the
/// dispatch guard refuses a key outside its own.
pub fn key_group(key: &str) -> u32 {
    (fnv1a128(FNV128_OFFSET_BASIS, key.as_bytes()) & (KEY_GROUPS as u128 - 1)) as u32
}

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
        to: KEY_GROUPS - 1,
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
        let group = key_group(key);
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
        // The runner override wins over the class const when present.
        let persistence_enabled = operator_override.unwrap_or(topology.persistence_enabled_const);
        Arc::new_cyclic(|weak: &Weak<StreamInstance>| {
            let procedure_count = topology.procedures.len();
            let mut entities: Vec<Vec<Arc<EntitySlot>>> = Vec::with_capacity(procedure_count);
            let mut entity_key_to_idx: Vec<HashMap<&'static str, usize>> =
                Vec::with_capacity(procedure_count);
            let mut dynamic_registries: Vec<Option<Arc<DynamicEntityRegistry>>> =
                Vec::with_capacity(procedure_count);

            for (procedure_id, procedure_desc) in topology.procedures.iter().enumerate() {
                let (slots, key_map, registry) = build_procedure_entities(
                    &topology,
                    weak,
                    &instance_id,
                    procedure_id as u32,
                    procedure_desc,
                    persistence_enabled,
                );
                entities.push(slots);
                entity_key_to_idx.push(key_map);
                dynamic_registries.push(registry);
            }

            let background_stop = Arc::new(AtomicBool::new(false));
            let background_handles =
                spawn_reapers(&topology, &dynamic_registries, &background_stop);

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

/// One reaper per dynamic Procedure that opts into a TTL. Default
/// `idle_ttl == None` spawns none — dynamic Entities then live as long as the
/// StreamOperative, like static ones.
fn spawn_reapers(
    topology: &OperativeTopology,
    dynamic_registries: &[Option<Arc<DynamicEntityRegistry>>],
    stop: &Arc<AtomicBool>,
) -> Vec<JoinHandle<()>> {
    let mut handles: Vec<JoinHandle<()>> = Vec::new();
    for (procedure_id, registry) in dynamic_registries.iter().enumerate() {
        if let (Some(registry), Some(idle_ttl)) =
            (registry, topology.procedures[procedure_id].idle_ttl)
        {
            handles.push(spawn_reaper(
                idle_ttl,
                Arc::clone(registry),
                Arc::clone(stop),
            ));
        }
    }
    handles
}

/// One procedure's Entities: `(slots, key → index, dynamic registry)`.
type ProcedureEntities = (
    Vec<Arc<EntitySlot>>,
    HashMap<&'static str, usize>,
    Option<Arc<DynamicEntityRegistry>>,
);

/// One procedure's Entities by mode. A dynamic procedure owns zero Entities
/// at startup — its registry allocates them lazily on first key; a
/// single-mode procedure owns one; a static one owns one per declared key.
fn build_procedure_entities(
    topology: &Arc<OperativeTopology>,
    weak: &Weak<StreamInstance>,
    instance_id: &Arc<str>,
    procedure_id: u32,
    procedure_desc: &crate::operative::topology::ProcedureDescriptor,
    persistence_enabled: bool,
) -> ProcedureEntities {
    let build = |entity_key: Option<&str>, entity_index: usize| {
        (procedure_desc.build_entity)(BuildEntityArgs {
            topology: Arc::clone(topology),
            instance: Weak::clone(weak),
            instance_id: Arc::clone(instance_id),
            procedure_id,
            entity_key,
            entity_index,
            persistence_enabled,
        })
    };
    if procedure_desc.mode == EntityMode::Dynamic {
        let registry = DynamicEntityRegistry::new(
            procedure_id,
            procedure_desc.max_entities,
            Arc::clone(topology),
            Weak::clone(weak),
            Arc::clone(instance_id),
            persistence_enabled,
        );
        return (Vec::new(), HashMap::new(), Some(Arc::new(registry)));
    }
    if procedure_desc.entity_keys.is_empty() {
        return (vec![build(None, 0)], HashMap::new(), None);
    }
    let mut slots = Vec::with_capacity(procedure_desc.entity_keys.len());
    let mut key_map = HashMap::with_capacity(procedure_desc.entity_keys.len());
    for (idx, key) in procedure_desc.entity_keys.iter().enumerate() {
        slots.push(build(Some(key), idx));
        key_map.insert(*key, idx);
    }
    (slots, key_map, None)
}

impl Drop for StreamInstance {
    fn drop(&mut self) {
        // Safety net for an instance dropped without `stop_operative`: signal the
        // background threads (don't join — Drop must not block on a sweep
        // interval) so each thread and the state it holds wind down.
        self.signal_background_stop();
    }
}

#[cfg(test)]
mod partition_hash_tests {
    //! The routing guard and the controller's placement must agree bit for
    //! bit: a one-bit drift parks every frame of a partitioned operative. The
    //! twin assertions live in `Flezha.PartitionTest` and `StableIdTest`.

    use super::{fnv1a128, key_group, FNV128_OFFSET_BASIS};

    #[test]
    fn key_group_matches_the_cross_language_fixture() {
        assert_eq!(key_group("BTC-USD"), 129);
        assert_eq!(key_group("ETH-USD"), 213);
        assert_eq!(key_group("SOL-USD"), 60);
        assert_eq!(key_group(""), 141);
        assert_eq!(key_group("a"), 100);
    }

    #[test]
    fn key_group_is_the_digest_low_byte() {
        for key in ["BTC-USD", "", "a", &"x".repeat(512)] {
            let digest = fnv1a128(FNV128_OFFSET_BASIS, key.as_bytes());
            assert_eq!(key_group(key) as u128, digest % 256);
            assert!(key_group(key) < 256);
        }
    }
}
