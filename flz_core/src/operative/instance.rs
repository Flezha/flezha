//! `StreamInstance`: one bound stream operative — its Entities by procedure
//! and mode, its partition range, its reapers — and the partition hash the
//! controller's placement mirrors bit for bit.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;

use crate::channel::BuildEntityArgs;
use crate::dynamic::{spawn_reaper, DynamicEntityRegistry};
use crate::entity::EntitySlot;
use crate::operative::topology::{EntityMode, OperativeTopology};

/// Frozen by the cross-language `StableId` spec.
pub const FNV128_OFFSET_BASIS: u128 = 0x6c62272e07bb014262b821756295c58d;

const FNV128_PRIME: u128 = 0x0000000001000000000000000000013b;

/// FNV-1a-128 continuing from `hash`; the one implementation behind
/// `StableId::derive` and [`key_group`], pinned against the controller's and
/// Java's by shared fixture vectors.
pub fn fnv1a128(mut hash: u128, bytes: &[u8]) -> u128 {
    for &b in bytes {
        hash ^= b as u128;
        hash = hash.wrapping_mul(FNV128_PRIME);
    }
    hash
}

pub const KEY_GROUPS: u32 = 256;

/// `fnv1a128(key) mod 256`; each partition owns a contiguous range.
pub fn key_group(key: &str) -> u32 {
    (fnv1a128(FNV128_OFFSET_BASIS, key.as_bytes()) & (KEY_GROUPS as u128 - 1)) as u32
}

/// The key-group range one partition member owns, shipped explicitly on
/// `AddOperative` so a rescale needs no format change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyGroupRange {
    pub index: u32,
    pub count: u32,
    pub from: u32,
    pub to: u32,
}

impl KeyGroupRange {
    /// The whole key space, guard inert.
    pub const UNPARTITIONED: Self = Self {
        index: 0,
        count: 0,
        from: 0,
        to: KEY_GROUPS - 1,
    };

    /// Whether the dispatch guard applies at all — one comparison.
    #[inline]
    pub fn partitioned(&self) -> bool {
        self.count >= 2
    }

    /// `Err` when `key` belongs to another member; the text states the rule
    /// the source and the topology disagree on.
    pub fn admit(&self, key: &str) -> Result<(), crate::error::Error> {
        let group = key_group(key);
        if group >= self.from && group <= self.to {
            return Ok(());
        }
        Err(crate::error::Error::Partition(format!(
            "key '{}' hashes to key group {}, which partition {} of {} does not own \
             (it owns groups {}..{}) — the routing item / flz.key must equal the key_by result",
            key, group, self.index, self.count, self.from, self.to
        )))
    }

    /// Whether `key` belongs here — the filtering form a static procedure
    /// uses to leave its out-of-range Entities idle.
    #[inline]
    pub fn owns(&self, key: &str) -> bool {
        !self.partitioned() || self.admit(key).is_ok()
    }
}

pub struct StreamInstance {
    pub topology: Arc<OperativeTopology>,
    pub instance_id: Arc<str>,
    /// This member's range, from `AddOperative`; a rebind re-ships it.
    pub partition: KeyGroupRange,
    /// By `ProcedureId`: one slot for single mode, one per key for static,
    /// none for dynamic (those live in `dynamic_registries`).
    pub entities: Vec<Vec<Arc<EntitySlot>>>,
    /// By `ProcedureId`: static keys to slot index.
    pub entity_key_to_idx: Vec<HashMap<&'static str, usize>>,
    /// By `ProcedureId`: `Some` for dynamic mode only.
    pub dynamic_registries: Vec<Option<Arc<DynamicEntityRegistry>>>,
    /// Stop flag and handles of the reapers.
    background_stop: Arc<AtomicBool>,
    background_handles: Mutex<Vec<JoinHandle<()>>>,
    /// The host prepare crossing's staging; a typed class never touches it.
    pub host_prepare: Mutex<crate::host::HostPrepareState>,
    /// Resolved once at start from the class const and the runner override.
    persistence_enabled: bool,
}

impl StreamInstance {
    /// No persistence override, unpartitioned.
    pub fn new(topology: Arc<OperativeTopology>, instance_id: Arc<str>) -> Arc<Self> {
        Self::with_options(topology, instance_id, None, KeyGroupRange::UNPARTITIONED)
    }

    /// With an explicit persistence override (which wins over the class
    /// const) and partition range.
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

    /// A live instance of the class already registered under `factory_id` —
    /// the one constructor a local run and this crate's own class fixtures
    /// share, so neither can drift from how a registered class is built.
    pub fn of_class(factory_id: &str, instance_id: &str) -> Result<Arc<Self>, crate::error::Error> {
        let topology = crate::operative::registry::get_class(factory_id).ok_or_else(|| {
            crate::error::Error::Declaration(format!(
                "no operative class is registered as `{factory_id}`"
            ))
        })?;
        Ok(Self::new(topology, Arc::from(instance_id)))
    }

    /// Every Entity this instance holds right now, static and dynamic alike.
    pub fn all_slots(&self) -> Vec<Arc<EntitySlot>> {
        let statics = self.entities.iter().flatten().cloned();
        let dynamics = self.dynamic_registries.iter().flatten().flat_map(|r| {
            r.entities
                .iter()
                .map(|e| Arc::clone(e.value()))
                .collect::<Vec<_>>()
        });
        statics.chain(dynamics).collect()
    }

    pub fn persistence_enabled(&self) -> bool {
        self.persistence_enabled
    }

    /// Stop and join the reapers; idempotent.
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

    fn signal_background_stop(&self) {
        self.background_stop
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

/// One reaper per dynamic Procedure that declares a TTL.
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

type ProcedureEntities = (
    Vec<Arc<EntitySlot>>,
    HashMap<&'static str, usize>,
    Option<Arc<DynamicEntityRegistry>>,
);

/// One procedure's Entities by mode: `(slots, key → index, dynamic registry)`.
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
    //! bit; the twin assertions are `Flezha.PartitionTest` and `StableIdTest`.

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
