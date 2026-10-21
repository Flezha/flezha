//! `RelayInstance`: one bound relay of either kind — its Cells by
//! tasklet and mode, its partition range, its idle sweeps, and for a steps relay its
//! run — and the partition hash the controller's placement mirrors bit for
//! bit.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::cell::CellSlot;
use crate::channel::{BuildCellArgs, TaskletId};
use crate::dynamic::DynamicCellRegistry;
use crate::relay::topology::{CellMode, RelayTopology};

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

/// Every instance's identity on the deferred-wake list: never reused, where
/// an instance id comes back with a rehydration.
static INSTANCE_SEQ: AtomicU64 = AtomicU64::new(0);

/// `fnv1a128(key) mod 256`; each partition owns a contiguous range.
pub fn key_group(key: &str) -> u32 {
    (fnv1a128(FNV128_OFFSET_BASIS, key.as_bytes()) & (KEY_GROUPS as u128 - 1)) as u32
}

/// The key-group range one partition member owns, shipped explicitly on
/// `AddRelay` so a rescale needs no format change.
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
        Err(crate::error::Error::Partition(
            crate::Code::KeyOutsidePartition.with([
                ("key", key.into()),
                ("group", group.into()),
                ("partition", self.index.into()),
                ("partitions", self.count.into()),
                ("from", self.from.into()),
                ("to", self.to.into()),
            ]),
        ))
    }

    /// Whether `key` belongs here — the filtering form a static tasklet
    /// uses to leave its out-of-range Cells idle.
    #[inline]
    pub fn owns(&self, key: &str) -> bool {
        !self.partitioned() || self.admit(key).is_ok()
    }
}

pub struct RelayInstance {
    pub topology: Arc<RelayTopology>,
    pub instance_id: Arc<str>,
    /// This member's range, from `AddRelay`; a rebind re-ships it.
    pub partition: KeyGroupRange,
    /// By `TaskletId`: one slot for single mode, one per key for static,
    /// none for dynamic (those live in `dynamic_registries`).
    pub cells: Vec<Vec<Arc<CellSlot>>>,
    /// By `TaskletId`: static keys to slot index.
    pub cell_key_to_idx: Vec<HashMap<&'static str, usize>>,
    /// By `TaskletId`: `Some` for dynamic mode only.
    pub dynamic_registries: Vec<Option<Arc<DynamicCellRegistry>>>,
    /// Set once the instance stops on this runner: its sweeps end, its Cells
    /// run nothing more and its parks stop waiting on the controller.
    background_stop: Arc<AtomicBool>,
    /// The host prepare crossing's staging; a typed class never touches it.
    pub host_prepare: Mutex<crate::host::HostPrepareState>,
    /// Retry ladders and parks the park owner holds for this instance, each
    /// a payload in no mailbox.
    pub(crate) ladders: AtomicUsize,
    /// Resolved once at start from the class const and the runner override.
    keeps_state: bool,
    pub(crate) seq: u64,
    /// The owed input tail and, for a flow relay, its idle deadline.
    pub(crate) idle: crate::relay::sleep::IdleWatch,
    /// `Some` for a steps class: the pass sheet its events advance.
    pub steps: Option<crate::steps::StepsRun>,
    /// A flow relay's processed prefix; a steps relay's floor is
    /// its journal.
    pub prefix: Option<Arc<crate::prefix::PrefixLedger>>,
    /// The end a flow relay asked for from inside, until it is reported.
    pub(crate) end: crate::relay::completion::FlowEnd,
}

impl RelayInstance {
    /// Whether a retry ladder still holds a payload of this instance.
    pub fn ladders_running(&self) -> bool {
        self.ladders.load(Ordering::Acquire) > 0
    }

    /// No persistence override, unpartitioned.
    pub fn new(topology: Arc<RelayTopology>, instance_id: Arc<str>) -> Arc<Self> {
        Self::with_options(topology, instance_id, None, KeyGroupRange::UNPARTITIONED)
    }

    /// With an explicit persistence override (which wins over the class
    /// const) and partition range.
    pub fn with_options(
        topology: Arc<RelayTopology>,
        instance_id: Arc<str>,
        config_override: Option<bool>,
        partition: KeyGroupRange,
    ) -> Arc<Self> {
        // The runner override wins over the class const when present.
        let keeps_state = config_override.unwrap_or(topology.durable.keeps());
        let prefix = (topology.steps.is_none() && keeps_state)
            .then(|| crate::prefix::PrefixLedger::new(Arc::clone(&instance_id)));
        // An instance whose input is journaled reports its prefix even when no
        // Cell of it keeps a record, or its log's floor never moves.
        if let Some(prefix) = prefix
            .as_ref()
            .filter(|_| topology.effectively_once || topology.sleep_after().is_some())
        {
            crate::flusher::CellFlusher::instance().watch_prefix(prefix);
        }
        // Started here, while threads can still be spawned, so a later park
        // never needs one; a hand-off retries the start if this one failed.
        if let Err(e) = crate::park::start_owner() {
            flz_transport::notice!(Warn, "the park owner could not start yet: {e}");
        }
        Arc::new_cyclic(|weak: &Weak<RelayInstance>| {
            let background_stop = Arc::new(AtomicBool::new(false));
            let topology_is_steps = topology.steps.is_some();
            let tasklet_count = topology.tasklets.len();
            let mut cells: Vec<Vec<Arc<CellSlot>>> = Vec::with_capacity(tasklet_count);
            let mut cell_key_to_idx: Vec<HashMap<&'static str, usize>> =
                Vec::with_capacity(tasklet_count);
            let mut dynamic_registries: Vec<Option<Arc<DynamicCellRegistry>>> =
                Vec::with_capacity(tasklet_count);

            for (tasklet_id, tasklet_desc) in topology.tasklets.iter().enumerate() {
                let (slots, key_map, registry) = build_tasklet_cells(
                    &topology,
                    weak,
                    &instance_id,
                    tasklet_id as u32,
                    tasklet_desc,
                    keeps_state,
                    &prefix,
                    &background_stop,
                );
                cells.push(slots);
                cell_key_to_idx.push(key_map);
                dynamic_registries.push(registry);
            }

            arm_sweeps(&topology, &dynamic_registries);

            RelayInstance {
                host_prepare: Mutex::new(crate::host::HostPrepareState::default()),
                ladders: AtomicUsize::new(0),
                topology,
                instance_id,
                partition,
                cells,
                cell_key_to_idx,
                dynamic_registries,
                background_stop,
                keeps_state,
                seq: INSTANCE_SEQ.fetch_add(1, Ordering::Relaxed),
                idle: Default::default(),
                steps: topology_is_steps.then(|| crate::steps::StepsRun::new(Weak::clone(weak))),
                prefix,
                end: Default::default(),
            }
        })
    }

    /// A live instance of the class already registered under `factory_id` —
    /// the one constructor a local run and this crate's own class fixtures
    /// share, so neither can drift from how a registered class is built.
    pub fn of_class(factory_id: &str, instance_id: &str) -> Result<Arc<Self>, crate::error::Error> {
        let topology = crate::relay::registry::get_class(factory_id).ok_or_else(|| {
            crate::error::Error::Declaration(
                crate::Code::ClassNotRegistered.with([("factory", factory_id.into())]),
            )
        })?;
        Ok(Self::new(topology, Arc::from(instance_id)))
    }

    /// Every Cell this instance holds right now, static and dynamic alike.
    pub fn all_slots(&self) -> Vec<Arc<CellSlot>> {
        (0..self.cells.len() as TaskletId)
            .flat_map(|tasklet_id| self.slots_of(tasklet_id))
            .collect()
    }

    /// A dynamic tasklet's registry; `None` for every other mode.
    pub fn dynamic_registry(&self, tasklet_id: TaskletId) -> Option<&Arc<DynamicCellRegistry>> {
        self.dynamic_registries
            .get(tasklet_id as usize)
            .and_then(Option::as_ref)
    }

    /// Every Cell one tasklet holds right now: its declared ones, or the
    /// keyed ones its registry minted.
    pub fn slots_of(&self, tasklet_id: TaskletId) -> Vec<Arc<CellSlot>> {
        let mut slots = Vec::new();
        self.for_each_slot(tasklet_id, |slot| slots.push(Arc::clone(slot)));
        slots
    }

    /// [`Self::slots_of`] without taking a reference to any Cell.
    pub fn for_each_slot(&self, tasklet_id: TaskletId, mut visit: impl FnMut(&Arc<CellSlot>)) {
        match self.dynamic_registry(tasklet_id) {
            Some(registry) => registry.cells.iter().for_each(|e| visit(e.value())),
            None => self
                .cells
                .get(tasklet_id as usize)
                .into_iter()
                .flatten()
                .for_each(visit),
        }
    }

    /// The Cell a wire identity names on this instance: a declared one, or a
    /// keyed one minted on first sight exactly as routing mints it — `None`
    /// past the tasklet's cap, and for an identity of another instance or of
    /// a tasklet the class does not declare.
    pub fn slot_by_identity(&self, cell_id: &[u8]) -> Option<Arc<CellSlot>> {
        let (instance_id, tasklet, key) = crate::flusher::decode_cell_identity(cell_id)?;
        if *instance_id != *self.instance_id {
            return None;
        }
        let tasklet_id = *self.topology.tasklet_name_to_id.get(tasklet.as_str())? as usize;
        match self.dynamic_registries.get(tasklet_id)? {
            Some(registry) => registry.get_or_create(&key).ok(),
            None => self
                .cells
                .get(tasklet_id)?
                .iter()
                .find(|slot| slot.cell_identity.as_slice() == cell_id)
                .cloned(),
        }
    }

    pub fn keeps_state(&self) -> bool {
        self.keeps_state
    }

    /// Whether this instance's state is durable: persistence on, and a
    /// controller to ship it to. A transport-less run keeps nothing.
    pub(crate) fn durable(&self) -> bool {
        self.keeps_state && crate::runner::coordinator_installed()
    }

    /// Stop the idle sweeps and wait out one in flight; idempotent.
    pub fn shutdown_background_threads(&self) {
        self.signal_background_stop();
        for registry in self.dynamic_registries.iter().flatten() {
            registry.sweep_settled();
        }
    }

    /// The instance stopped on this runner: removed from its registry, or
    /// dropped.
    pub fn removed(&self) -> bool {
        self.background_stop.load(Ordering::Acquire)
    }

    fn signal_background_stop(&self) {
        self.background_stop.store(true, Ordering::Release);
        crate::park::wake_waiters();
    }
}

/// One idle sweep per dynamic Tasklet that declares a TTL.
fn arm_sweeps(topology: &RelayTopology, dynamic_registries: &[Option<Arc<DynamicCellRegistry>>]) {
    for (tasklet_id, registry) in dynamic_registries.iter().enumerate() {
        if let (Some(registry), Some(idle_ttl)) = (registry, topology.tasklets[tasklet_id].idle_ttl)
        {
            crate::dynamic::arm_sweep(registry, idle_ttl);
        }
    }
}

type TaskletCells = (
    Vec<Arc<CellSlot>>,
    HashMap<&'static str, usize>,
    Option<Arc<DynamicCellRegistry>>,
);

/// One tasklet's Cells by mode: `(slots, key → index, dynamic registry)`.
#[allow(clippy::too_many_arguments)]
fn build_tasklet_cells(
    topology: &Arc<RelayTopology>,
    weak: &Weak<RelayInstance>,
    instance_id: &Arc<str>,
    tasklet_id: u32,
    tasklet_desc: &crate::relay::topology::TaskletDescriptor,
    keeps_state: bool,
    prefix: &Option<Arc<crate::prefix::PrefixLedger>>,
    relay_stopped: &Arc<AtomicBool>,
) -> TaskletCells {
    let build = |cell_key: Option<&str>, cell_index: usize| {
        (tasklet_desc.build_cell)(BuildCellArgs {
            topology: Arc::clone(topology),
            instance: Weak::clone(weak),
            instance_id: Arc::clone(instance_id),
            tasklet_id,
            cell_key,
            cell_index,
            keeps_state,
            prefix: prefix.clone(),
            relay_stopped: Arc::clone(relay_stopped),
        })
    };
    if tasklet_desc.mode == CellMode::Dynamic {
        let registry = DynamicCellRegistry::new(
            tasklet_id,
            tasklet_desc.max_cells,
            Arc::clone(topology),
            Weak::clone(weak),
            Arc::clone(instance_id),
            keeps_state,
            prefix.clone(),
            Arc::clone(relay_stopped),
        );
        return (Vec::new(), HashMap::new(), Some(Arc::new(registry)));
    }
    if tasklet_desc.cell_keys.is_empty() {
        return (vec![build(None, 0)], HashMap::new(), None);
    }
    let mut slots = Vec::with_capacity(tasklet_desc.cell_keys.len());
    let mut key_map = HashMap::with_capacity(tasklet_desc.cell_keys.len());
    for (idx, key) in tasklet_desc.cell_keys.iter().enumerate() {
        slots.push(build(Some(key), idx));
        key_map.insert(*key, idx);
    }
    (slots, key_map, None)
}

impl Drop for RelayInstance {
    fn drop(&mut self) {
        // Safety net for an instance dropped without `stop_relay`: signal the
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
