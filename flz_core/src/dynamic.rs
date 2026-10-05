//! The dynamic-mode registry: Cells minted on first sight of a key and,
//! under a TTL, evicted destructively by a per-tasklet reaper. The
//! invariant: an eviction takes the drain gate and re-checks the mailbox, so
//! it never drops an item a producer pushed after the idle check.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use dashmap::DashMap;

use crate::cell::CellSlot;
use crate::channel::{BuildCellArgs, TaskletId};
use crate::flusher::CellFlusher;
use crate::relay::instance::RelayInstance;
use crate::relay::topology::RelayTopology;
use crate::state_sink;

const ENV_REAP_INTERVAL_MS: &str = "FLEZHA_REAP_INTERVAL_MS";
const MAX_SWEEP_INTERVAL: Duration = Duration::from_secs(30);
/// How many evicted keys are remembered, so a live read can say
/// "idle-evicted" rather than "never existed".
const EVICTED_TRACE_CAP: usize = 1024;

#[derive(Debug, Default)]
pub struct DynamicCellCounters {
    pub cells_created_total: AtomicU64,
    pub cells_evicted_total: AtomicU64,
    pub inputs_dropped_capped_total: AtomicU64,
}

/// One dynamic tasklet's Cells and the context to mint one.
pub struct DynamicCellRegistry {
    pub cells: DashMap<String, Arc<CellSlot>>,
    pub counters: DynamicCellCounters,
    /// Evicted keys, newest last, capped at [`EVICTED_TRACE_CAP`].
    evicted: Mutex<VecDeque<String>>,
    tasklet_id: TaskletId,
    max_cells: usize,
    topology: Arc<RelayTopology>,
    instance: Weak<RelayInstance>,
    instance_id: Arc<str>,
    keeps_state: bool,
    prefix: Option<Arc<crate::prefix::PrefixLedger>>,
}

impl DynamicCellRegistry {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tasklet_id: TaskletId,
        max_cells: usize,
        topology: Arc<RelayTopology>,
        instance: Weak<RelayInstance>,
        instance_id: Arc<str>,
        keeps_state: bool,
        prefix: Option<Arc<crate::prefix::PrefixLedger>>,
    ) -> Self {
        Self {
            cells: DashMap::new(),
            counters: DynamicCellCounters::default(),
            evicted: Mutex::new(VecDeque::new()),
            tasklet_id,
            max_cells,
            topology,
            instance,
            instance_id,
            keeps_state,
            prefix,
        }
    }

    /// The Cell for `key`, minted on first sight; the cap is best-effort
    /// under concurrent inserts.
    pub fn get_or_create(&self, key: &str) -> Result<Arc<CellSlot>, crate::error::Error> {
        if let Some(slot) = self.cells.get(key) {
            return Ok(Arc::clone(slot.value()));
        }
        if self.cells.len() >= self.max_cells {
            self.counters
                .inputs_dropped_capped_total
                .fetch_add(1, Ordering::Relaxed);
            return Err(crate::error::Error::Capped);
        }
        let entry = self.cells.entry(key.to_owned()).or_insert_with(|| {
            self.counters
                .cells_created_total
                .fetch_add(1, Ordering::Relaxed);
            self.build_slot(key)
        });
        Ok(Arc::clone(entry.value()))
    }

    /// Evict every Cell idle longer than `idle_ttl`, each under its drain
    /// gate with an empty mailbox; a steps relay's only between passes, and
    /// never one a rollback could still reach.
    pub fn reap_sweep(&self, idle_ttl: Duration) {
        let now = crate::util::monotonic_ms();
        let ttl_ms = idle_ttl.as_millis() as i64;
        let held = match self.instance.upgrade() {
            Some(instance) => match crate::steps::reap_holds(&instance) {
                Some(held) => held,
                None => return,
            },
            None => Default::default(),
        };

        // Collect idle candidates, taking each one's drain gate. Removal is
        // deferred until the iterator drops (mutating mid-iteration would
        // deadlock on the DashMap tasklet lock).
        let mut candidates: Vec<(String, Arc<CellSlot>)> = Vec::new();
        for entry in self.cells.iter() {
            let slot = entry.value();
            if now - slot.last_activity_ms.load(Ordering::Relaxed) <= ttl_ms
                || held.contains(&slot.cell_identity)
            {
                continue;
            }
            if !slot.claim_drained() {
                continue; // queued or running; retry next sweep
            }
            candidates.push((entry.key().clone(), Arc::clone(slot)));
        }

        for (key, slot) in candidates {
            self.evict(key, slot);
        }
    }

    /// Drop one candidate whose gate this sweep holds; a push that landed
    /// after the idle check re-inserts it instead. Its timers go with it.
    fn evict(&self, key: String, slot: Arc<CellSlot>) {
        self.cells.remove(&key);
        if !slot.mailbox.is_empty() {
            self.cells.insert(key, Arc::clone(&slot));
            slot.release_drained();
            return;
        }
        if let Some(id) = slot.flusher_entry_id {
            CellFlusher::instance().deregister(id);
        }
        crate::poll_core::global().discard_cell_timers(&slot.cell_identity);
        if slot.keeps_state {
            // The delete is a durable frame: the class holds it for the next
            // bound stream, behind this Cell's own records, so an eviction
            // in a reconnect window cannot leave the controller holding state
            // for a Cell that no longer exists. The body is the bare
            // identity the controller's `decode_cell_id` reads.
            state_sink::send(
                state_sink::DELETE_CELL_STATE_PAYLOAD_ID,
                slot.cell_identity.to_vec(),
            );
        }
        self.counters
            .cells_evicted_total
            .fetch_add(1, Ordering::Relaxed);
        self.note_evicted(key);
    }

    /// Whether `key` is in the eviction trace; beyond the cap it reads as
    /// never existed, claiming no eviction it cannot show.
    pub fn recently_evicted(&self, key: &str) -> bool {
        self.evicted
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .any(|k| k == key)
    }

    fn note_evicted(&self, key: String) {
        let mut trace = self.evicted.lock().unwrap_or_else(|p| p.into_inner());
        if trace.len() == EVICTED_TRACE_CAP {
            trace.pop_front();
        }
        trace.push_back(key);
    }

    fn build_slot(&self, key: &str) -> Arc<CellSlot> {
        let tasklet_desc = self
            .topology
            .tasklet(self.tasklet_id)
            .expect("dynamic registry: tasklet id missing in topology");
        (tasklet_desc.build_cell)(BuildCellArgs {
            topology: Arc::clone(&self.topology),
            instance: Weak::clone(&self.instance),
            instance_id: Arc::clone(&self.instance_id),
            tasklet_id: self.tasklet_id,
            cell_key: Some(key),
            cell_index: 0,
            keeps_state: self.keeps_state,
            prefix: self.prefix.clone(),
        })
    }
}

/// One reaper, sweeping every `reap_interval(idle_ttl)` until `stop`.
pub fn spawn_reaper(
    idle_ttl: Duration,
    registry: Arc<DynamicCellRegistry>,
    stop: Arc<AtomicBool>,
) -> JoinHandle<()> {
    let interval = reap_interval(idle_ttl);
    thread::Builder::new()
        .name("flezha-reaper".to_string())
        .spawn(move || {
            while !stop.load(Ordering::Acquire) {
                sleep_responsive(interval, &stop);
                if !stop.load(Ordering::Acquire) {
                    registry.reap_sweep(idle_ttl);
                }
            }
        })
        .expect("spawning flezha-reaper thread should not fail")
}

/// `FLEZHA_REAP_INTERVAL_MS`, else `min(idle_ttl / 4, 30s)`, at least 1 ms.
fn reap_interval(idle_ttl: Duration) -> Duration {
    let computed = (idle_ttl / 4)
        .min(MAX_SWEEP_INTERVAL)
        .max(Duration::from_millis(1));

    match crate::util::env_parse_positive(ENV_REAP_INTERVAL_MS, 0) {
        0 => computed,
        ms => Duration::from_millis(ms as u64),
    }
}

fn sleep_responsive(total: Duration, stop: &AtomicBool) {
    let chunk = total.min(Duration::from_millis(200));
    let mut slept = Duration::ZERO;
    while slept < total && !stop.load(Ordering::Acquire) {
        thread::sleep(chunk);
        slept += chunk;
    }
}
