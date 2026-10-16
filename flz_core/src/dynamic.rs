//! The dynamic-mode registry: Cells minted on first sight of a key and,
//! under a TTL, evicted destructively by a per-tasklet reaper. The
//! invariant: an eviction takes the drain gate and removes the key only while
//! the mailbox is empty and nothing else holds the Cell, so no item a producer
//! pushed after the idle check, or pushes later through a Cell it took, is lost.

use std::collections::VecDeque;
use std::sync::atomic::{fence, AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use dashmap::DashMap;

use crate::cell::{CellCounters, CellSlot};
use crate::channel::{BuildCellArgs, TaskletId};
use crate::flusher::CellFlusher;
use crate::relay::instance::RelayInstance;
use crate::relay::topology::RelayTopology;
use crate::state_sink;
use crate::util::LockRecover;

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
    /// The totals of every Cell evicted so far, so a tasklet's totals never
    /// fall when one goes.
    pub retired: CellCounters,
    /// Evicted keys, newest last, capped at [`EVICTED_TRACE_CAP`].
    evicted: Mutex<VecDeque<String>>,
    tasklet_id: TaskletId,
    max_cells: usize,
    topology: Arc<RelayTopology>,
    instance: Weak<RelayInstance>,
    instance_id: Arc<str>,
    keeps_state: bool,
    prefix: Option<Arc<crate::prefix::PrefixLedger>>,
    relay_stopped: Arc<AtomicBool>,
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
        relay_stopped: Arc<AtomicBool>,
    ) -> Self {
        Self {
            cells: DashMap::new(),
            counters: DynamicCellCounters::default(),
            retired: CellCounters::default(),
            evicted: Mutex::new(VecDeque::new()),
            tasklet_id,
            max_cells,
            topology,
            instance,
            instance_id,
            keeps_state,
            prefix,
            relay_stopped,
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
        let (held, recorded) = match self.instance.upgrade() {
            Some(instance) => match crate::steps::reap_holds(&instance) {
                Some(holds) => holds,
                None => return,
            },
            None => Default::default(),
        };

        // Collect idle candidates, taking each one's drain gate. Removal is
        // deferred until the iterator drops (mutating mid-iteration would
        // deadlock on the DashMap tasklet lock).
        let mut candidates: Vec<(String, Arc<CellSlot>, usize)> = Vec::new();
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
            let records = recorded.get(&Arc::as_ptr(slot)).copied().unwrap_or(0);
            candidates.push((entry.key().clone(), Arc::clone(slot), records));
        }

        for (key, slot, records) in candidates {
            self.evict(key, slot, records);
        }
    }

    /// Drop one candidate whose gate this sweep holds, unless a push landed
    /// after the idle check or anything beyond the registry, this sweep and
    /// the run's `records` holds it (a router that took it may push yet). Its
    /// timers go with it. A host-run Cell's state is dropped by the host first,
    /// while the key still names this slot, so a Cell minted again under the
    /// key never meets it.
    fn evict(&self, key: String, slot: Arc<CellSlot>, records: usize) {
        let host = slot.dispatcher.host_facts().is_some();
        let held = |slot: &Arc<CellSlot>| Arc::strong_count(slot) > 2 + records;
        if held(&slot) || (host && !host_left(&slot)) {
            slot.release_drained();
            return;
        }
        // Under the key's lock nobody takes the Cell, so once it goes unheld
        // and empty nothing can push to it; the fence shows a push its last
        // holder made before letting go.
        let unpushed = |_: &String, slot: &Arc<CellSlot>| {
            !held(slot) && {
                fence(Ordering::Acquire);
                slot.mailbox.is_empty()
            }
        };
        if self.cells.remove_if(&key, unpushed).is_none() {
            if host {
                // The host was told the Cell left, and drops its state when it
                // takes the leave (a leave it has not answered may still be
                // pending), so the Cell starts again in place: its timers and
                // durable state go too. Its gates held, nothing was settled
                // since the check above.
                *slot.timers.lock_recover() = Default::default();
                drop_timers_and_durable_state(&slot);
                slot.drain_active.store(false, Ordering::Release);
            }
            slot.release_drained();
            return;
        }
        // Folded first, so an activity read between the removal and here
        // misses the Cell for as short a time as it can.
        self.retired.add(slot.counters());
        if let Some(id) = slot.flusher_entry_id {
            CellFlusher::instance().deregister(id);
        }
        drop_timers_and_durable_state(&slot);
        if host {
            // Whatever reaches it later through the run's records finds the
            // gate free rather than waiting for ever.
            slot.drain_active.store(false, Ordering::Release);
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
            relay_stopped: Arc::clone(&self.relay_stopped),
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

/// Take a host-run Cell's drain gate and tell the host it left; `false`, the
/// gate as found, when a command holds the Cell (a live read, say) or
/// something it settled has not shipped: the Cell may yet start again in
/// place, and its holds last until their records ship.
fn host_left(slot: &Arc<CellSlot>) -> bool {
    if slot.drain_active.swap(true, Ordering::AcqRel) {
        return false;
    }
    if !slot.flush_buffer.lock_recover().nothing_unshipped() {
        slot.drain_active.store(false, Ordering::Release);
        return false;
    }
    crate::host::cell_left_locked(slot);
    true
}

/// A Cell's pending timer wakes and, where it persists, its durable state.
/// The delete is a durable frame: the class holds it for the next bound
/// stream, behind this Cell's own records, so a reconnect window cannot leave
/// the controller holding state the Cell no longer has. The body is the bare
/// identity the controller's `decode_cell_id` reads.
fn drop_timers_and_durable_state(slot: &CellSlot) {
    crate::poll_core::global().discard_cell_timers(&slot.cell_identity);
    if slot.keeps_state {
        state_sink::send(
            state_sink::DELETE_CELL_STATE_PAYLOAD_ID,
            slot.cell_identity.to_vec(),
        );
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

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::JoinHandle;
    use std::time::Duration;

    use super::DynamicCellRegistry;
    use crate::admission::try_admit;
    use crate::cell::{CellItem, CellSlot};
    use crate::poll_core::{CommandReply, EngineActivation, EnginePayload};
    use crate::relay::instance::RelayInstance;
    use crate::schema;

    fn registry(id: &str) -> (Arc<RelayInstance>, Arc<DynamicCellRegistry>) {
        let id = format!("{id}_{}", std::process::id());
        let json = format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","durable":false,"run":"flow",
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"key"}},{{"link":"into","tasklet":0}}]]}}],
                 "tasklets":[{{"name":"trades","cell_mode":"dynamic","mailbox_capacity":16}}]}}"#
        );
        crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
        let instance = RelayInstance::of_class(&id, &format!("{id}_inst")).expect("registered");
        let registry = instance.dynamic_registries[0].clone().expect("dynamic");
        (instance, registry)
    }

    fn answered() -> schema::ActivationResult {
        let outcome = schema::ActivationOutcome::StateApplied;
        schema::ActivationResult { token: 0, outcome }
    }

    /// Answers every leave the sweep sends `relay`'s host, as a host would,
    /// running `meanwhile` while each one is pending, until the guard drops.
    pub(crate) struct LeaveAnswerer(Arc<AtomicBool>, Option<JoinHandle<()>>);

    impl LeaveAnswerer {
        pub(crate) fn start(relay: String) -> Self {
            Self::with(relay, || {})
        }

        fn with(relay: String, meanwhile: impl Fn() + Send + 'static) -> Self {
            let stop = Arc::new(AtomicBool::new(false));
            let stopping = Arc::clone(&stop);
            let handle = std::thread::spawn(move || {
                while !stopping.load(Ordering::Acquire) {
                    let Some(act) = crate::poll_core::global().take_leave(&relay) else {
                        std::thread::yield_now();
                        continue;
                    };
                    meanwhile();
                    let result = schema::ActivationResult {
                        token: act.token,
                        ..answered()
                    };
                    crate::host::apply_result(crate::poll_core::global(), act, result)
                        .expect("answered");
                }
            });
            Self(stop, Some(handle))
        }
    }

    impl Drop for LeaveAnswerer {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
            if let Some(handle) = self.1.take() {
                let _ = handle.join();
            }
        }
    }

    fn idle(slot: &CellSlot) {
        slot.last_activity_ms
            .store(crate::util::monotonic_ms() - 60_000, Ordering::Relaxed);
    }

    /// Whether `key` names the Cell a push went into.
    fn named(registry: &DynamicCellRegistry, key: &str, pushed: *const CellSlot) -> bool {
        registry
            .cells
            .get(key)
            .is_some_and(|slot| std::ptr::eq(Arc::as_ptr(slot.value()), pushed))
    }

    /// A router took the Cell before the sweep and pushes after it.
    #[test]
    fn a_push_after_an_eviction_reaches_a_cell_the_key_names() {
        let (instance, registry) = registry("kb501_evict_push");
        let _host = LeaveAnswerer::start(instance.instance_id.to_string());
        let routed = registry.get_or_create("a").expect("minted");
        idle(&routed);
        registry.reap_sweep(Duration::from_millis(1));
        assert!(try_admit(&routed, CellItem::CloseInterval).is_ok());
        assert!(
            named(&registry, "a", Arc::as_ptr(&routed)),
            "a push landed in an evicted slot: {} item(s) queued, drain gate still held: {}",
            routed.mailbox.len(),
            !routed.mailbox.claim_inflight()
        );
    }

    /// A router takes the Cell while its leave is pending and pushes once the
    /// sweep is done.
    #[test]
    fn a_cell_routed_during_its_leave_stays_named_by_its_key() {
        let (instance, registry) = registry("kb501_route_in_leave");
        let routed: Arc<Mutex<Option<Arc<CellSlot>>>> = Arc::default();
        let (routing, taken) = (Arc::clone(&registry), Arc::clone(&routed));
        let _host = LeaveAnswerer::with(instance.instance_id.to_string(), move || {
            *taken.lock().unwrap() = routing.get_or_create("a").ok();
        });
        idle(&registry.get_or_create("a").expect("minted"));
        registry.reap_sweep(Duration::from_millis(1));
        let routed = routed
            .lock()
            .unwrap()
            .take()
            .expect("routed during the leave");
        assert!(try_admit(&routed, CellItem::CloseInterval).is_ok());
        assert!(
            named(&registry, "a", Arc::as_ptr(&routed)),
            "a push landed in an evicted slot: {} item(s) queued",
            routed.mailbox.len()
        );
    }

    /// A host that answered a leave but still holds it, as a leave that timed
    /// out is held, keeps the Cell started again in place, and no sweep sends
    /// it a second leave until the host lets go.
    #[test]
    fn a_leave_the_host_still_holds_pins_its_cell() {
        let (instance, registry) = registry("kb501_held_leave");
        let relay = instance.instance_id.to_string();
        let slot = Arc::downgrade(&registry.get_or_create("a").expect("minted"));
        let sweep = |registry: &DynamicCellRegistry| {
            idle(registry.cells.get("a").expect("named").value());
            registry.reap_sweep(Duration::from_millis(1));
        };
        let sweeping = Arc::clone(&registry);
        let first = std::thread::spawn(move || sweep(&sweeping));
        let act = crate::host::leave_tests::take_leave(&relay);
        let EnginePayload::Command {
            slot: held, reply, ..
        } = act.payload
        else {
            panic!("a Cell's leave is a command");
        };
        reply.answer(answered().outcome);
        first.join().expect("the sweep ends");
        assert!(
            named(&registry, "a", slot.as_ptr()),
            "started again in place"
        );

        sweep(&registry);
        let again = crate::poll_core::global().take_leave(&relay);
        assert!(again.is_none(), "a second leave while the host holds one");
        assert!(named(&registry, "a", slot.as_ptr()));

        drop(held);
        let _host = LeaveAnswerer::start(relay);
        sweep(&registry);
        assert!(!registry.cells.contains_key("a"), "evicted once let go");
        let evicted = registry
            .counters
            .cells_evicted_total
            .load(Ordering::Relaxed);
        assert_eq!(evicted, 1);
    }

    /// The answer to a command reaches its waiter once the activation let go
    /// of the Cell, so an eviction counting who holds it finds no leave.
    #[test]
    fn a_command_answer_lets_go_of_its_cell_first() {
        let (_instance, registry) = registry("kb501_answer_order");
        let slot = registry.get_or_create("a").expect("minted");
        let watched = Arc::downgrade(&slot);
        let (tx, rx) = std::sync::mpsc::channel();
        let payload = EnginePayload::Command {
            slot,
            change_seq: 0,
            kind: schema::ActivationKind::Leave { whole_relay: false },
            values: Vec::new(),
            offset: None,
            reply: CommandReply::new(move |_| {
                let _ = tx.send(watched.strong_count());
            }),
        };
        let act = EngineActivation { token: 0, payload };
        crate::host::apply_result(crate::poll_core::global(), act, answered()).expect("answered");
        assert_eq!(
            rx.recv().expect("answered"),
            1,
            "Cell holders at the answer"
        );
    }
}
