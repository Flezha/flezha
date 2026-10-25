//! `CellState`: an instance's state as the per-tasklet / per-Cell JSON
//! shape, and `apply_init_values`, its inverse. The invariant: a read is
//! taken under the cell gate — the value is the freshest and the seq it
//! reports says which version it is — and only a migration's drains.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Map, Value};

use super::{CommandAction, Reply};
use crate::cell::CellSlot;
use crate::channel::TaskletId;
use crate::dynamic::DynamicCellRegistry;
use crate::error::{Code, Error};
use crate::pb::CellState;
use crate::relay::instance::RelayInstance;
use crate::relay::topology::TaskletDescriptor;
use crate::runner::RunnerNode;
use crate::util::LockRecover;

/// The `change_seq` a single selected Cell was serialized at.
pub const CHANGE_SEQ_META_KEY: &str = "flz.change_seq";

/// A serialize's wait for the Cell gate, and a graceful ship's for a host's
/// answer: inside the 10 s serializer ceiling and the container's grace period.
pub const SERIALIZE_GATE_WAIT: Duration = Duration::from_secs(5);

/// Which Cells a read covers; both absent is the whole-relay walk.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StateSelector<'a> {
    pub tasklet: Option<&'a str>,
    pub cell_key: Option<&'a str>,
}

impl CommandAction for CellState {
    fn handle(&self, runner: &mut RunnerNode, message: &str, errors: &mut Vec<Error>) -> Reply {
        self.reply(runner, message, errors).0
    }

    fn reply(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        errors: &mut Vec<Error>,
    ) -> (Reply, HashMap<String, String>) {
        let reply = |bytes| Reply {
            payload_id: "cell_state",
            bytes,
        };

        let instance = match runner.get_relay(&self.relay_id) {
            Some(i) => i,
            None => {
                errors.push(relay_not_found(&self.relay_id));
                return (reply(Vec::new()), HashMap::new());
            }
        };

        match serialize_selected_state(&instance, selector(self), StatePurpose::LiveRead) {
            Ok((payload, Some(seq))) => (
                reply(payload),
                HashMap::from([(CHANGE_SEQ_META_KEY.to_string(), seq.to_string())]),
            ),
            Ok((payload, None)) => (reply(payload), HashMap::new()),
            Err(refusal) => {
                errors.push(refusal);
                (reply(Vec::new()), HashMap::new())
            }
        }
    }
}

/// A command naming a relay this runner holds no instance of.
pub(crate) fn relay_not_found(relay_id: &str) -> Error {
    Error::State(Code::RelayNotFound.with([("relay", relay_id.into())]))
}

/// The command's selector; proto3 leaves an unset string empty.
fn selector(cmd: &CellState) -> StateSelector<'_> {
    StateSelector {
        tasklet: non_empty(&cmd.tasklet),
        cell_key: non_empty(&cmd.cell_key),
    }
}

fn non_empty(s: &str) -> Option<&str> {
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

/// Why a serialization was asked for; a host class is asked with the
/// matching activation kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatePurpose {
    /// Each Cell waits for its gate up to [`SERIALIZE_GATE_WAIT`].
    LiveRead,
    /// Every Cell of the walk waits, for its gate, its credit and its host's
    /// answer, only until this one deadline.
    Migration { until: Instant },
}

/// The whole-relay walk, which cannot be refused.
pub fn serialize_relay_state(instance: &Arc<RelayInstance>) -> Vec<u8> {
    let purpose = StatePurpose::Migration {
        until: Instant::now() + SERIALIZE_GATE_WAIT,
    };
    serialize_selected_state(instance, StateSelector::default(), purpose)
        .map(|(payload, _seq)| payload)
        .unwrap_or_default()
}

/// `{"<tasklet>": <state>}` for single mode, `{"<tasklet>": {"<key>":
/// <state>}}` for keyed, narrowed by `selector`. The seq is `Some` only for
/// exactly one Cell. In a walk a failing Cell is logged and omitted; a
/// read that names one Cell answers its failure, never an empty object.
pub fn serialize_selected_state(
    instance: &Arc<RelayInstance>,
    selector: StateSelector<'_>,
    purpose: StatePurpose,
) -> Result<(Vec<u8>, Option<u64>), Error> {
    if selector.cell_key.is_some() && selector.tasklet.is_none() {
        return Err(Error::State(Code::CellKeyWithoutTasklet.with([])));
    }

    let mut top: Map<String, Value> = Map::new();
    let mut seqs: Vec<u64> = Vec::new();
    let mut matched_tasklet = false;

    for (tasklet_id, tasklet_desc) in instance.topology.tasklets.iter().enumerate() {
        if selector
            .tasklet
            .is_some_and(|wanted| wanted != &*tasklet_desc.name)
        {
            continue;
        }
        matched_tasklet = true;
        collect_tasklet(instance, tasklet_id, selector, purpose, &mut top, &mut seqs)?;
    }

    if let Some(tasklet) = selector.tasklet {
        if !matched_tasklet {
            return Err(Error::State(Code::TaskletNotFound.with([
                ("relay", instance.instance_id.as_ref().into()),
                ("tasklet", tasklet.into()),
            ])));
        }
    }

    let payload = serde_json::to_vec(&Value::Object(top)).unwrap_or_default();
    let seq = match seqs.as_slice() {
        [only] => Some(*only),
        _ => None,
    };
    Ok((payload, seq))
}

/// One tasklet's state into `top`: a lookup for one key, a walk for all.
fn collect_tasklet(
    instance: &Arc<RelayInstance>,
    tasklet_id: usize,
    selector: StateSelector<'_>,
    purpose: StatePurpose,
    top: &mut Map<String, Value>,
    seqs: &mut Vec<u64>,
) -> Result<(), Error> {
    let tasklet_desc = &instance.topology.tasklets[tasklet_id];
    let tasklet_name = tasklet_desc.name.to_string();
    let Some(keyed) = Keyset::of(instance, tasklet_id) else {
        if let Some(key) = selector.cell_key {
            return Err(missing_cell_refusal(false, &tasklet_name, key));
        }
        if let Some(slot) = instance
            .cells
            .get(tasklet_id)
            .and_then(|cells| cells.first())
        {
            collect(tasklet_desc, slot, &tasklet_name, purpose, top, seqs);
        }
        return Ok(());
    };

    let mut inner: Map<String, Value> = Map::new();
    match selector.cell_key {
        Some(wanted) => {
            let Some(slot) = keyed.get(wanted) else {
                return Err(missing_cell_refusal(
                    keyed.was_evicted(wanted),
                    &tasklet_name,
                    wanted,
                ));
            };
            let (value, seq) = serialize_slot(tasklet_desc, &slot, purpose)?;
            inner.insert(wanted.to_string(), value);
            seqs.push(seq);
        }
        None => {
            for (key, slot) in keyed.pairs() {
                collect(tasklet_desc, &slot, &key, purpose, &mut inner, seqs);
            }
        }
    }
    top.insert(tasklet_name, Value::Object(inner));
    Ok(())
}

/// A keyed tasklet's live Cells, by declared keys or by registry.
enum Keyset<'a> {
    Static {
        keys: &'a [&'static str],
        slots: &'a [Arc<CellSlot>],
    },
    Dynamic(&'a DynamicCellRegistry),
}

impl<'a> Keyset<'a> {
    fn of(instance: &'a Arc<RelayInstance>, tasklet_id: usize) -> Option<Self> {
        if let Some(registry) = instance.dynamic_registry(tasklet_id as TaskletId) {
            return Some(Keyset::Dynamic(registry));
        }
        let tasklet_desc = instance.topology.tasklets.get(tasklet_id)?;
        if tasklet_desc.cell_keys.is_empty() {
            return None;
        }
        Some(Keyset::Static {
            keys: &tasklet_desc.cell_keys,
            slots: instance.cells.get(tasklet_id)?,
        })
    }

    fn get(&self, key: &str) -> Option<Arc<CellSlot>> {
        match self {
            Keyset::Static { keys, slots } => keys
                .iter()
                .position(|declared| *declared == key)
                .and_then(|idx| slots.get(idx))
                .map(Arc::clone),
            Keyset::Dynamic(registry) => registry.cells.get(key).map(|e| Arc::clone(e.value())),
        }
    }

    fn pairs(&self) -> Vec<(String, Arc<CellSlot>)> {
        match self {
            Keyset::Static { keys, slots } => keys
                .iter()
                .zip(slots.iter())
                .map(|(key, slot)| ((*key).to_string(), Arc::clone(slot)))
                .collect(),
            Keyset::Dynamic(registry) => registry
                .cells
                .iter()
                .map(|e| (e.key().clone(), Arc::clone(e.value())))
                .collect(),
        }
    }

    /// Only dynamic mode evicts.
    fn was_evicted(&self, key: &str) -> bool {
        match self {
            Keyset::Static { .. } => false,
            Keyset::Dynamic(registry) => registry.recently_evicted(key),
        }
    }
}

/// One Cell into `into` under `key`; a failing one is logged and omitted.
fn collect(
    tasklet_desc: &TaskletDescriptor,
    slot: &Arc<CellSlot>,
    key: &str,
    purpose: StatePurpose,
    into: &mut Map<String, Value>,
    seqs: &mut Vec<u64>,
) {
    match serialize_slot(tasklet_desc, slot, purpose) {
        Ok((value, seq)) => {
            into.insert(key.to_string(), value);
            seqs.push(seq);
        }
        Err(e) => flz_transport::notice!(
            Error,
            "serialize_relay_state: tasklet `{}` key `{key}` failed: {e}",
            tasklet_desc.name
        ),
    }
}

/// One Cell's state and its seq, under the gate. A live read never drains;
/// a migration ships from a cut ([`crate::host::ship_to_cut`]), so the
/// destination's re-forward above the consumed offset folds nothing the
/// state already holds.
fn serialize_slot(
    tasklet_desc: &TaskletDescriptor,
    slot: &Arc<CellSlot>,
    purpose: StatePurpose,
) -> Result<(Value, u64), Error> {
    // A graceful ship is inside a grace period, so the whole walk waits for
    // gates, credit and hosts' answers only until its one deadline; a live
    // read keeps the per-Cell gate wait and the command wait.
    let (gate_wait, host_wait) = match purpose {
        StatePurpose::Migration { until } => {
            let left = until.saturating_duration_since(Instant::now());
            (left, left)
        }
        StatePurpose::LiveRead => (SERIALIZE_GATE_WAIT, crate::poll_core::HOST_COMMAND_TIMEOUT),
    };
    // Under the cell's handout exclusivity: a live read or a migration never
    // observes a half-applied activation. A Cell still inside one at the gate
    // wait (a retry's backoff holds the gate too) has nothing newer than its
    // last completed activation, and is omitted.
    crate::poll_core::global()
        .try_with_cell_exclusive(slot, gate_wait, || {
            if let StatePurpose::Migration { .. } = purpose {
                // Released before the state lock, the order a dispatch takes them in.
                drop(crate::host::ship_to_cut(slot, host_wait)?);
            }
            if slot.dispatcher.host_facts().is_some() {
                return crate::host::state_json_locked(slot, host_wait);
            }
            let state_guard = slot.state.lock_recover();
            let value = (tasklet_desc.serialize_state)(&state_guard).map_err(|reason| {
                Error::State(Code::SerializeFailed.with([
                    ("tasklet", tasklet_desc.name.as_ref().into()),
                    ("reason", reason.into()),
                ]))
            })?;
            let seq = slot.change_seq.load(Ordering::Relaxed);
            Ok((value, seq))
        })
        .unwrap_or_else(|| {
            let why = format!("still inside an activation after {gate_wait:?}");
            Err(crate::host::cell_busy(slot, &why))
        })
}

/// Evicted and never-existed serve the same absence; the diagnosis differs.
fn missing_cell_refusal(evicted: bool, tasklet_name: &str, key: &str) -> Error {
    let code = if evicted {
        Code::CellEvicted
    } else {
        Code::CellNotFound
    };
    Error::State(code.with([("tasklet", tasklet_name.into()), ("cell_key", key.into())]))
}

/// Apply an `init_values` JSON blob onto a fresh instance;
/// empty is a no-op, an unknown tasklet or key is logged and skipped.
pub fn apply_init_values(instance: &RelayInstance, init_values: &str) -> Result<(), Error> {
    if init_values.is_empty() {
        return Ok(());
    }
    let parsed: Value = serde_json::from_str(init_values).map_err(|e| {
        Error::State(Code::InitValuesInvalidJson.with([("reason", e.to_string().into())]))
    })?;
    let top = match parsed {
        Value::Object(map) => map,
        other => {
            return Err(Error::State(
                Code::InitValuesNotObject.with([("kind", value_kind(&other).into())]),
            ));
        }
    };

    for (tasklet_name, tasklet_value) in top {
        apply_tasklet(instance, &tasklet_name, tasklet_value);
    }

    Ok(())
}

/// Where a move's state carries what each Cell holds beside its value: the
/// pending timers and the next change seq, which the values alone lose. The
/// destination's Cell would otherwise restart its seq under the records the
/// source shipped, so a compaction there truncates them, timers included.
const MOVED_CELLS: &str = "$cells";

/// One Cell's part of a move beyond its value, keyed by Cell identity.
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct MovedCell {
    identity: Vec<u8>,
    timers: crate::timer::PendingTimers,
    next_seq: u64,
}

/// What a move ships: the relay's state, and every Cell's timers and next seq.
pub fn serialize_moved(instance: &Arc<RelayInstance>) -> Vec<u8> {
    let state = serialize_relay_state(instance);
    let cells: Vec<MovedCell> = instance
        .all_slots()
        .iter()
        .map(|slot| MovedCell {
            identity: slot.cell_identity.to_vec(),
            timers: slot.timers.lock_recover().clone(),
            next_seq: slot.change_seq.load(Ordering::Acquire),
        })
        .collect();
    let Ok(Value::Object(mut top)) = serde_json::from_slice(&state) else {
        return state;
    };
    top.insert(MOVED_CELLS.into(), serde_json::json!(cells));
    serde_json::to_vec(&top).unwrap_or(state)
}

/// Apply what `serialize_moved` shipped: the values as `apply_init_values`
/// does, then each Cell's seq and timers, as a restore gives them.
pub fn apply_moved(
    runner: &mut RunnerNode,
    instance: &RelayInstance,
    state: &str,
) -> Result<(), Error> {
    let (values, cells) = take_moved_cells(state);
    let applied = apply_init_values(instance, &values);
    restore_moved(runner, cells);
    applied
}

/// A moved state split into its values and the Cells beside them.
fn take_moved_cells(state: &str) -> (String, Vec<MovedCell>) {
    let Ok(Value::Object(mut top)) = serde_json::from_str(state) else {
        return (state.to_string(), Vec::new());
    };
    let Some(cells) = top.remove(MOVED_CELLS) else {
        return (state.to_string(), Vec::new());
    };
    (
        Value::Object(top).to_string(),
        serde_json::from_value(cells).unwrap_or_default(),
    )
}

/// Give each moved Cell what its source held, as a restore does: the seq goes
/// on from the source's and the timers are armed.
fn restore_moved(runner: &mut RunnerNode, cells: Vec<MovedCell>) {
    for cell in cells {
        let Some((_, slot)) = runner.find_cell(&cell.identity) else {
            continue;
        };
        crate::poll_core::global().with_cell_exclusive(&slot, || {
            slot.change_seq.fetch_max(cell.next_seq, Ordering::AcqRel);
            *slot.timers.lock_recover() = cell.timers;
        });
        crate::timer::rearm_restored(&slot);
    }
}

/// One `init_values` entry onto its tasklet's Cells.
fn apply_tasklet(instance: &RelayInstance, tasklet_name: &str, tasklet_value: Value) {
    let Some(&tasklet_id) = instance.topology.tasklet_name_to_id.get(tasklet_name) else {
        flz_transport::notice!(
            Warn,
            "apply_init_values: ignoring unknown tasklet `{tasklet_name}`"
        );
        return;
    };
    let Some(tasklet_desc) = instance.topology.tasklet(tasklet_id) else {
        return;
    };
    let Some(tasklet_cells) = instance.cells.get(tasklet_id as usize) else {
        return;
    };

    if tasklet_desc.cell_keys.is_empty() {
        if let Some(slot) = tasklet_cells.first() {
            apply_one(tasklet_desc, slot, &tasklet_value, tasklet_name, None);
        }
    } else {
        apply_static(
            instance,
            tasklet_id,
            tasklet_desc,
            tasklet_cells,
            tasklet_name,
            tasklet_value,
        );
    }
}

/// Static mode: `{cell_key: state, ...}`.
fn apply_static(
    instance: &RelayInstance,
    tasklet_id: u32,
    tasklet_desc: &TaskletDescriptor,
    tasklet_cells: &[Arc<CellSlot>],
    tasklet_name: &str,
    tasklet_value: Value,
) {
    let inner = match tasklet_value {
        Value::Object(m) => m,
        other => {
            flz_transport::notice!(
                Warn,
                "apply_init_values: tasklet `{tasklet_name}` expected object of \
                 cell_keys, got {} — skipping",
                value_kind(&other)
            );
            return;
        }
    };
    for (key, value) in inner {
        let idx = match instance
            .cell_key_to_idx
            .get(tasklet_id as usize)
            .and_then(|keys| keys.get(key.as_str()))
        {
            Some(&i) => i,
            None => {
                flz_transport::notice!(
                    Warn,
                    "apply_init_values: ignoring unknown cell_key \
                     `{key}` for tasklet `{tasklet_name}`"
                );
                continue;
            }
        };
        let slot = match tasklet_cells.get(idx) {
            Some(s) => s,
            None => continue,
        };
        apply_one(tasklet_desc, slot, &value, tasklet_name, Some(&key));
    }
}

fn apply_one(
    tasklet_desc: &crate::relay::topology::TaskletDescriptor,
    slot: &Arc<crate::cell::CellSlot>,
    value: &Value,
    tasklet_name: &str,
    cell_key: Option<&str>,
) {
    let at = || match cell_key {
        Some(k) => format!("tasklet `{tasklet_name}` key `{k}`"),
        None => format!("tasklet `{tasklet_name}`"),
    };
    if slot.dispatcher.host_facts().is_some() {
        // Host-held state: the value crosses as an ApplyState activation
        // under the same handout gate the typed swap takes.
        let applied = crate::poll_core::global()
            .with_cell_exclusive(slot, || crate::host::apply_state_locked(slot, value));
        if let Err(e) = applied {
            flz_transport::notice!(Error, "apply_init_values: {} apply failed: {e}", at());
        }
        return;
    }
    match (tasklet_desc.deserialize_state)(value) {
        Ok(new_state) => {
            // Apply-state replaces the materialized value whole; the handout
            // gate keeps it from landing mid-activation.
            crate::poll_core::global().with_cell_exclusive(slot, || {
                *slot.state.lock_recover() = new_state;
            });
        }
        Err(e) => {
            flz_transport::notice!(Error, "apply_init_values: {} deserialize failed: {e}", at())
        }
    }
}

fn value_kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    //! The graceful ship's per-Cell serialize against a Cell whose
    //! handout never completes: the wait ends at the gate bound and the Cell is
    //! omitted.
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::time::Duration;

    // fails when: a live move ships the Cells' values without their timers
    // and seq, so a moved session never closes.
    #[test]
    fn a_moved_state_gives_back_its_values_and_the_cells_beside_them() {
        let cell = super::MovedCell {
            identity: b"cell".to_vec(),
            timers: crate::timer::PendingTimers::default(),
            next_seq: 4,
        };
        let shipped = serde_json::json!({"t": {"touched": ["hello"]}, super::MOVED_CELLS: [&cell]});

        let (values, cells) = super::take_moved_cells(&shipped.to_string());

        assert_eq!(values, r#"{"t":{"touched":["hello"]}}"#);
        assert_eq!(cells, vec![cell]);
        assert_eq!(
            super::take_moved_cells(&values),
            (values.clone(), Vec::new())
        );
    }

    use super::*;
    use crate::test_support::host_class;
    use crate::ErrorModel;

    #[test]
    fn a_wedged_cell_is_omitted_from_the_ship_within_the_bound() {
        let instance = host_class::instance("busy_ship", &["in"], &[], &[1]);
        let slot = Arc::clone(&instance.cells[0][0]);
        // The flag `try_assemble` sets for a handout, held for ever: the
        // host took the activation and never answered it.
        assert!(!slot.drain_active.swap(true, Ordering::AcqRel));

        let (tx, rx) = mpsc::channel();
        let started = std::time::Instant::now();
        std::thread::spawn(move || {
            let _ = tx.send(serialize_relay_state(&instance));
        });
        // The wedged Cell costs the gate bound; the consumer, healthy but
        // with no poller to answer its state read, waits only for what is
        // left of the walk's one deadline, not a bound of its own.
        let payload = rx
            .recv_timeout(SERIALIZE_GATE_WAIT + Duration::from_secs(2))
            .expect("the ship must return within the gate bound");
        assert!(
            started.elapsed() >= SERIALIZE_GATE_WAIT,
            "the busy Cell is waited for up to the bound, not skipped"
        );
        assert_eq!(
            String::from_utf8_lossy(&payload),
            "{}",
            "a busy Cell is omitted, not waited for"
        );
        assert!(
            slot.drain_active.load(Ordering::Acquire),
            "a gate the ship did not win is not released by it"
        );
    }

    #[test]
    fn a_read_naming_one_busy_cell_is_refused_not_answered_empty() {
        let instance = host_class::instance("busy_read", &["in"], &["k1"], &[1]);
        let slot = Arc::clone(&instance.cells[0][0]);
        assert!(!slot.drain_active.swap(true, Ordering::AcqRel));

        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let selector = StateSelector {
                tasklet: Some("p0"),
                cell_key: Some("k1"),
            };
            let _ = tx.send(serialize_selected_state(
                &instance,
                selector,
                StatePurpose::LiveRead,
            ));
        });
        let answer = rx
            .recv_timeout(SERIALIZE_GATE_WAIT + Duration::from_secs(2))
            .expect("the read must return within the gate bound");
        let refusal = answer.expect_err("a read that names a busy Cell is refused");
        assert_eq!(
            refusal.code(),
            "state.cell_busy",
            "the refusal names the Cell as busy: {refusal}"
        );
    }
}
