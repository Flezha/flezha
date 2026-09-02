//! `EntityState`: an instance's state as the per-procedure / per-Entity JSON
//! shape, and `apply_init_values`, its inverse. The invariant: a read is
//! taken under the entity gate and never drains — the value is the freshest
//! and the seq it reports says which version it is.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value};

use super::{CommandAction, Reply};
use crate::dynamic::DynamicEntityRegistry;
use crate::entity::EntitySlot;
use crate::error::Error;
use crate::operative::instance::StreamInstance;
use crate::operative::topology::ProcedureDescriptor;
use crate::pb::EntityState;
use crate::runner::RunnerNode;
use crate::util::LockRecover;

/// The `change_seq` a single selected Entity was serialized at.
pub const CHANGE_SEQ_META_KEY: &str = "flz.change_seq";

/// Refusal tokens the controller matches as prefixes.
pub const ENTITY_KEY_WITHOUT_PROCEDURE: &str = "entity_key_without_procedure";
pub const PROCEDURE_NOT_FOUND: &str = "procedure_not_found";
pub const ENTITY_NOT_FOUND: &str = "entity_not_found";
pub const ENTITY_EVICTED: &str = "entity_evicted";
pub const ENTITY_BUSY: &str = "entity_busy";

/// A serialize's wait for the Entity gate, and a graceful ship's for a host's
/// answer: inside the 10 s serializer ceiling and the container's grace period.
pub const SERIALIZE_GATE_WAIT: Duration = Duration::from_secs(5);

/// Which Entities a read covers; both absent is the whole-operative walk.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StateSelector<'a> {
    pub procedure: Option<&'a str>,
    pub entity_key: Option<&'a str>,
}

impl CommandAction for EntityState {
    fn handle(&self, runner: &mut RunnerNode, message: &str, errors: &mut Vec<String>) -> Reply {
        self.reply(runner, message, errors).0
    }

    fn reply(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        errors: &mut Vec<String>,
    ) -> (Reply, HashMap<String, String>) {
        let reply = |bytes| Reply {
            payload_id: "entity_state",
            bytes,
        };

        let instance = match runner.get_stream(&self.operative_id) {
            Some(i) => i,
            None => {
                errors.push(format!(
                    "StreamOperative instance with id {} not found",
                    self.operative_id
                ));
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
                errors.push(refusal.to_string());
                (reply(Vec::new()), HashMap::new())
            }
        }
    }
}

/// The command's selector; proto3 leaves an unset string empty.
fn selector(cmd: &EntityState) -> StateSelector<'_> {
    StateSelector {
        procedure: non_empty(&cmd.procedure),
        entity_key: non_empty(&cmd.entity_key),
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
    LiveRead,
    Migration,
}

/// The whole-operative walk, which cannot be refused.
pub fn serialize_operative_state(instance: &Arc<StreamInstance>) -> Vec<u8> {
    serialize_selected_state(instance, StateSelector::default(), StatePurpose::Migration)
        .map(|(payload, _seq)| payload)
        .unwrap_or_default()
}

/// `{"<procedure>": <state>}` for single mode, `{"<procedure>": {"<key>":
/// <state>}}` for keyed, narrowed by `selector`. The seq is `Some` only for
/// exactly one Entity. In a walk a failing Entity is logged and omitted; a
/// read that names one Entity answers its failure, never an empty object.
pub fn serialize_selected_state(
    instance: &Arc<StreamInstance>,
    selector: StateSelector<'_>,
    purpose: StatePurpose,
) -> Result<(Vec<u8>, Option<u64>), Error> {
    if selector.entity_key.is_some() && selector.procedure.is_none() {
        return Err(Error::State(format!(
            "{}: an Entity key is unique only within its Procedure — name the \
             Procedure as well",
            ENTITY_KEY_WITHOUT_PROCEDURE
        )));
    }

    let mut top: Map<String, Value> = Map::new();
    let mut seqs: Vec<u64> = Vec::new();
    let mut matched_procedure = false;

    for (procedure_id, procedure_desc) in instance.topology.procedures.iter().enumerate() {
        if selector
            .procedure
            .is_some_and(|wanted| wanted != &*procedure_desc.name)
        {
            continue;
        }
        matched_procedure = true;
        collect_procedure(
            instance,
            procedure_id,
            selector,
            purpose,
            &mut top,
            &mut seqs,
        )?;
    }

    if let Some(procedure) = selector.procedure {
        if !matched_procedure {
            return Err(Error::State(format!(
                "{}: operative `{}` has no Procedure named `{}`",
                PROCEDURE_NOT_FOUND, instance.instance_id, procedure
            )));
        }
    }

    let payload = serde_json::to_vec(&Value::Object(top)).unwrap_or_default();
    let seq = match seqs.as_slice() {
        [only] => Some(*only),
        _ => None,
    };
    Ok((payload, seq))
}

/// One procedure's state into `top`: a lookup for one key, a walk for all.
fn collect_procedure(
    instance: &Arc<StreamInstance>,
    procedure_id: usize,
    selector: StateSelector<'_>,
    purpose: StatePurpose,
    top: &mut Map<String, Value>,
    seqs: &mut Vec<u64>,
) -> Result<(), Error> {
    let procedure_desc = &instance.topology.procedures[procedure_id];
    let procedure_name = procedure_desc.name.to_string();
    let Some(keyed) = Keyset::of(instance, procedure_id) else {
        if let Some(key) = selector.entity_key {
            return Err(missing_entity_refusal(false, &procedure_name, key));
        }
        if let Some(slot) = instance
            .entities
            .get(procedure_id)
            .and_then(|entities| entities.first())
        {
            collect(procedure_desc, slot, &procedure_name, purpose, top, seqs);
        }
        return Ok(());
    };

    let mut inner: Map<String, Value> = Map::new();
    match selector.entity_key {
        Some(wanted) => {
            let Some(slot) = keyed.get(wanted) else {
                return Err(missing_entity_refusal(
                    keyed.was_evicted(wanted),
                    &procedure_name,
                    wanted,
                ));
            };
            let (value, seq) = serialize_slot(procedure_desc, &slot, purpose)?;
            inner.insert(wanted.to_string(), value);
            seqs.push(seq);
        }
        None => {
            for (key, slot) in keyed.pairs() {
                collect(procedure_desc, &slot, &key, purpose, &mut inner, seqs);
            }
        }
    }
    top.insert(procedure_name, Value::Object(inner));
    Ok(())
}

/// A keyed procedure's live Entities, by declared keys or by registry.
enum Keyset<'a> {
    Static {
        keys: &'a [&'static str],
        slots: &'a [Arc<EntitySlot>],
    },
    Dynamic(&'a DynamicEntityRegistry),
}

impl<'a> Keyset<'a> {
    fn of(instance: &'a Arc<StreamInstance>, procedure_id: usize) -> Option<Self> {
        if let Some(Some(registry)) = instance.dynamic_registries.get(procedure_id) {
            return Some(Keyset::Dynamic(registry));
        }
        let procedure_desc = instance.topology.procedures.get(procedure_id)?;
        if procedure_desc.entity_keys.is_empty() {
            return None;
        }
        Some(Keyset::Static {
            keys: &procedure_desc.entity_keys,
            slots: instance.entities.get(procedure_id)?,
        })
    }

    fn get(&self, key: &str) -> Option<Arc<EntitySlot>> {
        match self {
            Keyset::Static { keys, slots } => keys
                .iter()
                .position(|declared| *declared == key)
                .and_then(|idx| slots.get(idx))
                .map(Arc::clone),
            Keyset::Dynamic(registry) => registry.entities.get(key).map(|e| Arc::clone(e.value())),
        }
    }

    fn pairs(&self) -> Vec<(String, Arc<EntitySlot>)> {
        match self {
            Keyset::Static { keys, slots } => keys
                .iter()
                .zip(slots.iter())
                .map(|(key, slot)| ((*key).to_string(), Arc::clone(slot)))
                .collect(),
            Keyset::Dynamic(registry) => registry
                .entities
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

/// One Entity into `into` under `key`; a failing one is logged and omitted.
fn collect(
    procedure_desc: &ProcedureDescriptor,
    slot: &Arc<EntitySlot>,
    key: &str,
    purpose: StatePurpose,
    into: &mut Map<String, Value>,
    seqs: &mut Vec<u64>,
) {
    match serialize_slot(procedure_desc, slot, purpose) {
        Ok((value, seq)) => {
            into.insert(key.to_string(), value);
            seqs.push(seq);
        }
        Err(e) => eprintln!(
            "[flezha] serialize_operative_state: procedure `{}` key `{}` failed: {}",
            procedure_desc.name, key, e
        ),
    }
}

/// One Entity's state and its seq, under the gate and without draining.
fn serialize_slot(
    procedure_desc: &ProcedureDescriptor,
    slot: &Arc<EntitySlot>,
    purpose: StatePurpose,
) -> Result<(Value, u64), Error> {
    // A graceful ship is inside a grace period, so it waits for a host's
    // answer only as long as for the gate; a live read keeps the command wait.
    let migration = purpose == StatePurpose::Migration;
    let host_wait = if migration {
        SERIALIZE_GATE_WAIT
    } else {
        crate::poll_core::HOST_COMMAND_TIMEOUT
    };
    // Under the entity's handout exclusivity: a live read (or a migration
    // serialize, which shares this seam) never observes a half-applied
    // activation. An Entity still inside one at the gate wait has nothing
    // newer than its last completed activation, and is omitted.
    crate::poll_core::global()
        .try_with_entity_exclusive(slot, SERIALIZE_GATE_WAIT, || {
            if slot.dispatcher.host_facts().is_some() {
                return crate::host::state_json_locked(slot, migration, host_wait);
            }
            let state_guard = slot.state.lock_recover();
            let value = (procedure_desc.serialize_state)(&state_guard).map_err(Error::State)?;
            let seq = slot.change_seq.load(Ordering::Relaxed);
            Ok((value, seq))
        })
        .unwrap_or_else(|| {
            Err(Error::State(format!(
                "{ENTITY_BUSY}: Entity `{}` is inside an activation older than {}s",
                slot.dispatcher.entity_label(),
                SERIALIZE_GATE_WAIT.as_secs()
            )))
        })
}

/// Evicted and never-existed serve the same absence; the diagnosis differs.
fn missing_entity_refusal(evicted: bool, procedure_name: &str, key: &str) -> Error {
    Error::State(if evicted {
        format!(
            "{}: Entity `{}` of procedure `{}` was idle-evicted; eviction is \
             destructive, so there is no durable value to serve",
            ENTITY_EVICTED, key, procedure_name
        )
    } else {
        format!(
            "{}: procedure `{}` holds no Entity keyed `{}`",
            ENTITY_NOT_FOUND, procedure_name, key
        )
    })
}

/// Apply an `init_values` JSON blob onto a fresh instance; empty is a no-op,
/// an unknown procedure or key is logged and skipped.
pub fn apply_init_values(instance: &Arc<StreamInstance>, init_values: &str) -> Result<(), Error> {
    if init_values.is_empty() {
        return Ok(());
    }
    let parsed: Value = serde_json::from_str(init_values)
        .map_err(|e| Error::State(format!("Failed to parse init_values as JSON: {}", e)))?;
    let top = match parsed {
        Value::Object(map) => map,
        other => {
            return Err(Error::State(format!(
                "init_values must be a JSON object, got {}",
                value_kind(&other)
            )));
        }
    };

    for (procedure_name, procedure_value) in top {
        apply_procedure(instance, &procedure_name, procedure_value);
    }

    Ok(())
}

/// One `init_values` entry onto its procedure's Entities.
fn apply_procedure(instance: &Arc<StreamInstance>, procedure_name: &str, procedure_value: Value) {
    let Some(&procedure_id) = instance.topology.procedure_name_to_id.get(procedure_name) else {
        eprintln!(
            "[flezha] apply_init_values: ignoring unknown procedure `{}`",
            procedure_name
        );
        return;
    };
    let Some(procedure_desc) = instance.topology.procedure(procedure_id) else {
        return;
    };
    let Some(procedure_entities) = instance.entities.get(procedure_id as usize) else {
        return;
    };

    if procedure_desc.entity_keys.is_empty() {
        if let Some(slot) = procedure_entities.first() {
            apply_one(procedure_desc, slot, &procedure_value, procedure_name, None);
        }
    } else {
        apply_static(
            instance,
            procedure_id,
            procedure_desc,
            procedure_entities,
            procedure_name,
            procedure_value,
        );
    }
}

/// Static mode: `{entity_key: state, ...}`.
fn apply_static(
    instance: &Arc<StreamInstance>,
    procedure_id: u32,
    procedure_desc: &ProcedureDescriptor,
    procedure_entities: &[Arc<EntitySlot>],
    procedure_name: &str,
    procedure_value: Value,
) {
    let inner = match procedure_value {
        Value::Object(m) => m,
        other => {
            eprintln!(
                "[flezha] apply_init_values: procedure `{}` expected object of \
                 entity_keys, got {} — skipping",
                procedure_name,
                value_kind(&other)
            );
            return;
        }
    };
    let Some(key_to_idx) = instance.entity_key_to_idx.get(procedure_id as usize) else {
        return;
    };
    for (key, value) in inner {
        let idx = match key_to_idx.get(key.as_str()) {
            Some(&i) => i,
            None => {
                eprintln!(
                    "[flezha] apply_init_values: ignoring unknown entity_key \
                     `{}` for procedure `{}`",
                    key, procedure_name
                );
                continue;
            }
        };
        let slot = match procedure_entities.get(idx) {
            Some(s) => s,
            None => continue,
        };
        apply_one(procedure_desc, slot, &value, procedure_name, Some(&key));
    }
}

fn apply_one(
    procedure_desc: &crate::operative::topology::ProcedureDescriptor,
    slot: &Arc<crate::entity::EntitySlot>,
    value: &Value,
    procedure_name: &str,
    entity_key: Option<&str>,
) {
    if slot.dispatcher.host_facts().is_some() {
        // Host-held state: the value crosses as an ApplyState activation
        // under the same handout gate the typed swap takes.
        let applied = crate::poll_core::global()
            .with_entity_exclusive(slot, || crate::host::apply_state_locked(slot, value));
        if let Err(e) = applied {
            match entity_key {
                Some(k) => eprintln!(
                    "[flezha] apply_init_values: procedure `{}` key `{}` apply failed: {}",
                    procedure_name, k, e
                ),
                None => eprintln!(
                    "[flezha] apply_init_values: procedure `{}` apply failed: {}",
                    procedure_name, e
                ),
            }
        }
        return;
    }
    match (procedure_desc.deserialize_state)(value) {
        Ok(new_state) => {
            // Apply-state replaces the materialized value whole; the handout
            // gate keeps it from landing mid-activation.
            crate::poll_core::global().with_entity_exclusive(slot, || {
                *slot.state.lock_recover() = new_state;
            });
        }
        Err(e) => match entity_key {
            Some(k) => eprintln!(
                "[flezha] apply_init_values: procedure `{}` key `{}` deserialize failed: {}",
                procedure_name, k, e
            ),
            None => eprintln!(
                "[flezha] apply_init_values: procedure `{}` deserialize failed: {}",
                procedure_name, e
            ),
        },
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
    //! The graceful ship's per-Entity serialize against an Entity whose
    //! handout never completes: the wait ends at the gate bound and the Entity is
    //! omitted.
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;
    use crate::test_support::host_class;

    #[test]
    fn a_wedged_entity_is_omitted_from_the_ship_within_the_bound() {
        let instance = host_class::instance("busy_ship", &[], &[1]);
        let slot = Arc::clone(&instance.entities[0][0]);
        // The flag `try_assemble` sets for a handout, held for ever: the
        // host took the activation and never answered it.
        assert!(!slot.drain_active.swap(true, Ordering::AcqRel));

        let (tx, rx) = mpsc::channel();
        let started = std::time::Instant::now();
        std::thread::spawn(move || {
            let _ = tx.send(serialize_operative_state(&instance));
        });
        // The wedged Entity costs the gate bound; the consumer, healthy but
        // with no poller to answer its migration serialize, costs the host
        // bound — the same figure, by design.
        let payload = rx
            .recv_timeout(2 * SERIALIZE_GATE_WAIT + Duration::from_secs(2))
            .expect("the ship must return within the gate bound");
        assert!(
            started.elapsed() >= SERIALIZE_GATE_WAIT,
            "the busy Entity is waited for up to the bound, not skipped"
        );
        assert_eq!(
            String::from_utf8_lossy(&payload),
            "{}",
            "a busy Entity is omitted, not waited for"
        );
        assert!(
            slot.drain_active.load(Ordering::Acquire),
            "a gate the ship did not win is not released by it"
        );
    }

    #[test]
    fn a_read_naming_one_busy_entity_is_refused_not_answered_empty() {
        let instance = host_class::instance("busy_read", &["k1"], &[1]);
        let slot = Arc::clone(&instance.entities[0][0]);
        assert!(!slot.drain_active.swap(true, Ordering::AcqRel));

        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let selector = StateSelector {
                procedure: Some("p0"),
                entity_key: Some("k1"),
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
        let refusal = answer.expect_err("a read that names a busy Entity is refused");
        assert!(
            refusal.to_string().starts_with(ENTITY_BUSY),
            "the refusal names the Entity as busy: {refusal}"
        );
    }
}
