use prost::Message;

use super::CommandAction;
use crate::operative::instance::KeyGroupRange;
use crate::pb::{AddOperative, AddOperativeResponse};
use crate::runner::RunnerNode;
use crate::workflow::spec::WorkflowSpec;

impl CommandAction for AddOperative {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let instance_id = self.operative_id.clone();
        let factory_id = self.factory_id.clone();

        // Empty kind ⇒ `stream`, the wire default.
        let kind = if self.kind.is_empty() {
            "stream"
        } else {
            self.kind.as_str()
        };

        // Idempotent on reconnect: the controller re-issues AddOperative for
        // every StreamOperative instance ProcessHub still has tagged to this
        // runner's profile after a restart. If the instance is already
        // in the local registry, skip without re-registering — keeps
        // the existing per-Entity state intact and prevents
        // duplicate-init crashes.
        if runner.has_operative(&instance_id) {
            return (
                "start_operative".to_string(),
                metadata_response(kind, &factory_id),
            );
        }

        match kind {
            "workflow" => {
                if !runner.start_workflow_resumable(
                    factory_id.clone(),
                    instance_id.clone(),
                    self.init_values.clone(),
                    self.resume_entity_count,
                    self.proc_journal.as_ref().map(Into::into),
                ) {
                    errors.push(format!(
                        "WorkflowOperative class with factory_id {} is not registered",
                        factory_id
                    ));
                }
            }
            "stream" => match partition_refusal(self, &factory_id) {
                // A keyless class cannot be partitioned: each member would
                // fold its own input slice into a private copy of "the one"
                // state — N divergent partial truths, silently. Only the SDK
                // can tell (key closures are compiled code, never on the
                // wire), so the authoritative refusal is here, at the bind.
                Some(refusal) => errors.push(refusal),
                None => match runner.start_operative(
                    factory_id.clone(),
                    instance_id.clone(),
                    self.key_group_range(),
                ) {
                    Some(instance) => {
                        if !self.init_values.is_empty() {
                            if let Err(e) = crate::command::entity_state::apply_init_values(
                                &instance,
                                &self.init_values,
                            ) {
                                errors.push(e);
                            }
                        }
                    }
                    None => {
                        errors.push(format!(
                            "StreamOperative class with factory_id {} is not registered",
                            factory_id
                        ));
                    }
                },
            },
            other => {
                errors.push(format!(
                    "unknown compute-unit kind '{}' for unit {}",
                    other, instance_id
                ));
            }
        }

        (
            "start_operative".to_string(),
            metadata_response(kind, &factory_id),
        )
    }
}

impl AddOperative {
    /// The member's owned key-group range. `partition_count < 2` is every
    /// pre-partition bind and every `--partitions 1` start; it reads as the
    /// whole space with the dispatch guard inert.
    fn key_group_range(&self) -> KeyGroupRange {
        if self.partition_count < 2 {
            return KeyGroupRange::UNPARTITIONED;
        }
        KeyGroupRange {
            index: self.partition_index,
            count: self.partition_count,
            from: self.key_group_from,
            to: self.key_group_to,
        }
    }
}

/// Whether at least one procedure fed by an entry channel is keyed — a
/// `key_by` edge (`Dynamic`) or a declared keyset (`Static`). The one
/// definition of "partitionable", read by both the bind refusal and the
/// declaration the controller preflights against.
fn has_keyed_entry_procedure(topology: &crate::operative::topology::OperativeTopology) -> bool {
    use crate::channel::ChannelKind;
    use crate::operative::topology::EntityMode;

    topology
        .channels
        .iter()
        .filter(|channel| channel.kind == ChannelKind::Entry)
        .flat_map(|channel| channel.consumers.iter())
        .filter_map(|edge| topology.procedure(edge.procedure))
        .any(|procedure| matches!(procedure.mode, EntityMode::Dynamic | EntityMode::Static))
}

/// `Some(error)` when this bind asks a class that cannot be partitioned to be.
fn partition_refusal(command: &AddOperative, factory_id: &str) -> Option<String> {
    if command.partition_count < 2 {
        return None;
    }
    let topology = crate::operative::registry::get_class(factory_id)?;
    if has_keyed_entry_procedure(&topology) {
        None
    } else {
        Some(format!(
            "PartitionsWithoutKeyedProcedure: class {} has no keyed entry-fed procedure, \
             so its {} partitions would each fold a slice of the input into a private copy \
             of the same state — declare `key_by` on the entry edge (or entity keys on the \
             procedure) before partitioning",
            factory_id, command.partition_count
        ))
    }
}

/// Encode the class's SDK-side declarations as `AddOperativeResponse`
/// metadata for the controller's capability resolution. Returns empty
/// bytes when nothing is declared (the default), keeping the response
/// byte-identical to pre-tier builds.
fn metadata_response(kind: &str, factory_id: &str) -> Vec<u8> {
    use crate::operative::topology::BeyondWindowPolicy;

    let mut metadata = std::collections::HashMap::new();

    // Every stream-only declaration reads the same topology; resolve it once.
    // `None` for a workflow class, or one that never registered.
    let stream = match kind {
        "workflow" => None,
        _ => crate::operative::registry::get_class(factory_id),
    };

    let effectively_once = match kind {
        "workflow" => crate::workflow::registry::get_class(factory_id)
            .and_then(|spec| spec.topology.as_ref().map(|t| t.effectively_once))
            .unwrap_or(false),
        _ => stream.as_ref().is_some_and(|t| t.effectively_once),
    };
    if effectively_once {
        metadata.insert("effectively_once".to_string(), "true".to_string());
    }

    // Synchronous remote durability (`remote-durability-tier`): the controller
    // withholds the commit-barrier ack until the barrier is on the durability
    // backend, so the class must say so before the first barrier arrives.
    // Workflow-only — `register_host_class` refuses it on a stream class.
    if kind == "workflow"
        && crate::workflow::registry::get_class(factory_id).is_some_and(|spec| spec.sync_durability)
    {
        metadata.insert("durability".to_string(), "sync_remote".to_string());
    }

    if let Some(topology) = &stream {
        // Partitionability. Declared only when the answer is `false` — that
        // is the refusable fact, and saying nothing otherwise keeps a keyed
        // class's response byte-identical. With it recorded, a later
        // `--partitions N` start of this class is refused at the controller
        // before a runner is touched.
        if !has_keyed_entry_procedure(topology) {
            metadata.insert("keyed".to_string(), "false".to_string());
        }

        // Late-input correction. The controller owns the two prerequisites the
        // SDK cannot see — the input-log hold window and the snapshot age-floor
        // must both cover this window, or the past it promises to repair is
        // already gone — so it needs the window before it binds the operative.
        if let Some(policy) = topology.correction {
            metadata.insert(
                "correction_window_ms".to_string(),
                policy.window_ms.to_string(),
            );
            metadata.insert(
                "beyond_window_policy".to_string(),
                match policy.beyond_window {
                    BeyondWindowPolicy::ProcessAsArrival => "process_as_arrival",
                    BeyondWindowPolicy::Drop => "drop",
                }
                .to_string(),
            );
        }

        // Checkpoint ack-granularity. An Entity that only acknowledges at a
        // checkpoint holds its input-log watermark still for the whole interval
        // — the controller's silence-based liveness detectors have to read that
        // as the steady state it is, not as a stall.
        if topology.ack_granularity.is_checkpoint() {
            metadata.insert("ack_granularity".to_string(), "checkpoint".to_string());
        }

        // The per-Entity WAL size compaction trigger. The controller resolves
        // it per operative_class, so ship the most conservative (smallest)
        // declared threshold across this operative's procedures — only when it
        // differs from the shared default, keeping an unchanged class's reply
        // byte-identical.
        if let Some(min_bytes) = topology
            .procedures
            .iter()
            .map(|proc| proc.compact_after_bytes)
            .min()
        {
            if min_bytes != crate::persistence::DEFAULT_COMPACT_AFTER_BYTES {
                metadata.insert("compact_after_bytes".to_string(), min_bytes.to_string());
            }
        }
    }

    // The tree positions whose procedure declares `compensate`. A rewind can
    // only unwind a procedure whose plugin output is semantically undoable, and
    // the controller refuses one that is not — so it needs this before it wakes
    // the instance (workflow-loop-rewind D5).
    if let Some(procs) = declared_procs(kind, factory_id, |spec, node| {
        spec.topology
            .as_ref()
            .and_then(|t| t.procedure(spec.dag.order[node]))
            .is_some_and(|proc| proc.compensate_overridden)
    }) {
        metadata.insert("compensable_procs".to_string(), procs);
    }

    // The tree's leaf positions. The operative is DONE when all of them have
    // committed, and the controller derives that from the journal alone —
    // which needs the leaf set (workflow-branching D4).
    if let Some(procs) = declared_procs(kind, factory_id, |spec, node| {
        spec.dag.children[node].is_empty()
    }) {
        metadata.insert("leaf_procs".to_string(), procs);
    }

    if metadata.is_empty() {
        return Vec::new();
    }
    AddOperativeResponse { metadata }.encode_to_vec()
}

/// Comma-separated tree positions of the procedures `select` picks out, or
/// `None` for anything without a structured tree (run-sugar, stream) — the
/// one seam every workflow procedure-set declaration rides. An empty string is
/// meaningful — "a tree, none of it selected" — and is not the same as absent.
fn declared_procs(
    kind: &str,
    factory_id: &str,
    select: impl Fn(&WorkflowSpec, usize) -> bool,
) -> Option<String> {
    if kind != "workflow" {
        return None;
    }
    let spec = crate::workflow::registry::get_class(factory_id)?;
    spec.topology.as_ref()?;
    Some(
        (0..spec.dag.len())
            .filter(|&node| select(&spec, node))
            .map(|node| node.to_string())
            .collect::<Vec<_>>()
            .join(","),
    )
}

#[cfg(test)]
mod metadata_tests {
    //! The `AddOperativeResponse` metadata is the controller's only view of
    //! what a class declared. A capability the controller must gate on — here
    //! synchronous remote durability — is worthless if it never reaches this
    //! map, and a class that declares nothing must still encode to zero bytes.

    use super::{metadata_response, AddOperativeResponse, Message};

    fn register_workflow(id: &str, sync: bool) {
        let json = format!(
            r#"{{"factory_id":"{id}","persistent":true,"sync_durability":{sync},
                 "channels":[],"procedures":[],"workflow":{{}}}}"#
        );
        crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    }

    #[test]
    fn a_sync_declaring_workflow_class_ships_the_durability_key() {
        let id = format!("meta_sync_{}", std::process::id());
        register_workflow(&id, true);

        let encoded = metadata_response("workflow", &id);
        assert!(!encoded.is_empty());

        let decoded = AddOperativeResponse::decode(encoded.as_slice()).expect("response decodes");

        assert_eq!(
            decoded.metadata.get("durability").map(String::as_str),
            Some("sync_remote")
        );
    }

    #[test]
    fn a_class_declaring_nothing_still_encodes_to_zero_bytes() {
        let id = format!("meta_plain_{}", std::process::id());
        register_workflow(&id, false);

        assert!(metadata_response("workflow", &id).is_empty());
    }
}

#[cfg(test)]
mod partition_tests {
    //! The bind is where partitioning is refused or armed. Only the SDK can
    //! tell whether a class is keyed — key closures are compiled code, never
    //! on the wire — so a keyless class must be turned away here, and a keyed
    //! one must come back with its range on the instance.

    use super::{partition_refusal, AddOperative, AddOperativeResponse, KeyGroupRange, Message};

    fn register(id: &str, entity_mode: &str, entity_keys: &str) {
        let json = format!(
            r#"{{"factory_id":"{id}","persistent":true,
                 "channels":[{{"name":"in","kind":"entry","consumers":[0]}}],
                 "procedures":[{{"name":"p","entity_mode":"{entity_mode}",
                                 "entity_keys":[{entity_keys}]}}]}}"#
        );
        crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    }

    fn partitioned(factory_id: &str, count: u32) -> AddOperative {
        AddOperative {
            operative_id: "inst".into(),
            factory_id: factory_id.into(),
            partition_index: 1,
            partition_count: count,
            key_group_from: 85,
            key_group_to: 169,
            ..Default::default()
        }
    }

    #[test]
    fn a_keyless_class_is_refused_with_the_contract_named() {
        let id = format!("part_keyless_{}", std::process::id());
        register(&id, "single", "");

        let refusal = partition_refusal(&partitioned(&id, 3), &id).expect("refused");
        assert!(
            refusal.contains("PartitionsWithoutKeyedProcedure"),
            "{refusal}"
        );
        assert!(refusal.contains("key_by"), "{refusal}");
    }

    #[test]
    fn a_keyless_class_declares_itself_so_the_controller_can_refuse_first() {
        let id = format!("part_declare_{}", std::process::id());
        register(&id, "single", "");

        let encoded = super::metadata_response("stream", &id);
        let decoded = AddOperativeResponse::decode(encoded.as_slice()).expect("decodes");
        assert_eq!(
            decoded.metadata.get("keyed").map(String::as_str),
            Some("false")
        );
    }

    #[test]
    fn a_keyed_class_is_accepted_and_says_nothing_extra() {
        let id = format!("part_dynamic_{}", std::process::id());
        register(&id, "dynamic", "");

        assert!(partition_refusal(&partitioned(&id, 3), &id).is_none());
        // Declaring nothing keeps a keyed class's response byte-identical.
        assert!(super::metadata_response("stream", &id).is_empty());
    }

    #[test]
    fn a_statically_keyed_class_partitions_normally() {
        let id = format!("part_static_{}", std::process::id());
        register(&id, "static", r#""BTC-USD","ETH-USD""#);

        assert!(partition_refusal(&partitioned(&id, 3), &id).is_none());
    }

    #[test]
    fn an_unpartitioned_bind_is_never_refused_and_owns_the_whole_space() {
        let id = format!("part_none_{}", std::process::id());
        register(&id, "single", "");

        let mut command = partitioned(&id, 3);
        command.partition_count = 0;

        assert!(partition_refusal(&command, &id).is_none());
        assert_eq!(command.key_group_range(), KeyGroupRange::UNPARTITIONED);
        assert!(!command.key_group_range().partitioned());
    }

    #[test]
    fn a_one_member_group_is_unpartitioned_too() {
        let id = format!("part_one_{}", std::process::id());
        register(&id, "single", "");

        let mut command = partitioned(&id, 1);
        command.partition_count = 1;

        assert!(partition_refusal(&command, &id).is_none());
        assert_eq!(command.key_group_range(), KeyGroupRange::UNPARTITIONED);
    }

    #[test]
    fn a_partitioned_bind_carries_its_range_verbatim() {
        let id = format!("part_range_{}", std::process::id());
        register(&id, "dynamic", "");

        let range = partitioned(&id, 3).key_group_range();
        assert_eq!(range.index, 1);
        assert_eq!(range.count, 3);
        assert_eq!((range.from, range.to), (85, 169));
        assert!(range.partitioned());
    }
}
