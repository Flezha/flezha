use std::collections::HashMap;

use prost::Message;

use super::{CommandAction, Reply};
use crate::operative::instance::KeyGroupRange;
use crate::operative::topology::OperativeTopology;
use crate::pb::{AddOperative, AddOperativeResponse};
use crate::runner::RunnerNode;
use crate::workflow::spec::WorkflowSpec;

impl CommandAction for AddOperative {
    fn handle(&self, runner: &mut RunnerNode, _message: &str, errors: &mut Vec<String>) -> Reply {
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
            return Reply {
                payload_id: "start_operative",
                bytes: metadata_response(kind, &factory_id),
            };
        }

        match kind {
            "workflow" => self.start_workflow(runner, &factory_id, &instance_id, errors),
            "stream" => self.start_stream(runner, &factory_id, &instance_id, errors),
            other => {
                errors.push(format!(
                    "unknown compute-unit kind '{}' for unit {}",
                    other, instance_id
                ));
            }
        }

        Reply {
            payload_id: "start_operative",
            bytes: metadata_response(kind, &factory_id),
        }
    }
}

impl AddOperative {
    fn start_workflow(
        &self,
        runner: &mut RunnerNode,
        factory_id: &str,
        instance_id: &str,
        errors: &mut Vec<String>,
    ) {
        if !runner.start_workflow_resumable(
            factory_id.to_string(),
            instance_id.to_string(),
            self.init_values.clone(),
            self.resume_entity_count,
            self.inbox_tail_count,
            self.proc_journal.as_ref().map(Into::into),
        ) {
            errors.push(format!(
                "WorkflowOperative class with factory_id {} is not registered",
                factory_id
            ));
        }
    }

    /// Bind a stream operative and seed it from `init_values`. A keyless
    /// class cannot be partitioned, and only the SDK can tell — key closures
    /// are compiled code — so the refusal is here.
    fn start_stream(
        &self,
        runner: &mut RunnerNode,
        factory_id: &str,
        instance_id: &str,
        errors: &mut Vec<String>,
    ) {
        if let Some(refusal) = partition_refusal(self, factory_id) {
            errors.push(refusal);
            return;
        }
        match runner.start_operative(
            factory_id.to_string(),
            instance_id.to_string(),
            self.key_group_range(),
        ) {
            Some(instance) => {
                if !self.init_values.is_empty() {
                    if let Err(e) = crate::command::entity_state::apply_init_values(
                        &instance,
                        &self.init_values,
                    ) {
                        errors.push(e.to_string());
                    }
                }
            }
            None => {
                errors.push(format!(
                    "StreamOperative class with factory_id {} is not registered",
                    factory_id
                ));
            }
        }
    }

    /// The member's range; `partition_count < 2` is the whole space, guard
    /// inert.
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

/// Whether an entry-fed procedure is keyed — the one definition of
/// "partitionable", read by the bind refusal and the declaration alike.
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

/// The class's declarations as `AddOperativeResponse` metadata; empty bytes
/// when nothing is declared.
fn metadata_response(kind: &str, factory_id: &str) -> Vec<u8> {
    let mut metadata = std::collections::HashMap::new();
    match kind {
        "workflow" => workflow_metadata(factory_id, &mut metadata),
        // A stream class that never registered declares nothing.
        _ => {
            if let Some(topology) = crate::operative::registry::get_class(factory_id) {
                stream_metadata(&topology, &mut metadata);
            }
        }
    }
    if metadata.is_empty() {
        return Vec::new();
    }
    AddOperativeResponse { metadata }.encode_to_vec()
}

/// What a workflow class declares: the tier, synchronous durability, the
/// compensable positions (a rewind may only unwind those) and the leaves
/// (the operative is done when all committed).
fn workflow_metadata(factory_id: &str, metadata: &mut HashMap<String, String>) {
    let spec = crate::workflow::registry::get_class(factory_id);
    if spec
        .as_ref()
        .and_then(|spec| spec.topology.as_ref().map(|t| t.effectively_once))
        .unwrap_or(false)
    {
        metadata.insert("effectively_once".to_string(), "true".to_string());
    }
    if spec.as_ref().is_some_and(|spec| spec.sync_durability) {
        metadata.insert("durability".to_string(), "sync_remote".to_string());
    }
    if let Some(procs) = declared_procs("workflow", factory_id, |spec, node| {
        spec.topology
            .as_ref()
            .and_then(|t| t.procedure(spec.dag.order[node]))
            .is_some_and(|proc| proc.compensate_overridden)
    }) {
        metadata.insert("compensable_procs".to_string(), procs);
    }
    if let Some(procs) = declared_procs("workflow", factory_id, |spec, node| {
        spec.dag.children[node].is_empty()
    }) {
        metadata.insert("leaf_procs".to_string(), procs);
    }
}

/// What a stream class declares: the tier; partitionability only when
/// `false` (the refusable fact); the correction window the controller's
/// retention must cover; checkpoint ack-granularity, which the liveness
/// detectors read as a still watermark; the compaction threshold when it
/// differs from the default.
fn stream_metadata(topology: &OperativeTopology, metadata: &mut HashMap<String, String>) {
    use crate::operative::topology::BeyondWindowPolicy;

    if topology.effectively_once {
        metadata.insert("effectively_once".to_string(), "true".to_string());
    }
    if !has_keyed_entry_procedure(topology) {
        metadata.insert("keyed".to_string(), "false".to_string());
    }
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
    if topology.ack_granularity.is_checkpoint() {
        metadata.insert("ack_granularity".to_string(), "checkpoint".to_string());
    }
    if let Some(min_bytes) = topology
        .procedures
        .iter()
        .map(|proc| proc.compact_after_bytes)
        .min()
    {
        if min_bytes != crate::operative::topology::DEFAULT_COMPACT_AFTER_BYTES {
            metadata.insert("compact_after_bytes".to_string(), min_bytes.to_string());
        }
    }
}

/// The positions `select` picks out, comma-separated; `None` without a
/// tree, and an empty string is "a tree, none selected".
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
    //! what a class declared; a class declaring nothing encodes to zero bytes.

    use super::{metadata_response, AddOperativeResponse, Message};

    fn register_workflow(id: &str, sync: bool) {
        let json = format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","persistent":true,"sync_durability":{sync},
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
    //! The bind refuses partitioning of a keyless class and arms it with the
    //! range on a keyed one.

    use super::{partition_refusal, AddOperative, AddOperativeResponse, KeyGroupRange, Message};

    fn register(id: &str, entity_mode: &str, entity_keys: &str) {
        let json = format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","persistent":true,
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","procedure":0}}]]}}],
                 "procedures":[{{"name":"p","entity_mode":"{entity_mode}","entities_for_overridden":true,
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
