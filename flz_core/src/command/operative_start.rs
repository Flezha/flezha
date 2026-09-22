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
        // The class says what kind it is; the wire's kind string is not asked.
        let class = crate::operative::registry::get_class(&factory_id);

        // Idempotent on reconnect: the controller re-issues AddOperative for
        // every instance ProcessHub still has tagged to this runner's profile
        // after a restart. An instance already in the local registry keeps
        // its per-Cell state, and a duplicate never initializes twice.
        if !runner.has_operative(&instance_id) {
            match &class {
                Some(topology) => self.start(runner, topology, &factory_id, &instance_id, errors),
                None => errors.push(format!(
                    "Operative class with factory_id {factory_id} is not registered"
                )),
            }
        }

        Reply {
            payload_id: "start_operative",
            bytes: metadata_response(class.as_deref()),
        }
    }
}

impl AddOperative {
    /// Bind an instance of either kind and seed it from `init_values`. A
    /// keyless stream class cannot be partitioned, and only the SDK can tell
    /// — key closures are compiled code — so the refusal is here. A workflow
    /// takes the bind's resume facts and opens once its state is in place.
    fn start(
        &self,
        runner: &mut RunnerNode,
        topology: &OperativeTopology,
        factory_id: &str,
        instance_id: &str,
        errors: &mut Vec<String>,
    ) {
        let workflow = topology.workflow.is_some();
        if !workflow {
            if let Some(refusal) = partition_refusal(self, topology, factory_id) {
                errors.push(refusal);
                return;
            }
        }
        let resume = crate::workflow::Resume {
            expected: self.resume_cell_count as usize,
            owed_inbox: self.inbox_tail_count,
            journal: self.task_journal.as_ref().map(Into::into),
        };
        let Some(instance) = runner.start_operative(
            factory_id.to_string(),
            instance_id.to_string(),
            self.key_group_range(),
            resume,
        ) else {
            return;
        };
        if let Err(e) = crate::command::cell_state::apply_init_values(&instance, &self.init_values)
        {
            errors.push(e.to_string());
            // A workflow refused before it ran leaves nothing behind.
            if workflow {
                runner.stop_operative(instance_id);
            }
            return;
        }
        crate::workflow::open(&instance);
        crate::operative::sleep::bound(&instance);
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

/// Whether an entry-fed tasklet is keyed — the one definition of
/// "partitionable", read by the bind refusal and the declaration alike.
fn has_keyed_entry_tasklet(topology: &crate::operative::topology::OperativeTopology) -> bool {
    use crate::channel::ChannelKind;
    use crate::operative::topology::CellMode;

    topology
        .channels
        .iter()
        .filter(|channel| channel.kind == ChannelKind::Entry)
        .flat_map(|channel| channel.consumers.iter())
        .filter_map(|edge| topology.tasklet(edge.tasklet))
        .any(|tasklet| matches!(tasklet.mode, CellMode::Dynamic | CellMode::Static))
}

fn partition_refusal(
    command: &AddOperative,
    topology: &OperativeTopology,
    factory_id: &str,
) -> Option<String> {
    if command.partition_count < 2 {
        return None;
    }
    if has_keyed_entry_tasklet(topology) {
        None
    } else {
        Some(format!(
            "PartitionsWithoutKeyedTasklet: class {} has no keyed entry-fed tasklet, \
             so its {} partitions would each fold a slice of the input into a private copy \
             of the same state — declare `key_by` on the entry edge (or cell keys on the \
             tasklet) before partitioning",
            factory_id, command.partition_count
        ))
    }
}

/// The class's declarations as `AddOperativeResponse` metadata; empty bytes
/// when nothing is declared, and for a class that never registered. The
/// persistence switch rides only when off and the sleep setting only when on,
/// for either kind.
fn metadata_response(class: Option<&OperativeTopology>) -> Vec<u8> {
    let mut metadata = std::collections::HashMap::new();
    if let Some(topology) = class {
        if !topology.persistence_enabled_const {
            metadata.insert("persistent".to_string(), "false".to_string());
        }
        if topology.sleep_when_idle {
            metadata.insert("sleep_when_idle".to_string(), "true".to_string());
        }
        match &topology.workflow {
            Some(spec) => workflow_metadata(topology, spec, &mut metadata),
            None => stream_metadata(topology, &mut metadata),
        }
    }
    if metadata.is_empty() {
        return Vec::new();
    }
    AddOperativeResponse { metadata }.encode_to_vec()
}

/// What a workflow class declares: synchronous durability, the compensable
/// positions (a rewind may only unwind those) and the leaves (the operative
/// is done when all committed).
fn workflow_metadata(
    topology: &OperativeTopology,
    spec: &WorkflowSpec,
    metadata: &mut HashMap<String, String>,
) {
    if spec.sync_durability {
        metadata.insert("durability".to_string(), "sync_remote".to_string());
    }
    metadata.insert(
        "compensable_tasks".to_string(),
        declared_tasks(spec, |node| {
            topology
                .tasklet(spec.dag.order[node])
                .is_some_and(|task| task.compensate_overridden)
        }),
    );
    metadata.insert(
        "leaf_tasks".to_string(),
        declared_tasks(spec, |node| spec.dag.children[node].is_empty()),
    );
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
    if !has_keyed_entry_tasklet(topology) {
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
        .tasklets
        .iter()
        .map(|task| task.compact_after_bytes)
        .min()
    {
        if min_bytes != crate::operative::topology::DEFAULT_COMPACT_AFTER_BYTES {
            metadata.insert("compact_after_bytes".to_string(), min_bytes.to_string());
        }
    }
}

/// The positions `select` picks out, comma-separated; an empty string is "a
/// tree, none selected".
fn declared_tasks(spec: &WorkflowSpec, select: impl Fn(usize) -> bool) -> String {
    (0..spec.dag.len())
        .filter(|&node| select(node))
        .map(|node| node.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod metadata_tests {
    //! The `AddOperativeResponse` metadata is the controller's only view of
    //! what a class declared; a class nothing is known of encodes to zero
    //! bytes.

    use super::{metadata_response, AddOperativeResponse, Message};

    fn register_workflow(id: &str, sync: bool) {
        let json = format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","persistent":true,"sync_durability":{sync},
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
                 "tasklets":[{{"name":"p0"}}],"workflow":{{}}}}"#
        );
        crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    }

    #[test]
    fn a_sync_declaring_workflow_class_ships_the_durability_key() {
        let id = format!("meta_sync_{}", std::process::id());
        register_workflow(&id, true);

        let encoded = metadata_response(crate::operative::registry::get_class(&id).as_deref());
        assert!(!encoded.is_empty());

        let decoded = AddOperativeResponse::decode(encoded.as_slice()).expect("response decodes");

        assert_eq!(
            decoded.metadata.get("durability").map(String::as_str),
            Some("sync_remote")
        );
    }

    #[test]
    fn the_persistence_switch_and_the_sleep_setting_ride_for_either_kind() {
        let keys = |id: &str, facts: &str, kind: &str| {
            let json = format!(
                r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0",{facts}
                     "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
                     "tasklets":[{{"name":"p0"}}]{kind}}}"#
            );
            crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
            let encoded = metadata_response(crate::operative::registry::get_class(id).as_deref());
            AddOperativeResponse::decode(encoded.as_slice())
                .expect("response decodes")
                .metadata
        };
        for (suffix, kind) in [("stream", ""), ("workflow", r#","workflow":{}"#)] {
            let off = keys(
                &format!("meta_off_{suffix}_{}", std::process::id()),
                r#""persistent":false,"#,
                kind,
            );
            assert_eq!(off.get("persistent").map(String::as_str), Some("false"));
            assert!(!off.contains_key("sleep_when_idle"));

            let sleeps = keys(
                &format!("meta_sleeps_{suffix}_{}", std::process::id()),
                r#""persistent":true,"sleep_when_idle":true,"#,
                kind,
            );
            assert_eq!(
                sleeps.get("sleep_when_idle").map(String::as_str),
                Some("true")
            );
            assert!(!sleeps.contains_key("persistent"));
        }
    }

    #[test]
    fn a_workflow_declaring_nothing_extra_ships_only_its_graph() {
        let id = format!("meta_plain_{}", std::process::id());
        register_workflow(&id, false);

        let encoded = metadata_response(crate::operative::registry::get_class(&id).as_deref());
        let decoded = AddOperativeResponse::decode(encoded.as_slice()).expect("response decodes");
        let keys: std::collections::BTreeMap<_, _> = decoded.metadata.into_iter().collect();
        assert_eq!(
            keys.into_iter().collect::<Vec<_>>(),
            vec![
                ("compensable_tasks".to_string(), String::new()),
                ("leaf_tasks".to_string(), "0".to_string())
            ]
        );
        assert!(metadata_response(
            crate::operative::registry::get_class("meta_never_registered").as_deref()
        )
        .is_empty());
    }
}

#[cfg(test)]
mod partition_tests {
    //! The bind refuses partitioning of a keyless class and arms it with the
    //! range on a keyed one.

    use super::{partition_refusal, AddOperative, AddOperativeResponse, KeyGroupRange, Message};

    fn register(id: &str, cell_mode: &str, cell_keys: &str) {
        let json = format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","persistent":true,
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
                 "tasklets":[{{"name":"p","cell_mode":"{cell_mode}","cells_for_overridden":true,
                                 "cell_keys":[{cell_keys}]}}]}}"#
        );
        crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    }

    fn class_of(factory_id: &str) -> std::sync::Arc<crate::operative::topology::OperativeTopology> {
        crate::operative::registry::get_class(factory_id).expect("class registered")
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

        let refusal =
            partition_refusal(&partitioned(&id, 3), &class_of(&id), &id).expect("refused");
        assert!(
            refusal.contains("PartitionsWithoutKeyedTasklet"),
            "{refusal}"
        );
        assert!(refusal.contains("key_by"), "{refusal}");
    }

    #[test]
    fn a_keyless_class_declares_itself_so_the_controller_can_refuse_first() {
        let id = format!("part_declare_{}", std::process::id());
        register(&id, "single", "");

        let encoded =
            super::metadata_response(crate::operative::registry::get_class(&id).as_deref());
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

        assert!(partition_refusal(&partitioned(&id, 3), &class_of(&id), &id).is_none());
        // Declaring nothing keeps a keyed class's response byte-identical.
        assert!(
            super::metadata_response(crate::operative::registry::get_class(&id).as_deref())
                .is_empty()
        );
    }

    #[test]
    fn a_statically_keyed_class_partitions_normally() {
        let id = format!("part_static_{}", std::process::id());
        register(&id, "static", r#""BTC-USD","ETH-USD""#);

        assert!(partition_refusal(&partitioned(&id, 3), &class_of(&id), &id).is_none());
    }

    #[test]
    fn an_unpartitioned_bind_is_never_refused_and_owns_the_whole_space() {
        let id = format!("part_none_{}", std::process::id());
        register(&id, "single", "");

        let mut command = partitioned(&id, 3);
        command.partition_count = 0;

        assert!(partition_refusal(&command, &class_of(&id), &id).is_none());
        assert_eq!(command.key_group_range(), KeyGroupRange::UNPARTITIONED);
        assert!(!command.key_group_range().partitioned());
    }

    #[test]
    fn a_one_member_group_is_unpartitioned_too() {
        let id = format!("part_one_{}", std::process::id());
        register(&id, "single", "");

        let mut command = partitioned(&id, 1);
        command.partition_count = 1;

        assert!(partition_refusal(&command, &class_of(&id), &id).is_none());
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
