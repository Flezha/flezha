use std::collections::HashMap;

use prost::Message;

use super::{CommandAction, Reply};
use crate::bounded::spec::BoundedSpec;
use crate::bounded::LoopMode;
use crate::error::Code;
use crate::operator::instance::KeyGroupRange;
use crate::operator::topology::OperatorTopology;
use crate::pb::{AddOperator, AddOperatorResponse};
use crate::runner::RunnerNode;

impl CommandAction for AddOperator {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        errors: &mut Vec<crate::Error>,
    ) -> Reply {
        let instance_id = self.operator_id.clone();
        let factory_id = self.factory_id.clone();
        // The class says what kind it is; the wire's kind string is not asked.
        let class = crate::operator::registry::get_class(&factory_id);

        // Idempotent on reconnect: the controller re-issues AddOperator for
        // every instance ProcessHub still has tagged to this runner's profile
        // after a restart. An instance already in the local registry keeps
        // its per-Cell state, and a duplicate never initializes twice.
        if !runner.has_operator(&instance_id) {
            match &class {
                Some(topology) => self.start(runner, topology, &factory_id, &instance_id, errors),
                None => errors.push(class_not_registered(&factory_id)),
            }
        }

        Reply {
            payload_id: "start_operator",
            bytes: metadata_response(class.as_deref()),
        }
    }
}

impl AddOperator {
    /// Bind an instance of either kind and seed it from `init_values`. A
    /// keyless class, or a bounded operator that reads one message, cannot be
    /// partitioned, and only the SDK can tell — key closures are compiled
    /// code — so the refusal is here. A bounded operator takes the bind's resume facts
    /// and opens once its state is in place.
    fn start(
        &self,
        runner: &mut RunnerNode,
        topology: &OperatorTopology,
        factory_id: &str,
        instance_id: &str,
        errors: &mut Vec<crate::Error>,
    ) {
        if let Some(refusal) = partition_refusal(self, topology, factory_id) {
            errors.push(refusal);
            return;
        }
        let bounded = topology.bounded.is_some();
        let resume = crate::bounded::Resume {
            expected: self.resume_cell_count as usize,
            owed_inbox: self.inbox_tail_count,
            journal: self.task_journal.as_ref().map(Into::into),
        };
        let Some(instance) = runner.start_operator(
            factory_id.to_string(),
            instance_id.to_string(),
            self.key_group_range(),
            resume,
        ) else {
            return;
        };
        if let Err(e) = crate::command::cell_state::apply_init_values(&instance, &self.init_values)
        {
            errors.push(e);
            // A bounded operator refused before it ran leaves nothing behind.
            if bounded {
                runner.stop_operator(instance_id);
            }
            return;
        }
        crate::bounded::open(&instance);
        crate::operator::sleep::bound(&instance);
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
fn has_keyed_entry_tasklet(topology: &crate::operator::topology::OperatorTopology) -> bool {
    use crate::channel::ChannelKind;
    use crate::operator::topology::CellMode;

    topology
        .channels
        .iter()
        .filter(|channel| channel.kind == ChannelKind::Entry)
        .flat_map(|channel| channel.consumers.iter())
        .filter_map(|edge| topology.tasklet(edge.tasklet))
        .any(|tasklet| matches!(tasklet.mode, CellMode::Dynamic | CellMode::Static))
}

/// A bind or migration naming a class this runner never registered.
pub(super) fn class_not_registered(factory_id: &str) -> crate::Error {
    crate::Error::Declaration(Code::ClassNotRegistered.with([("factory", factory_id.into())]))
}

fn partition_refusal(
    command: &AddOperator,
    topology: &OperatorTopology,
    factory_id: &str,
) -> Option<crate::Error> {
    if command.partition_count < 2 {
        return None;
    }
    // Ahead of the keyless arm: a one-message loop is unpartitionable however
    // it is keyed, and its loop mode is what the author can change.
    if let Some(spec) = topology
        .bounded
        .as_ref()
        .filter(|spec| spec.loop_mode != LoopMode::PerInput)
    {
        return Some(crate::Error::Declaration(
            Code::PartitionsWithoutPerInputLoop.with([
                ("factory", factory_id.into()),
                ("loop_mode", spec.loop_mode.as_str().into()),
                ("partitions", command.partition_count.into()),
            ]),
        ));
    }
    if has_keyed_entry_tasklet(topology) {
        None
    } else {
        Some(crate::Error::Declaration(
            Code::PartitionsWithoutKeyedTasklet.with([("factory", factory_id.into())]),
        ))
    }
}

/// The class's declarations as `AddOperatorResponse` metadata; empty bytes
/// when nothing is declared, and for a class that never registered. The
/// persistence switch rides only when off and the sleep setting only when on,
/// for either kind.
fn metadata_response(class: Option<&OperatorTopology>) -> Vec<u8> {
    let mut metadata = std::collections::HashMap::new();
    if let Some(topology) = class {
        if !topology.persistence_enabled_const {
            metadata.insert("persistent".to_string(), "false".to_string());
        }
        if topology.sleep_when_idle {
            metadata.insert("sleep_when_idle".to_string(), "true".to_string());
        }
        match &topology.bounded {
            Some(spec) => bounded_metadata(topology, spec, &mut metadata),
            None => unbounded_metadata(topology, &mut metadata),
        }
    }
    if metadata.is_empty() {
        return Vec::new();
    }
    AddOperatorResponse { metadata }.encode_to_vec()
}

/// What a bounded class declares: synchronous durability, its loop mode
/// (only a `per_input` loop partitions), the compensable positions (a rewind
/// may only unwind those) and the leaves (the operator is done when all
/// committed).
fn bounded_metadata(
    topology: &OperatorTopology,
    spec: &BoundedSpec,
    metadata: &mut HashMap<String, String>,
) {
    metadata.insert("loop_mode".to_string(), spec.loop_mode.as_str().to_string());
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

/// What an unbounded class declares: the tier; partitionability only when
/// `false` (the refusable fact); the correction window the controller's
/// retention must cover; checkpoint ack-granularity, which the liveness
/// detectors read as a still watermark; the compaction threshold when it
/// differs from the default.
fn unbounded_metadata(topology: &OperatorTopology, metadata: &mut HashMap<String, String>) {
    use crate::operator::topology::BeyondWindowPolicy;

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
        if min_bytes != crate::operator::topology::DEFAULT_COMPACT_AFTER_BYTES {
            metadata.insert("compact_after_bytes".to_string(), min_bytes.to_string());
        }
    }
}

/// The positions `select` picks out, comma-separated; an empty string is "a
/// tree, none selected".
fn declared_tasks(spec: &BoundedSpec, select: impl Fn(usize) -> bool) -> String {
    (0..spec.dag.len())
        .filter(|&node| select(node))
        .map(|node| node.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod metadata_tests {
    //! The `AddOperatorResponse` metadata is the controller's only view of
    //! what a class declared; a class nothing is known of encodes to zero
    //! bytes.

    use super::{metadata_response, AddOperatorResponse, Message};

    fn register_bounded(id: &str, sync: bool) {
        let json = format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","persistent":true,"sync_durability":{sync},
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
                 "tasklets":[{{"name":"p0"}}],"bounded":{{}}}}"#
        );
        crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    }

    #[test]
    fn a_sync_declaring_bounded_class_ships_the_durability_key() {
        let id = format!("meta_sync_{}", std::process::id());
        register_bounded(&id, true);

        let encoded = metadata_response(crate::operator::registry::get_class(&id).as_deref());
        assert!(!encoded.is_empty());

        let decoded = AddOperatorResponse::decode(encoded.as_slice()).expect("response decodes");

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
            let encoded = metadata_response(crate::operator::registry::get_class(id).as_deref());
            AddOperatorResponse::decode(encoded.as_slice())
                .expect("response decodes")
                .metadata
        };
        for (suffix, kind) in [("unbounded", ""), ("bounded", r#","bounded":{}"#)] {
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
    fn a_bounded_declaring_nothing_extra_ships_only_its_graph() {
        let id = format!("meta_plain_{}", std::process::id());
        register_bounded(&id, false);

        let encoded = metadata_response(crate::operator::registry::get_class(&id).as_deref());
        let decoded = AddOperatorResponse::decode(encoded.as_slice()).expect("response decodes");
        let keys: std::collections::BTreeMap<_, _> = decoded.metadata.into_iter().collect();
        assert_eq!(
            keys.into_iter().collect::<Vec<_>>(),
            vec![
                ("compensable_tasks".to_string(), String::new()),
                ("leaf_tasks".to_string(), "0".to_string()),
                ("loop_mode".to_string(), "once".to_string())
            ]
        );
        assert!(metadata_response(
            crate::operator::registry::get_class("meta_never_registered").as_deref()
        )
        .is_empty());
    }
}

#[cfg(test)]
mod partition_tests {
    //! The bind refuses partitioning of a keyless class and arms it with the
    //! range on a keyed one.

    use super::{partition_refusal, AddOperator, AddOperatorResponse, KeyGroupRange, Message};
    use crate::ErrorModel;

    fn register(id: &str, cell_mode: &str, cell_keys: &str) {
        let json = format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","persistent":true,
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
                 "tasklets":[{{"name":"p","cell_mode":"{cell_mode}","cells_for_overridden":true,
                                 "cell_keys":[{cell_keys}]}}]}}"#
        );
        crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    }

    fn class_of(factory_id: &str) -> std::sync::Arc<crate::operator::topology::OperatorTopology> {
        crate::operator::registry::get_class(factory_id).expect("class registered")
    }

    fn partitioned(factory_id: &str, count: u32) -> AddOperator {
        AddOperator {
            operator_id: "inst".into(),
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
        assert_eq!(refusal.code(), "topology.partitions_without_keyed_tasklet");
        assert_eq!(refusal.to_error().details["factory"], id.as_str());
    }

    #[test]
    fn a_keyless_class_declares_itself_so_the_controller_can_refuse_first() {
        let id = format!("part_declare_{}", std::process::id());
        register(&id, "single", "");

        let encoded =
            super::metadata_response(crate::operator::registry::get_class(&id).as_deref());
        let decoded = AddOperatorResponse::decode(encoded.as_slice()).expect("decodes");
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
            super::metadata_response(crate::operator::registry::get_class(&id).as_deref())
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

    /// A chained loop registers non-durable: a durable one needs a terminal
    /// handoff this one-tasklet class does not have.
    fn register_bounded(id: &str, cell_mode: &str, loop_mode: &str) {
        let persistent = loop_mode != "chained";
        let json = format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","persistent":{persistent},
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
                 "tasklets":[{{"name":"p","cell_mode":"{cell_mode}"}}],
                 "bounded":{{"loop_mode":"{loop_mode}"}}}}"#
        );
        crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    }

    fn declared_loop_mode(id: &str) -> Option<String> {
        let encoded = super::metadata_response(crate::operator::registry::get_class(id).as_deref());
        AddOperatorResponse::decode(encoded.as_slice())
            .expect("decodes")
            .metadata
            .get("loop_mode")
            .cloned()
    }

    #[test]
    fn a_bounded_that_reads_one_message_is_refused_before_the_keyless_arm() {
        for loop_mode in ["once", "chained"] {
            let id = format!("part_bounded_{loop_mode}_{}", std::process::id());
            register_bounded(&id, "dynamic", loop_mode);

            let refusal =
                partition_refusal(&partitioned(&id, 2), &class_of(&id), &id).expect("refused");
            assert_eq!(refusal.code(), "topology.partitions_without_per_input_loop");
            let details = refusal.to_error().details;
            assert_eq!(details["factory"], id.as_str());
            assert_eq!(details["loop_mode"], loop_mode);
            assert_eq!(details["partitions"], 2);
            assert_eq!(declared_loop_mode(&id).as_deref(), Some(loop_mode));

            let mut single = partitioned(&id, 2);
            single.partition_count = 0;
            assert!(partition_refusal(&single, &class_of(&id), &id).is_none());
        }
    }

    #[test]
    fn a_per_input_bounded_partitions_when_keyed_and_is_refused_as_keyless_otherwise() {
        let keyed = format!("part_bounded_keyed_{}", std::process::id());
        register_bounded(&keyed, "dynamic", "per_input");
        assert!(partition_refusal(&partitioned(&keyed, 2), &class_of(&keyed), &keyed).is_none());
        assert_eq!(declared_loop_mode(&keyed).as_deref(), Some("per_input"));

        let keyless = format!("part_bounded_keyless_{}", std::process::id());
        register_bounded(&keyless, "single", "per_input");
        let refusal = partition_refusal(&partitioned(&keyless, 2), &class_of(&keyless), &keyless)
            .expect("refused");
        assert_eq!(refusal.code(), "topology.partitions_without_keyed_tasklet");
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
