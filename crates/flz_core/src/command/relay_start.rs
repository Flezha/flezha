use std::collections::HashMap;

use prost::Message;

use super::{CommandAction, Reply};
use crate::error::Code;
use crate::pb::{AddRelay, AddRelayResponse};
use crate::relay::instance::KeyGroupRange;
use crate::relay::topology::RelayTopology;
use crate::runner::RunnerNode;
use crate::steps::spec::StepsSpec;

impl CommandAction for AddRelay {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        errors: &mut Vec<crate::Error>,
    ) -> Reply {
        let instance_id = self.relay_id.clone();
        let factory_id = self.factory_id.clone();
        // The class says what kind it is; the wire's kind string is not asked.
        let class = crate::relay::registry::get_class(&factory_id);

        // Idempotent on reconnect: the controller re-issues AddRelay for
        // every instance ProcessHub still has tagged to this runner's profile
        // after a restart. An instance already in the local registry keeps
        // its per-Cell state, and a duplicate never initializes twice.
        if !runner.has_relay(&instance_id) {
            match &class {
                Some(topology) => self.start(runner, topology, &factory_id, &instance_id, errors),
                None => errors.push(class_not_registered(&factory_id)),
            }
        }

        Reply {
            payload_id: "start_relay",
            bytes: metadata_response(class.as_deref()),
        }
    }
}

impl AddRelay {
    /// Bind an instance of either kind and seed it from `init_values`. A
    /// keyless class, or a steps relay that reads one message, cannot be
    /// partitioned, and only the SDK can tell — key closures are compiled
    /// code — so the refusal is here. A steps relay takes the bind's resume facts
    /// and opens once its state is in place.
    fn start(
        &self,
        runner: &mut RunnerNode,
        topology: &RelayTopology,
        factory_id: &str,
        instance_id: &str,
        errors: &mut Vec<crate::Error>,
    ) {
        if let Some(refusal) = partition_refusal(self, topology, factory_id) {
            errors.push(refusal);
            return;
        }
        let steps = topology.steps.is_some();
        let resume = crate::steps::Resume {
            expected: self.resume_cell_count as usize + self.effect_result_count as usize,
            owed_inbox: self.inbox_tail_count,
            journal: self.task_journal.as_ref().map(Into::into),
        };
        let Some(instance) = runner.start_relay(
            factory_id.to_string(),
            instance_id.to_string(),
            self.key_group_range(),
            resume,
        ) else {
            return;
        };
        instance.bind_window(self.max_duration_remaining_ms);
        if let Err(e) = crate::command::cell_state::apply_init_values(&instance, &self.init_values)
        {
            errors.push(e);
            // A steps relay refused before it ran leaves nothing behind.
            if steps {
                runner.stop_relay(instance_id);
            }
            return;
        }
        crate::steps::open(&instance);
        crate::relay::sleep::bound(&instance);
        crate::relay::completion::bound(&instance);
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
fn has_keyed_entry_tasklet(topology: &crate::relay::topology::RelayTopology) -> bool {
    use crate::channel::ChannelKind;
    use crate::relay::topology::CellMode;

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
    command: &AddRelay,
    topology: &RelayTopology,
    factory_id: &str,
) -> Option<crate::Error> {
    if command.partition_count < 2 {
        return None;
    }
    // Ahead of the keyless arm: a one-message loop is unpartitionable however
    // it is keyed, and its loop mode is what the author can change.
    if let Some(spec) = topology
        .steps
        .as_ref()
        .filter(|spec| !spec.each.partitionable())
    {
        return Some(crate::Error::Declaration(
            Code::PartitionsWithoutPerInputLoop.with([
                ("factory", factory_id.into()),
                ("each", spec.each.as_str().into()),
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

/// The class's metadata as `AddRelayResponse` bytes; empty bytes for a class
/// that never registered.
fn metadata_response(class: Option<&RelayTopology>) -> Vec<u8> {
    match class {
        Some(topology) => AddRelayResponse {
            metadata: class_metadata(topology),
        }
        .encode_to_vec(),
        None => Vec::new(),
    }
}

/// The class's settings and declarations, which the `AddRelay` answer and the
/// factory's announcement both carry, so the two cannot differ. The
/// durability always rides; a member rides only when its parent is chosen.
/// The run and a steps run's shape ride the announcement's own fields.
pub(crate) fn class_metadata(topology: &RelayTopology) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    let mut put = |key: &str, value: String| metadata.insert(key.to_string(), value);
    put("durable", topology.durable.as_str().to_string());
    if let Some(after) = topology.sleep_after() {
        put("sleep_after_ms", after.as_millis().to_string());
    }
    if let Some(limit) = topology.max_duration {
        put("max_duration_ms", limit.as_millis().to_string());
    }
    match &topology.steps {
        Some(spec) => steps_metadata(topology, spec, &mut metadata),
        None => flow_metadata(topology, &mut metadata),
    }
    metadata
}

/// What a steps class declares: the compensable positions, comma-separated (a
/// rewind may only unwind those; an empty string is "a tree, none
/// compensable").
fn steps_metadata(
    topology: &RelayTopology,
    spec: &StepsSpec,
    metadata: &mut HashMap<String, String>,
) {
    let compensable = (0..spec.dag.len())
        .filter(|&node| {
            topology
                .tasklet(spec.dag.order[node])
                .is_some_and(|task| task.compensate_overridden)
        })
        .map(|node| node.to_string())
        .collect::<Vec<_>>()
        .join(",");
    metadata.insert("compensable_tasks".to_string(), compensable);
}

/// What a flow class declares: the tier; partitionability only when
/// `false` (the refusable fact); the correction window the controller's
/// retention must cover; checkpoint ack, which the liveness detectors read as
/// a still watermark; the compaction threshold when it differs from the
/// default.
fn flow_metadata(topology: &RelayTopology, metadata: &mut HashMap<String, String>) {
    use crate::relay::topology::BeyondWindowPolicy;

    if topology.effectively_once {
        metadata.insert("exactly".to_string(), "effectively_once".to_string());
    }
    if !has_keyed_entry_tasklet(topology) {
        metadata.insert("keyed".to_string(), "false".to_string());
    }
    if let Some(policy) = topology.correction {
        metadata.insert("correction_ms".to_string(), policy.window_ms.to_string());
        metadata.insert(
            "beyond_window".to_string(),
            match policy.beyond_window {
                BeyondWindowPolicy::ProcessAsArrival => "process_as_arrival",
                BeyondWindowPolicy::Drop => "drop",
            }
            .to_string(),
        );
    }
    if topology.ack_granularity.is_checkpoint() {
        metadata.insert("ack".to_string(), "checkpoint".to_string());
    }
    if let Some(min_bytes) = topology
        .tasklets
        .iter()
        .map(|task| task.compact_after_bytes)
        .min()
    {
        if min_bytes != crate::relay::topology::DEFAULT_COMPACT_AFTER_BYTES {
            metadata.insert("compact_after_bytes".to_string(), min_bytes.to_string());
        }
    }
}

#[cfg(test)]
mod metadata_tests {
    //! The `AddRelayResponse` metadata is the controller's view of what a
    //! class declared beyond its announcement; a class nothing is known of encodes to zero
    //! bytes.

    use super::{metadata_response, AddRelayResponse, Message};

    fn register_steps(id: &str, durable: &str) {
        let json = format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","durable":{durable},
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
                 "tasklets":[{{"name":"p0"}}],"run":"steps"}}"#
        );
        crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    }

    fn metadata_of(id: &str) -> std::collections::BTreeMap<String, String> {
        let encoded = metadata_response(crate::relay::registry::get_class(id).as_deref());
        AddRelayResponse::decode(encoded.as_slice())
            .expect("response decodes")
            .metadata
            .into_iter()
            .collect()
    }

    // The announcement and the `AddRelay` answer read one function, so a
    // class's declarations are the same in both, for either run.
    #[test]
    fn the_announced_declarations_are_the_add_relay_metadata() {
        let steps = format!("meta_announced_steps_{}", std::process::id());
        register_steps(&steps, r#"{"local":{"sleep_after":2000}}"#);
        let flow = format!("meta_announced_flow_{}", std::process::id());
        let json = format!(
            r#"{{"factory_id":"{flow}","sdk_lang":1,"sdk_version":"0.1.0","durable":"remote",
                 "max_duration_ms":120000,"compact_after_bytes":4096,"run":"flow",
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
                 "tasklets":[{{"name":"p0"}}]}}"#
        );
        crate::host::register_host_class_json(json.as_bytes()).expect("class registers");

        for id in [&steps, &flow] {
            let announced = crate::relay::registry::advertised_for_factory(id).declarations;
            assert_eq!(
                announced
                    .into_iter()
                    .collect::<std::collections::BTreeMap<_, _>>(),
                metadata_of(id)
            );
        }
        assert_eq!(
            metadata_of(&flow)
                .get("max_duration_ms")
                .map(String::as_str),
            Some("120000")
        );
        assert_eq!(
            metadata_of(&steps)
                .get("sleep_after_ms")
                .map(String::as_str),
            Some("2000")
        );
    }

    #[test]
    fn a_remote_steps_class_ships_its_durability_level() {
        let id = format!("meta_remote_{}", std::process::id());
        register_steps(&id, r#""remote""#);

        let metadata = metadata_of(&id);
        assert_eq!(metadata.get("durable").map(String::as_str), Some("remote"));
        assert_eq!(
            metadata.get("sleep_after_ms").map(String::as_str),
            Some("21600000"),
            "the six-hour default is the class's, announced"
        );
    }

    #[test]
    fn the_durability_and_the_sleep_interval_ride_for_either_run() {
        for (suffix, run) in [("flow", r#""flow""#), ("steps", r#""steps""#)] {
            let keys = |name: &str, durable: &str| {
                let id = format!("meta_{name}_{suffix}_{}", std::process::id());
                let json = format!(
                    r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","durable":{durable},"run":{run},
                         "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
                         "tasklets":[{{"name":"p0"}}]}}"#
                );
                crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
                metadata_of(&id)
            };
            let memory = keys("memory", "false");
            assert_eq!(memory.get("durable").map(String::as_str), Some("memory"));
            assert!(!memory.contains_key("sleep_after_ms"));

            let sleeps = keys("sleeps", r#"{"local":{"sleep_after":2000}}"#);
            assert_eq!(sleeps.get("durable").map(String::as_str), Some("local"));
            assert_eq!(
                sleeps.get("sleep_after_ms").map(String::as_str),
                Some("2000")
            );
            assert!(
                !sleeps.contains_key("run"),
                "the run rides the factory's announcement"
            );
        }
    }

    #[test]
    fn a_steps_declaring_nothing_extra_ships_its_durability_and_its_positions() {
        let id = format!("meta_plain_{}", std::process::id());
        register_steps(&id, r#"{"local":{"sleep_after":false}}"#);

        assert_eq!(
            metadata_of(&id).into_iter().collect::<Vec<_>>(),
            vec![
                ("compensable_tasks".to_string(), String::new()),
                ("durable".to_string(), "local".to_string()),
            ]
        );
        assert!(metadata_response(
            crate::relay::registry::get_class("meta_never_registered").as_deref()
        )
        .is_empty());
    }
}

#[cfg(test)]
mod partition_tests {
    //! The bind refuses partitioning of a keyless class and arms it with the
    //! range on a keyed one.

    use super::{partition_refusal, AddRelay, AddRelayResponse, KeyGroupRange, Message};
    use crate::ErrorModel;

    /// A keyed chain into `p` makes it dynamic; cell keys make it static.
    fn register(id: &str, keyed: bool, cell_keys: &str) {
        let key = if keyed { r#"{"link":"key"},"# } else { "" };
        let json = format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","durable":{{"local":{{"sleep_after":false}}}},
                 "channels":[{{"name":"in","kind":"entry","chains":[[{key}{{"link":"into","tasklet":0}}]]}}],
                 "tasklets":[{{"name":"p","cells_for_overridden":true,
                                 "cell_keys":[{cell_keys}]}}]}}"#
        );
        crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    }

    fn class_of(factory_id: &str) -> std::sync::Arc<crate::relay::topology::RelayTopology> {
        crate::relay::registry::get_class(factory_id).expect("class registered")
    }

    fn partitioned(factory_id: &str, count: u32) -> AddRelay {
        AddRelay {
            relay_id: "inst".into(),
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
        register(&id, false, "");

        let refusal =
            partition_refusal(&partitioned(&id, 3), &class_of(&id), &id).expect("refused");
        assert_eq!(refusal.code(), "topology.partitions_without_keyed_tasklet");
        assert_eq!(refusal.to_error().details["factory"], id.as_str());
    }

    #[test]
    fn a_keyless_class_declares_itself_so_the_controller_can_refuse_first() {
        let id = format!("part_declare_{}", std::process::id());
        register(&id, false, "");

        let encoded = super::metadata_response(crate::relay::registry::get_class(&id).as_deref());
        let decoded = AddRelayResponse::decode(encoded.as_slice()).expect("decodes");
        assert_eq!(
            decoded.metadata.get("keyed").map(String::as_str),
            Some("false")
        );
    }

    #[test]
    fn a_keyed_class_is_accepted_and_says_nothing_extra() {
        let id = format!("part_dynamic_{}", std::process::id());
        register(&id, true, "");

        assert!(partition_refusal(&partitioned(&id, 3), &class_of(&id), &id).is_none());
        let encoded = super::metadata_response(crate::relay::registry::get_class(&id).as_deref());
        let decoded = AddRelayResponse::decode(encoded.as_slice()).expect("decodes");
        assert!(
            !decoded.metadata.contains_key("keyed"),
            "{:?}",
            decoded.metadata
        );
    }

    #[test]
    fn a_statically_keyed_class_partitions_normally() {
        let id = format!("part_static_{}", std::process::id());
        register(&id, false, r#""BTC-USD","ETH-USD""#);

        assert!(partition_refusal(&partitioned(&id, 3), &class_of(&id), &id).is_none());
    }

    #[test]
    fn an_unpartitioned_bind_is_never_refused_and_owns_the_whole_space() {
        let id = format!("part_none_{}", std::process::id());
        register(&id, false, "");

        let mut command = partitioned(&id, 3);
        command.partition_count = 0;

        assert!(partition_refusal(&command, &class_of(&id), &id).is_none());
        assert_eq!(command.key_group_range(), KeyGroupRange::UNPARTITIONED);
        assert!(!command.key_group_range().partitioned());
    }

    #[test]
    fn a_one_member_group_is_unpartitioned_too() {
        let id = format!("part_one_{}", std::process::id());
        register(&id, false, "");

        let mut command = partitioned(&id, 1);
        command.partition_count = 1;

        assert!(partition_refusal(&command, &class_of(&id), &id).is_none());
        assert_eq!(command.key_group_range(), KeyGroupRange::UNPARTITIONED);
    }

    /// A chained loop registers non-durable: a durable one needs a terminal
    /// handoff this one-tasklet class does not have.
    fn register_steps(id: &str, keyed: bool, each: &str) {
        let key = if keyed { r#"{"link":"key"},"# } else { "" };
        let durable = if each == "chained" {
            "false"
        } else {
            r#""local""#
        };
        let json = format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","durable":{durable},
                 "channels":[{{"name":"in","kind":"entry","chains":[[{key}{{"link":"into","tasklet":0}}]]}}],
                 "tasklets":[{{"name":"p"}}],
                 "run":{{"steps":{{"each":"{each}"}}}}}}"#
        );
        crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    }

    #[test]
    fn a_steps_that_reads_one_message_is_refused_before_the_keyless_arm() {
        for each in ["once", "chained"] {
            let id = format!("part_steps_{each}_{}", std::process::id());
            register_steps(&id, false, each);

            let refusal =
                partition_refusal(&partitioned(&id, 2), &class_of(&id), &id).expect("refused");
            assert_eq!(refusal.code(), "topology.partitions_without_per_input_loop");
            let details = refusal.to_error().details;
            assert_eq!(details["factory"], id.as_str());
            assert_eq!(details["each"], each);
            assert_eq!(details["partitions"], 2);

            let mut single = partitioned(&id, 2);
            single.partition_count = 0;
            assert!(partition_refusal(&single, &class_of(&id), &id).is_none());
        }
    }

    #[test]
    fn a_per_input_steps_partitions_when_keyed_and_is_refused_as_keyless_otherwise() {
        let keyed = format!("part_steps_keyed_{}", std::process::id());
        register_steps(&keyed, true, "per_input");
        assert!(partition_refusal(&partitioned(&keyed, 2), &class_of(&keyed), &keyed).is_none());

        let keyless = format!("part_steps_keyless_{}", std::process::id());
        register_steps(&keyless, false, "per_input");
        let refusal = partition_refusal(&partitioned(&keyless, 2), &class_of(&keyless), &keyless)
            .expect("refused");
        assert_eq!(refusal.code(), "topology.partitions_without_keyed_tasklet");
    }

    #[test]
    fn a_partitioned_bind_carries_its_range_verbatim() {
        let id = format!("part_range_{}", std::process::id());
        register(&id, true, "");

        let range = partitioned(&id, 3).key_group_range();
        assert_eq!(range.index, 1);
        assert_eq!(range.count, 3);
        assert_eq!((range.from, range.to), (85, 169));
        assert!(range.partitioned());
    }
}
