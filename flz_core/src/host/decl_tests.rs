//! Declaration-time refusals and acceptances of a host class. They live at the
//! host boundary rather than in a typed SDK because a declaration can arrive
//! from any host language, and `deny_unknown_fields` makes a field the core
//! never learned a refusal rather than a silent degrade.

fn decl(json: &str) -> Result<(), String> {
    crate::host::register_host_class_json(json.as_bytes())
}

fn factory(name: &str) -> String {
    format!("{name}_{}", std::process::id())
}

mod entry_observation_decl_tests {
    //! A host-language class reaches the wire through the same `dispatch` the
    //! typed Rust classes do, so its entry channels must carry the same
    //! observation cell — that is what makes the input-liveness signal one
    //! implementation for both SDKs rather than two.

    use super::{decl, factory};
    use crate::channel::ChannelKind;

    #[test]
    fn a_host_declared_entry_channel_carries_the_observation_its_peers_read() {
        let id = factory("host_entry_observation");
        decl(&format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"persistent":false,
                 "channels":[{{"name":"host_trades_in","kind":"entry","consumers":[0]}},
                             {{"name":"host_out","kind":"plugin","producer":0}}],
                 "procedures":[{{"name":"p0"}}]}}"#
        ))
        .expect("the class registers");

        let topology = crate::operative::registry::get_class(&id).expect("class registered");
        let entry = topology
            .channels
            .iter()
            .find(|c| c.kind == ChannelKind::Entry)
            .expect("entry channel");
        let cell = entry
            .entry_observation
            .as_ref()
            .expect("an entry channel is observed");
        assert!(
            crate::entry_input::observation_of("host_trades_in")
                .is_some_and(|shared| std::sync::Arc::ptr_eq(&shared, cell)),
            "the descriptor caches the cell a reader reaches by name"
        );

        assert!(
            topology
                .channels
                .iter()
                .filter(|c| c.kind != ChannelKind::Entry)
                .all(|c| c.entry_observation.is_none()),
            "only entry channels are observed"
        );

        entry.observe_delivery();
        assert!(crate::entry_input::last_delivery("host_trades_in").is_some());
    }
}

mod checkpoint_interval_decl_tests {
    //! The checkpoint interval's declaration crosses the C ABI as JSON, so its
    //! refusals live here rather than in a typed SDK.

    use super::{decl, factory};
    use crate::operative::topology::CheckpointOutputs;

    fn tier_class(id: &str, extra: &str) -> String {
        format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"persistent":true,"effectively_once":true,{extra}
                 "channels":[{{"name":"in","kind":"entry","consumers":[0]}},
                             {{"name":"out","kind":"plugin","producer":0,"keyed":true}}],
                 "procedures":[{{"name":"p0"}}]}}"#
        )
    }

    #[test]
    fn a_declared_interval_reaches_the_topology() {
        let id = factory("ckpt_full");
        decl(&tier_class(
            &id,
            r#""ack_granularity":"checkpoint","checkpoint_every":500,
               "checkpoint_outputs":"at_checkpoint","checkpoint_max_delay_ms":50,"#,
        ))
        .expect("a fully declared checkpoint interval registers");

        let policy = crate::operative::registry::get_class(&id)
            .expect("class registered")
            .ack_granularity
            .checkpoint()
            .expect("the class declared the checkpoint granularity");

        assert_eq!(policy.every, Some(500));
        assert_eq!(policy.outputs, CheckpointOutputs::AtCheckpoint);
        assert_eq!(policy.max_delay_ms, Some(50));
    }

    #[test]
    fn undeclared_options_are_the_as_produced_default() {
        let id = factory("ckpt_bare");
        decl(&tier_class(&id, r#""ack_granularity":"checkpoint","#))
            .expect("a bare checkpoint declaration registers");

        let policy = crate::operative::registry::get_class(&id)
            .expect("class registered")
            .ack_granularity
            .checkpoint()
            .expect("the class declared the checkpoint granularity");

        assert_eq!(policy.every, None);
        assert_eq!(policy.outputs, CheckpointOutputs::AsProduced);
        assert_eq!(policy.max_delay_ms, None);
    }

    #[test]
    fn holding_outputs_without_a_delay_is_refused_by_name() {
        let id = factory("ckpt_no_delay");
        let err = decl(&tier_class(
            &id,
            r#""ack_granularity":"checkpoint","checkpoint_outputs":"at_checkpoint","#,
        ))
        .expect_err("an unbounded output hold must not be declarable");

        assert!(
            err.contains("checkpoint_max_delay_ms"),
            "the refusal names the missing field: {err}"
        );
    }

    #[test]
    fn interval_options_without_the_checkpoint_granularity_are_refused() {
        let id = factory("ckpt_orphan");
        let err = decl(&tier_class(&id, r#""checkpoint_every":500,"#))
            .expect_err("an interval option means nothing without an interval");

        assert!(err.contains("ack_granularity checkpoint"), "{err}");
    }

    #[test]
    fn a_zero_interval_is_refused() {
        let id = factory("ckpt_zero");
        let err = decl(&tier_class(
            &id,
            r#""ack_granularity":"checkpoint","checkpoint_every":0,"#,
        ))
        .expect_err("an interval that closes every zero inputs is not an interval");

        assert!(err.contains("greater than zero"), "{err}");
    }
}

mod sync_durability_decl_tests {
    //! Synchronous remote durability is a workflow-only declaration
    //! (`remote-durability-tier`).

    use super::{decl, factory};

    #[test]
    fn a_stream_class_declaring_sync_durability_is_refused_by_name() {
        let id = factory("sync_stream");
        let err = decl(&format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"persistent":true,"sync_durability":true,
                 "channels":[{{"name":"in","kind":"entry","consumers":[0]}}],
                 "procedures":[{{"name":"p0"}}]}}"#
        ))
        .expect_err("a stream class must not be able to declare sync durability");

        assert!(err.contains(&id), "the refusal names the class: {err}");
        assert!(
            err.contains("sync_durability"),
            "the refusal names the reason: {err}"
        );
    }

    #[test]
    fn a_workflow_class_declaring_sync_durability_without_persistence_is_refused() {
        let id = factory("sync_wf_transient");
        let err = decl(&format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"persistent":false,"sync_durability":true,
                 "channels":[],"procedures":[],"workflow":{{}}}}"#
        ))
        .expect_err("there is no barrier to ship for a class that journals nothing");

        assert!(err.contains("sync_durability requires persistent"), "{err}");
    }

    #[test]
    fn a_durable_workflow_class_may_declare_it() {
        let id = factory("sync_wf_ok");
        decl(&format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"persistent":true,"sync_durability":true,
                 "channels":[],"procedures":[],"workflow":{{}}}}"#
        ))
        .expect("a durable workflow class declares sync durability");

        let spec = crate::workflow::registry::get_class(&id).expect("class registered");
        assert!(spec.sync_durability);
    }

    #[test]
    fn undeclared_is_the_default() {
        let id = factory("sync_wf_default");
        decl(&format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"persistent":true,
                 "channels":[],"procedures":[],"workflow":{{}}}}"#
        ))
        .expect("a workflow class declaring nothing registers");

        let spec = crate::workflow::registry::get_class(&id).expect("class registered");
        assert!(!spec.sync_durability);
    }
}

mod dropping_policy_decl_tests {
    //! A shedding policy is refused where the loss it permits cannot be
    //! accounted for. Declaration time is the only place the author can still
    //! see it: both facts — the tier and the correction window — are frozen at
    //! registration, so the contradiction is decidable before any data moves.

    use super::{decl, factory};

    /// One entry channel with a declared overflow policy, plus whatever
    /// durability contract `extra` adds.
    fn class(id: &str, overflow: &str, extra: &str) -> String {
        format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"persistent":true,{extra}
                 "channels":[{{"name":"in","kind":"entry","consumers":[0],
                               "overflow":"{overflow}"}},
                             {{"name":"out","kind":"plugin","producer":0,"keyed":true}}],
                 "procedures":[{{"name":"p0","clock":"event_time"}}]}}"#
        )
    }

    #[test]
    fn the_tier_refuses_a_shedding_channel_naming_both_declarations() {
        for policy in ["drop_newest", "drop_oldest"] {
            let id = factory(&format!("shed_tier_{policy}"));
            let err = decl(&class(&id, policy, r#""effectively_once":true,"#))
                .expect_err("shedding under the tier must not be declarable");
            assert!(err.contains("`in`"), "the refusal names the channel: {err}");
            assert!(err.contains(policy), "the refusal names the policy: {err}");
            assert!(
                err.contains("effectively_once"),
                "the refusal names the conflicting declaration: {err}"
            );
        }
    }

    #[test]
    fn a_correction_window_refuses_a_shedding_channel() {
        let id = factory("shed_correction");
        let err = decl(&class(
            &id,
            "drop_oldest",
            r#""correction_window_ms":600000,"#,
        ))
        .expect_err("shedding under a correction window must not be declarable");
        assert!(err.contains("`in`"), "{err}");
        assert!(err.contains("correction_window_ms"), "{err}");
    }

    #[test]
    fn shedding_stays_available_to_a_class_that_promises_neither() {
        let id = factory("shed_plain");
        decl(&class(&id, "drop_newest", "")).expect("shedding is a legitimate intent");
    }

    #[test]
    fn the_tier_accepts_the_policy_that_discards_nothing() {
        let id = factory("shed_none");
        decl(&class(&id, "drop_nothing", r#""effectively_once":true,"#))
            .expect("the default policy is what the tier requires");
    }
}

mod overflow_literal_tests {
    //! The wire spelling of the overflow policy is derived from the variant
    //! name by `rename_all = "snake_case"`, so the name and the literal cannot
    //! drift. What that derivation gives is worth pinning: it is the only
    //! cross-SDK statement of the policy every host language has to emit.

    use crate::host::HostOverflow;

    fn parse(literal: &str) -> Result<HostOverflow, String> {
        serde_json::from_str(literal).map_err(|e| e.to_string())
    }

    #[test]
    fn the_wire_literal_follows_the_variant() {
        assert_eq!(
            parse(r#""drop_nothing""#).unwrap(),
            HostOverflow::DropNothing
        );
        assert_eq!(parse(r#""drop_newest""#).unwrap(), HostOverflow::DropNewest);
        assert_eq!(parse(r#""drop_oldest""#).unwrap(), HostOverflow::DropOldest);
    }

    #[test]
    fn a_channel_declaring_nothing_discards_nothing() {
        assert_eq!(HostOverflow::default(), HostOverflow::DropNothing);
    }

    #[test]
    fn the_retired_spelling_is_refused_listing_what_is_accepted() {
        let err = parse(r#""block""#).expect_err("`block` is not a policy");
        assert!(err.contains("unknown variant `block`"), "{err}");
        assert!(
            err.contains("drop_nothing"),
            "the refusal names the replacement: {err}"
        );
    }
}

mod sdk_lang_declaration_tests {
    //! The language byte is the one declaration a veneer cannot omit: an
    //! untagged class reaches placement, and a placed class is handed durable
    //! bodies only its own SDK can decode.

    use super::{decl, factory};

    fn class(id: &str, lang: &str) -> String {
        format!(
            r#"{{"factory_id":"{id}",{lang}"persistent":true,
                 "channels":[{{"name":"in","kind":"entry","consumers":[0]}},
                             {{"name":"out","kind":"plugin","producer":0}}],
                 "procedures":[{{"name":"p0"}}]}}"#
        )
    }

    #[test]
    fn a_class_declaring_no_language_is_refused_naming_the_field() {
        let id = factory("host_lang_absent");
        let err = decl(&class(&id, "")).expect_err("an untagged class is refused");

        assert!(err.contains("sdk_lang"), "{err}");
        assert!(
            crate::operative::registry::get_class(&id).is_none(),
            "a refused declaration registers nothing"
        );
    }

    #[test]
    fn an_unspecified_language_is_refused_rather_than_defaulted() {
        let id = factory("host_lang_zero");
        let err = decl(&class(&id, r#""sdk_lang":0,"#)).expect_err("sdk_lang 0 is reserved");

        assert!(err.contains("sdk_lang"), "{err}");
    }

    #[test]
    fn an_unallocated_language_is_refused_rather_than_interpreted() {
        let id = factory("host_lang_unallocated");
        let err = decl(&class(&id, r#""sdk_lang":99,"#)).expect_err("99 is allocated to no SDK");

        assert!(err.contains("99"), "{err}");
    }

    /// The host restore arm compares a record's byte 0 against the READING
    /// class's declared language. Without this, a Java class would refuse its
    /// own Kryo records (or, worse, accept another SDK's).
    #[test]
    fn the_declared_language_reaches_the_restore_template_and_the_advertisement() {
        let id = factory("host_lang_java");
        decl(&class(&id, r#""sdk_lang":2,"change_format_id":4,"#))
            .expect("a Java-declared class registers");

        let topology = crate::operative::registry::get_class(&id).expect("class registered");
        assert_eq!(topology.sdk_lang, crate::header::SDK_LANG_JAVA);

        let facts = topology.procedures[0]
            .host_facts
            .as_ref()
            .expect("a host class carries host facts");
        assert_eq!(facts.header().sdk_lang, crate::header::SDK_LANG_JAVA);

        assert_eq!(
            crate::operative::registry::sdk_lang_for_factory(&id),
            crate::header::SDK_LANG_JAVA,
            "the advertisement reads the declared value from the core"
        );
        assert_eq!(
            crate::operative::registry::sdk_lang_for_factory("never_declared"),
            crate::header::SDK_LANG_UNSPECIFIED,
            "an undeclared factory advertises no language rather than a guess"
        );
    }

    /// The same path for the Node SDK, which is the first to declare a format
    /// byte that is neither postcard nor Kryo: the class's own template is
    /// what a restore compares against, so the format has to survive the
    /// declaration alongside the language.
    #[test]
    fn a_node_class_declares_its_language_and_the_msgpack_format() {
        let id = factory("host_lang_node");
        decl(&class(&id, r#""sdk_lang":4,"change_format_id":5,"#))
            .expect("a Node-declared class registers");

        let topology = crate::operative::registry::get_class(&id).expect("class registered");
        assert_eq!(topology.sdk_lang, crate::header::SDK_LANG_NODE);

        let header = topology.procedures[0]
            .host_facts
            .as_ref()
            .expect("a host class carries host facts")
            .header();
        assert_eq!(header.sdk_lang, crate::header::SDK_LANG_NODE);
        assert_eq!(header.format_id, crate::header::FORMAT_ID_MSGPACK);

        assert_eq!(
            crate::operative::registry::sdk_lang_for_factory(&id),
            crate::header::SDK_LANG_NODE
        );
    }

    /// The same path for the PHP SDK, whose format byte is JSON — the third
    /// distinct change format a host has declared, and the one that proves the
    /// declaration carries the format rather than the core inferring it from
    /// the language.
    #[test]
    fn a_php_class_declares_its_language_and_the_json_format() {
        let id = factory("host_lang_php");
        decl(&class(&id, r#""sdk_lang":6,"change_format_id":6,"#))
            .expect("a PHP-declared class registers");

        let topology = crate::operative::registry::get_class(&id).expect("class registered");
        assert_eq!(topology.sdk_lang, crate::header::SDK_LANG_PHP);

        let header = topology.procedures[0]
            .host_facts
            .as_ref()
            .expect("a host class carries host facts")
            .header();
        assert_eq!(header.sdk_lang, crate::header::SDK_LANG_PHP);
        assert_eq!(header.format_id, crate::header::FORMAT_ID_JSON);

        assert_eq!(
            crate::operative::registry::sdk_lang_for_factory(&id),
            crate::header::SDK_LANG_PHP,
            "the advertisement reads the declared value from the core"
        );
    }
}

mod declaration_key_tests {
    //! The declaration key set is a cross-SDK contract, so it is pinned in a
    //! file both sides read: `test/fixtures/host_class_decl_keys.txt`. Here the
    //! core proves the file matches what it accepts; `HostClassDeclarationTest`
    //! proves the Java builder emits every key in it. A key added to one side
    //! alone fails one of the two.

    const FIXTURE: &str = include_str!("../../../test/fixtures/host_class_decl_keys.txt");
    const SOURCE: &str = include_str!("decl.rs");

    fn pinned(section: &str) -> Vec<String> {
        FIXTURE
            .lines()
            .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
            .filter_map(|line| line.strip_prefix(&format!("{section}.")))
            .map(str::to_string)
            .collect()
    }

    /// The `pub <name>:` fields of one declaration struct, in declaration order.
    fn fields_of(struct_name: &str) -> Vec<String> {
        let body = SOURCE
            .split_once(&format!("pub struct {struct_name} {{"))
            .unwrap_or_else(|| panic!("{struct_name} is declared in decl.rs"))
            .1;
        let body = body.split_once("\n}").expect("the struct body ends").0;

        body.lines()
            .filter_map(|line| line.trim().strip_prefix("pub "))
            .filter_map(|rest| rest.split_once(':'))
            .map(|(name, _)| name.to_string())
            .collect()
    }

    #[test]
    fn every_declaration_field_is_pinned_by_the_shared_fixture() {
        for (section, struct_name) in [
            ("class", "HostClassDecl"),
            ("channel", "HostChannelDecl"),
            ("procedure", "HostProcedureDecl"),
            ("workflow", "HostWorkflowDecl"),
        ] {
            assert_eq!(
                fields_of(struct_name),
                pinned(section),
                "{struct_name} and the `{section}.` lines of \
                 test/fixtures/host_class_decl_keys.txt disagree — teach the fixture, \
                 the Rust builder and the Java builder in the same change"
            );
        }
    }

    #[test]
    fn a_declaration_carrying_every_pinned_key_is_accepted() {
        // `deny_unknown_fields` makes this exact: a key the fixture spells
        // differently from the core is a parse error naming it.
        let value = |section: &str, key: &str| -> &'static str {
            match (section, key) {
                (_, "factory_id") | (_, "name") => r#""pinned_decl""#,
                ("class", "sdk_lang") | ("class", "change_format_id") => "1",
                ("procedure", "schema_version") | ("procedure", "min_readable_schema_version") => {
                    "1"
                }
                ("class", "ack_granularity") => r#""checkpoint""#,
                ("class", "checkpoint_outputs") => r#""at_checkpoint""#,
                ("class", "beyond_window_policy") => r#""drop""#,
                ("class", "channels") => r#"[{"name":"in","kind":"entry","consumers":[0]}]"#,
                ("class", "procedures") => r#"[{"name":"p0"}]"#,
                ("class", "workflow") => "null",
                ("channel", "kind") => r#""entry""#,
                ("channel", "producer") => "null",
                ("channel", "consumers") => "[0]",
                ("channel", "overflow") => r#""drop_nothing""#,
                ("procedure", "entity_mode") => r#""single""#,
                ("procedure", "entity_keys") => "[]",
                ("procedure", "clock") => r#""wall""#,
                ("workflow", "loop_mode") => r#""once""#,
                ("workflow", "rollback_mode") => r#""all_or_nothing""#,
                (_, k) if k.ends_with("_ms") || k.ends_with("_bytes") => "1000",
                (_, k) if k.ends_with("_every") || k.ends_with("_attempts") => "1",
                (_, k) if k.ends_with("_max") || k.ends_with("_capacity") => "1",
                (_, k) if k.ends_with("_entities") => "1",
                _ => "true",
            }
        };

        let object = |section: &str| -> String {
            pinned(section)
                .iter()
                .map(|key| format!(r#""{key}":{}"#, value(section, key)))
                .collect::<Vec<_>>()
                .join(",")
        };

        let id = format!("pinned_decl_{}", std::process::id());
        let json = format!(
            "{{{},{},{},{}}}",
            object("class").replace(
                r#""factory_id":"pinned_decl""#,
                &format!(r#""factory_id":"{id}""#)
            ),
            format_args!(r#""channels":[{{{}}}]"#, object("channel")),
            format_args!(r#""procedures":[{{{}}}]"#, object("procedure")),
            format_args!(r#""workflow":{{{}}}"#, object("workflow")),
        );

        // The class-level `channels` / `procedures` / `workflow` placeholders
        // above are superseded by the fully populated ones appended after them,
        // so strip the placeholders before parsing.
        let json = json
            .replace(
                r#""channels":[{"name":"in","kind":"entry","consumers":[0]}],"#,
                "",
            )
            .replace(r#""procedures":[{"name":"p0"}],"#, "")
            .replace(r#""workflow":null,"#, "");

        let decl: crate::host::HostClassDecl = serde_json::from_slice(json.as_bytes())
            .unwrap_or_else(|e| panic!("a fully pinned declaration must parse: {e}\n{json}"));

        assert_eq!(decl.factory_id, id);
        assert_eq!(decl.channels.len(), 1);
        assert_eq!(decl.procedures.len(), 1);
        assert!(decl.workflow.is_some());
    }
}
