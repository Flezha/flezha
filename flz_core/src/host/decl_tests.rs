//! Declaration-time refusals and acceptances of a host class. They live at the
//! host boundary rather than in a typed SDK because a declaration can arrive
//! from any host language, and `deny_unknown_fields` makes a field the core
//! never learned a refusal rather than a silent degrade.

use flz_transport::errors::{ErrorModel, FlezhaError};
use serde_json::{Map, Value};

fn decl(json: &str) -> Result<(), FlezhaError> {
    crate::host::register_host_class_json(json.as_bytes()).map_err(|e| e.to_error())
}

/// The details of the refusal `code` among everything `err` names: one of its
/// `details.issues`, or the error itself.
fn issue<'e>(err: &'e FlezhaError, code: &str) -> &'e Map<String, Value> {
    err.details
        .get("issues")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|listed| listed["code"] == code)
        .and_then(|listed| listed["details"].as_object())
        .or_else(|| (err.code == code).then_some(&err.details))
        .unwrap_or_else(|| panic!("{code} is not among the refusals: {err:?}"))
}

fn factory(name: &str) -> String {
    format!("{name}_{}", std::process::id())
}

mod checkpoint_interval_decl_tests {
    //! The checkpoint interval's declaration crosses the C ABI as JSON, so its
    //! refusals live here rather than in a typed SDK.

    use super::{decl, factory, issue};
    use crate::operator::topology::CheckpointOutputs;

    fn tier_class(id: &str, extra: &str) -> String {
        format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","persistent":true,"effectively_once":true,{extra}
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}},
                             {{"name":"out","kind":"plugin","producer":0,"keyed":true,
                               "chains":[[{{"link":"sink","name":"out"}}]]}}],
                 "tasklets":[{{"name":"p0"}}]}}"#
        )
    }

    #[test]
    fn a_declared_interval_reaches_the_topology() {
        let id = factory("ckpt_full");
        decl(&tier_class(
            &id,
            r#""checkpoint_ack":true,"checkpoint_every":500,
               "checkpoint_outputs":"at_checkpoint","checkpoint_max_delay_ms":50,"#,
        ))
        .expect("a fully declared checkpoint interval registers");

        let policy = crate::operator::registry::get_class(&id)
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
        decl(&tier_class(&id, r#""checkpoint_ack":true,"#))
            .expect("a bare checkpoint declaration registers");

        let policy = crate::operator::registry::get_class(&id)
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
            r#""checkpoint_ack":true,"checkpoint_outputs":"at_checkpoint","#,
        ))
        .expect_err("an unbounded output hold must not be declarable");

        issue(&err, "topology.at_checkpoint_without_max_delay");
    }

    #[test]
    fn an_interval_option_implies_the_checkpoint_granularity() {
        let id = factory("ckpt_implied");
        decl(&tier_class(&id, r#""checkpoint_every":500,"#))
            .expect("an interval option is the granularity, asked for by its knob");

        let policy = crate::operator::registry::get_class(&id)
            .expect("class registered")
            .ack_granularity
            .checkpoint()
            .expect("the interval implied the granularity");

        assert_eq!(policy.every, Some(500));
    }

    #[test]
    fn a_zero_interval_is_refused() {
        let id = factory("ckpt_zero");
        let err = decl(&tier_class(
            &id,
            r#""checkpoint_ack":true,"checkpoint_every":0,"#,
        ))
        .expect_err("an interval that closes every zero inputs is not an interval");

        issue(&err, "topology.checkpoint_every_zero");
    }
}

mod sync_durability_decl_tests {
    //! Synchronous remote durability is a bounded-only declaration
    //! (`remote-durability-tier`).

    use super::{decl, factory, issue};

    #[test]
    fn a_stream_class_declaring_sync_durability_is_refused_by_name() {
        let id = factory("sync_unbounded");
        let err = decl(&format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","persistent":true,"sync_durability":true,
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
                 "tasklets":[{{"name":"p0"}}]}}"#
        ))
        .expect_err("an unbounded class must not be able to declare sync durability");

        let details = issue(&err, "topology.sync_durability_on_unbounded");
        assert_eq!(
            details["factory_id"],
            id.as_str(),
            "the refusal names the class"
        );
    }

    #[test]
    fn a_bounded_class_declaring_sync_durability_without_persistence_is_refused() {
        let id = factory("sync_bounded_transient");
        let err = decl(&format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","persistent":false,"sync_durability":true,
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
                 "tasklets":[{{"name":"p0"}}],"bounded":{{}}}}"#
        ))
        .expect_err("there is no barrier to ship for a class that journals nothing");

        issue(&err, "topology.sync_durability_without_persistence");
    }

    #[test]
    fn a_durable_bounded_class_may_declare_it() {
        let id = factory("sync_bounded_ok");
        decl(&format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","persistent":true,"sync_durability":true,
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
                 "tasklets":[{{"name":"p0"}}],"bounded":{{}}}}"#
        ))
        .expect("a durable bounded class declares sync durability");

        let class = crate::operator::registry::get_class(&id).expect("class registered");
        assert!(
            class
                .bounded
                .as_ref()
                .expect("a bounded class")
                .sync_durability
        );
    }

    #[test]
    fn undeclared_is_the_default() {
        let id = factory("sync_bounded_default");
        decl(&format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","persistent":true,
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
                 "tasklets":[{{"name":"p0"}}],"bounded":{{}}}}"#
        ))
        .expect("a bounded class declaring nothing registers");

        let class = crate::operator::registry::get_class(&id).expect("class registered");
        assert!(
            !class
                .bounded
                .as_ref()
                .expect("a bounded class")
                .sync_durability
        );
    }
}

mod dropping_policy_decl_tests {
    //! A shedding policy is refused where the loss it permits cannot be
    //! accounted for. Declaration time is the only place the author can still
    //! see it: both facts — the tier and the correction window — are frozen at
    //! registration, so the contradiction is decidable before any data moves.

    use super::{decl, factory, issue};

    /// One entry channel with a declared overflow policy, plus whatever
    /// durability contract `extra` adds.
    fn class(id: &str, overflow: &str, extra: &str) -> String {
        format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","persistent":true,{extra}
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]],
                               "overflow":"{overflow}"}},
                             {{"name":"out","kind":"plugin","producer":0,"keyed":true,
                               "chains":[[{{"link":"sink","name":"out"}}]]}}],
                 "tasklets":[{{"name":"p0","clock":"event_time"}}]}}"#
        )
    }

    #[test]
    fn the_tier_refuses_a_shedding_channel_naming_both_declarations() {
        for policy in ["drop_newest", "drop_oldest"] {
            let id = factory(&format!("shed_tier_{policy}"));
            let err = decl(&class(&id, policy, r#""effectively_once":true,"#))
                .expect_err("shedding under the tier must not be declarable");
            let details = issue(&err, "topology.dropping_policy_under_effectively_once");
            assert_eq!(details["channel"], "in", "the refusal names the channel");
            assert_eq!(details["policy"], policy, "the refusal names the policy");
        }
    }

    #[test]
    fn a_correction_window_refuses_a_shedding_channel() {
        let id = factory("shed_correction");
        let err = decl(&class(
            &id,
            "drop_oldest",
            r#""effectively_once":true,"correction_window_ms":600000,"#,
        ))
        .expect_err("shedding under a correction window must not be declarable");
        let details = issue(&err, "topology.dropping_policy_under_effectively_once");
        assert_eq!(details["channel"], "in");
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

    use super::{decl, factory, issue};

    fn class(id: &str, lang: &str) -> String {
        format!(
            r#"{{"factory_id":"{id}",{lang}"sdk_version":"0.1.0","persistent":true,
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}},
                             {{"name":"out","kind":"plugin","producer":0}}],
                 "tasklets":[{{"name":"p0"}}]}}"#
        )
    }

    #[test]
    fn a_class_declaring_no_language_is_refused_naming_the_field() {
        let id = factory("host_lang_absent");
        let err = decl(&class(&id, "")).expect_err("an untagged class is refused");

        let details = issue(&err, "topology.declaration_unreadable");
        assert!(
            details["reason"]
                .as_str()
                .is_some_and(|r| r.contains("sdk_lang")),
            "the refusal names the missing field: {err:?}"
        );
        assert!(
            crate::operator::registry::get_class(&id).is_none(),
            "a refused declaration registers nothing"
        );
    }

    #[test]
    fn an_unspecified_language_is_refused_rather_than_defaulted() {
        let id = factory("host_lang_zero");
        let err = decl(&class(&id, r#""sdk_lang":0,"#)).expect_err("sdk_lang 0 is reserved");

        assert_eq!(issue(&err, "topology.unknown_sdk_lang")["sdk_lang"], 0);
    }

    #[test]
    fn an_unallocated_language_is_refused_rather_than_interpreted() {
        let id = factory("host_lang_unallocated");
        let err = decl(&class(&id, r#""sdk_lang":99,"#)).expect_err("99 is allocated to no SDK");

        assert_eq!(issue(&err, "topology.unknown_sdk_lang")["sdk_lang"], 99);
    }

    /// The declared language and format reach the restore template and the
    /// advertisement — one representative SDK per format byte (Kryo,
    /// MessagePack, JSON), since the declaration carries the format rather
    /// than the core inferring it from the language.
    #[test]
    fn the_declared_language_and_format_reach_the_template_and_the_advertisement() {
        use crate::header::{
            FORMAT_ID_JSON, FORMAT_ID_KRYO, FORMAT_ID_MSGPACK, SDK_LANG_JAVA, SDK_LANG_NODE,
            SDK_LANG_PHP, SDK_LANG_UNSPECIFIED,
        };
        for (name, lang, format) in [
            ("java", SDK_LANG_JAVA, FORMAT_ID_KRYO),
            ("node", SDK_LANG_NODE, FORMAT_ID_MSGPACK),
            ("php", SDK_LANG_PHP, FORMAT_ID_JSON),
        ] {
            let id = factory(&format!("host_lang_{name}"));
            decl(&class(
                &id,
                &format!(r#""sdk_lang":{lang},"change_format_id":{format},"#),
            ))
            .expect("a declared class registers");

            let topology = crate::operator::registry::get_class(&id).expect("class registered");
            assert_eq!(topology.sdk_lang, lang);
            let header = topology.tasklets[0]
                .host_facts
                .as_ref()
                .expect("a host class carries host facts")
                .header();
            assert_eq!((header.sdk_lang, header.format_id), (lang, format));
            assert_eq!(
                crate::operator::registry::advertised_for_factory(&id),
                (lang, "unbounded"),
                "the advertisement reads the declared value and kind from the core"
            );
        }
        let bounded = factory("host_lang_bounded");
        crate::host::register_host_class_json(
            crate::test_support::host_class::bounded_json(&bounded, &["in"], 1).as_bytes(),
        )
        .expect("a bounded class registers");
        assert_eq!(
            crate::operator::registry::advertised_for_factory(&bounded).1,
            "bounded",
            "the kind is the registry the class declaration landed in"
        );
        assert_eq!(
            crate::operator::registry::advertised_for_factory("never_declared"),
            (SDK_LANG_UNSPECIFIED, ""),
            "an undeclared factory advertises no language and no kind rather than a guess"
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
            ("tasklet", "HostTaskletDecl"),
            ("bounded", "HostBoundedDecl"),
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
                ("class", "sdk_version") => r#""0.1.0""#,
                ("tasklet", "schema_version") | ("tasklet", "min_readable_schema_version") => "1",
                ("class", "checkpoint_ack") => "true",
                ("class", "checkpoint_outputs") => r#""at_checkpoint""#,
                ("class", "beyond_window_policy") => r#""drop""#,
                ("class", "channels") => r#"[{"name":"in","kind":"entry","chains":[]}]"#,
                ("class", "tasklets") => r#"[{"name":"p0"}]"#,
                ("class", "bounded") => "null",
                ("channel", "kind") => r#""entry""#,
                ("channel", "producer") => "null",
                ("channel", "chains") => r#"[[{"link":"into","tasklet":0}]]"#,
                ("channel", "overflow") => r#""drop_nothing""#,
                ("tasklet", "cell_mode") => r#""single""#,
                ("tasklet", "cell_keys") => "[]",
                ("tasklet", "clock") => r#""wall""#,
                ("bounded", "loop_mode") => r#""once""#,
                ("bounded", "rollback_mode") => r#""all_or_nothing""#,
                (_, k) if k.ends_with("_ms") || k.ends_with("_bytes") => "1000",
                (_, k) if k.ends_with("_every") || k.ends_with("_attempts") => "1",
                (_, k) if k.ends_with("_max") || k.ends_with("_capacity") => "1",
                (_, k) if k.ends_with("_cells") => "1",
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
            format_args!(r#""tasklets":[{{{}}}]"#, object("tasklet")),
            format_args!(r#""bounded":{{{}}}"#, object("bounded")),
        );

        // The class-level `channels` / `tasklets` / `bounded` placeholders
        // above are superseded by the fully populated ones appended after them,
        // so strip the placeholders before parsing.
        let json = json
            .replace(
                r#""channels":[{"name":"in","kind":"entry","chains":[]}],"#,
                "",
            )
            .replace(r#""tasklets":[{"name":"p0"}],"#, "")
            .replace(r#""bounded":null,"#, "");

        let decl: crate::host::HostClassDecl = serde_json::from_slice(json.as_bytes())
            .unwrap_or_else(|e| panic!("a fully pinned declaration must parse: {e}\n{json}"));

        assert_eq!(decl.factory_id, id);
        assert_eq!(decl.channels.len(), 1);
        assert_eq!(decl.tasklets.len(), 1);
        assert!(decl.bounded.is_some());
    }
}

mod sdk_version_decl_tests {
    //! The durable header's `sdk_version` is the *writing* SDK's version. A
    //! hosted veneer is a different release train from the core it links
    //! against, so the value crosses the ABI on the declaration rather than
    //! being read from the core's own package metadata.

    use super::{decl, factory, issue};

    #[test]
    fn the_declared_version_reaches_the_record_header() {
        let id = factory("host_sdk_version");
        decl(&format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"1.2.3",
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
                 "tasklets":[{{"name":"p0"}}]}}"#
        ))
        .expect("the class registers");

        let topology = crate::operator::registry::get_class(&id).expect("class registered");
        let facts = topology
            .tasklet(0)
            .and_then(|p| p.host_facts.clone())
            .expect("a host tasklet carries its declared facts");

        assert_eq!(
            facts.header().sdk_version,
            0x01_02_03,
            "the header carries the version the veneer declared, not the core's"
        );
    }

    /// Omitting the key, sending an empty string, sending `0.0.0` and sending
    /// something that is not a version are one defect — the class named no
    /// usable version — so one clause refuses them with one message.
    #[test]
    fn a_class_declaring_no_usable_version_is_refused_naming_the_field() {
        for (name, version) in [
            ("host_version_absent", ""),
            ("host_version_empty", r#""sdk_version":"","#),
            ("host_version_zero", r#""sdk_version":"0.0.0","#),
            ("host_version_unparseable", r#""sdk_version":"rolling","#),
        ] {
            let id = factory(name);
            let err = decl(&format!(
                r#"{{"factory_id":"{id}","sdk_lang":1,{version}
                     "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
                     "tasklets":[{{"name":"p0"}}]}}"#
            ))
            .expect_err("a class that names no SDK version is refused");

            issue(&err, "topology.unusable_sdk_version");
            assert!(
                crate::operator::registry::get_class(&id).is_none(),
                "a refused declaration registers nothing"
            );
        }
    }
}

mod journal_codec_tests {
    //! Which channels the journal carries, decided once at the host boundary.
    //!
    //! A durable chained loop re-seeds iteration N+1 from its terminal handoff,
    //! and `encode_handoff` ships an empty batch for a channel with no codec —
    //! which the resumed loop reads as "converged". So the rule that a codec
    //! rides an inter-tasklet channel and nothing else is what makes the
    //! registration refusal on the typed side decisive, and it is pinned here
    //! rather than left to the one line that implements it.

    use super::factory;
    use crate::channel::ChannelKind;
    use crate::host::build::build_channels;
    use crate::host::register::check_class_json;

    fn channels(json: &str) -> Vec<(ChannelKind, bool)> {
        let derived = check_class_json(json.as_bytes()).expect("the class is well formed");
        build_channels(&derived.decl, &derived.channels)
            .0
            .into_iter()
            .map(|c| (c.kind, c.codec.is_some()))
            .collect()
    }

    fn chained_loop(terminal_links: &str) -> String {
        format!(
            r#"{{"factory_id":"{}","sdk_lang":1,"sdk_version":"0.1.0","persistent":true,
                 "bounded":{{"loop_mode":"chained"}},
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}},
                             {{"name":"out","kind":"inter_task","producer":0,
                               "chains":[{terminal_links}]}}],
                 "tasklets":[{{"name":"p0"}}]}}"#,
            factory("chained_terminal")
        )
    }

    /// The refusal every host reaches. Java, Node, Python, Ruby and PHP all
    /// register through `declare_class`, so this is the site that covers them;
    /// the typed Rust SDK refuses the same shape in its own vocabulary because
    /// it never passes through here.
    #[test]
    fn a_durable_chained_loop_whose_terminal_only_sinks_is_refused() {
        let err = super::decl(&chained_loop(r#"[{"link":"sink","name":"s"}]"#))
            .expect_err("a plugin-bound terminal carries no handoff");
        let details = super::issue(&err, "topology.chained_loop_terminal_without_handoff");
        assert_eq!(
            details["tasklet"], "p0",
            "the refusal names the terminal tasklet"
        );
    }

    /// The same class with a consumer behind the terminal channel keeps the
    /// channel inter-tasklet, so the journal carries it and it registers.
    #[test]
    fn a_durable_chained_loop_with_an_inter_task_terminal_registers() {
        let ok = format!(
            r#"{{"factory_id":"{}","sdk_lang":1,"sdk_version":"0.1.0","persistent":true,
                 "bounded":{{"loop_mode":"chained"}},
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}},
                             {{"name":"out","kind":"inter_task","producer":0,
                               "chains":[[{{"link":"sink","name":"s"}}],[{{"link":"into","tasklet":1}}]]}},
                             {{"name":"tail","kind":"inter_task","producer":1,"chains":[[]]}}],
                 "tasklets":[{{"name":"p0"}},{{"name":"p1"}}]}}"#,
            factory("chained_ok")
        );
        super::decl(&ok).expect("an inter-tasklet terminal carries the handoff");
    }

    #[test]
    fn a_response_link_marks_its_descriptor_and_a_sink_does_not() {
        let derived = check_class_json(
            r#"{"factory_id":"response_link","sdk_lang":1,"sdk_version":"0.1.0",
                 "channels":[{"name":"in","kind":"entry",
                              "chains":[[{"link":"into","tasklet":0}],[{"link":"into","tasklet":1}]]},
                             {"name":"ans","kind":"inter_task","producer":0,
                              "chains":[[{"link":"response"}]]},
                             {"name":"out","kind":"inter_task","producer":1,
                              "chains":[[{"link":"sink","name":"out"}]]}],
                 "tasklets":[{"name":"p0"},{"name":"p1"}]}"#
                .as_bytes(),
        )
        .expect("the class is well formed");
        let built: Vec<(String, ChannelKind, bool)> =
            build_channels(&derived.decl, &derived.channels)
                .0
                .into_iter()
                .map(|c| (c.name.to_string(), c.kind, c.response))
                .collect();
        assert_eq!(
            built,
            vec![
                ("in".to_string(), ChannelKind::Entry, false),
                ("ans".to_string(), ChannelKind::PluginBound, true),
                ("out".to_string(), ChannelKind::PluginBound, false),
            ]
        );
    }

    #[test]
    fn a_call_link_builds_its_end_and_create_defaults_to_true() {
        let derived = check_class_json(
            r#"{"factory_id":"call_link","sdk_lang":1,"sdk_version":"0.1.0",
                 "channels":[{"name":"in","kind":"entry","chains":[[{"link":"into","tasklet":0}]]},
                             {"name":"ask","kind":"inter_task","producer":0,
                              "chains":[[{"link":"key"},{"link":"call","factory":"agent",
                                          "deadline_ms":120000,"channel":"turn",
                                          "response":"ask.response"}]]},
                             {"name":"ask.response","kind":"entry",
                              "chains":[[{"link":"into","tasklet":0}]]}],
                 "tasklets":[{"name":"p0"}]}"#
                .as_bytes(),
        )
        .expect("the class is well formed");
        let built = build_channels(&derived.decl, &derived.channels).0;
        let end = built[1].call.as_ref().expect("the call end");
        assert_eq!(built[1].kind, ChannelKind::PluginBound);
        assert!(!built[1].sink && !built[1].response);
        assert_eq!(&*end.target, "factory:agent");
        assert!(
            end.create,
            "a call creates its callee unless it says otherwise"
        );
        assert_eq!(end.deadline_ms, 120_000);
        assert_eq!(end.channel.as_deref(), Some("turn"));
        assert_eq!(&*end.response, "ask.response");
        assert!(built[0].call.is_none() && built[2].call.is_none());
        assert_eq!(
            end.stamps(Some("anna-42"), 1_000),
            vec![
                ("flz.call".to_string(), "factory:agent".to_string()),
                ("flz.call_response".to_string(), "ask.response".to_string()),
                ("flz.deadline_at".to_string(), "121000".to_string()),
                ("flz.call_channel".to_string(), "turn".to_string()),
                ("flz.key".to_string(), "anna-42".to_string()),
            ]
        );
    }

    #[test]
    fn a_codec_rides_an_inter_task_channel_and_nothing_else() {
        let rows = channels(
            r#"{"factory_id":"codec_kinds","sdk_lang":1,"sdk_version":"0.1.0","persistent":true,
                 "channels":[{"name":"in","kind":"entry","chains":[[{"link":"into","tasklet":0}]]},
                             {"name":"mid","kind":"inter_task","producer":0,
                              "chains":[[{"link":"into","tasklet":1}]]},
                             {"name":"out","kind":"inter_task","producer":1,
                              "chains":[[{"link":"sink","name":"out"}]]}],
                 "tasklets":[{"name":"p0"},{"name":"p1"}]}"#,
        );

        assert_eq!(
            rows,
            vec![
                (ChannelKind::Entry, false),
                (ChannelKind::InterTasklet, true),
                // Declared inter_task, derived plugin-bound: a sink with no
                // consumer behind it IS the terminal, and nothing re-seeds from
                // it, so the journal does not carry it.
                (ChannelKind::PluginBound, false),
            ]
        );
    }
}
