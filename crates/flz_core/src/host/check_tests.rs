//! The reference run of the refusal table: every row of
//! `test/fixtures/reference_topologies.txt` driven against `check` itself.
//!
//! Each SDK's conformance run maps the fixture's steps to its own builder; this
//! one maps them straight to the record, because the core has no builder. So a
//! refusal no record can produce is caught here rather than blamed on seven
//! builders, and a row added to the fixture is driven with no Rust edit — the
//! fixture is the contract, not a shape re-typed once per language.
//!
//! It is also the negative control for the two clauses that need a runner: what
//! the core answers is settled before any runtime is asked to agree.

use std::collections::HashMap;

use super::decl::{HostAckGranularity, HostChannelKind, HostClassDecl};
use super::issue::TopologyIssue;
use super::register::{check, Derived};
use crate::relay::topology::CellMode;
use crate::ErrorModel;

const FIXTURE: &str = "../../test/fixtures/reference_topologies.txt";

/// A row of the four-column refusal table; `refusal` is `ok` or a code.
struct Row {
    refusal: String,
    topology: String,
    steps: String,
    needs: String,
}

/// The rows a record can be judged on. A short-form row is refused by its SDK
/// before any declaration exists, so the core never sees one.
fn rows() -> Vec<Row> {
    let body = std::fs::read_to_string(FIXTURE)
        .unwrap_or_else(|e| panic!("the fixture is the contract and must be readable: {e}"));
    body.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| {
            let cols: Vec<&str> = line.split('|').map(str::trim).collect();
            (cols.len() == 4).then(|| Row {
                refusal: cols[0].to_string(),
                topology: cols[1].to_string(),
                steps: cols[2].to_string(),
                needs: cols[3].to_string(),
            })
        })
        .filter(|row| !row.needs.starts_with("shorthand"))
        .collect()
}

/// A variant's code, as the fixture spells its refusal.
fn code_of(variant: &str) -> String {
    let mut code = String::from("topology.");
    for (i, c) in variant.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            code.push('_');
        }
        code.push(c.to_ascii_lowercase());
    }
    code
}

/// The codes the fixture lists as `unrowed`: raised from a record's raw shape
/// no builder writes, or from a steps shape no row states yet.
fn unrowed() -> Vec<String> {
    std::fs::read_to_string(FIXTURE)
        .expect("the fixture")
        .lines()
        .filter_map(|line| line.trim().strip_prefix("unrowed |"))
        .map(|code| code.trim().to_string())
        .collect()
}

/// The palette: the facts each role the fixture header names declares. The
/// core's "SDK" is the record, so a role IS its tasklet declaration.
fn palette(role: &str) -> Option<&'static str> {
    Some(match role {
        // A dynamic tasklet is one a keyed chain reaches: the row wires it.
        "plain" | "second" | "twin" | "dynamic" => "",
        "static" => r#""cell_keys":["a","b"]"#,
        "routed" => r#""cell_keys":["a","b"],"cells_for_overridden":true"#,
        "nochange" => r#""clock":"logical","change_is_nochange":true"#,
        "eventtime" => r#""clock":"event_time""#,
        _ => return None,
    })
}

/// One channel under construction: what the record will spell for it.
#[derive(Default)]
struct Channel {
    name: String,
    kind: &'static str,
    producer: Option<usize>,
    chains: Vec<String>,
    keyed: bool,
    retry_max: Option<u32>,
    overflow: Option<String>,
    schema: Option<String>,
}

/// The record a row's steps describe. The step vocabulary is the fixture
/// header's; every interpreter in the tree maps the same words.
#[derive(Default)]
struct Interpreter {
    channels: Vec<Channel>,
    /// Role → tasklet index, in the order the row first names each role.
    roles: Vec<String>,
    mailbox: HashMap<String, usize>,
    class: Vec<String>,
    merge_group: u32,
}

impl Interpreter {
    /// The index of `role`, minting the tasklet the first time a row names it.
    fn tasklet(&mut self, role: &str) -> usize {
        if let Some(at) = self.roles.iter().position(|r| r == role) {
            return at;
        }
        self.roles.push(role.to_string());
        self.roles.len() - 1
    }

    /// The channel `handle` addresses: an entry channel by name, or the output
    /// channel of the tasklet a role names — minted on first use, exactly as
    /// a fluent builder mints a producer's output.
    fn channel(&mut self, handle: &str) -> usize {
        if let Some(at) = self.channels.iter().position(|c| c.name == handle) {
            return at;
        }
        let out = format!("{handle}_out");
        if let Some(at) = self.channels.iter().position(|c| c.name == out) {
            return at;
        }
        let producer = self.tasklet(handle);
        self.channels.push(Channel {
            name: out,
            kind: "inter_task",
            producer: Some(producer),
            ..Channel::default()
        });
        self.channels.len() - 1
    }

    fn run(&mut self, steps: &str) {
        for step in steps.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            self.step(step);
        }
    }

    fn step(&mut self, step: &str) {
        let (verb, arg) = split_call(step);
        match verb {
            "entry" => self.channels.push(Channel {
                name: arg.to_string(),
                kind: "entry",
                ..Channel::default()
            }),
            "merge" => {
                let parts: Vec<&str> = arg.split(',').map(str::trim).collect();
                let tasklet = self.tasklet(parts[0]);
                let group = self.merge_group;
                self.merge_group += 1;
                for source in &parts[1..] {
                    let at = self.channel(source);
                    self.channels[at].chains.push(format!(
                        r#"[{{"link":"merge","tasklet":{tasklet},"group":{group}}}]"#
                    ));
                }
            }
            "branch" => {
                let parts: Vec<&str> = arg.split(',').map(str::trim).collect();
                let at = self.channel(parts[0]);
                let arms: usize = parts[1].parse().expect("branch arity");
                self.channels[at]
                    .chains
                    .push(format!(r#"[{{"link":"branch","arms":{arms}}}]"#));
            }
            _ => self.dotted(step),
        }
    }

    /// `<handle>.<link>…` — a chain declared off a handle, a channel knob, a
    /// tasklet knob, or a class flag.
    fn dotted(&mut self, step: &str) {
        let (head, rest) = step.split_once('.').expect("a step is a call or a chain");
        if head == "class" {
            let (flag, value) = split_call(rest);
            self.class_flag(flag, value);
            return;
        }
        let (verb, arg) = split_call(rest);
        match verb {
            "mailbox" => {
                self.tasklet(head);
                self.mailbox
                    .insert(head.to_string(), arg.parse().expect("mailbox capacity"));
            }
            "keyed" => {
                let at = self.channel(head);
                self.channels[at].keyed = true;
            }
            "retry_max" => {
                let at = self.channel(head);
                self.channels[at].retry_max = Some(arg.parse().expect("retry ceiling"));
            }
            "on_full" => {
                let at = self.channel(head);
                self.channels[at].overflow = Some(arg.to_string());
            }
            "schema" => {
                let at = self.channel(head);
                self.channels[at].schema = Some(arg.to_string());
            }
            _ => self.chain(head, rest),
        }
    }

    /// One fluent sequence: the links left to right, ending in whatever closes
    /// it. `into` answers the consumer's output channel, so `a.into(b)` then
    /// `b.into(c)` chains the way a builder's handles chain.
    fn chain(&mut self, head: &str, rest: &str) {
        let at = self.channel(head);
        let mut links = Vec::new();
        for link in rest.split('.') {
            let (verb, arg) = split_call(link);
            if verb.starts_with("call") {
                links.push(self.call(at, verb, arg));
                continue;
            }
            links.push(match verb {
                "map" | "filter" | "flat_map" => r#"{"link":"transform"}"#.to_string(),
                "key" => r#"{"link":"key"}"#.to_string(),
                "named" => format!(r#"{{"link":"named","name":"{arg}"}}"#),
                "output" if arg.is_empty() => r#"{"link":"sink"}"#.to_string(),
                "output" => format!(r#"{{"link":"sink","name":"{arg}"}}"#),
                "into" => {
                    let tasklet = self.tasklet(arg);
                    format!(r#"{{"link":"into","tasklet":{tasklet}}}"#)
                }
                other => panic!("the fixture spells a link this interpreter does not map: {other}"),
            });
        }
        self.channels[at]
            .chains
            .push(format!("[{}]", links.join(",")));
    }

    /// A call end off channel `at`: its link, and its Response channel wired
    /// into the tasklet producing `at` unless the step leaves it unread.
    fn call(&mut self, at: usize, verb: &str, arg: &str) -> String {
        let response = format!("{}.response", self.derived_name(at));
        let (target, deadline, create) = match verb {
            "call" | "call_unread" => (format!(r#""target":"{arg}""#), "500", true),
            "call_factory" => (format!(r#""factory":"{arg}""#), "500", true),
            "call_factory_nocreate" => (format!(r#""factory":"{arg}""#), "500", false),
            "call_deadline" => (r#""target":"pricing""#.to_string(), arg, true),
            other => panic!("the fixture spells a call this interpreter does not map: {other}"),
        };
        let chains = match (verb, self.channels[at].producer) {
            ("call_unread", _) | (_, None) => Vec::new(),
            (_, Some(producer)) => vec![format!(r#"[{{"link":"into","tasklet":{producer}}}]"#)],
        };
        self.channels.push(Channel {
            name: response.clone(),
            kind: "entry",
            chains,
            ..Channel::default()
        });
        format!(
            r#"{{"link":"call",{target},"deadline_ms":{deadline},"create":{create},"response":"{response}"}}"#
        )
    }

    /// The name channel `at` takes: its last `named` link, else the name its
    /// sink gives it, else the name it was declared with.
    fn derived_name(&self, at: usize) -> String {
        let channel = &self.channels[at];
        let named = |key: &str| {
            channel.chains.iter().rev().find_map(|chain| {
                let (_, rest) = chain.split_once(&format!(r#""link":"{key}","name":""#))?;
                rest.split_once('"').map(|(name, _)| name.to_string())
            })
        };
        named("named")
            .or_else(|| named("sink"))
            .unwrap_or_else(|| channel.name.clone())
    }

    /// `class.run(<tree>)` and `class.durable(<tree>)`: the tree's JSON is the
    /// record's own member.
    fn class_flag(&mut self, flag: &str, value: &str) {
        match flag {
            "run" | "durable" => self.class.push(format!(r#""{flag}":{value}"#)),
            other => {
                panic!("the fixture spells a class setting this interpreter does not map: {other}")
            }
        }
    }

    fn json(&self, id: &str) -> String {
        let channels: Vec<String> = self
            .channels
            .iter()
            .map(|c| {
                let mut fields = vec![
                    format!(r#""name":"{}""#, c.name),
                    format!(r#""kind":"{}""#, c.kind),
                    format!(r#""chains":[{}]"#, c.chains.join(",")),
                ];
                if let Some(p) = c.producer {
                    fields.push(format!(r#""producer":{p}"#));
                }
                if c.keyed {
                    fields.push(r#""keyed":true"#.to_string());
                }
                if let Some(retry) = c.retry_max {
                    fields.push(format!(r#""retry_max":{retry}"#));
                }
                if let Some(overflow) = &c.overflow {
                    fields.push(format!(r#""overflow":"{overflow}""#));
                }
                if let Some(schema) = &c.schema {
                    fields.push(format!(r#""schema":{schema}"#));
                }
                format!("{{{}}}", fields.join(","))
            })
            .collect();

        let tasklets: Vec<String> = self
            .roles
            .iter()
            .map(|role| {
                let facts = palette(role).unwrap_or_else(|| {
                    panic!("the fixture names a palette role that is not one: {role}")
                });
                // A twin declares the SAME name as `plain`: that IS the collision.
                let name = if role == "twin" {
                    "plain"
                } else {
                    role.as_str()
                };
                let mut fields = vec![format!(r#""name":"{name}""#)];
                if !facts.is_empty() {
                    fields.push(facts.to_string());
                }
                if let Some(capacity) = self.mailbox.get(role) {
                    fields.push(format!(r#""mailbox_capacity":{capacity}"#));
                }
                format!("{{{}}}", fields.join(","))
            })
            .collect();

        let class: String = self.class.iter().map(|f| format!(",{f}")).collect();
        format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0"{class},
                 "channels":[{}],"tasklets":[{}]}}"#,
            channels.join(","),
            tasklets.join(",")
        )
    }
}

/// `verb(arg)` → `("verb", "arg")`; a bare word → `("word", "")`.
fn split_call(text: &str) -> (&str, &str) {
    match text.split_once('(') {
        Some((verb, rest)) => (verb.trim(), rest.trim_end_matches(')').trim()),
        None => (text.trim(), ""),
    }
}

fn record_of(row: &Row) -> HostClassDecl {
    let mut interpreter = Interpreter::default();
    interpreter.run(&row.steps);
    let json = interpreter.json(&row.topology);
    serde_json::from_str(&json)
        .unwrap_or_else(|e| panic!("row `{}` must parse: {e}\n{json}", row.topology))
}

fn drivable(row: &Row) -> bool {
    row.steps != "cited above"
}

/// Every row of the refusal table, judged. This is the whole reference run: a
/// row that names a refusal must get it, a row that is `ok` must register.
#[test]
fn every_fixture_row_answers_what_it_names() {
    let rows = rows();
    assert!(
        rows.len() > 30,
        "the fixture is the contract; read only {} rows",
        rows.len()
    );

    let mut driven = 0;
    for row in rows.iter().filter(|r| drivable(r)) {
        driven += 1;
        let names: Vec<String> = match check(record_of(row)) {
            Ok(_) => Vec::new(),
            Err(issues) => issues.iter().map(|i| i.code().to_string()).collect(),
        };

        if row.refusal == "ok" {
            assert!(
                names.is_empty(),
                "row `{}` is well-formed and must register, read {names:?}",
                row.topology
            );
        } else {
            assert!(
                names.contains(&row.refusal),
                "row `{}` must refuse with {}, read {names:?}",
                row.topology,
                row.refusal
            );
        }
    }

    assert!(
        driven >= rows.len() - 5,
        "the run drove {driven} of {} rows; a row that stops interpreting is a refusal \
         nothing measures",
        rows.len()
    );
}

/// A kind literal where a run is read is refused naming the literal.
#[test]
fn a_kind_literal_is_refused_by_name() {
    let row = Row {
        refusal: "topology.invalid_setting".into(),
        topology: "kind_literal_unbounded".into(),
        steps: r#"class.run("unbounded"); entry(in); in.into(plain)"#.into(),
        needs: "untyped_tree".into(),
    };
    let issues = check(record_of(&row)).expect_err("a kind literal is refused");
    let error = issues[0].to_error();
    assert_eq!(error.code, "topology.invalid_setting");
    assert_eq!(error.details["value"], "unbounded");
    assert_eq!(error.details["expected"], "flow | steps");
}

/// A setting written under a parent that does not carry it is refused naming
/// both, and a class that says nothing is a flow relay kept locally.
#[test]
fn a_misplaced_setting_names_itself_and_its_parent() {
    let row = |steps: &str| Row {
        refusal: "topology.unknown_field".into(),
        topology: "misplaced".into(),
        steps: format!("{steps}; entry(in); in.into(plain)"),
        needs: "untyped_tree".into(),
    };
    let details = |steps: &str| {
        let issues = check(record_of(&row(steps))).expect_err("a misplaced setting is refused");
        let error = issues[0].to_error();
        assert_eq!(error.code, "topology.unknown_field", "{steps}");
        (
            error.details["declaration"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            error.details["fields"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
        )
    };
    assert_eq!(
        details(r#"class.run({"flow":{"each":"per_input"}})"#),
        (
            "'flow' of relay 'misplaced'".to_string(),
            "each".to_string()
        )
    );
    assert_eq!(
        details(r#"class.durable({"false":{"sleep_after":1000}})"#),
        (
            "'false' of relay 'misplaced'".to_string(),
            "sleep_after".to_string()
        )
    );
    let accepted = check(record_of(&Row {
        refusal: "ok".into(),
        topology: "plain".into(),
        steps: "entry(in); in.into(plain)".into(),
        needs: "-".into(),
    }))
    .expect("a class that says nothing registers");
    assert_eq!(accepted.run, crate::relay::settings::Run::default());
    assert_eq!(accepted.durable, crate::relay::settings::Durable::local());
}

/// Every variant a builder can reach is produced by some row, except the
/// `unrowed` codes the fixture exempts.
#[test]
fn the_table_reaches_every_variant_the_vocabulary_carries() {
    let vocabulary = std::fs::read_to_string("src/host/issue.rs").expect("the vocabulary");
    let declared: Vec<String> = vocabulary
        .lines()
        .filter_map(|line| line.strip_prefix("    "))
        .filter(|rest| rest.starts_with(char::is_uppercase))
        .filter_map(|rest| {
            rest.split(['(', '{', ' ', ','])
                .next()
                .filter(|name| !name.is_empty())
                .map(str::to_string)
        })
        .collect();
    assert!(declared.len() > 30, "read only {} variants", declared.len());

    let named: Vec<String> = rows().into_iter().map(|r| r.refusal).collect();
    let unrowed = unrowed();
    for variant in declared {
        if unrowed.contains(&code_of(&variant)) {
            continue;
        }
        assert!(
            named.contains(&code_of(&variant)),
            "`{variant}` can be raised and no fixture row drives it"
        );
    }
}

mod call;

/// The derivation rules, which no refusal row states: what a well-formed record
/// MEANS once its chains are read.
mod derivation {
    use super::*;

    fn derive(steps: &str) -> Derived {
        check(record_of(&row_of(steps))).expect("a well-formed record")
    }

    fn refuse(steps: &str) -> Vec<TopologyIssue> {
        check(record_of(&row_of(steps))).expect_err("the steps break a rule")
    }

    fn row_of(steps: &str) -> Row {
        Row {
            refusal: "ok".into(),
            topology: "derivation".into(),
            steps: steps.into(),
            needs: "-".into(),
        }
    }

    #[test]
    fn only_a_chain_that_keys_makes_its_target_dynamic() {
        let fixture = std::fs::read_to_string("../../test/fixtures/derived_cell_modes.txt")
            .expect("the shared case");
        let mut lines = fixture
            .lines()
            .filter(|line| !line.starts_with('#'))
            .filter_map(|line| line.split_once('|'));
        let (_, shape) = lines.next().expect("the shape comes first");
        let derived = derive(shape);
        let modes: Vec<(&str, &str)> = lines.collect();
        assert_eq!(modes.len(), derived.decl.tasklets.len());
        for (tasklet, mode) in modes {
            let at = derived.decl.tasklets.iter().position(|t| t.name == tasklet);
            assert_eq!(
                derived.modes[at.expect("a tasklet the shape names")].as_str(),
                mode
            );
        }
    }

    #[test]
    fn cell_keys_make_a_tasklet_static() {
        assert_eq!(
            derive("entry(in); in.into(routed)").modes,
            [CellMode::Static]
        );
    }

    #[test]
    fn a_schema_floor_is_judged_against_the_version_and_its_migration() {
        let judge = |facts: &str| {
            let json = format!(
                r#"{{"factory_id":"floor","sdk_lang":1,"sdk_version":"0.1.0",
                     "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
                     "tasklets":[{{"name":"p",{facts}}}]}}"#
            );
            check(serde_json::from_str(&json).expect("a record")).err()
        };
        let tasklet = || "p".to_string();
        assert_eq!(
            judge(r#""schema_version":2,"min_readable_schema_version":3"#),
            Some(vec![TopologyIssue::SchemaFloorAboveVersion {
                tasklet: tasklet(),
                floor: 3,
                version: 2
            }])
        );
        assert_eq!(
            judge(r#""schema_version":3,"min_readable_schema_version":1"#),
            Some(vec![TopologyIssue::SchemaFloorWithoutMigration {
                tasklet: tasklet(),
                floor: 1
            }])
        );
        assert_eq!(
            judge(r#""schema_version":3,"min_readable_schema_version":1,"migrates":true"#),
            None
        );
        assert_eq!(
            judge(r#""schema_version":3,"min_readable_schema_version":3"#),
            None
        );
    }

    #[test]
    fn a_bare_re_declaration_is_the_fan_in_no_op() {
        let derived = derive("entry(in); in.into(plain); in.into(plain)");
        assert_eq!(
            derived.channels[0].consumers.len(),
            1,
            "a re-declaration carrying nothing appends nothing"
        );
    }

    #[test]
    fn a_named_link_renames_the_channel_for_every_terminator() {
        let derived = derive("entry(in); in.into(plain); plain.named(renamed); plain.output");

        assert_eq!(
            derived.channels[1].name, "renamed",
            "the override is read once"
        );
        assert_eq!(
            derived.channels[1].sink.as_deref(),
            Some("renamed"),
            "the sink IS the channel, so the descriptor is the channel's name"
        );
        assert_eq!(
            derived.channels[1].kind,
            HostChannelKind::Plugin,
            "a sink with no consumer IS the plugin terminal"
        );
    }

    #[test]
    fn a_sink_names_the_channel() {
        let derived = derive("entry(in); in.into(plain); plain.output(items_output)");

        assert_eq!(
            derived.channels[1].name, "items_output",
            "the name written on the sink IS the channel's name"
        );
        assert_eq!(derived.channels[1].sink.as_deref(), Some("items_output"));
    }

    #[test]
    fn a_nameless_sink_leaves_the_derived_name_alone() {
        let derived = derive("entry(in); in.into(plain); plain.output");

        assert_eq!(
            derived.channels[1].name, "plain_out",
            "`sink()` derives the name exactly as a channel with no sink does"
        );
        assert_eq!(derived.channels[1].sink.as_deref(), Some("plain_out"));
    }

    #[test]
    fn only_two_output_named_channels_collide_as_a_duplicate_output() {
        // A `named(...)` channel colliding with a `sink(...)` one is a bare
        // duplicate: nobody pointed two channels at one sink.
        let mixed = refuse(
            "entry(in); in.into(plain); in.into(second); plain.named(out); plain.output;              second.output(out)",
        );
        assert!(
            mixed
                .iter()
                .any(|i| matches!(i, TopologyIssue::DuplicateChannel(name) if name == "out")),
            "read {mixed:?}"
        );
    }

    #[test]
    fn a_channel_named_twice_is_refused_even_when_the_names_agree() {
        let issues = refuse("entry(in); in.into(plain); plain.named(same); plain.output(same)");

        assert!(
            issues.iter().any(|i| matches!(
                i,
                TopologyIssue::ChannelNamedTwice { named, output, .. } if named == "same" && output == "same"
            )),
            "an agreeing pair is still two declarations, read {issues:?}"
        );
    }

    #[test]
    fn two_sinks_stay_a_duplicate_sink_and_never_ends_twice() {
        let issues = refuse("entry(in); in.into(plain); plain.output(out); plain.output(out)");
        assert!(
            issues
                .iter()
                .all(|i| !matches!(i, TopologyIssue::ChannelEndsTwice { .. })),
            "read {issues:?}"
        );
    }

    #[test]
    fn a_hybrid_channel_keeps_the_kind_it_declared() {
        let derived = derive("entry(in); in.into(plain); plain.output(out); plain.into(second)");
        assert_eq!(
            derived.channels[1].kind,
            HostChannelKind::InterTask,
            "a sink WITH a consumer behind it is hybrid, not a terminal"
        );
    }

    #[test]
    fn a_checkpoint_knob_implies_the_granularity() {
        let derived = derive(
            r#"class.run({"flow":{"exactly":{"effectively_once":{"ack":{"checkpoint":{"every":500}}}}}}); entry(in); in.into(plain)"#,
        );
        assert_eq!(
            derived.ack_granularity,
            HostAckGranularity::Checkpoint,
            "a checkpoint ack is the granularity the derived answer names"
        );
    }

    #[test]
    fn a_derived_edge_names_the_chain_that_produced_it() {
        let derived = derive("entry(in); in.map.into(plain); in.into(second)");
        let edges = &derived.channels[0].consumers;

        assert_eq!(edges.len(), 2);
        assert_eq!(edges[0].chain, 0, "the transformed edge came from chain 0");
        assert_eq!(edges[1].chain, 1, "the bare edge from chain 1");
    }

    #[test]
    fn a_verdict_lists_every_refusal_it_found() {
        let row = Row {
            refusal: "topology.duplicate_key_by".into(),
            topology: "accumulates".into(),
            steps: "entry(in); in.key.key.map.into(static)".into(),
            needs: "-".into(),
        };
        let issues = check(record_of(&row)).expect_err("three rules are broken at once");
        let codes: Vec<String> = issues.iter().map(ErrorModel::code).collect();

        assert!(
            codes.iter().any(|c| c == "topology.duplicate_key_by"),
            "{codes:?}"
        );
        assert!(
            codes.iter().any(|c| c == "topology.transform_after_key_by"),
            "{codes:?}"
        );
        assert!(
            codes.iter().any(|c| c == "topology.cells_for_missing"),
            "{codes:?}"
        );

        let rendered = super::super::issue::render_issues(&issues);
        assert_eq!(rendered.code, codes[0], "the first issue names the refusal");
        let listed: Vec<&str> = rendered.details["issues"]
            .as_array()
            .expect("every issue is listed")
            .iter()
            .map(|issue| issue["code"].as_str().expect("each issue has its code"))
            .collect();
        assert_eq!(listed, codes, "every issue rides details.issues");
    }
}
