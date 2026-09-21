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

const FIXTURE: &str = "../test/fixtures/reference_topologies.txt";

/// A row of the four-column refusal table.
struct Row {
    refusal: String,
    topology: String,
    steps: String,
    needs: String,
}

/// The surfaces no record can express either. `detached` is authorable here —
/// the record is data, and a tasklet nothing wires is one line of it — so
/// only the two flag families the fixture header names are skipped.
const UNWRITABLE: [&str; 2] = ["raw_flags", "raw_class_flags"];

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

/// The palette: the facts each role the fixture header names declares. The
/// core's "SDK" is the record, so a role IS its tasklet declaration.
fn palette(role: &str) -> Option<&'static str> {
    Some(match role {
        "plain" | "second" | "twin" => "",
        "static" => r#""cell_mode":"static","cell_keys":["a","b"]"#,
        "routed" => r#""cell_mode":"static","cell_keys":["a","b"],"cells_for_overridden":true"#,
        "dynamic" => r#""cell_mode":"dynamic""#,
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
    workflow: bool,
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
            links.push(match verb {
                "map" | "filter" | "flat_map" => r#"{"link":"transform"}"#.to_string(),
                "key" => r#"{"link":"key"}"#.to_string(),
                "named" => format!(r#"{{"link":"named","name":"{arg}"}}"#),
                "sink" if arg.is_empty() => r#"{"link":"sink"}"#.to_string(),
                "sink" => format!(r#"{{"link":"sink","name":"{arg}"}}"#),
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

    fn class_flag(&mut self, flag: &str, value: &str) {
        match flag {
            "workflow" => self.workflow = true,
            "persistent" | "effectively_once" | "checkpoint_ack" | "sync_durability" => {
                self.class.push(format!(r#""{flag}":true"#));
            }
            "checkpoint_every" => self.class.push(format!(r#""checkpoint_every":{value}"#)),
            "correction_window" => self
                .class
                .push(format!(r#""correction_window_ms":{value}"#)),
            "outputs_at_checkpoint" => {
                self.class
                    .push(r#""checkpoint_outputs":"at_checkpoint""#.to_string());
                if !value.is_empty() {
                    self.class
                        .push(format!(r#""checkpoint_max_delay_ms":{value}"#));
                }
            }
            other => {
                panic!("the fixture spells a class flag this interpreter does not map: {other}")
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
        let workflow = if self.workflow {
            r#","workflow":{}"#
        } else {
            ""
        };
        format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0"{class}{workflow},
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
    row.steps != "cited above" && !UNWRITABLE.contains(&row.needs.as_str())
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
            Err(issues) => issues.iter().map(|i| i.name().to_string()).collect(),
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

/// The two rows no builder can author still have to be reachable from a record,
/// or the refusal that catches a regressing builder is dead code.
#[test]
fn the_rows_no_builder_can_author_are_still_reachable_from_a_record() {
    let raw: Vec<Row> = rows()
        .into_iter()
        .filter(|r| r.needs == "raw_class_flags")
        .collect();
    assert!(!raw.is_empty(), "the fixture names the rows KI-147 guards");

    for row in raw {
        let issues = check(record_of(&row)).expect_err("the row names a refusal");
        let names: Vec<&str> = issues.iter().map(TopologyIssue::name).collect();
        assert!(
            names.contains(&row.refusal.as_str()),
            "row `{}` must refuse with {}, read {names:?}",
            row.topology,
            row.refusal
        );
    }
}

/// Every named variant is produced by some row. `Declaration` is the residue
/// the vocabulary has not named and is exempt by name here and in the seam test.
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
    for variant in declared {
        if variant == "Declaration" {
            continue;
        }
        assert!(
            named.contains(&variant),
            "`{variant}` can be raised and no fixture row drives it"
        );
    }
}

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
        let derived = derive("entry(in); in.into(plain); plain.named(renamed); plain.sink");

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
        let derived = derive("entry(in); in.into(plain); plain.sink(items_output)");

        assert_eq!(
            derived.channels[1].name, "items_output",
            "the name written on the sink IS the channel's name"
        );
        assert_eq!(derived.channels[1].sink.as_deref(), Some("items_output"));
    }

    #[test]
    fn a_nameless_sink_leaves_the_derived_name_alone() {
        let derived = derive("entry(in); in.into(plain); plain.sink");

        assert_eq!(
            derived.channels[1].name, "plain_out",
            "`sink()` derives the name exactly as a channel with no sink does"
        );
        assert_eq!(derived.channels[1].sink.as_deref(), Some("plain_out"));
    }

    #[test]
    fn only_two_sink_named_channels_collide_as_a_duplicate_sink() {
        // A `named(...)` channel colliding with a `sink(...)` one is a bare
        // duplicate: nobody pointed two channels at one sink.
        let mixed = refuse(
            "entry(in); in.into(plain); in.into(second); plain.named(out); plain.sink;              second.sink(out)",
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
        let issues = refuse("entry(in); in.into(plain); plain.named(same); plain.sink(same)");

        assert!(
            issues.iter().any(|i| matches!(
                i,
                TopologyIssue::ChannelNamedTwice { named, sink, .. } if named == "same" && sink == "same"
            )),
            "an agreeing pair is still two declarations, read {issues:?}"
        );
    }

    #[test]
    fn a_hybrid_channel_keeps_the_kind_it_declared() {
        let derived = derive("entry(in); in.into(plain); plain.sink(out); plain.into(second)");
        assert_eq!(
            derived.channels[1].kind,
            HostChannelKind::InterTask,
            "a sink WITH a consumer behind it is hybrid, not a terminal"
        );
    }

    #[test]
    fn a_checkpoint_knob_implies_the_granularity() {
        let derived = derive(
            "class.persistent; class.effectively_once; class.checkpoint_every(500); \
             entry(in); in.into(plain)",
        );
        assert_eq!(
            derived.ack_granularity,
            HostAckGranularity::Checkpoint,
            "six per-SDK helpers carried this implication; the core states it once"
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
            refusal: "DuplicateKeyBy".into(),
            topology: "accumulates".into(),
            steps: "entry(in); in.key.key.map.into(static)".into(),
            needs: "-".into(),
        };
        let issues = check(record_of(&row)).expect_err("three rules are broken at once");
        let names: Vec<&str> = issues.iter().map(TopologyIssue::name).collect();

        assert!(names.contains(&"DuplicateKeyBy"), "{names:?}");
        assert!(names.contains(&"TransformAfterKeyBy"), "{names:?}");
        assert!(names.contains(&"CellsForMissing"), "{names:?}");

        let rendered = super::super::issue::render_issues(&issues);
        assert!(
            rendered.starts_with("topology rejected ["),
            "every surface that prints reads the names off the bracketed list: {rendered}"
        );
    }
}
