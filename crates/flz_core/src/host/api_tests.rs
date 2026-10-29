//! A class's API and graph as its runner announces them: what
//! `advertised_for_factory` builds from the host's channel facts and the
//! topology, and the schema a registration refuses.

use flz_transport::errors::ErrorModel;

use crate::pb::ApiEntry;
use crate::relay::registry::advertised_for_factory;

const PLACE_SCHEMA: &str = r#"{"type":"object","properties":{"pair":{"type":"string"}}}"#;
const PLACED_SCHEMA: &str = r#"{"type":"object","properties":{"status":{"type":"string"}}}"#;

fn register(id: &str, channels: &str, tasklets: &str) {
    let json = format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0",
             "channels":[{channels}],"tasklets":[{tasklets}]}}"#
    );
    crate::host::register_host_class_json(json.as_bytes()).expect("the class registers");
}

fn json(text: &str) -> serde_json::Value {
    serde_json::from_str(text).expect("a schema is JSON")
}

fn factory(name: &str) -> String {
    format!("{name}_{}", std::process::id())
}

fn entry<'a>(api: &'a [ApiEntry], channel: &str) -> &'a ApiEntry {
    api.iter()
        .find(|entry| entry.channel == channel)
        .unwrap_or_else(|| panic!("{channel} is not in the API: {api:?}"))
}

/// The `orders` shape: `place` answers through the `placed` output, `audit`
/// reaches no output.
fn orders(id: &str) {
    register(
        id,
        &format!(
            r#"{{"name":"place","kind":"entry","codec":"json","schema":{PLACE_SCHEMA},
                 "description":"Places an order","chains":[[{{"link":"into","tasklet":0}}]]}},
               {{"name":"placed","kind":"plugin","producer":0,"codec":"json",
                 "schema":{PLACED_SCHEMA},"chains":[[{{"link":"sink","name":"placed"}}]]}},
               {{"name":"audit","kind":"entry","codec":"text",
                 "chains":[[{{"link":"into","tasklet":1}}]]}}"#
        ),
        r#"{"name":"place"},{"name":"audit"}"#,
    );
}

#[test]
fn an_answering_entry_carries_its_answer_and_its_facts() {
    let id = factory("api_answering");
    orders(&id);

    let api = advertised_for_factory(&id).api;
    let place = entry(&api, "place");

    assert_eq!(
        (place.codec.as_str(), place.description.as_str()),
        ("json", "Places an order")
    );
    assert_eq!(json(&place.schema_json), json(PLACE_SCHEMA));
    assert_eq!(place.answer_codec, "json");
    assert_eq!(json(&place.answer_schema_json), json(PLACED_SCHEMA));
    assert_eq!(api.len(), 2, "one entry per entry channel: {api:?}");
}

#[test]
fn a_send_only_entry_has_no_answer() {
    let id = factory("api_send_only");
    orders(&id);

    let api = advertised_for_factory(&id).api;
    let audit = entry(&api, "audit");

    assert_eq!(audit.codec, "text");
    assert_eq!(
        (audit.answer_codec.as_str(), audit.schema_json.as_str()),
        ("", "")
    );
}

#[test]
fn two_reachable_outputs_that_disagree_answer_bytes_with_no_schema() {
    let id = factory("api_disagree");
    register(
        &id,
        r#"{"name":"in","kind":"entry",
            "chains":[[{"link":"into","tasklet":0}],[{"link":"into","tasklet":1}]]},
           {"name":"a","kind":"plugin","producer":0,"codec":"json",
            "chains":[[{"link":"sink","name":"a"}]]},
           {"name":"b","kind":"plugin","producer":1,"codec":"text",
            "chains":[[{"link":"sink","name":"b"}]]}"#,
        r#"{"name":"left"},{"name":"right"}"#,
    );

    let api = advertised_for_factory(&id).api;
    let entry = entry(&api, "in");

    assert_eq!(entry.codec, "json", "an entry naming no codec reads JSON");
    assert_eq!(
        (
            entry.answer_codec.as_str(),
            entry.answer_schema_json.as_str()
        ),
        ("bytes", "")
    );
}

#[test]
fn a_schema_that_is_not_an_object_with_a_type_refuses_the_class() {
    for (name, facts) in [
        ("api_schema_string", r#""codec":"json","schema":"object""#),
        ("api_schema_untyped", r#""schema":{"properties":{}}"#),
        (
            "api_schema_on_text",
            r#""codec":"text","schema":{"type":"string"}"#,
        ),
    ] {
        let id = factory(name);
        let json = format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0",
                 "channels":[{{"name":"in","kind":"entry",{facts},
                               "chains":[[{{"link":"into","tasklet":0}}]]}}],
                 "tasklets":[{{"name":"p0"}}]}}"#
        );
        let err = crate::host::register_host_class_json(json.as_bytes())
            .expect_err("a broken schema is refused")
            .to_error();

        assert_eq!(err.code, "topology.schema_invalid", "{name}: {err:?}");
        assert_eq!(err.details["channel"], "in");
        assert!(
            crate::relay::registry::get_class(&id).is_none(),
            "{name}: nothing is registered for a refused class"
        );
    }
}

/// The channel rows of an announced graph: name, kind, producer, consumers,
/// plugin end, call target and overflow.
fn graph_rows(id: &str) -> Vec<[String; 7]> {
    let graph = advertised_for_factory(id)
        .graph
        .expect("a registered class has a graph");
    graph
        .channels
        .into_iter()
        .map(|c| {
            [
                c.name,
                c.kind,
                c.producer,
                c.consumers.join(","),
                c.plugin_end,
                c.call_target,
                c.overflow,
            ]
        })
        .collect()
}

#[test]
fn a_host_class_announces_every_channel_with_its_ends() {
    let id = factory("graph_host");
    register(
        &id,
        r#"{"name":"trades_in","kind":"entry","overflow":"drop_newest",
            "chains":[[{"link":"into","tasklet":0}],[{"link":"key"},{"link":"into","tasklet":1}]]},
           {"name":"ohlc_out","kind":"inter_task","producer":0,
            "chains":[[{"link":"sink","name":"ohlc_out"}],[{"link":"into","tasklet":2}]]},
           {"name":"trades_out","kind":"inter_task","producer":1,
            "chains":[[{"link":"into","tasklet":2}]]},
           {"name":"ask","kind":"plugin","producer":2,
            "chains":[[{"link":"call","factory":"pricing","deadline_ms":500,"response":"ask.response"}]]},
           {"name":"ask.response","kind":"entry","chains":[[{"link":"into","tasklet":3}]]}"#,
        r#"{"name":"ohlc","cell_keys":["a","b"],"cells_for_overridden":true},
           {"name":"trades","idle_ttl_ms":30000,"max_cells":200},
           {"name":"atr","mailbox_capacity":64},
           {"name":"read"}"#,
    );

    let row = |cols: [&str; 7]| cols.map(str::to_string);
    assert_eq!(
        graph_rows(&id),
        [
            row([
                "trades_in",
                "entry",
                "",
                "ohlc,trades",
                "none",
                "",
                "drop_newest"
            ]),
            row([
                "ohlc_out",
                "inter_tasklet",
                "ohlc",
                "atr",
                "output",
                "",
                "drop_nothing"
            ]),
            row([
                "trades_out",
                "inter_tasklet",
                "trades",
                "atr",
                "none",
                "",
                "drop_nothing"
            ]),
            row([
                "ask",
                "plugin_bound",
                "atr",
                "",
                "call",
                "factory:pricing",
                "drop_nothing"
            ]),
            row([
                "ask.response",
                "entry",
                "",
                "read",
                "none",
                "",
                "drop_nothing"
            ]),
        ]
    );

    let tasklets: Vec<_> = advertised_for_factory(&id)
        .graph
        .expect("graph")
        .tasklets
        .into_iter()
        .map(|t| (t.name, t.mode, t.cell_keys.join(","), t.mailbox_capacity))
        .collect();
    let capacity = tasklets[0].3;
    assert_eq!(
        tasklets,
        [
            ("ohlc".into(), "static".into(), "a,b".into(), capacity),
            ("trades".into(), "dynamic".into(), "".into(), capacity),
            ("atr".into(), "single".into(), "".into(), 64),
            ("read".into(), "single".into(), "".into(), capacity),
        ]
    );
}

#[test]
fn a_graph_carries_each_tasklets_bounds_and_each_channels_retry_and_call() {
    let id = factory("graph_bounds");
    register(
        &id,
        r#"{"name":"in","kind":"entry",
            "chains":[[{"link":"key"},{"link":"into","tasklet":0}],[{"link":"key"},{"link":"into","tasklet":1}]]},
           {"name":"mid","kind":"inter_task","producer":0,"retry_max":5,"retry_backoff_ms":20,
            "chains":[[{"link":"into","tasklet":2}]]},
           {"name":"ask","kind":"plugin","producer":2,
            "chains":[[{"link":"call","factory":"pricing","deadline_ms":2000,"create":true,
                        "response":"ask.response"}]]},
           {"name":"ask.response","kind":"entry","chains":[[{"link":"into","tasklet":2}]]}"#,
        r#"{"name":"evicts","idle_ttl_ms":30000,"max_cells":200},
           {"name":"unbounded"},
           {"name":"single"}"#,
    );
    let graph = advertised_for_factory(&id).graph.expect("graph");

    let tasklets: Vec<_> = graph
        .tasklets
        .iter()
        .map(|t| (t.name.as_str(), t.compensates, t.idle_ttl_ms, t.max_cells))
        .collect();
    assert_eq!(
        tasklets,
        [
            ("evicts", Some(false), Some(30_000), Some(200)),
            ("unbounded", Some(false), None, None),
            ("single", Some(false), None, None),
        ],
        "a flow class compensates nothing; only a dynamic tasklet has bounds"
    );

    let channel = |name: &str| {
        graph
            .channels
            .iter()
            .find(|c| c.name == name)
            .cloned()
            .expect("channel")
    };
    let mid = channel("mid");
    assert_eq!((mid.retry_max, mid.retry_backoff_ms), (Some(5), Some(20)));
    assert_eq!((mid.call_deadline_ms, mid.call_create), (None, None));
    let ask = channel("ask");
    assert_eq!(
        (ask.call_deadline_ms, ask.call_create),
        (Some(2000), Some(true))
    );
}

#[test]
fn a_steps_class_marks_exactly_its_compensating_tasklets() {
    let id = factory("graph_saga");
    let json = format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","durable":false,"run":"steps",
             "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}},
                         {{"name":"s1","kind":"inter_task","producer":0,"chains":[[{{"link":"into","tasklet":1}}]]}},
                         {{"name":"s2","kind":"inter_task","producer":1,"chains":[[{{"link":"into","tasklet":2}}]]}}],
             "tasklets":[{{"name":"reserve","compensate_overridden":true}},
                         {{"name":"place","compensate_overridden":true}},
                         {{"name":"notify"}}]}}"#
    );
    crate::host::register_host_class_json(json.as_bytes()).expect("the class registers");

    let marks: Vec<_> = advertised_for_factory(&id)
        .graph
        .expect("graph")
        .tasklets
        .into_iter()
        .map(|t| (t.name, t.compensates))
        .collect();
    assert_eq!(
        marks,
        [
            ("reserve".to_string(), Some(true)),
            ("place".to_string(), Some(true)),
            ("notify".to_string(), Some(false)),
        ]
    );

    // The connect frame writes the announcement as this JSON.
    let frame = serde_json::to_value(advertised_for_factory(&id)).expect("serialises");
    assert_eq!(frame["declarations"]["durable"], "memory");
    assert_eq!(frame["graph"]["tasklets"][0]["compensates"], true);
    assert_eq!(
        frame["graph"]["tasklets"][0]["max_cells"],
        serde_json::Value::Null
    );
}
