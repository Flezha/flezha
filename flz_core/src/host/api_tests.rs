//! A class's API as its runner announces it: the entries `advertised_for_factory`
//! builds from the host's channel facts and the topology, and the schema a
//! registration refuses.

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
        ("api_schema_on_text", r#""codec":"text","schema":{"type":"string"}"#),
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
