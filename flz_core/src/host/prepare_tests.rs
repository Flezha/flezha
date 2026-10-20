//! The prepare crossing's answer routed per consumer and Cell: each keyed
//! consumer's Cells receive the values its own edge yielded, once each.

use std::os::unix::net::UnixStream;
use std::sync::Arc;

use flz_transport::transport::FlezhaStream;

use super::prepare::{apply_prepare_outcome, PrepareCrossing, StagedEntry};
use super::HostPayload;
use crate::cell::CellItem;
use crate::relay::instance::RelayInstance;
use crate::schema::{ActivationOutcome, InputForm, PrepareAnswer, PreparedValue};

/// A host class whose entry `in` feeds one dynamic tasklet per chain.
fn keyed_consumers(id: &str, consumers: usize) -> Arc<RelayInstance> {
    let chains: Vec<String> = (0..consumers)
        .map(|t| format!(r#"[{{"link":"key"}},{{"link":"into","tasklet":{t}}}]"#))
        .collect();
    let tasklets: Vec<String> = (0..consumers)
        .map(|t| format!(r#"{{"name":"p{t}"}}"#))
        .collect();
    let json = format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0",
             "channels":[{{"name":"in","kind":"entry","chains":[{}]}}],
             "tasklets":[{}]}}"#,
        chains.join(","),
        tasklets.join(",")
    );
    crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    RelayInstance::of_class(id, &format!("{id}_inst")).expect("class registered")
}

fn value(consumer: u32, key: &str, bytes: &[u8]) -> PreparedValue {
    PreparedValue {
        consumer,
        keys: vec![key.to_string()],
        bytes: bytes.to_vec(),
    }
}

/// Cross `payload` on channel 0 and apply `values` as the host's answer.
fn cross(instance: &Arc<RelayInstance>, payload: &[u8], values: Vec<PreparedValue>) {
    let (near, far) = UnixStream::pair().expect("a socket pair");
    drop(far);
    let crossing = PrepareCrossing {
        instance: Arc::clone(instance),
        channel: 0,
        want_keys: true,
        want_event_time: false,
        entries: vec![StagedEntry {
            bytes: payload.to_vec(),
            consumed_offset: None,
            call: None,
            replay: false,
            stream: Arc::new(FlezhaStream::unix(near)),
            stage: None,
        }],
        reply: None,
    };
    let answer = PrepareAnswer {
        values,
        event_time: None,
    };
    apply_prepare_outcome(crossing, ActivationOutcome::Prepared(vec![Ok(answer)]))
        .expect("the crossing applies");
}

/// What each Cell of `tasklet` received, `(key, payload, form)` per item,
/// sorted by key.
fn received(instance: &RelayInstance, tasklet: usize) -> Vec<(String, Vec<u8>, InputForm)> {
    let registry = instance.dynamic_registries[tasklet]
        .as_ref()
        .expect("a dynamic consumer has a registry");
    let mut items = Vec::new();
    for entry in registry.cells.iter() {
        let (key, slot) = (entry.key(), entry.value());
        while let Some(item) = slot.mailbox.pop() {
            let CellItem::Input(payload) = item else {
                panic!("an input was queued");
            };
            let host = payload
                .value
                .downcast_ref::<HostPayload>()
                .expect("a host payload");
            items.push((key.to_string(), host.bytes.clone(), host.form.clone()));
        }
    }
    items.sort_by(|a, b| a.0.cmp(&b.0));
    items
}

#[test]
fn a_keyed_entry_payload_reaches_each_cell_once_with_its_own_values() {
    let instance = keyed_consumers("prepared_once", 1);
    let answered = vec![
        value(0, "a", b"1"),
        value(0, "a", b"2"),
        value(0, "b", b"3"),
    ];

    cross(&instance, b"a:1,a:2,b:3", answered);

    let prepared =
        |values: &[&[u8]]| InputForm::Prepared(values.iter().map(|v| v.to_vec()).collect());
    assert_eq!(
        received(&instance, 0),
        vec![
            (
                "a".to_string(),
                b"a:1,a:2,b:3".to_vec(),
                prepared(&[b"1", b"2"])
            ),
            ("b".to_string(), b"a:1,a:2,b:3".to_vec(), prepared(&[b"3"])),
        ],
        "one item per Cell, carrying the entry payload and that Cell's values in order"
    );
}

#[test]
fn two_keyed_consumers_each_route_by_their_own_edge() {
    let instance = keyed_consumers("prepared_two_edges", 2);
    let answered = vec![value(0, "x", b"for p0"), value(1, "y", b"for p1")];

    cross(&instance, b"payload", answered);

    let form = |v: &[u8]| InputForm::Prepared(vec![v.to_vec()]);
    let p0 = received(&instance, 0);
    let p1 = received(&instance, 1);
    assert_eq!(
        p0,
        vec![("x".to_string(), b"payload".to_vec(), form(b"for p0"))]
    );
    assert_eq!(
        p1,
        vec![("y".to_string(), b"payload".to_vec(), form(b"for p1"))]
    );
}
