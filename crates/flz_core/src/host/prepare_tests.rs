//! The prepare crossing's answer routed per consumer and Cell: each keyed
//! consumer's Cells receive the values its own edge yielded, once each.

use std::os::unix::net::UnixStream;
use std::sync::Arc;

use flz_transport::transport::FlezhaStream;

use super::activation::host_bytes;
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

/// What each Cell of `tasklet` received, sorted by key: per item its key,
/// what a park of it records, its form, and the shared entry payload. A
/// prepared item ships no payload bytes to its host.
fn received(instance: &RelayInstance, tasklet: usize) -> Vec<Received> {
    let registry = instance.dynamic_registries[tasklet]
        .as_ref()
        .expect("a dynamic consumer has a registry");
    let mut items = Vec::new();
    for entry in registry.cells.iter() {
        let (key, slot) = (entry.key(), entry.value());
        while let Some(item) = slot.mailbox.pop() {
            let CellItem::Input(payload) = &item else {
                panic!("an input was queued");
            };
            let host = payload
                .value
                .downcast_ref::<HostPayload>()
                .expect("a host payload");
            assert!(host.bytes.is_empty(), "a prepared item ships no payload");
            let parked = host_bytes(&item).expect("an input records its payload");
            let shared = host
                .entry
                .clone()
                .expect("a prepared item shares its entry");
            items.push((key.to_string(), parked, host.form.clone(), shared));
        }
    }
    items.sort_by(|a, b| a.0.cmp(&b.0));
    items
}

type Received = (String, Vec<u8>, InputForm, Arc<Vec<u8>>);

/// The received items without their shared entry handle.
fn parts(items: Vec<Received>) -> Vec<(String, Vec<u8>, InputForm)> {
    (items.into_iter())
        .map(|(key, parked, form, _)| (key, parked, form))
        .collect()
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
        parts(received(&instance, 0)),
        vec![
            (
                "a".to_string(),
                b"a:1,a:2,b:3".to_vec(),
                prepared(&[b"1", b"2"])
            ),
            ("b".to_string(), b"a:1,a:2,b:3".to_vec(), prepared(&[b"3"])),
        ],
        "one item per Cell, recording the entry payload and carrying that Cell's values in order"
    );
}

#[test]
fn every_cell_of_one_payload_shares_a_single_copy_of_it() {
    let instance = keyed_consumers("prepared_shared", 1);
    let answered = (0..3).map(|i| value(0, &i.to_string(), b"v")).collect();

    cross(&instance, b"one entry payload", answered);

    let items = received(&instance, 0);
    assert_eq!(items.len(), 3, "one item per Cell");
    assert!(
        (items.iter()).all(|item| Arc::ptr_eq(&item.3, &items[0].3)),
        "the Cells of one payload share one copy of it, never one each"
    );
    assert_eq!(*items[0].3, b"one entry payload".to_vec());
}

#[test]
fn two_keyed_consumers_each_route_by_their_own_edge() {
    let instance = keyed_consumers("prepared_two_edges", 2);
    let answered = vec![value(0, "x", b"for p0"), value(1, "y", b"for p1")];

    cross(&instance, b"payload", answered);

    let form = |v: &[u8]| InputForm::Prepared(vec![v.to_vec()]);
    let p0 = parts(received(&instance, 0));
    let p1 = parts(received(&instance, 1));
    assert_eq!(
        p0,
        vec![("x".to_string(), b"payload".to_vec(), form(b"for p0"))]
    );
    assert_eq!(
        p1,
        vec![("y".to_string(), b"payload".to_vec(), form(b"for p1"))]
    );
}
