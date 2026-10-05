use std::any::Any;
use std::collections::HashMap;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use flz_transport::errors::ErrorModel;
use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;

use super::*;
use crate::cell::CellPayload;
use crate::relay::instance::RelayInstance;

/// The counters are process-wide, so the tests that read them take turns.
fn serial() -> MutexGuard<'static, ()> {
    static GUARD: Mutex<()> = Mutex::new(());
    GUARD.lock().unwrap_or_else(|p| p.into_inner())
}

/// A host class whose one tasklet `p0` ends its output channel in `end`: a
/// sink named `answer`, a call, or an edge into `p1`.
fn slot(id: &str, end: &str, keys: &[&str]) -> Arc<crate::cell::CellSlot> {
    slot_named(id, &format!("{id}_inst"), end, keys)
}

fn slot_named(id: &str, instance: &str, end: &str, keys: &[&str]) -> Arc<crate::cell::CellSlot> {
    let p0 = if keys.is_empty() {
        r#"{"name":"p0"}"#.to_string()
    } else {
        let keys = keys.iter().map(|k| format!("\"{k}\"")).collect::<Vec<_>>();
        format!(
            r#"{{"name":"p0","cell_mode":"static","cell_keys":[{}],"cells_for_overridden":true}}"#,
            keys.join(",")
        )
    };
    let (out, tasklets) = match end {
        "sink" => (r#"{"name":"answer","kind":"plugin","producer":0,"chains":[[{"link":"sink","name":"answer"}]]}"#.to_string(), p0),
        "call" => (r#"{"name":"answer","kind":"plugin","producer":0,"chains":[[{"link":"call","target":"pricing","deadline_ms":500,"response":"answer.response"}]]},
               {"name":"answer.response","kind":"entry","chains":[[{"link":"into","tasklet":0}]]}"#.to_string(), p0),
        _ => (
            r#"{"name":"mid","kind":"inter_task","producer":0,"chains":[[{"link":"into","tasklet":1}]]},
               {"name":"answer","kind":"plugin","producer":1,"chains":[[{"link":"sink","name":"answer"}]]}"#
                .to_string(),
            format!(r#"{p0},{{"name":"p1"}}"#),
        ),
    };
    let json = format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","durable":false,
             "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}},{out}],
             "tasklets":[{tasklets}]}}"#
    );
    crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    let instance = RelayInstance::of_class(id, instance).expect("registered");
    Arc::clone(&instance.cells[0][0])
}

fn work(stream: Arc<FlezhaStream>, attempt: u32, replay: bool) -> ItemAttempt {
    ItemAttempt {
        item: CellItem::Input(CellPayload {
            value: Arc::new(()) as Arc<dyn Any + Send + Sync>,
            stream,
            consumed_offset: Some(3),
            call: None,
            replay,
            forwarded: false,
            hold: None,
        }),
        now: None,
        attempt,
        is_final: false,
    }
}

fn read_part(peer: &mut UnixStream) -> (String, Vec<u8>, HashMap<String, String>) {
    peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    payloader::extract_payload(peer).expect("a part frame")
}

/// End of stream or nothing to read: no byte of a frame reached the peer.
fn assert_nothing_sent(peer: &mut UnixStream) {
    peer.set_nonblocking(true).unwrap();
    let read = std::io::Read::read(peer, &mut [0u8; 1]);
    assert!(
        matches!(read, Ok(0) | Err(_)),
        "no frame leaves the runner: {read:?}"
    );
}

fn counter(name: &str) -> u64 {
    counters()
        .into_iter()
        .find(|(key, _)| *key == name)
        .map(|(_, value)| value)
        .expect("a part counter")
}

#[test]
fn an_attempt_numbers_its_parts_from_zero_and_names_its_cell() {
    let _g = serial();
    let slot = slot("part_numbers", "sink", &[]);
    let (stream, mut peer) = crate::test_support::stream_pair();
    let sent_before = counter("parts_sent");

    let first = work(Arc::clone(&stream), 1, false);
    let mut sequence = 0;
    for value in ["1", "2", "3"] {
        let sent = send(&slot, &first, &mut sequence, value.as_bytes()).expect("sent");
        assert_eq!(sent, Sent::Written);
    }
    for (number, value) in ["1", "2", "3"].iter().enumerate() {
        let (payload_id, payload, metadata) = read_part(&mut peer);
        assert_eq!(payload_id, PART_PAYLOAD_ID);
        assert_eq!(payload, value.as_bytes());
        assert_eq!(metadata["flz.part"], number.to_string());
        assert_eq!(metadata["flz.part_attempt"], "1");
        assert_eq!(metadata["channel"], "answer");
        assert_eq!(metadata["flz.cell"], "part_numbers_inst/p0/");
        assert_eq!(metadata["flz.relay"], "part_numbers_inst");
        assert!(!metadata.contains_key("flz.key"));
    }
    assert_eq!(counter("parts_sent"), sent_before + 3);

    // A retried attempt is a new invocation with a counter of its own.
    let second = work(stream, 2, false);
    let mut sequence = 0;
    send(&slot, &second, &mut sequence, b"1").expect("sent");
    let (_, _, metadata) = read_part(&mut peer);
    assert_eq!(
        (
            metadata["flz.part"].as_str(),
            metadata["flz.part_attempt"].as_str()
        ),
        ("0", "2")
    );
}

#[test]
fn a_keyed_cell_stamps_its_key() {
    let _g = serial();
    let slot = slot("part_keyed", "sink", &["s-7"]);
    let (stream, mut peer) = crate::test_support::stream_pair();
    send(&slot, &work(stream, 1, false), &mut 0, b"x").expect("sent");
    let (_, _, metadata) = read_part(&mut peer);
    assert_eq!(metadata["flz.key"], "s-7");
    assert_eq!(metadata["flz.cell"], "part_keyed_inst/p0/s-7");
}

#[test]
fn a_channel_without_a_sink_refuses_a_part() {
    let _g = serial();
    for (id, end, channel) in [
        ("part_edge", "edge", "mid"),
        ("part_call", "call", "answer"),
    ] {
        let slot = slot(id, end, &[]);
        let (stream, mut peer) = crate::test_support::stream_pair();
        let mut sequence = 0;
        let refusal = send(&slot, &work(stream, 1, false), &mut sequence, b"x")
            .expect_err("only a sink satisfies a part");
        let error = refusal.to_error();
        assert_eq!(error.code, "part.no_output", "{end}");
        assert_eq!(error.details["channel"], channel);
        assert_eq!(error.details["tasklet"], "p0");
        assert_eq!(sequence, 0, "a refused part takes no number");
        assert_nothing_sent(&mut peer);
    }
}

#[test]
fn a_replayed_input_sends_no_part() {
    let _g = serial();
    let slot = slot("part_replay", "sink", &[]);
    let (stream, mut peer) = crate::test_support::stream_pair();
    let sent = send(&slot, &work(stream, 1, true), &mut 0, b"x").expect("accepted");
    assert_eq!(sent, Sent::Replayed);
    assert_nothing_sent(&mut peer);
}

#[test]
fn a_part_with_nowhere_to_go_is_dropped_and_counted() {
    let _g = serial();
    let slot = slot("part_drops", "sink", &[]);

    let local = Arc::new(FlezhaStream::Local(Arc::new(|_: &[u8]| Ok(()))));
    let before = counter("parts_dropped_no_stream");
    let mut sequence = 0;
    assert_eq!(
        send(&slot, &work(local, 1, false), &mut sequence, b"x").unwrap(),
        Sent::NoStream
    );
    assert_eq!(counter("parts_dropped_no_stream"), before + 1);
    assert_eq!(sequence, 1, "a dropped part still takes its number");

    let (stream, _peer) = crate::test_support::stream_pair();
    let before = counter("parts_dropped_too_large");
    let sent = send_within(&slot, &work(stream, 1, false), &mut 0, &[0u8; 64], 16).unwrap();
    assert_eq!(sent, Sent::TooLarge);
    assert_eq!(counter("parts_dropped_too_large"), before + 1);
}

#[test]
fn a_full_stream_refuses_a_part_without_waiting() {
    let _g = serial();
    let slot = slot("part_full", "sink", &[]);
    let (stream, _peer) = crate::test_support::stream_pair();
    let FlezhaStream::Unix(socket) = stream.as_ref() else {
        unreachable!("the pair is a socket")
    };
    socket.set_nonblocking(true).unwrap();
    for size in [4096, 1] {
        while (&*socket).write(&vec![0u8; size]).is_ok() {}
    }
    socket.set_nonblocking(false).unwrap();

    let before = counter("parts_dropped_refused");
    let started = Instant::now();
    let sent = send(&slot, &work(stream, 1, false), &mut 0, b"x").unwrap();
    assert_eq!(sent, Sent::Refused);
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(counter("parts_dropped_refused"), before + 1);
}

#[test]
fn a_context_past_its_invocation_is_refused_with_the_catalogue_code() {
    let error = outside_invocation(Some("p0")).to_error();
    assert_eq!(error.code, "part.outside_invocation");
    assert_eq!(error.class, flz_transport::errors::Class::Invalid);
    assert_eq!(
        flz_transport::errors::entry("part.no_output").map(|e| e.class),
        Some(flz_transport::errors::Class::Invalid)
    );
}

/// The frames `test/fixtures/part_frames.json` pins, which the Elixir SDK
/// produces too: the input `3`, then `retry:2`'s two attempts.
#[test]
fn the_frames_are_the_shared_fixture() {
    let _g = serial();
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../test/fixtures/part_frames.json")).unwrap();
    let slot = slot_named("part_fixture_class", "part_fixture", "sink", &[]);
    let (stream, mut peer) = crate::test_support::stream_pair();
    for (attempt, count) in [(1, 3), (1, 2), (2, 2)] {
        let invocation = work(Arc::clone(&stream), attempt, false);
        let mut sequence = 0;
        for value in 1..=count {
            send(
                &slot,
                &invocation,
                &mut sequence,
                value.to_string().as_bytes(),
            )
            .unwrap();
        }
    }
    for expected in fixture["frames"].as_array().expect("frames") {
        let (_, payload, metadata) = read_part(&mut peer);
        assert_eq!(payload, expected["payload"].as_str().unwrap().as_bytes());
        let expected: HashMap<String, String> =
            serde_json::from_value(expected["metadata"].clone()).unwrap();
        assert_eq!(metadata, expected);
    }
}
