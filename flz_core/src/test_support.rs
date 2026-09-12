//! Helpers this crate's test modules share.

use std::os::unix::net::UnixStream;
use std::sync::Arc;
#[cfg(test)]
use std::time::{Duration, Instant};

use flz_transport::transport::FlezhaStream;

#[cfg(test)]
use crate::mailbox::Mailbox;

/// Yield until `done` holds; a 5 s deadline names `what` on failure.
#[cfg(test)]
pub(crate) fn spin_until(what: &str, done: impl Fn() -> bool) {
    let started = Instant::now();
    while !done() {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "timed out waiting for {what}"
        );
        std::thread::yield_now();
    }
}

/// A connected pair: the SDK's end as a `FlezhaStream`, the controller's raw.
pub fn stream_pair() -> (Arc<FlezhaStream>, UnixStream) {
    let (sdk_end, ctrl_end) = UnixStream::pair().expect("UnixStream::pair");
    (Arc::new(FlezhaStream::Unix(sdk_end)), ctrl_end)
}

/// Push `item` and assert the mailbox is now full.
#[cfg(test)]
pub(crate) fn saturate<P: Send + 'static>(mailbox: &Mailbox<P>, item: P) {
    assert!(mailbox.try_push(item).is_ok());
    assert!(mailbox.is_full());
}

/// One park as the controller receives it: the entity identity the frame was
/// addressed to, and the body inside it.
#[cfg(test)]
pub(crate) struct CapturedPark {
    /// `(operative_class, procedure_id, entity_key)` — the triple the
    /// controller keys the row by, and whose middle element it makes a
    /// directory.
    pub identity: (String, String, String),
    pub body: crate::park::ParkBody,
}

/// Every `park_payload` frame the core shipped, decoded and acked as it
/// arrives so the parking thread finishes instead of re-sending for the rest
/// of the run. Hold [`crate::state_sink::test_guard`] around it.
#[cfg(test)]
pub(crate) fn capture_parks() -> Arc<std::sync::Mutex<Vec<CapturedPark>>> {
    let parks = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = Arc::clone(&parks);
    let sink: crate::flusher::SendFrameFn = Arc::new(move |payload_id: &str, frame: &[u8]| {
        if payload_id == crate::state_sink::PARK_PAYLOAD_PAYLOAD_ID {
            let park = decode_park(frame);
            crate::park::complete_ack(&park.body.park_id);
            seen.lock().expect("park capture poisoned").push(park);
        }
        Ok(())
    });
    crate::state_sink::install_global_sink(sink);
    parks
}

/// The identity and `ParkBody` inside a frame `build_entity_frame` wrote:
/// `[id_len(2)][identity][seq(8)][header(16)][body_len(4)][body][crc(4)]`,
/// where the identity is `encode_entity_identity`'s length-prefixed triple.
#[cfg(test)]
fn decode_park(frame: &[u8]) -> CapturedPark {
    let id_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
    let identity = decode_identity(&frame[2..2 + id_len]);
    let rest = &frame[2 + id_len + 8 + 16..];
    let body_len = u32::from_be_bytes(rest[..4].try_into().expect("a 4-byte length")) as usize;
    let body = postcard::from_bytes(&rest[4..4 + body_len]).expect("a postcard ParkBody");
    CapturedPark { identity, body }
}

#[cfg(test)]
fn decode_identity(bytes: &[u8]) -> (String, String, String) {
    let take = |at: usize, len: usize| String::from_utf8_lossy(&bytes[at..at + len]).into_owned();
    let class_len = bytes[0] as usize;
    let proc_at = 1 + class_len + 1;
    let proc_len = bytes[1 + class_len] as usize;
    let key_at = proc_at + proc_len + 2;
    let key_len =
        u16::from_be_bytes([bytes[proc_at + proc_len], bytes[proc_at + proc_len + 1]]) as usize;
    (
        take(1, class_len),
        take(proc_at, proc_len),
        take(key_at, key_len),
    )
}

/// A host class registered from JSON, shared by the command and activation
/// tests.
#[cfg(test)]
pub(crate) mod host_class {
    use std::sync::Arc;

    use crate::operative::instance::StreamInstance;

    /// One entry-channel declaration per name, each chaining into `p0`.
    fn entry_channels(names: &[&str]) -> String {
        names
            .iter()
            .map(|name| {
                format!(
                    r#"{{"name":"{name}","kind":"entry","chains":[[{{"link":"into","procedure":0}}]]}},"#
                )
            })
            .collect()
    }

    /// A tier class: an entry channel per name in `entries`, each into `p0`
    /// — static-keyed by `keys` when there are any — one inter-procedure
    /// channel from `p0` to every consumer in `consumers`, each with a
    /// one-slot mailbox so it is full after a single push. An empty `entries`
    /// declares a class no frame can enter.
    pub(crate) fn instance(
        id: &str,
        entries: &[&str],
        keys: &[&str],
        consumers: &[u32],
    ) -> Arc<StreamInstance> {
        let chain_list = consumers
            .iter()
            .map(|c| format!(r#"[{{"link":"into","procedure":{c}}}]"#))
            .collect::<Vec<_>>()
            .join(",");
        let p0 = if keys.is_empty() {
            r#"{"name":"p0"}"#.to_string()
        } else {
            let key_list = keys
                .iter()
                .map(|k| format!("\"{k}\""))
                .collect::<Vec<_>>()
                .join(",");
            format!(
                r#"{{"name":"p0","entity_mode":"static","entity_keys":[{key_list}],"entities_for_overridden":true}}"#
            )
        };
        let procs = std::iter::once(p0)
            .chain(
                consumers
                    .iter()
                    .map(|c| format!(r#"{{"name":"p{c}","mailbox_capacity":1}}"#)),
            )
            .collect::<Vec<_>>()
            .join(",");
        let entry_json = entry_channels(entries);
        let json = format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","persistent":true,"effectively_once":true,
                 "channels":[{entry_json}
                             {{"name":"mid","kind":"inter_proc","producer":0,
                               "chains":[{chain_list}]}},
                             {{"name":"out","kind":"plugin","producer":0,"keyed":true,
                               "chains":[[{{"link":"sink","name":"out"}}]]}}],
                 "procedures":[{procs}]}}"#
        );
        crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
        StreamInstance::of_class(id, &format!("{id}_inst")).expect("class registered")
    }
}
