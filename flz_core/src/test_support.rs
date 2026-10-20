//! Helpers this crate's test modules share, and through `test-support` the
//! tests of the crates built on this one.

use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flz_transport::transport::FlezhaStream;
use prost::Message;

#[cfg(test)]
use crate::mailbox::Mailbox;
use crate::pb::control_frame::Frame;
use crate::pb::{CommandResponse, ControlFrame};

/// Poll `done` every `interval` (a yield when zero) until it holds, or `false`
/// once `timeout` has passed. The one polling loop of the Rust tests.
pub fn wait_until(timeout: Duration, interval: Duration, mut done: impl FnMut() -> bool) -> bool {
    let started = Instant::now();
    while !done() {
        if started.elapsed() >= timeout {
            return false;
        }
        if interval.is_zero() {
            std::thread::yield_now();
        } else {
            std::thread::sleep(interval);
        }
    }
    true
}

/// Yield until `done` holds; a 5 s deadline names `what` on failure.
#[cfg(test)]
pub(crate) fn spin_until(what: &str, done: impl Fn() -> bool) {
    assert!(
        wait_until(Duration::from_secs(5), Duration::ZERO, done),
        "timed out waiting for {what}"
    );
}

/// A connected pair: the SDK's end as a `FlezhaStream`, the controller's raw.
pub fn stream_pair() -> (Arc<FlezhaStream>, UnixStream) {
    let (sdk_end, ctrl_end) = UnixStream::pair().expect("UnixStream::pair");
    (Arc::new(FlezhaStream::unix(sdk_end)), ctrl_end)
}

/// An input carrying `ordinal`; the poll core only carries its stream.
pub fn input_item(ordinal: usize, stream: &Arc<FlezhaStream>) -> crate::cell::CellItem {
    let value: Arc<dyn std::any::Any + Send + Sync> = Arc::new(ordinal);
    crate::cell::CellItem::Input(crate::cell::make_cell_payload(&value, stream, None, false))
}

/// Queue one input and claim its round — the producer protocol.
pub fn push_input(
    core: &crate::poll_core::PollCore,
    slot: &Arc<crate::cell::CellSlot>,
    value: usize,
) {
    assert!(slot
        .mailbox
        .try_push(input_item(value, &stream_pair().0))
        .is_ok());
    if slot.mailbox.claim_inflight() {
        core.schedule(slot);
    }
}

/// The response a control payload carries, if it carries one.
pub fn response_in(message: &[u8]) -> Option<CommandResponse> {
    match ControlFrame::decode(message).ok()?.frame? {
        Frame::Response(response) => Some(response),
        Frame::Request(_) => None,
    }
}

/// Push `item` and assert the mailbox is now full.
#[cfg(test)]
pub(crate) fn saturate<P: Send + 'static>(mailbox: &Mailbox<P>, item: P) {
    assert!(mailbox.try_push(item).is_ok());
    assert!(mailbox.is_full());
}

/// One park as the controller receives it: the cell identity the frame was
/// addressed to, and the body inside it.
#[cfg(test)]
pub(crate) struct CapturedPark {
    /// `(relay_class, tasklet_id, cell_key)` — the triple the
    /// controller keys the row by, and whose middle element it makes a
    /// directory.
    pub identity: (String, String, String),
    pub body: crate::park::ParkBody,
}

/// Every `park_payload` frame the core shipped, decoded and acked as it
/// arrives so the park is let go of instead of re-sent for the rest of the
/// run. Hold [`crate::state_sink::test_guard`] around it.
#[cfg(test)]
pub(crate) fn capture_parks() -> Arc<std::sync::Mutex<Vec<CapturedPark>>> {
    capture_parks_acking(true)
}

/// [`capture_parks`] from a controller that acks nothing: every re-send of a
/// park is captured again.
#[cfg(test)]
pub(crate) fn capture_unacked_parks() -> Arc<std::sync::Mutex<Vec<CapturedPark>>> {
    capture_parks_acking(false)
}

#[cfg(test)]
fn capture_parks_acking(ack: bool) -> Arc<std::sync::Mutex<Vec<CapturedPark>>> {
    let parks = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = Arc::clone(&parks);
    let sink: crate::flusher::SendFrameFn = Arc::new(move |payload_id: &str, frame: &[u8]| {
        if payload_id == crate::state_sink::PARK_PAYLOAD_PAYLOAD_ID {
            let park = decode_park(frame);
            if ack {
                crate::park::complete_ack(&park.body.park_id);
            }
            seen.lock().expect("park capture poisoned").push(park);
        }
        Ok(())
    });
    crate::state_sink::install_global_sink(sink);
    parks
}

/// Run the test at path `test` alone in a child process of this test
/// binary, for a test that changes process-wide state: `None` inside that
/// child, the child's output in the parent.
#[cfg(any(test, feature = "bench-internals"))]
pub fn run_alone(test: &str) -> Option<std::process::Output> {
    const CHILD: &str = "FLZ_TEST_ALONE_CHILD";
    if crate::util::env_raw(CHILD).is_some() {
        return None;
    }
    let child = std::process::Command::new(std::env::current_exe().expect("the test binary"))
        .args(["--exact", test, "--nocapture", "--test-threads=1"])
        .env(CHILD, "1")
        .output()
        .expect("the child test runs");
    Some(child)
}

/// [`run_alone`] for the test at path `test`, asserting it passed there:
/// `None` inside the child, the child's stderr in the parent.
#[cfg(any(test, feature = "bench-internals"))]
pub fn passes_alone(test: &str) -> Option<String> {
    let out = run_alone(test)?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        out.status.success() && stdout.contains("1 passed"),
        "{test} failed in its own process:\n{stdout}\n{stderr}"
    );
    Some(stderr)
}

/// The identity and `ParkBody` inside a frame `build_cell_frame` wrote:
/// `[id_len(2)][identity][seq(8)][header(16)][body_len(4)][body][crc(4)]`. The
/// identity is read back with the flusher's own inverse, so a capture cannot
/// disagree with what the controller decodes.
#[cfg(test)]
fn decode_park(frame: &[u8]) -> CapturedPark {
    let id_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
    let identity = crate::flusher::decode_cell_identity(&frame[2..2 + id_len])
        .expect("a park frame carries an encoded cell identity");
    let rest = &frame[2 + id_len + 8 + 16..];
    let body_len = u32::from_be_bytes(rest[..4].try_into().expect("a 4-byte length")) as usize;
    let body = postcard::from_bytes(&rest[4..4 + body_len]).expect("a postcard ParkBody");
    CapturedPark { identity, body }
}

/// A host class registered from JSON, shared by the command and activation
/// tests.
#[cfg(test)]
pub(crate) mod host_class {
    use std::sync::Arc;

    use crate::relay::instance::RelayInstance;

    /// One entry-channel declaration per name, each chaining into `p0`.
    /// An entry channel per name, each into tasklet 0; `facts` are extra
    /// channel keys, written before the chains.
    fn entry_channels(names: &[&str], facts: &str) -> String {
        names
            .iter()
            .map(|name| {
                format!(
                    r#"{{"name":"{name}","kind":"entry",{facts}"chains":[[{{"link":"into","tasklet":0}}]]}},"#
                )
            })
            .collect()
    }

    /// A tier class: an entry channel per name in `entries`, each into `p0`
    /// — static-keyed by `keys` when there are any — one inter-tasklet
    /// channel from `p0` to every consumer in `consumers`, each with a
    /// one-slot mailbox so it is full after a single push. An empty `entries`
    /// declares a class no frame can enter.
    pub(crate) fn instance(
        id: &str,
        entries: &[&str],
        keys: &[&str],
        consumers: &[u32],
    ) -> Arc<RelayInstance> {
        instance_of(id, entries, keys, consumers, true)
    }

    /// [`instance`], on the tier or off it.
    pub(crate) fn instance_of(
        id: &str,
        entries: &[&str],
        keys: &[&str],
        consumers: &[u32],
        tier: bool,
    ) -> Arc<RelayInstance> {
        let chain_list = consumers
            .iter()
            .map(|c| format!(r#"[{{"link":"into","tasklet":{c}}}]"#))
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
            format!(r#"{{"name":"p0","cell_keys":[{key_list}],"cells_for_overridden":true}}"#)
        };
        let tasks = std::iter::once(p0)
            .chain(
                consumers
                    .iter()
                    .map(|c| format!(r#"{{"name":"p{c}","mailbox_capacity":1}}"#)),
            )
            .collect::<Vec<_>>()
            .join(",");
        let entry_json = entry_channels(entries, "");
        let run = if tier {
            r#"{"flow":{"exactly":"effectively_once"}}"#
        } else {
            r#""flow""#
        };
        let json = format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","durable":{{"local":{{"sleep_after":false}}}},"run":{run},
                 "channels":[{entry_json}
                             {{"name":"mid","kind":"inter_task","producer":0,
                               "chains":[{chain_list}]}},
                             {{"name":"out","kind":"plugin","producer":0,"keyed":true,
                               "chains":[[{{"link":"sink","name":"out"}}]]}}],
                 "tasklets":[{tasks}]}}"#
        );
        crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
        RelayInstance::of_class(id, &format!("{id}_inst")).expect("class registered")
    }

    /// A host `once` steps class: an entry channel per name in `entries`,
    /// each into the one tasklet `p0` on an edge whose ladder has `retry_max`
    /// rungs of 1 ms.
    pub(crate) fn steps_json(id: &str, entries: &[&str], retry_max: u32) -> String {
        let entry_json = entry_channels(
            entries,
            &format!(r#""retry_max":{retry_max},"retry_backoff_ms":1,"#),
        );
        format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","durable":false,
                 "channels":[{entry_json}
                             {{"name":"out","kind":"plugin","producer":0,
                               "chains":[[{{"link":"sink","name":"out"}}]]}}],
                 "tasklets":[{{"name":"p0"}}],
                 "run":"steps"}}"#
        )
    }

    /// Bind `id` of `factory` as the controller does, through `AddRelay`;
    /// the refusals it answered.
    pub(crate) fn bind(
        node: &mut crate::runner::RunnerNode,
        factory: &str,
        id: &str,
        init: &str,
        owed: u32,
    ) -> Vec<crate::Error> {
        use crate::command::CommandAction;
        let mut errors = Vec::new();
        crate::pb::AddRelay {
            relay_id: id.to_string(),
            factory_id: factory.to_string(),
            init_values: init.to_string(),
            inbox_tail_count: owed,
            ..Default::default()
        }
        .handle(node, "", &mut errors);
        errors
    }

    /// A host steps instance built as the bind builds it and not yet
    /// opened, so a test reads the messages its run queues.
    pub(crate) fn steps(id: &str, entries: &[&str]) -> Arc<RelayInstance> {
        crate::host::register_host_class_json(steps_json(id, entries, 1).as_bytes())
            .expect("class registers");
        RelayInstance::of_class(id, &format!("{id}_inst")).expect("class registered")
    }
}
