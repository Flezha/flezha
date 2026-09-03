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

/// A host class registered from JSON, shared by the command and activation
/// tests.
#[cfg(test)]
pub(crate) mod host_class {
    use std::sync::Arc;

    use crate::operative::instance::StreamInstance;

    /// A tier class: one entry channel into `p0` — static-keyed by `keys`
    /// when there are any — one inter-procedure channel from `p0` to every
    /// consumer in `consumers`, each with a one-slot mailbox so it is full
    /// after a single push.
    pub(crate) fn instance(id: &str, keys: &[&str], consumers: &[u32]) -> Arc<StreamInstance> {
        let consumer_list = consumers
            .iter()
            .map(|c| c.to_string())
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
            format!(r#"{{"name":"p0","entity_mode":"static","entity_keys":[{key_list}]}}"#)
        };
        let procs = std::iter::once(p0)
            .chain(
                consumers
                    .iter()
                    .map(|c| format!(r#"{{"name":"p{c}","mailbox_capacity":1}}"#)),
            )
            .collect::<Vec<_>>()
            .join(",");
        let json = format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","persistent":true,"effectively_once":true,
                 "channels":[{{"name":"in","kind":"entry","consumers":[0]}},
                             {{"name":"mid","kind":"inter_proc","producer":0,
                               "consumers":[{consumer_list}]}},
                             {{"name":"out","kind":"plugin","producer":0,"keyed":true}}],
                 "procedures":[{procs}]}}"#
        );
        crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
        let topology = crate::operative::registry::get_class(id).expect("class registered");
        StreamInstance::new(topology, Arc::from(format!("{id}_inst").as_str()))
    }
}
