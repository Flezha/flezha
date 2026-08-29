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
