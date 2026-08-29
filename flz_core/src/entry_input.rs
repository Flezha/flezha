//! When an entry channel last received a frame — the one fact only the core
//! has, recorded as one relaxed store per delivery into a cell resolved once
//! at topology build. Deciding whether the silence is anomalous is the host
//! SDK's.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use dashmap::DashMap;

/// One entry channel's delivery observation, interned by name so the stamp
/// and the observer share one cell.
#[derive(Debug, Default)]
pub struct EntryObservation {
    /// Nanoseconds since the monotonic anchor; `0` before the first frame.
    last_delivery: AtomicU64,
}

impl EntryObservation {
    /// One relaxed store, with no lock held.
    #[inline]
    pub fn stamp(&self) {
        self.stamp_at(crate::util::monotonic_nanos());
    }

    /// [`stamp`](Self::stamp) at a chosen instant, for the host SDKs' tests.
    #[doc(hidden)]
    pub fn stamp_at(&self, nanos: u64) {
        // `0` is the never-delivered sentinel, so a delivery landing in the
        // first nanosecond of the process must not read as "never".
        self.last_delivery.store(nanos.max(1), Ordering::Relaxed);
    }

    pub fn last_delivery_nanos(&self) -> Option<u64> {
        match self.last_delivery.load(Ordering::Relaxed) {
            0 => None,
            nanos => Some(nanos),
        }
    }

    pub fn last_delivery(&self) -> Option<Instant> {
        self.last_delivery_nanos()
            .map(|nanos| crate::util::monotonic_anchor() + std::time::Duration::from_nanos(nanos))
    }
}

/// Every entry channel mentioned so far, by name: one name is one observed
/// input however many classes declare it.
static OBSERVATIONS: LazyLock<DashMap<String, Arc<EntryObservation>>> = LazyLock::new(DashMap::new);

/// The cell for `channel`, created on first mention.
pub fn observation_for(channel: &str) -> Arc<EntryObservation> {
    Arc::clone(OBSERVATIONS.entry(channel.to_owned()).or_default().value())
}

/// The cell for `channel` if anything mentioned it; a read never interns.
pub fn observation_of(channel: &str) -> Option<Arc<EntryObservation>> {
    OBSERVATIONS
        .get(channel)
        .map(|cell| Arc::clone(cell.value()))
}

/// When `channel` last received a frame; `None` before its first, and for a
/// channel nothing declares.
pub fn last_delivery(channel: &str) -> Option<Instant> {
    observation_of(channel).and_then(|cell| cell.last_delivery())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cell_is_absent_until_a_delivery_stamps_it() {
        let cell = observation_for("entry_input_tests::first");
        assert_eq!(cell.last_delivery(), None);
        assert_eq!(last_delivery("entry_input_tests::first"), None);

        let before = Instant::now();
        cell.stamp();
        let stamped = cell.last_delivery().expect("stamped");
        assert!(stamped >= before, "the stamp is at or after the delivery");
        assert_eq!(last_delivery("entry_input_tests::first"), Some(stamped));
    }

    #[test]
    fn one_name_is_one_cell_and_an_unmentioned_name_is_absent() {
        let a = observation_for("entry_input_tests::shared");
        let b = observation_for("entry_input_tests::shared");
        assert!(Arc::ptr_eq(&a, &b));

        assert!(observation_of("entry_input_tests::never_mentioned").is_none());
        assert!(last_delivery("entry_input_tests::never_mentioned").is_none());
        // A read must not intern the name it failed to find.
        assert!(!OBSERVATIONS.contains_key("entry_input_tests::never_mentioned"));
    }
}
