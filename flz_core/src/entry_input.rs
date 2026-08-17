//! Entry-input observability: when did an entry channel last receive a frame.
//!
//! The SDK owns the IO threads, so application code never sees the data
//! stream — when input goes silent every procedure stops, output freezes, and
//! nothing outside the core knows why. This module records the one fact only
//! the core has: the instant a frame was last handed to each entry channel.
//! Deciding whether that silence is anomalous is the application's, and lives
//! in the host SDKs (Rust `entry_idle`, Java `EntryIdleWatcher`).
//!
//! The cell is resolved once, at topology build, and cached on the channel's
//! [`ChannelDescriptor`](crate::channel::ChannelDescriptor), so the delivery
//! path pays one relaxed store with no lookup, no lock and no allocation.
//! Readers reach the same cell by channel name.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use dashmap::DashMap;

/// One entry channel's delivery observation. Interned by channel name, so the
/// descriptor that stamps it and the observer that reads it share one cell.
#[derive(Debug, Default)]
pub struct EntryObservation {
    /// Nanoseconds since [`crate::util::monotonic_anchor`] at the last
    /// delivery; `0` means no frame has ever been delivered.
    last_delivery: AtomicU64,
}

impl EntryObservation {
    /// Record a delivery. The whole hot-path cost of the feature: one relaxed
    /// store, called with no lock held.
    #[inline]
    pub fn stamp(&self) {
        self.stamp_at(crate::util::monotonic_nanos());
    }

    /// [`stamp`](Self::stamp) at a chosen point on the monotonic clock — the
    /// seam the host SDKs' episode tests drive instead of waiting on a clock.
    #[doc(hidden)]
    pub fn stamp_at(&self, nanos: u64) {
        // `0` is the never-delivered sentinel, so a delivery landing in the
        // first nanosecond of the process must not read as "never".
        self.last_delivery.store(nanos.max(1), Ordering::Relaxed);
    }

    /// Nanoseconds since the monotonic anchor at the last delivery, or `None`
    /// before the first frame.
    pub fn last_delivery_nanos(&self) -> Option<u64> {
        match self.last_delivery.load(Ordering::Relaxed) {
            0 => None,
            nanos => Some(nanos),
        }
    }

    /// The last delivery as an [`Instant`], or `None` before the first frame.
    pub fn last_delivery(&self) -> Option<Instant> {
        self.last_delivery_nanos()
            .map(|nanos| crate::util::monotonic_anchor() + std::time::Duration::from_nanos(nanos))
    }
}

/// Every entry channel mentioned so far, by name. A channel name is an
/// operative's external address, so one name is one observed input regardless
/// of how many classes declare it.
static OBSERVATIONS: LazyLock<DashMap<String, Arc<EntryObservation>>> = LazyLock::new(DashMap::new);

/// The observation cell for `channel`, created on first mention. Called at
/// topology build (which caches the `Arc` on the descriptor) and by an
/// observer registered before any class is defined — both reach one cell.
pub fn observation_for(channel: &str) -> Arc<EntryObservation> {
    Arc::clone(OBSERVATIONS.entry(channel.to_owned()).or_default().value())
}

/// The observation cell for `channel` if anything has ever mentioned it.
/// Reads never intern: a typo stays absent instead of growing the map.
pub fn observation_of(channel: &str) -> Option<Arc<EntryObservation>> {
    OBSERVATIONS
        .get(channel)
        .map(|cell| Arc::clone(cell.value()))
}

/// When `channel` last received a frame, or `None` before its first frame —
/// and for a channel no declared operative has (the two are the same answer
/// to "has anything arrived here").
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
