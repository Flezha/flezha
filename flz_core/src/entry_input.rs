//! When an entry channel last received a frame — the one fact only the core
//! has, recorded as one relaxed store per delivery into a cell resolved once
//! at topology build. Deciding whether the silence is anomalous is the host
//! SDK's.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

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

/// One episode boundary an [`IdleWatch`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Episode {
    /// The channel's silence first exceeded the threshold, by this gap.
    Idle(Duration),
    /// A frame ended an episode that had lasted this gap.
    Resumed(Duration),
}

/// One entry channel's episode latch over its observation: the half of idle
/// observation every host shares, so no SDK re-derives the state machine. The
/// tick that drives it and the observer it reports to are the host's — the
/// Rust SDK calls [`evaluate`](Self::evaluate) directly, the C ABI hosts by
/// watch id. Only the caller's tick mutates the latch, so it is
/// single-threaded by construction.
#[derive(Debug)]
pub struct IdleWatch {
    observation: Arc<EntryObservation>,
    threshold: Duration,
    /// The delivery stamp the current episode fired against; `None` while
    /// armed. Comparing stamps (not clock readings) is what makes a resume
    /// exact: the stamp moves if and only if a frame arrived.
    fired_at: Option<u64>,
}

impl IdleWatch {
    /// A latch over `channel`. Legal before the channel's class is declared:
    /// the observation cell is interned by name, so this and the later
    /// topology resolve the same cell.
    pub fn new(channel: &str, threshold: Duration) -> Self {
        Self::over(observation_for(channel), threshold)
    }

    fn over(observation: Arc<EntryObservation>, threshold: Duration) -> Self {
        Self {
            observation,
            threshold,
            fired_at: None,
        }
    }

    pub fn threshold(&self) -> Duration {
        self.threshold
    }

    /// Advance the latch to `now_nanos` and report the boundary it crossed,
    /// if any.
    pub fn evaluate(&mut self, now_nanos: u64) -> Option<Episode> {
        // No frame has ever arrived: a cold start is not an idle episode.
        let last = self.observation.last_delivery_nanos()?;
        match self.fired_at {
            None => {
                let gap = Duration::from_nanos(now_nanos.saturating_sub(last));
                if gap <= self.threshold {
                    return None;
                }
                self.fired_at = Some(last);
                Some(Episode::Idle(gap))
            }
            Some(fired_at) if last != fired_at => {
                self.fired_at = None;
                Some(Episode::Resumed(Duration::from_nanos(
                    last.saturating_sub(fired_at),
                )))
            }
            Some(_) => None,
        }
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

    const SECOND: u64 = 1_000_000_000;

    fn watch(threshold: Duration) -> (IdleWatch, Arc<EntryObservation>) {
        let observation = Arc::new(EntryObservation::default());
        (
            IdleWatch::over(Arc::clone(&observation), threshold),
            observation,
        )
    }

    #[test]
    fn a_cold_start_never_fires_however_long_it_lasts() {
        let (mut watch, _observation) = watch(Duration::from_secs(1));
        assert_eq!(watch.evaluate(3600 * SECOND), None);
    }

    #[test]
    fn an_episode_fires_once_and_a_resume_re_arms_it() {
        let (mut watch, observation) = watch(Duration::from_secs(1));
        observation.stamp_at(SECOND);

        assert_eq!(
            watch.evaluate(SECOND + SECOND / 2),
            None,
            "inside threshold"
        );
        assert_eq!(
            watch.evaluate(3 * SECOND),
            Some(Episode::Idle(Duration::from_secs(2)))
        );
        assert_eq!(watch.evaluate(4 * SECOND), None, "latched for the episode");
        assert_eq!(watch.evaluate(60 * SECOND), None, "still latched");

        observation.stamp_at(60 * SECOND);
        assert_eq!(
            watch.evaluate(61 * SECOND),
            Some(Episode::Resumed(Duration::from_secs(59))),
            "the resume reports the silence that ended"
        );
        assert_eq!(
            watch.evaluate(61 * SECOND),
            None,
            "re-armed, inside threshold"
        );
        assert_eq!(
            watch.evaluate(63 * SECOND),
            Some(Episode::Idle(Duration::from_secs(3))),
            "a later gap fires a second episode"
        );
    }

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
