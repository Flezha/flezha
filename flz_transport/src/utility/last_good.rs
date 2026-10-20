//! The endpoint that last worked, held for the process lifetime.
//!
//! A reconnect to the controller that already holds a runner's bindings is a
//! binding swap; a reconnect elsewhere is a fresh connect that leaves the
//! previous controller to heal that runner's relays on its own. So the
//! preference is for stability, not balance — and the remaining candidates are
//! shuffled, so a restart that drops a whole fleet does not converge every
//! runner onto one node.
//!
//! Deliberately not persisted: a restarted runner has no bindings to return to.

use std::sync::RwLock;

use crate::utility::noise::{noise, xorshift64};

pub struct LastGood<T>(RwLock<Option<T>>);

impl<T> Default for LastGood<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> LastGood<T> {
    pub const fn new() -> Self {
        LastGood(RwLock::new(None))
    }
}

impl<T: Clone + PartialEq> LastGood<T> {
    /// Record the endpoint a connection was established through.
    pub fn note(&self, value: T) {
        if let Ok(mut slot) = self.0.write() {
            *slot = Some(value);
        }
    }

    pub fn get(&self) -> Option<T> {
        self.0.read().ok().and_then(|slot| slot.clone())
    }

    /// Last-good first, the rest shuffled. A recorded endpoint that is no
    /// longer a candidate is simply not preferred.
    pub fn prefer(&self, mut candidates: Vec<T>) -> Vec<T> {
        let preferred = self.get().and_then(|last| {
            candidates
                .iter()
                .position(|c| *c == last)
                .map(|i| (last, i))
        });

        match preferred {
            Some((last, index)) => {
                candidates.remove(index);
                shuffle(&mut candidates);
                candidates.insert(0, last);
            }
            None => shuffle(&mut candidates),
        }

        candidates
    }

    #[cfg(test)]
    pub fn forget(&self) {
        if let Ok(mut slot) = self.0.write() {
            *slot = None;
        }
    }
}

/// Shuffle in place from the same noise `backoff::jitter` decorrelates
/// timing with — the jitter argument, applied to choice instead of delay.
fn shuffle<T>(items: &mut [T]) {
    let mut state = noise() | 1;
    for i in (1..items.len()).rev() {
        xorshift64(&mut state);
        items.swap(i, (state % (i as u64 + 1)) as usize);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reconnect_returns_to_the_endpoint_that_worked() {
        let slot: LastGood<u8> = LastGood::new();
        let candidates = vec![1, 2, 3, 4];

        // No preference yet: every candidate is still offered.
        let fresh = slot.prefer(candidates.clone());
        assert_eq!(fresh.len(), 4);
        for candidate in &candidates {
            assert!(fresh.contains(candidate));
        }

        slot.note(3);
        let reconnect = slot.prefer(candidates.clone());
        assert_eq!(reconnect[0], 3);
        assert_eq!(reconnect.len(), 4);

        // A recorded endpoint the list stopped offering must not be dialled,
        // and must not drop the others.
        slot.note(9);
        let moved = slot.prefer(candidates.clone());
        assert_eq!(moved.len(), 4);
        assert!(!moved.contains(&9));
    }
}
