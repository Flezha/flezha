//! What a dispatch bench needs of the core, so the erased-path bench in
//! `flz_core_ffi` and the typed benches in `sdk/rust` share one scaffolding.
//! Not a stable surface.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use flz_transport::transport::FlezhaStream;

use crate::runner::OperativeDispatchHandle;
use crate::schema::ItemEffect;
pub use crate::test_support::stream_pair;
use crate::util::LockRecover;

/// What a host answers for an item it processed with no effect.
pub fn ok_effect() -> ItemEffect {
    crate::host::empty_effect()
}

/// `FLEZHA_BENCH_MEASUREMENT_SECS`, default 30.
pub fn measurement_secs() -> u64 {
    std::env::var("FLEZHA_BENCH_MEASUREMENT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30)
}

/// `per_id` copies of `payload` into every instance's `input` channel, each
/// under a fresh metadata map — the shape both regression twins measure.
pub fn fan_in(
    handle: &OperativeDispatchHandle,
    ids: &[String],
    per_id: usize,
    payload: &[u8],
    stream: &Arc<FlezhaStream>,
) {
    for id in ids {
        for _ in 0..per_id {
            let mut md = HashMap::new();
            md.insert("channel".to_string(), "input".to_string());
            handle.dispatch(id, payload.to_vec(), &md, Arc::clone(stream));
        }
    }
}

/// A count the workers advance and the bench waits on: a condvar wait
/// with a deadline, never a polling sleep.
pub struct Settle {
    count: Mutex<usize>,
    reached: Condvar,
}

impl Settle {
    pub const fn new() -> Self {
        Settle {
            count: Mutex::new(0),
            reached: Condvar::new(),
        }
    }

    pub fn reset(&self) {
        *self.count.lock_recover() = 0;
    }

    pub fn add(&self, n: usize) {
        *self.count.lock_recover() += n;
        self.reached.notify_all();
    }

    pub fn current(&self) -> usize {
        *self.count.lock_recover()
    }

    /// `true` once the count reaches `total`, `false` when `timeout` elapsed
    /// first.
    pub fn wait_for(&self, total: usize, timeout: Duration) -> bool {
        let guard = self.count.lock_recover();
        let (guard, _) = self
            .reached
            .wait_timeout_while(guard, timeout, |count| *count < total)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard >= total
    }
}

impl Default for Settle {
    fn default() -> Self {
        Self::new()
    }
}
