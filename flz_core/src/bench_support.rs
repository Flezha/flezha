//! What a dispatch bench needs of the core, so the erased-path bench in
//! `flz_core_ffi` and the typed benches in `sdk/rust` share one scaffolding.
//! Not a stable surface.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashMap;
use std::io::Read;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use flz_transport::transport::FlezhaStream;

use crate::runner::RelayDispatchHandle;
use crate::schema::ItemEffect;
pub use crate::test_support::stream_pair;
use crate::util::LockRecover;

/// What a host answers for an item it processed with no effect.
pub fn ok_effect() -> ItemEffect {
    crate::host::empty_effect()
}

/// `FLEZHA_BENCH_MEASUREMENT_SECS`, or `default` when it is unset — each row
/// keeps the window it measures.
pub fn measurement_secs_default(default: u64) -> u64 {
    crate::util::env_parse("FLEZHA_BENCH_MEASUREMENT_SECS", default)
}

/// `FLEZHA_BENCH_MEASUREMENT_SECS`, default 30.
pub fn measurement_secs() -> u64 {
    measurement_secs_default(30)
}

/// A [`stream_pair`] whose controller end a reader thread drains, so emits on
/// the SDK end never block on a full socket buffer.
pub fn drained_pair() -> Arc<FlezhaStream> {
    let (sdk_end, mut ctrl_end) = stream_pair();
    std::thread::spawn(move || {
        let mut buf = [0u8; 65536];
        while ctrl_end
            .read(&mut buf)
            .map(|read| read > 0)
            .unwrap_or(false)
        {}
    });
    sdk_end
}

/// `per_id` items into every id's `input` channel, each under a fresh metadata
/// map — the shape every regression twin measures. `payloads` is round-robined
/// by the item's index within its id (one element feeds them all), and
/// `index_mark`, when given, stamps that index under its own key.
pub fn fan_in(
    handle: &RelayDispatchHandle,
    ids: &[String],
    per_id: usize,
    payloads: &[&[u8]],
    index_mark: Option<&str>,
    stream: &Arc<FlezhaStream>,
) {
    for id in ids {
        let mut cursor = 0usize;
        for index in 0..per_id {
            let mut md = HashMap::new();
            md.insert("channel".to_string(), "input".to_string());
            if let Some(key) = index_mark {
                md.insert(key.to_string(), index.to_string());
            }
            handle.dispatch(id, payloads[cursor].to_vec(), &md, Arc::clone(stream));
            cursor += 1;
            if cursor == payloads.len() {
                cursor = 0;
            }
        }
    }
}

/// A count the workers advance and the bench waits on: a condvar wait with a
/// deadline, never a polling sleep. [`add`](Settle::add) runs once per
/// processed item — millions of times per measured iteration — so it is one
/// atomic increment plus one predictable branch, and reaches the mutex only
/// when the count could cross a parked waiter's total.
pub struct Settle {
    count: AtomicUsize,
    /// The smallest total any parked waiter waits for; `usize::MAX` when none.
    awaited: AtomicUsize,
    waiters: AtomicUsize,
    guard: Mutex<()>,
    reached: Condvar,
}

impl Settle {
    pub const fn new() -> Self {
        Settle {
            count: AtomicUsize::new(0),
            awaited: AtomicUsize::new(usize::MAX),
            waiters: AtomicUsize::new(0),
            guard: Mutex::new(()),
            reached: Condvar::new(),
        }
    }

    pub fn reset(&self) {
        let _guard = self.guard.lock_recover();
        self.count.store(0, Ordering::SeqCst);
    }

    pub fn add(&self, n: usize) {
        // `SeqCst` on both halves of this store-then-load and of `wait_for`'s:
        // whichever runs second in the total order observes the other, so a
        // waiter that parks is always woken.
        let reached = self.count.fetch_add(n, Ordering::SeqCst) + n;
        if reached >= self.awaited.load(Ordering::SeqCst) {
            let _guard = self.guard.lock_recover();
            self.reached.notify_all();
        }
    }

    pub fn current(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    /// `true` once the count reaches `total`, `false` when `timeout` elapsed
    /// first.
    pub fn wait_for(&self, total: usize, timeout: Duration) -> bool {
        if self.current() >= total {
            return true;
        }
        self.waiters.fetch_add(1, Ordering::SeqCst);
        self.awaited.fetch_min(total, Ordering::SeqCst);
        let guard = self.guard.lock_recover();
        let held = self
            .reached
            .wait_timeout_while(guard, timeout, |_| self.current() < total)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        drop(held);
        if self.waiters.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.awaited.store(usize::MAX, Ordering::SeqCst);
        }
        self.current() >= total
    }
}

impl Default for Settle {
    fn default() -> Self {
        Self::new()
    }
}

/// Every allocation event (`alloc`, `alloc_zeroed`, `realloc`) on any thread
/// bumps one of 64 cache-padded shards — one atomic on a shared line under
/// eight workers would be the measurement perturbing itself. Read with
/// [`allocations`], reported per element with [`report`]. Each bench binary
/// declares its own `#[global_allocator]` static over this type: there may be
/// at most one per binary.
pub struct Counting;

#[repr(align(128))]
struct Shard(AtomicU64);

const SHARDS: usize = 64;
static COUNTS: [Shard; SHARDS] = [const { Shard(AtomicU64::new(0)) }; SHARDS];
static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static MY_SHARD: Cell<usize> = const { Cell::new(usize::MAX) };
}

fn bump() {
    let idx = MY_SHARD.with(|shard| {
        let current = shard.get();
        if current != usize::MAX {
            return current;
        }
        let assigned = NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % SHARDS;
        shard.set(assigned);
        assigned
    });
    COUNTS[idx].0.fetch_add(1, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        bump();
        System.alloc(layout)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        bump();
        System.alloc_zeroed(layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        bump();
        System.realloc(ptr, layout, new_size)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}

/// Allocation events so far, process-wide.
pub fn allocations() -> u64 {
    COUNTS
        .iter()
        .map(|shard| shard.0.load(Ordering::Relaxed))
        .sum()
}

/// Print the allocation events per element since `before`.
pub fn report(bench: &str, before: u64, elements: u64) {
    let events = allocations().saturating_sub(before);
    eprintln!(
        "[allocs] {bench}: {:.3} allocs/elem ({events} events over {elements} elems)",
        events as f64 / elements as f64
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_settle_already_past_its_total_never_parks() {
        let settle = Settle::new();
        settle.add(7);
        assert!(settle.wait_for(5, Duration::from_millis(0)));
        assert_eq!(settle.current(), 7);
    }

    #[test]
    fn a_settle_wakes_the_waiter_that_registered_before_the_crossing() {
        let settle = Arc::new(Settle::new());
        let producer = Arc::clone(&settle);
        std::thread::spawn(move || {
            for _ in 0..1_000 {
                producer.add(1);
            }
        });
        assert!(settle.wait_for(1_000, Duration::from_secs(5)));
        assert!(settle.wait_for(1_000, Duration::from_secs(0)));
        settle.reset();
        assert_eq!(settle.current(), 0);
        assert!(!settle.wait_for(1, Duration::from_millis(20)));
    }
}
