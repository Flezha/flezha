//! Allocation counting for the dispatcher-seam A/B: every allocation event
//! (`alloc`, `alloc_zeroed`, `realloc`) on any thread bumps one of 64
//! cache-padded shards — one atomic on a shared line under eight workers would
//! be the measurement perturbing itself. Read with [`allocations`], reported per
//! element with [`report`].

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

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

pub struct Counting;

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

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Allocation events so far, process-wide.
pub fn allocations() -> u64 {
    COUNTS.iter().map(|s| s.0.load(Ordering::Relaxed)).sum()
}

/// Print the allocation events per element since `before`.
pub fn report(bench: &str, before: u64, elements: u64) {
    let events = allocations().saturating_sub(before);
    eprintln!(
        "[allocs] {bench}: {:.3} allocs/elem ({events} events over {elements} elems)",
        events as f64 / elements as f64
    );
}
