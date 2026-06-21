use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

/// Per-stream liveness snapshot, rebuilt once per driver-loop iteration.
/// `last_write_ms` / `last_read_ms` are driver-elapsed milliseconds at the
/// last successful socket write / read (0 = never), comparable against
/// `DriverStats::now_ms`.
#[derive(Default, Clone)]
pub struct StreamSnapshot {
    pub write_buffer_len: u64,
    pub write_buffer_dropped: u64,
    pub frames_written: u64,
    pub frames_read: u64,
    pub last_write_ms: u64,
    pub last_read_ms: u64,
}

/// Self-reported liveness of one driver thread. The loop counters are
/// lock-free atomics so a frozen `loop_iterations` (with a stale
/// `last_loop_ms`) unambiguously means the thread is wedged. The per-stream
/// snapshot map is taken under a Mutex once per loop iteration (write) and by
/// the `driver_stats` NIF ~1 Hz (read) — never on a per-frame path.
pub struct DriverStats {
    start: Instant,
    pub loop_iterations: AtomicU64,
    pub last_loop_ms: AtomicU64,
    pub cmd_queue_depth: AtomicU64,
    pub streams: Mutex<HashMap<u64, StreamSnapshot>>,
}

impl DriverStats {
    pub fn new() -> Self {
        DriverStats {
            start: Instant::now(),
            loop_iterations: AtomicU64::new(0),
            last_loop_ms: AtomicU64::new(0),
            cmd_queue_depth: AtomicU64::new(0),
            streams: Mutex::new(HashMap::new()),
        }
    }

    /// Driver-elapsed milliseconds since this stats struct was created.
    pub fn now_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    /// Record the top of a loop iteration: bump the heartbeat counter, stamp
    /// the wall position, and snapshot the command-queue backlog. `now` is the
    /// driver-elapsed ms already computed by the caller for this iteration.
    pub fn mark_loop(&self, now: u64, queue_depth: u64) {
        self.loop_iterations.fetch_add(1, Ordering::Relaxed);
        self.last_loop_ms.store(now, Ordering::Relaxed);
        self.cmd_queue_depth.store(queue_depth, Ordering::Relaxed);
    }

    /// Replace the per-stream snapshot map (called once per iteration).
    pub fn set_streams(&self, snap: HashMap<u64, StreamSnapshot>) {
        if let Ok(mut guard) = self.streams.lock() {
            *guard = snap;
        }
    }
}

impl Default for DriverStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Wire shape returned by the `driver_stats/1` NIF. `now_ms` is the driver's
/// current elapsed clock so Elixir can age `last_*_ms` without a separate
/// cross-process time source.
#[derive(rustler::NifMap)]
pub struct DriverStatsOut {
    pub now_ms: u64,
    pub loop_iterations: u64,
    pub last_loop_ms: u64,
    pub cmd_queue_depth: u64,
    pub streams: Vec<StreamStatsOut>,
}

#[derive(rustler::NifMap)]
pub struct StreamStatsOut {
    pub stream_id: u64,
    pub write_buffer_len: u64,
    pub write_buffer_dropped: u64,
    pub frames_written: u64,
    pub frames_read: u64,
    pub last_write_ms: u64,
    pub last_read_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mark_loop_advances_heartbeat() {
        let stats = DriverStats::new();
        assert_eq!(stats.loop_iterations.load(Ordering::Relaxed), 0);

        stats.mark_loop(100, 3);
        stats.mark_loop(150, 0);

        assert_eq!(stats.loop_iterations.load(Ordering::Relaxed), 2);
        assert_eq!(stats.last_loop_ms.load(Ordering::Relaxed), 150);
        assert_eq!(stats.cmd_queue_depth.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn set_streams_replaces_snapshot() {
        let stats = DriverStats::new();
        let mut snap = HashMap::new();
        snap.insert(
            7,
            StreamSnapshot {
                write_buffer_len: 42,
                write_buffer_dropped: 2,
                frames_written: 5,
                frames_read: 3,
                last_write_ms: 90,
                last_read_ms: 80,
            },
        );
        stats.set_streams(snap);

        let guard = stats.streams.lock().unwrap();
        let s = guard.get(&7).unwrap();
        assert_eq!(s.write_buffer_len, 42);
        assert_eq!(s.write_buffer_dropped, 2);
        assert_eq!(s.frames_written, 5);
        assert_eq!(s.frames_read, 3);
    }

    #[test]
    fn now_ms_is_monotonic() {
        let stats = DriverStats::new();
        let a = stats.now_ms();
        let b = stats.now_ms();
        assert!(b >= a);
    }
}
