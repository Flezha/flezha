//! The monotonic anchor, env parsing, lock poison recovery, big-endian reads
//! and nonces; not a stable surface.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime};

/// One origin for every monotonic stamp in the process.
static MONOTONIC_ANCHOR: LazyLock<Instant> = LazyLock::new(Instant::now);

/// The [`Instant`] every monotonic stamp is measured from.
pub fn monotonic_anchor() -> Instant {
    *MONOTONIC_ANCHOR
}

/// Nanoseconds since [`monotonic_anchor`].
pub fn monotonic_nanos() -> u64 {
    MONOTONIC_ANCHOR.elapsed().as_nanos() as u64
}

/// Milliseconds since [`monotonic_anchor`]; signed as the atomic it feeds.
pub fn monotonic_ms() -> i64 {
    MONOTONIC_ANCHOR.elapsed().as_millis() as i64
}

const SLEEP_POLL_CHUNK: Duration = Duration::from_millis(200);

/// Sleep `duration`, waking every [`SLEEP_POLL_CHUNK`] to observe `stop`;
/// `false` when `stop` cut it short.
pub fn sleep_interruptible(duration: Duration, stop: Option<&AtomicBool>) -> bool {
    let deadline = Instant::now() + duration;
    loop {
        if stop.is_some_and(|flag| flag.load(Ordering::Acquire)) {
            return false;
        }
        let now = Instant::now();
        if now >= deadline {
            return true;
        }
        std::thread::sleep(SLEEP_POLL_CHUNK.min(deadline - now));
    }
}

/// The environment readers, re-exported so a crate above this one reaches one
/// parse rule per type without naming the transport crate for it.
pub use flz_transport::env::{
    env_bool, env_dir, env_ms, env_parse, env_parse_positive, env_raw, env_string,
};

/// For uniqueness only, never for ordering. The clock alone repeats for two
/// threads reading it in the same nanosecond, so a process-wide count fills
/// the low bits.
pub fn nonce_suffix() -> u128 {
    static COUNT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let count = COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    (nanos << 32) | u128::from(count)
}

/// `bytes` as lowercase hex, two digits a byte.
pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

pub fn read_u32_be(buf: &[u8], off: usize) -> u32 {
    u32::from_be_bytes(buf[off..off + 4].try_into().unwrap())
}

pub fn read_u64_be(buf: &[u8], off: usize) -> u64 {
    u64::from_be_bytes(buf[off..off + 8].try_into().unwrap())
}

/// Lock, recovering a poisoned guard: the data behind these locks is
/// consistent on panic.
pub trait LockRecover<T> {
    fn lock_recover(&self) -> MutexGuard<'_, T>;
}

impl<T> LockRecover<T> for Mutex<T> {
    fn lock_recover(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

pub fn format_panic(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// Wall time as milliseconds since the Unix epoch.
pub fn wall_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
