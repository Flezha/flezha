//! The monotonic anchor, env parsing, lock poison recovery, big-endian reads
//! and nonces; not a stable surface.

use std::str::FromStr;
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

pub fn env_parse<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// A default-OFF knob: set and not a negative spelling means on.
pub fn env_flag(key: &str) -> bool {
    std::env::var(key).is_ok_and(|v| {
        !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "" | "0" | "false" | "off" | "no"
        )
    })
}

/// [`env_parse`] for `usize`, with `0` read as unset.
pub fn env_parse_positive(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

/// For uniqueness only, never for ordering.
pub fn nonce_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
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
