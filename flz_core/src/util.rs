//! Small shared helpers (env parsing, lock poison recovery, big-endian
//! reads, the monotonic anchor, nonces). Not a stable public surface — exported for
//! the SDK's typed layer, hidden from docs.

use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime};

/// The process's monotonic origin. One anchor for every "how long ago" the
/// runtime records, so a stamp taken by one subsystem is comparable with a
/// reading taken by another.
static MONOTONIC_ANCHOR: LazyLock<Instant> = LazyLock::new(Instant::now);

/// The [`Instant`] every monotonic stamp in this process is measured from.
/// Turns a stored offset back into an `Instant` for the SDK-facing accessors.
pub fn monotonic_anchor() -> Instant {
    *MONOTONIC_ANCHOR
}

/// Nanoseconds since [`monotonic_anchor`]. `u64` covers 584 years, so the
/// wrap this narrowing could suffer is not reachable.
pub fn monotonic_nanos() -> u64 {
    MONOTONIC_ANCHOR.elapsed().as_nanos() as u64
}

/// Milliseconds since [`monotonic_anchor`]. Stamped on every push into a
/// dynamic Entity's mailbox and read by the reaper; signed because the
/// `AtomicI64` it feeds is.
pub fn monotonic_ms() -> i64 {
    MONOTONIC_ANCHOR.elapsed().as_millis() as i64
}

/// Wake granularity of [`sleep_interruptible`]: how promptly a tripped flag is
/// observed inside a longer sleep.
const SLEEP_POLL_CHUNK: Duration = Duration::from_millis(200);

/// Sleep for `duration`, waking every [`SLEEP_POLL_CHUNK`] so a tripped `stop`
/// flag (a cooperative cancel, a body that already finished) ends the wait
/// promptly. `true` when the full duration elapsed, `false` when `stop` cut it
/// short. `None` sleeps it out uninterruptibly.
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

/// Parse an env var as `T`, falling back to `default` when unset or
/// unparseable.
pub fn env_parse<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// A default-OFF boolean knob: set and not spelled as a negative means on.
/// Unset, empty or an explicit negative means off.
pub fn env_flag(key: &str) -> bool {
    std::env::var(key).is_ok_and(|v| {
        !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "" | "0" | "false" | "off" | "no"
        )
    })
}

/// Like [`env_parse`] for `usize`, additionally rejecting `0` (treated as
/// "unset") so a `0` override falls back to `default`.
pub fn env_parse_positive(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

/// Nanoseconds since the Unix epoch, `0` if the clock is before it. Used
/// only to make reference ids / nonces unique, never for ordering.
pub fn nonce_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// Read a big-endian `u32` at `off`. Caller must ensure `off + 4 <= buf.len()`.
pub fn read_u32_be(buf: &[u8], off: usize) -> u32 {
    u32::from_be_bytes(buf[off..off + 4].try_into().unwrap())
}

/// Read a big-endian `u64` at `off`. Caller must ensure `off + 8 <= buf.len()`.
pub fn read_u64_be(buf: &[u8], off: usize) -> u64 {
    u64::from_be_bytes(buf[off..off + 8].try_into().unwrap())
}

/// Lock a `Mutex`, recovering the guard if a previous holder panicked.
/// The data behind these locks is always left consistent on panic, so
/// proceeding with the poisoned guard is correct and avoids a cascade.
pub trait LockRecover<T> {
    fn lock_recover(&self) -> MutexGuard<'_, T>;
}

impl<T> LockRecover<T> for Mutex<T> {
    fn lock_recover(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Render a contained panic payload as the error string ladders and abort
/// paths report.
pub fn format_panic(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// Wall time as milliseconds since the Unix epoch. The dispatcher's wall
/// source; the test harness injects fixed/scripted values by supplying
/// its own `wall_ms` closure to [`ClockFor::evaluate`] instead.
pub fn wall_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
