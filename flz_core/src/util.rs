//! Small shared helpers (env parsing, lock poison recovery, big-endian
//! reads, monotonic-ish nonces). Not a stable public surface — exported for
//! the SDK's typed layer, hidden from docs.

use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime};

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

#[cfg(test)]
mod partition_hash_tests {
    //! The routing guard and the controller's placement must agree bit for
    //! bit: a one-bit drift parks every frame of a partitioned operative. The
    //! twin assertions live in `Flezha.PartitionTest` and `StableIdTest`.

    use super::{fnv1a128, key_group, FNV128_OFFSET_BASIS};

    #[test]
    fn key_group_matches_the_cross_language_fixture() {
        assert_eq!(key_group("BTC-USD"), 129);
        assert_eq!(key_group("ETH-USD"), 213);
        assert_eq!(key_group("SOL-USD"), 60);
        assert_eq!(key_group(""), 141);
        assert_eq!(key_group("a"), 100);
    }

    #[test]
    fn key_group_is_the_digest_low_byte() {
        for key in ["BTC-USD", "", "a", &"x".repeat(512)] {
            let digest = fnv1a128(FNV128_OFFSET_BASIS, key.as_bytes());
            assert_eq!(key_group(key) as u128, digest % 256);
            assert!(key_group(key) < 256);
        }
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

/// FNV-1a-128 offset basis, frozen by the cross-language `StableId` spec.
pub const FNV128_OFFSET_BASIS: u128 = 0x6c62272e07bb014262b821756295c58d;

/// The FNV-128 prime, `2^88 + 0x13b`.
const FNV128_PRIME: u128 = 0x0000000001000000000000000000013b;

/// FNV-1a-128 (frozen by spec: xor byte, multiply by the FNV 128 prime),
/// continuing from `hash` so a caller can chain several byte runs.
///
/// The one implementation behind both `StableId::derive` and [`key_group`];
/// the controller's `Flezha.Partition` and Java's `StableId` carry the same
/// constants, pinned by one shared fixture-vector set.
pub fn fnv1a128(mut hash: u128, bytes: &[u8]) -> u128 {
    for &b in bytes {
        hash ^= b as u128;
        hash = hash.wrapping_mul(FNV128_PRIME);
    }
    hash
}

/// How many key groups a partitioned operative's key space is divided into.
pub const KEY_GROUPS: u32 = 256;

/// The key group a routing key belongs to: `fnv1a128(key) mod 256`. The
/// controller assigns each partition a contiguous range of these and the
/// dispatch guard refuses a key outside its own.
pub fn key_group(key: &str) -> u32 {
    (fnv1a128(FNV128_OFFSET_BASIS, key.as_bytes()) & (KEY_GROUPS as u128 - 1)) as u32
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
