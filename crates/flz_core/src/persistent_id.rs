//! The runner's durable stable identity, persisted at `FLEZHA_RUNNER_ID_PATH`
//! and trusted by the controller as the binding key. The invariant: one
//! process presents one id on every connect — loaded, or derived once and
//! cached when the path is unwritable — and never refuses to start over it.
//! The hostname is deliberately not load-bearing: a container's churns.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use flz_transport::utility::noise::{noise, xorshift64};

const ENV_ID_PATH: &str = "FLEZHA_RUNNER_ID_PATH";
const DEFAULT_ID_PATH: &str = "/var/lib/flezha-runner/id";

// One stable id per process: derive-or-load runs once and reconnects within the
// same process return the SAME id — critical for the degraded, no-persist case
// where nothing is written to disk to re-load.
static STABLE_ID: OnceLock<String> = OnceLock::new();

// One incarnation per process: a reconnect presents the one it connected with
// before, a restart a new one, which is how the controller tells a process that
// still runs its relays from one that runs none.
static INCARNATION: OnceLock<String> = OnceLock::new();

pub fn id_path() -> PathBuf {
    PathBuf::from(crate::util::env_string(ENV_ID_PATH, DEFAULT_ID_PATH))
}

/// The runner's stable identity: loaded, or derived from `namespace` and
/// the factory set, persisted best-effort, and cached for the process.
pub fn stable_id(namespace: &str, factory_ids: &[String]) -> String {
    STABLE_ID
        .get_or_init(|| load_or_derive(&id_path(), namespace, factory_ids))
        .clone()
}

/// This process's incarnation: when it first connected, and a random suffix.
pub fn incarnation() -> String {
    INCARNATION
        .get_or_init(|| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            format!("{now:x}-{}", rand6())
        })
        .clone()
}

fn load_or_derive(path: &Path, namespace: &str, factory_ids: &[String]) -> String {
    if let Some(existing) = load_from(path) {
        return existing;
    }

    let id = derive_id(namespace, factory_ids);

    // Without persistence the runner still runs, with a fresh id each boot
    // and so no binding continuity across a restart. The default path is a
    // system directory an ordinary user cannot write, so this is not reported.
    let _ = persist_to(path, &id);

    id
}

/// `"<namespace>-<profile_hash8>-<rand6>"`: an order-independent hash of the
/// factory set, and a suffix that keeps two fresh runners distinct.
fn derive_id(namespace: &str, factory_ids: &[String]) -> String {
    let mut ids: Vec<&str> = factory_ids.iter().map(String::as_str).collect();
    ids.sort_unstable();
    format!("{}-{}-{}", namespace, profile_hash8(&ids), rand6())
}

fn profile_hash8(sorted_ids: &[&str]) -> String {
    // FNV-1a over the sorted, NUL-delimited factory ids.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for id in sorted_ids {
        for b in id.bytes().chain(std::iter::once(0u8)) {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    format!("{:08x}", (hash & 0xffff_ffff) as u32)
}

fn rand6() -> String {
    // Clock noise mixed with the pid keeps two fresh runners distinct; the
    // persistence path already scopes uniqueness per runner.
    let nanos = noise();
    let mut seed = nanos ^ ((std::process::id() as u64) << 32) ^ nanos.rotate_left(17);
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut out = String::with_capacity(6);
    for _ in 0..6 {
        xorshift64(&mut seed);
        out.push(ALPHABET[(seed % ALPHABET.len() as u64) as usize] as char);
    }
    out
}

fn load_from(path: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(path).ok()?;
    let id = raw.trim();
    if id.is_empty() {
        None
    } else {
        Some(id.to_string())
    }
}

fn persist_to(path: &Path, id: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(id.as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("flezha-pid-test-{}-{}", std::process::id(), name));
        p.push("id");
        p
    }

    #[test]
    fn incarnation_is_minted_once_per_process() {
        let first = incarnation();
        assert!(!first.is_empty());
        assert_eq!(incarnation(), first);
    }

    #[test]
    fn persist_then_load_round_trips() {
        let path = tmp_path("round-trip");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
        persist_to(&path, "exec-123").unwrap();
        assert_eq!(load_from(&path), Some("exec-123".to_string()));
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn load_missing_file_is_none() {
        let path = tmp_path("missing");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
        assert_eq!(load_from(&path), None);
    }

    #[test]
    fn blank_or_whitespace_id_is_none() {
        let path = tmp_path("blank");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
        persist_to(&path, "   \n").unwrap();
        assert_eq!(load_from(&path), None);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn derive_id_has_namespace_prefix_profile_hash_and_random_suffix() {
        let factories = vec!["b_factory".to_string(), "a_factory".to_string()];
        let id = derive_id("default", &factories);
        let parts: Vec<&str> = id.splitn(3, '-').collect();
        assert_eq!(parts.len(), 3, "id must be ns-profile-rand, got {id}");
        assert_eq!(parts[0], "default");
        assert_eq!(parts[1].len(), 8, "profile hash must be 8 hex chars");
        assert!(parts[1].chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(parts[2].len(), 6, "random suffix must be 6 chars");
    }

    #[test]
    fn derive_id_profile_cell_is_order_independent_and_set_sensitive() {
        // derive_id sorts the factory ids before hashing, so the profile cell
        // (the middle of ns-profile-rand) is identical regardless of input
        // order...
        let profile = |ids: &[&str]| {
            let owned: Vec<String> = ids.iter().map(|s| s.to_string()).collect();
            derive_id("ns", &owned)
                .split('-')
                .nth(1)
                .unwrap()
                .to_string()
        };
        assert_eq!(profile(&["a", "b", "c"]), profile(&["c", "a", "b"]));
        // ...and differs for a different factory set.
        assert_ne!(profile(&["a", "b", "c"]), profile(&["a", "b"]));
    }

    #[test]
    fn load_or_derive_reloads_a_persisted_id() {
        // First call derives + persists; a second load_or_derive against the
        // same path returns the SAME id (binding continuity across restart).
        let path = tmp_path("reload");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
        let first = load_or_derive(&path, "ns", &["f1".to_string()]);
        let second = load_or_derive(&path, "ns", &["f1".to_string()]);
        assert_eq!(first, second);
        assert_eq!(load_from(&path), Some(first));
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn unwritable_path_degrades_to_a_fresh_id_without_panicking() {
        // A path under a non-existent, non-creatable parent (a file used as a
        // directory) makes persist fail; load_or_derive must still return a
        // usable id and never panic (degradation contract).
        let mut blocker = std::env::temp_dir();
        blocker.push(format!("flezha-pid-blocker-{}", std::process::id()));
        std::fs::write(&blocker, b"x").unwrap();
        let mut path = blocker.clone();
        path.push("nested"); // parent is a FILE → create_dir_all fails
        let id = load_or_derive(&path, "ns", &["f1".to_string()]);
        assert!(id.starts_with("ns-"), "degraded id is still derived: {id}");
        assert_eq!(load_from(&path), None, "nothing was persisted");
        let _ = std::fs::remove_file(&blocker);
    }
}
