//! Measuring and unlinking one root's entries.

use std::fs;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};

use super::plan::{canonicalize_or_self, Source};

// What this command does to one root: measure the entries the plan covers, and
// unlink them when applying. One entry list serves both, so the preview cannot
// describe anything but what the apply would remove. The measurement stands
// even when the unlink fails partway — it says what the plan covered, and the
// exit code reads it.
pub(super) fn measure_and_clear(
    root: &Path,
    source: Source,
    app_dir: &Path,
    apply: bool,
) -> (io::Result<()>, u64, u64) {
    let entries = match entries_to_clear(root, source) {
        Ok(entries) => entries,
        Err(e) => return (Err(e), 0, 0),
    };

    let (files, bytes) = match stats_of(&entries) {
        Ok(stats) => stats,
        Err(e) => return (Err(e), 0, 0),
    };

    let unlinked = if apply {
        unlink_all(&entries, app_dir, root)
    } else {
        Ok(())
    };

    (unlinked, files, bytes)
}

// A missing root yields nothing: it is success, not error.
fn entries_to_clear(root: &Path, source: Source) -> io::Result<Vec<PathBuf>> {
    let iter = match fs::read_dir(root) {
        Ok(it) => it,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };

    let mut out = Vec::new();
    for entry in iter {
        let entry = entry?;
        if source.covers(&entry.file_name()) {
            out.push(entry.path());
        }
    }
    Ok(out)
}

// Recursive count of everything under `entries`. A directory contributes its
// contents, never itself; everything else counts, socket inodes included — a
// summary that says "0 files" over two unlinked sockets tells the caller the
// opposite of what happened. A symlink counts as itself, never as its target.
fn stats_of(entries: &[PathBuf]) -> io::Result<(u64, u64)> {
    let mut files: u64 = 0;
    let mut bytes: u64 = 0;
    let mut stack: Vec<PathBuf> = entries.to_vec();

    while let Some(p) = stack.pop() {
        let md = fs::symlink_metadata(&p)?;

        if md.is_dir() {
            for entry in fs::read_dir(&p)? {
                stack.push(entry?.path());
            }
        } else {
            files += 1;
            bytes += md.len();
        }
    }

    Ok((files, bytes))
}

fn unlink_all(entries: &[PathBuf], app_dir: &Path, root: &Path) -> io::Result<()> {
    for entry_path in entries {
        if let Err(msg) = assert_safe_to_unlink(entry_path, app_dir, root) {
            return Err(io::Error::other(msg));
        }

        if fs::symlink_metadata(entry_path)?.is_dir() {
            fs::remove_dir_all(entry_path)?;
        } else {
            fs::remove_file(entry_path)?;
        }
    }

    Ok(())
}

fn assert_safe_to_unlink(entry: &Path, app_dir: &Path, cleared_root: &Path) -> Result<(), String> {
    let entry_c = canonicalize_or_self(entry);
    let root_c = canonicalize_or_self(app_dir);
    let cleared_c = canonicalize_or_self(cleared_root);
    if entry_c == root_c {
        return Err(format!(
            "refusing to unlink configured storage app directory itself: {}",
            entry.display()
        ));
    }
    if entry_c == cleared_c {
        return Err(format!(
            "refusing to unlink cleared root itself: {}",
            entry.display()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::*;

    #[test]
    fn assert_safe_to_unlink_rejects_root_itself() {
        let tmp = std::env::temp_dir().join(format!("flz-reset-guard-{}", std::process::id()));
        let _ = fs::create_dir_all(&tmp);
        let err = assert_safe_to_unlink(&tmp, &tmp, &tmp.join("state")).unwrap_err();
        assert!(err.contains("refusing"));
        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn assert_safe_to_unlink_rejects_cleared_root_itself() {
        let tmp = std::env::temp_dir().join(format!("flz-reset-guard2-{}", std::process::id()));
        let cleared = tmp.join("state");
        let _ = fs::create_dir_all(&cleared);
        let err = assert_safe_to_unlink(&cleared, &tmp, &cleared).unwrap_err();
        assert!(err.contains("refusing"));
        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn assert_safe_to_unlink_accepts_child_entry() {
        let tmp = std::env::temp_dir().join(format!("flz-reset-guard3-{}", std::process::id()));
        let cleared = tmp.join("state");
        let entry = cleared.join("file.txt");
        let _ = fs::create_dir_all(&cleared);
        let _ = File::create(&entry);
        assert!(assert_safe_to_unlink(&entry, &tmp, &cleared).is_ok());
        let _ = fs::remove_dir_all(&tmp);
    }

    const PREVIEW: bool = false;
    const APPLY: bool = true;

    #[test]
    fn a_missing_root_measures_zero_and_is_not_an_error() {
        let p = std::env::temp_dir().join(format!("flz-reset-missing-{}", std::process::id()));
        let _ = fs::remove_dir_all(&p);
        let (result, files, bytes) = measure_and_clear(&p, Source::AppData("state"), &p, APPLY);
        assert!(result.is_ok());
        assert_eq!((files, bytes), (0, 0));
    }

    #[test]
    fn a_data_root_measures_every_entry_and_leaves_them_in_preview() {
        let tmp = std::env::temp_dir().join(format!("flz-reset-walk-{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(tmp.join("nested")).unwrap();
        std::fs::write(tmp.join("a.bin"), vec![0u8; 10]).unwrap();
        std::fs::write(tmp.join("nested/b.bin"), vec![0u8; 25]).unwrap();

        let (result, files, bytes) =
            measure_and_clear(&tmp, Source::AppData("state"), &tmp, PREVIEW);
        assert!(result.is_ok());
        assert_eq!((files, bytes), (2, 35));
        assert!(tmp.join("a.bin").exists(), "a preview unlinks nothing");

        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn the_socket_root_measures_and_clears_the_same_entries() {
        let tmp = std::env::temp_dir().join(format!("flz-reset-sockets-{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();
        // Real socket inodes: a socket is neither a regular file nor a
        // directory, and the count has to see it anyway.
        for name in ["runner_control_channel.sock", "primary_stream_r1.sock"] {
            drop(std::os::unix::net::UnixListener::bind(tmp.join(name)).unwrap());
        }
        std::fs::write(tmp.join("user-notes.txt"), vec![0u8; 99]).unwrap();

        let (result, files, _bytes) =
            measure_and_clear(&tmp, Source::SocketLadder, &tmp.join("app"), APPLY);
        assert!(result.is_ok());
        assert_eq!(files, 2, "the count is of what the unlink removed");

        assert!(!tmp.join("runner_control_channel.sock").exists());
        assert!(!tmp.join("primary_stream_r1.sock").exists());
        assert!(tmp.join("user-notes.txt").exists());
        assert!(tmp.is_dir(), "the directory itself is never removed");

        let _ = fs::remove_dir_all(&tmp);
    }
}
