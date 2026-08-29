//! What one controller instance claims exclusively on this host: the socket
//! directory it listens in and the state root it writes.
//!
//! Both are selected by an environment variable, so two instances coexist on
//! one machine by configuration alone — and a missed variable is either noisy
//! destruction or a silent one. The socket directory is asked of
//! `flz_transport`'s probe, the same question a runner's diagnosis asks. The
//! state root is asked of the kernel: a POSIX record lock is released when its
//! holder exits, so there is no stale lock file to recover, and `F_GETLK`
//! reports the process still holding it, so a refusal can name it.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use flz_transport::utility::socket_probe;

/// The claim lives as long as this open file description, and a controller
/// owns its state root until the process exits — so the file is held here
/// rather than in a resource an Elixir process could let go of.
static HELD: Mutex<Option<(PathBuf, File)>> = Mutex::new(None);

const LOCK_FILE: &str = ".instance.lock";

/// Whether a controller is listening in `socket_dir` right now. A directory a
/// live peer answers in is that peer's, and preparing it would take its
/// control socket and every attached runner's stream socket with it.
#[rustler::nif(schedule = "DirtyIo")]
fn socket_dir_live(socket_dir: String) -> bool {
    socket_probe::probe(Path::new(&socket_dir)).reachable()
}

/// Claim `state_root` for this controller. `Ok` carries the lock file;
/// `Err` names the holder, in the words a refusal repeats.
#[rustler::nif(schedule = "DirtyIo")]
fn lock_state_root(state_root: String) -> Result<String, String> {
    let root = Path::new(&state_root);
    let path = root.join(LOCK_FILE);
    let mut held = HELD.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

    // Record locks belong to the process, not the descriptor: re-taking one
    // this process already holds would succeed and then release the whole
    // claim the moment either descriptor closed.
    if held.as_ref().is_some_and(|(current, _)| *current == path) {
        return Ok(path.display().to_string());
    }

    fs::create_dir_all(root)
        .map_err(|e| format!("{} could not be created: {}", root.display(), e))?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|e| format!("{} could not be opened: {}", path.display(), e))?;

    take(&file)?;
    // The previous claim is dropped only once the new one is taken, and only
    // because it names a different file.
    let rendered = path.display().to_string();
    *held = Some((path, file));
    Ok(rendered)
}

/// The lock file this process holds, `""` before it has claimed one — asked of
/// the claim itself rather than of a copy kept beside it.
#[rustler::nif]
fn held_state_root() -> String {
    HELD.lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_ref()
        .map(|(path, _)| path.display().to_string())
        .unwrap_or_default()
}

fn take(file: &File) -> Result<(), String> {
    match fcntl(file, libc::F_SETLK) {
        Ok(_) => Ok(()),
        // The two errnos that mean "someone else holds it"; anything else is a
        // filesystem that will not lock at all, which is not a peer.
        Err(e) if matches!(e.raw_os_error(), Some(libc::EACCES) | Some(libc::EAGAIN)) => {
            Err(holder(file))
        }
        Err(e) => Err(format!("it could not be locked: {}", e)),
    }
}

/// Who is holding it, asked of the kernel rather than of a file we wrote.
fn holder(file: &File) -> String {
    match fcntl(file, libc::F_GETLK) {
        Ok(blocking) if blocking.l_type != libc::F_UNLCK as libc::c_short => {
            format!("held by pid {}", blocking.l_pid)
        }
        _ => "held by another process".to_string(),
    }
}

/// One `fcntl` write-lock call, reporting `errno` from the syscall that set it.
fn fcntl(file: &File, command: libc::c_int) -> io::Result<libc::flock> {
    let mut lock = libc::flock {
        l_type: libc::F_WRLCK as libc::c_short,
        l_whence: libc::SEEK_SET as libc::c_short,
        l_start: 0,
        l_len: 0,
        l_pid: 0,
    };

    // Safe: `lock` is a live, fully initialised `flock` and the descriptor is
    // owned by `file` for the duration of the call.
    if unsafe { libc::fcntl(file.as_raw_fd(), command, &mut lock) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(lock)
    }
}
