//! What a probe of a socket directory finds and how it is told to an
//! operator: one `stat` and one connect per candidate, four failures with four
//! different fixes never collapsed into one, and the one-line diagnosis every
//! SDK and `flz runner check` render. `AccessDenied` is deliberately not
//! "live": a directory this user cannot traverse must not manufacture a
//! phantom ambiguity.

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::{fmt, fs};

use crate::transport::config::{self, TransportType};
use crate::utility::socket_dir::{
    control_socket_file_in, join_paths, resolve_socket_dir, Role, SocketDir, SocketDirError,
    SOCKET_DIR_VAR,
};

/// What a probe of a socket directory found. Four failures with four different
/// fixes, never collapsed into one, plus the success case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reach {
    /// A control socket answered a connect.
    Reachable,
    /// No controller has ever used this path.
    DirectoryMissing,
    /// The directory is there but holds no control socket.
    SocketMissing,
    /// The directory is the UDS authentication boundary and this user is
    /// outside it.
    AccessDenied {
        dir_uid: Option<u32>,
        dir_mode: Option<u32>,
        caller_uid: u32,
    },
    /// A socket file nobody is listening on — a controller that died hard.
    StaleSocket,
    /// A socket that refused the connect for some other reason.
    Unreachable(String),
}

impl Reach {
    pub fn reachable(&self) -> bool {
        matches!(self, Reach::Reachable)
    }

    /// A control socket is there, whatever it answered — evidence a controller
    /// used this directory.
    pub(crate) fn holds_a_socket(&self) -> bool {
        matches!(
            self,
            Reach::StaleSocket | Reach::AccessDenied { .. } | Reach::Unreachable(_)
        )
    }

    /// The remedy an operator applies, in the words of the situation.
    pub fn fix(&self) -> String {
        match self {
            Reach::Reachable => "nothing to fix — the controller answered".to_string(),
            Reach::DirectoryMissing => format!(
                "start the controller, or set {} to the directory it established",
                SOCKET_DIR_VAR
            ),
            Reach::SocketMissing => {
                "start the controller, or wait for it to finish booting".to_string()
            }
            Reach::AccessDenied {
                dir_uid,
                dir_mode,
                caller_uid,
            } => format!(
                "the socket directory is the access boundary: it is owned by uid {} with mode {}, \
                 and this process runs as uid {}. Run the runner as the owning user, or start the \
                 controller with FLEZHA_SOCKET_DIR_GROUP set to a group both users belong to",
                render_uid(*dir_uid),
                render_mode(*dir_mode),
                caller_uid
            ),
            Reach::StaleSocket => {
                "a controller died leaving this socket behind; it clears on the next controller \
                 start"
                    .to_string()
            }
            Reach::Unreachable(reason) => {
                format!("the control socket did not answer: {}", reason)
            }
        }
    }
}

impl fmt::Display for Reach {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Reach::Reachable => write!(f, "controller reachable"),
            Reach::DirectoryMissing => write!(f, "socket directory absent"),
            Reach::SocketMissing => write!(f, "control socket absent"),
            Reach::AccessDenied { .. } => write!(f, "access denied"),
            Reach::StaleSocket => write!(f, "stale control socket"),
            Reach::Unreachable(_) => write!(f, "control socket unreachable"),
        }
    }
}

/// A resolved directory and what probing it found — the text a runner prints
/// once before its retry loop, and what `flz runner check` prints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnosis {
    pub dir: SocketDir,
    pub reach: Reach,
}

impl fmt::Display for Diagnosis {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} at {} (from {}); walked {}. Fix: {}",
            self.reach,
            self.dir.path.display(),
            self.dir.rung.label(),
            join_paths(&self.dir.walked),
            self.reach.fix()
        )
    }
}

/// Resolve and probe in one step — the single implementation behind the
/// runner's pre-retry diagnosis, `flz runner check` and `flz reset`'s socket
/// root. The role decides which directory is probed: a runner asks after the
/// one that answers, a controller after the one it would establish, and a tool
/// acting on a controller's own state asks the controller's question.
pub fn diagnose(role: Role, explicit: Option<&str>) -> Result<Diagnosis, SocketDirError> {
    let dir = resolve_socket_dir(role, explicit)?;
    let reach = probe(&dir.path);

    Ok(Diagnosis { dir, reach })
}

/// Why this process cannot reach a controller, as one line, for whichever
/// transport it dials: the socket directory's situation and its fix for UDS,
/// the candidate list for QUIC. The single implementation behind the runner's
/// pre-retry diagnosis, `flz runner check` and the Elixir SDK's — a second
/// rendering per host is a second thing to keep true.
pub fn connect_diagnosis(
    transport: TransportType,
    socket_dir: Option<&str>,
    quic_addresses: Option<&str>,
) -> String {
    match transport {
        TransportType::Uds => match diagnose(Role::Runner, socket_dir) {
            Ok(diagnosis) => diagnosis.to_string(),
            Err(refusal) => refusal.to_string(),
        },
        TransportType::Quic => quic_candidate_note(quic_addresses),
    }
}

/// The QUIC half: what the list resolves to right now, and what it does not.
fn quic_candidate_note(quic_addresses: Option<&str>) -> String {
    let entries = match config::quic_addresses_with(quic_addresses) {
        Ok(entries) => entries,
        Err(refusal) => return refusal.to_string(),
    };

    let (resolved, unresolved) = config::resolve_entries(&entries);
    let resolved: Vec<String> = resolved.iter().map(|a| a.to_string()).collect();

    let walked = format!("QUIC controller candidates walked: {}", resolved.join(", "));
    if unresolved.is_empty() {
        walked
    } else {
        format!("{}; unresolved: {}", walked, unresolved.join("; "))
    }
}

/// Which of the situations a socket directory is in, by `stat` and one
/// connect.
pub fn probe(dir: &Path) -> Reach {
    let socket = PathBuf::from(control_socket_file_in(dir));

    let dir_meta = match fs::metadata(dir) {
        Ok(meta) => Some(meta),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Reach::DirectoryMissing,
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            return access_denied(None);
        }
        Err(e) => return Reach::Unreachable(e.to_string()),
    };

    match fs::metadata(&socket) {
        Ok(_) => connect_probe(&socket, dir_meta.as_ref()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Reach::SocketMissing,
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            access_denied(dir_meta.as_ref())
        }
        Err(e) => Reach::Unreachable(e.to_string()),
    }
}

fn render_uid(uid: Option<u32>) -> String {
    uid.map(|v| v.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn render_mode(mode: Option<u32>) -> String {
    mode.map(|v| format!("{:04o}", v & 0o7777))
        .unwrap_or_else(|| "unknown".to_string())
}

// --- platform probes ---

#[cfg(unix)]
fn access_denied(dir_meta: Option<&fs::Metadata>) -> Reach {
    Reach::AccessDenied {
        dir_uid: dir_meta.map(|m| m.uid()),
        dir_mode: dir_meta.map(|m| m.mode()),
        caller_uid: caller_uid(),
    }
}

#[cfg(not(unix))]
fn access_denied(_dir_meta: Option<&fs::Metadata>) -> Reach {
    Reach::AccessDenied {
        dir_uid: None,
        dir_mode: None,
        caller_uid: 0,
    }
}

#[cfg(unix)]
fn caller_uid() -> u32 {
    // SAFETY: `getuid` is always safe — it takes no arguments and cannot fail.
    unsafe { libc::getuid() }
}

#[cfg(unix)]
fn connect_probe(socket: &Path, dir_meta: Option<&fs::Metadata>) -> Reach {
    match UnixStream::connect(socket) {
        Ok(_) => Reach::Reachable,
        Err(e) => match e.kind() {
            std::io::ErrorKind::ConnectionRefused => Reach::StaleSocket,
            std::io::ErrorKind::PermissionDenied => access_denied(dir_meta),
            _ => Reach::Unreachable(e.to_string()),
        },
    }
}

#[cfg(not(unix))]
fn connect_probe(_socket: &Path, _dir_meta: Option<&fs::Metadata>) -> Reach {
    Reach::Unreachable("UDS is not available on this platform".to_string())
}

#[cfg(all(test, unix))]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::utility::socket_dir::listen;

    #[test]
    fn the_probe_tells_the_four_situations_apart() {
        let missing = TempDir::new().unwrap().path().join("never-created");
        assert_eq!(probe(&missing), Reach::DirectoryMissing);

        let empty = TempDir::new().unwrap();
        assert_eq!(probe(empty.path()), Reach::SocketMissing);

        let stale = TempDir::new().unwrap();
        fs::write(control_socket_file_in(stale.path()), b"").unwrap();
        assert_eq!(probe(stale.path()), Reach::StaleSocket);

        let live = TempDir::new().unwrap();
        let _listener = listen(live.path());
        assert_eq!(probe(live.path()), Reach::Reachable);

        // Access denied needs a directory this process cannot traverse, which
        // only another user's directory can be; `/proc/1/fd` is one on every
        // Linux box where the tests do not run as root.
        let denied = probe(Path::new("/proc/1/fd"));
        if caller_uid() != 0 {
            match denied {
                Reach::AccessDenied { caller_uid, .. } => {
                    assert_eq!(caller_uid, super::caller_uid());
                    assert!(Reach::AccessDenied {
                        dir_uid: Some(0),
                        dir_mode: Some(0o40500),
                        caller_uid
                    }
                    .fix()
                    .contains("FLEZHA_SOCKET_DIR_GROUP"));
                }
                other => panic!("expected access denied, got {:?}", other),
            }
        }
    }
}
