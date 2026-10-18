//! The UDS socket-directory ladder: where a controller establishes its
//! directory and where a runner finds it, and the socket names inside the
//! resolved directory. One implementation, shared by every SDK — Rust, Java
//! and Node reach it through `flz_core`, Elixir through `flz_nif`. The
//! invariant: an explicit `FLEZHA_SOCKET_FILES_PATH` wins outright, and a
//! runner refuses to guess between two live directories.

use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::{env, fmt, fs};

use ring::digest::{digest, SHA256};

use crate::env::{connect_dir, env_dir};
use crate::utility::last_good::LastGood;
use crate::utility::socket_probe::{probe, Reach};

/// Set, this directory is used outright — no later candidate is tried.
pub const SOCKET_DIR_VAR: &str = "FLEZHA_SOCKET_FILES_PATH";
/// Per-user runtime directory every desktop and systemd session already sets.
pub const XDG_RUNTIME_VAR: &str = "XDG_RUNTIME_DIR";
/// System-wide runtime directory, for a controller running as a service.
pub const RUN_CANDIDATE: &str = "/run/flezha";
/// The repository fallback, so the development loop needs no configuration.
pub const REPOSITORY_CANDIDATE: &str = "app_data/sockets";

const CONTROL_SOCKET_FILE: &str = "runner_control_channel.sock";
/// A per-runner data socket is `<stream type>_stream_<id field>.sock`.
const STREAM_SOCKET_INFIX: &str = "_stream_";
const SOCKET_SUFFIX: &str = ".sock";

/// A Unix domain socket address holds `sun_path` less the NUL the kernel
/// appends, and the kernel refuses anything longer before a socket exists.
const SOCKET_PATH_MAX_BYTES: usize = 107;
/// The id field's ceiling. The longest name around it is 22 bytes
/// (`secondary_stream_` + `.sock`), so 24 here leaves 60 bytes for the socket
/// directory — more than any candidate the ladder produces on a normal host,
/// and more than the longest id `Identificator.unique_id/1` mints in the
/// default namespace, which is 24 and is therefore never shortened.
const STREAM_SOCKET_ID_MAX: usize = 24;
/// How much of a shortened id is the id's own leading bytes; the rest is `~`
/// and 12 hex characters of its digest, so a shortened field is exactly
/// `STREAM_SOCKET_ID_MAX`.
const STREAM_SOCKET_ID_PREFIX: usize = 11;
const STREAM_SOCKET_ID_DIGEST_BYTES: usize = 6;

/// The directory a control channel connected through: it settles an ambiguity
/// a first connect would refuse, and it is where the data sockets are opened,
/// so control and data can never land in two different directories.
static LAST_GOOD_DIR: LastGood<PathBuf> = LastGood::new();

/// Which side is asking. A controller establishes a directory; a runner finds
/// one, and refuses to guess between two live ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Controller,
    Runner,
}

/// Which rung of the ladder answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rung {
    Explicit,
    XdgRuntime,
    Run,
    Repository,
}

impl Rung {
    /// How the rung is named to a user — the source, not the value.
    pub fn label(self) -> &'static str {
        match self {
            Rung::Explicit => "$FLEZHA_SOCKET_FILES_PATH",
            Rung::XdgRuntime => "$XDG_RUNTIME_DIR/flezha",
            Rung::Run => RUN_CANDIDATE,
            Rung::Repository => REPOSITORY_CANDIDATE,
        }
    }
}

/// A resolved socket directory: an absolute path, the rung that produced it,
/// and every candidate considered — the last so a diagnosis can name them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SocketDir {
    pub path: PathBuf,
    pub rung: Rung,
    pub walked: Vec<PathBuf>,
}

/// The two ways resolution refuses rather than answering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SocketDirError {
    /// More than one candidate holds a live control socket and no variable
    /// says which is meant. Naming one would attach the runner to a controller
    /// the user did not choose.
    Ambiguous { live: Vec<PathBuf> },
    /// No candidate could be created — the controller has nowhere to listen.
    Uncreatable {
        walked: Vec<PathBuf>,
        reason: String,
    },
    /// The composed path is longer than a Unix domain socket address, so
    /// nothing can bind it and nothing can dial it.
    PathTooLong { path: String, len: usize },
}

impl fmt::Display for SocketDirError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SocketDirError::Ambiguous { live } => write!(
                f,
                "more than one controller is listening and none was named: {}. \
                 Set {} to the one this runner should attach to.",
                join_paths(live),
                SOCKET_DIR_VAR
            ),
            SocketDirError::Uncreatable { walked, reason } => write!(
                f,
                "no socket directory could be created; tried {} ({}). \
                 Set {} to a directory this user can write.",
                join_paths(walked),
                reason,
                SOCKET_DIR_VAR
            ),
            SocketDirError::PathTooLong { path, len } => write!(
                f,
                "the socket path {} is {} bytes; a Unix domain socket address \
                 holds {}. Set {} to a shorter directory.",
                path, len, SOCKET_PATH_MAX_BYTES, SOCKET_DIR_VAR
            ),
        }
    }
}

/// Walk the ladder for `role`. Every candidate is reported as an absolute
/// path, whichever rung produced it.
///
/// `explicit` is the caller's own view of `FLEZHA_SOCKET_FILES_PATH`, `None`
/// to read the process environment. A host whose runtime keeps its own
/// environment table — the BEAM does — supplies what it reads there; the rule
/// itself stays here, in one place.
pub fn resolve_socket_dir(role: Role, explicit: Option<&str>) -> Result<SocketDir, SocketDirError> {
    let explicit = explicit
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .or_else(|| connect_dir(SOCKET_DIR_VAR));

    resolve_from(candidates_from(explicit, env_dir(XDG_RUNTIME_VAR)), role)
}

fn resolve_from(candidates: Vec<(Rung, PathBuf)>, role: Role) -> Result<SocketDir, SocketDirError> {
    let walked: Vec<PathBuf> = candidates.iter().map(|(_, p)| p.clone()).collect();

    // An explicit path wins outright: a runner quietly attaching to a
    // controller the user did not name is worse than one that will not
    // start.
    if let Some(&(rung @ Rung::Explicit, ref path)) = candidates.first() {
        if role == Role::Controller {
            if let Err(e) = create_dir(path) {
                return Err(SocketDirError::Uncreatable {
                    walked,
                    reason: e.to_string(),
                });
            }
        }
        return Ok(SocketDir {
            path: path.clone(),
            rung,
            walked,
        });
    }

    match role {
        Role::Controller => first_creatable(&candidates, walked),
        Role::Runner => first_live(&candidates, walked),
    }
}

/// The absolute control-socket path a runner dials, resolved through the
/// ladder.
pub fn control_socket_path() -> Result<String, SocketDirError> {
    control_socket_path_in(&resolve_socket_dir(Role::Runner, None)?.path)
}

/// The control-socket path in `dir`. The composer beside
/// `stream_socket_path_in`, so a caller that resolved its own directory asks
/// for the name here rather than spelling it: the controller listening and
/// every runner dialling then read one file name from one place.
pub fn control_socket_path_in(dir: &Path) -> Result<String, SocketDirError> {
    within_sun_path(control_socket_file_in(dir))
}

/// The control socket's path in `dir` without the address-length refusal: the
/// crate's own probe asks whether a file is there, and a path too long to bind
/// is unreachable rather than malformed. The one place the file name is
/// joined to a directory.
pub(crate) fn control_socket_file_in(dir: &Path) -> String {
    path_in(dir, CONTROL_SOCKET_FILE)
}

/// The id field's ceiling, published so the side that MINTS a runner id can
/// bound what it mints. A longer id is still accepted and shortened, but the
/// socket then carries a name the minting side did not choose.
pub const fn stream_socket_id_max() -> usize {
    STREAM_SOCKET_ID_MAX
}

/// A per-runner data-socket path, in the directory the control channel
/// actually reached — never re-resolved, so data and control cannot land in
/// two different directories.
pub fn stream_socket_path(runner_id: &str, stream_type: String) -> Result<String, SocketDirError> {
    let dir = match LAST_GOOD_DIR.get() {
        Some(dir) => dir,
        None => resolve_socket_dir(Role::Runner, None)?.path,
    };

    stream_socket_path_in(&dir, runner_id, &stream_type)
}

/// A per-runner data-socket path in `dir`. The controller and the runner reach
/// this from directories they resolved separately, so it is the one place the
/// name is composed and the one place the length is refused.
pub fn stream_socket_path_in(
    dir: &Path,
    runner_id: &str,
    stream_type: &str,
) -> Result<String, SocketDirError> {
    within_sun_path(path_in(
        dir,
        &format!(
            "{}{}{}{}",
            stream_type,
            STREAM_SOCKET_INFIX,
            steps_id(runner_id),
            SOCKET_SUFFIX
        ),
    ))
}

/// The id field of a data socket name, at most `STREAM_SOCKET_ID_MAX` bytes.
/// It reads the runner id and nothing else: the controller and the runner
/// compose this name in different processes, and a rule that consulted either
/// one's directory could give one runner two names.
fn steps_id(runner_id: &str) -> Cow<'_, str> {
    if runner_id.len() <= STREAM_SOCKET_ID_MAX {
        return Cow::Borrowed(runner_id);
    }

    let digest = digest(&SHA256, runner_id.as_bytes());
    let hex: String = digest.as_ref()[..STREAM_SOCKET_ID_DIGEST_BYTES]
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect();

    Cow::Owned(format!(
        "{}~{}",
        char_prefix(runner_id, STREAM_SOCKET_ID_PREFIX),
        hex
    ))
}

fn char_prefix(value: &str, max_bytes: usize) -> &str {
    let mut end = max_bytes.min(value.len());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn within_sun_path(path: String) -> Result<String, SocketDirError> {
    if path.len() > SOCKET_PATH_MAX_BYTES {
        return Err(SocketDirError::PathTooLong {
            len: path.len(),
            path,
        });
    }
    Ok(path)
}

/// Whether a file in a socket directory is one Flezha itself created — the
/// control socket every runner dials, or a per-runner data socket opened
/// beside it. The directory may be one a user named through
/// `FLEZHA_SOCKET_FILES_PATH` and shared with something else, so a caller
/// clearing it asks here rather than carrying its own spelling of these names.
pub fn is_flezha_socket(file_name: &str) -> bool {
    file_name == CONTROL_SOCKET_FILE
        || (file_name.ends_with(SOCKET_SUFFIX) && file_name.contains(STREAM_SOCKET_INFIX))
}

/// Record the directory a control channel connected through, named by the
/// control socket it reached.
pub fn note_connected(control_socket: &str) {
    if let Some(dir) = Path::new(control_socket).parent() {
        LAST_GOOD_DIR.note(dir.to_path_buf());
    }
}

// --- ladder internals ---

fn candidates_from(explicit: Option<PathBuf>, xdg: Option<PathBuf>) -> Vec<(Rung, PathBuf)> {
    if let Some(explicit) = explicit {
        return vec![(Rung::Explicit, absolutise(&explicit))];
    }

    let mut out = Vec::with_capacity(3);
    if let Some(runtime) = xdg {
        out.push((Rung::XdgRuntime, absolutise(&runtime.join("flezha"))));
    }
    out.push((Rung::Run, absolutise(Path::new(RUN_CANDIDATE))));
    out.push((
        Rung::Repository,
        absolutise(Path::new(REPOSITORY_CANDIDATE)),
    ));
    out
}

fn first_creatable(
    candidates: &[(Rung, PathBuf)],
    walked: Vec<PathBuf>,
) -> Result<SocketDir, SocketDirError> {
    let mut last_error = String::from("no candidate offered");

    for (rung, path) in candidates {
        // A candidate another controller is listening in is not one this
        // controller can establish: taking it would wipe that controller's
        // live sockets and strand its whole fleet. Only an explicit path is
        // taken unconditionally, and that rung never reaches here.
        if probe(path).reachable() {
            last_error = format!("{}: another controller is listening there", path.display());
            continue;
        }

        match create_dir(path) {
            Ok(()) => {
                return Ok(SocketDir {
                    path: path.clone(),
                    rung: *rung,
                    walked,
                })
            }
            Err(e) => last_error = format!("{}: {}", path.display(), e),
        }
    }

    Err(SocketDirError::Uncreatable {
        walked,
        reason: last_error,
    })
}

fn first_live(
    candidates: &[(Rung, PathBuf)],
    walked: Vec<PathBuf>,
) -> Result<SocketDir, SocketDirError> {
    let probed: Vec<(&(Rung, PathBuf), Reach)> = candidates
        .iter()
        .map(|candidate| (candidate, probe(&candidate.1)))
        .collect();

    let found = |keep: fn(&Reach) -> bool| probed.iter().find(|(_, reach)| keep(reach));
    let answer = |(candidate, _): &(&(Rung, PathBuf), Reach)| SocketDir {
        path: candidate.1.clone(),
        rung: candidate.0,
        walked: walked.clone(),
    };

    let live: Vec<&PathBuf> = probed
        .iter()
        .filter(|(_, reach)| reach.reachable())
        .map(|((_, path), _)| path)
        .collect();

    // A reconnect returns to the controller that already holds this runner's
    // bindings, so a recorded directory that is still live settles the
    // ambiguity a first connect would have refused.
    if live.len() > 1 {
        let previous = LAST_GOOD_DIR.get();
        let recorded = probed
            .iter()
            .find(|((_, path), reach)| reach.reachable() && Some(path) == previous.as_ref());

        return match recorded {
            Some(candidate) => Ok(answer(candidate)),
            None => Err(SocketDirError::Ambiguous {
                live: live.into_iter().cloned().collect(),
            }),
        };
    }

    // Nothing live: report the candidate with the most to say — a socket
    // present beats a directory present beats the last rung — so the diagnosis
    // names the real obstacle rather than the tail of the ladder.
    let chosen = found(Reach::reachable)
        .or_else(|| found(Reach::holds_a_socket))
        .or_else(|| found(|reach| *reach != Reach::DirectoryMissing))
        .or_else(|| probed.last())
        .expect("the ladder always offers a candidate");

    Ok(answer(chosen))
}

fn create_dir(path: &Path) -> std::io::Result<()> {
    fs::create_dir_all(path)?;
    // Creatable is not the same as usable: a directory this process cannot
    // write is not somewhere it can bind a socket.
    let meta = fs::metadata(path)?;
    if meta.permissions().readonly() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "directory is not writable",
        ));
    }
    Ok(())
}

fn absolutise(path: &Path) -> PathBuf {
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    };

    fs::canonicalize(&joined).unwrap_or(joined)
}

pub(crate) fn path_in(dir: &Path, file: &str) -> String {
    format!("{}/{}", dir.display(), file)
}

pub(crate) fn join_paths(paths: &[PathBuf]) -> String {
    paths
        .iter()
        .map(|p| p.display().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// A live controller for a test: a bound, listening control socket in `dir`.
#[cfg(all(test, unix))]
pub(crate) fn listen(dir: &Path) -> std::os::unix::net::UnixListener {
    std::os::unix::net::UnixListener::bind(path_in(dir, CONTROL_SOCKET_FILE)).expect("bind")
}

#[cfg(all(test, unix))]
#[path = "socket_dir_tests.rs"]
mod tests;
