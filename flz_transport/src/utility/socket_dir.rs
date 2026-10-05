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

use crate::env::env_dir;
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
        .or_else(|| env_dir(SOCKET_DIR_VAR));

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
mod tests {
    use tempfile::TempDir;

    use super::*;

    fn rung(dir: &TempDir, rung: Rung) -> (Rung, PathBuf) {
        (rung, dir.path().to_path_buf())
    }

    #[test]
    fn each_rung_answers_when_it_is_the_only_live_one() {
        let xdg = TempDir::new().unwrap();
        let run = TempDir::new().unwrap();
        let repo = TempDir::new().unwrap();

        for (expected, holder) in [
            (Rung::XdgRuntime, &xdg),
            (Rung::Run, &run),
            (Rung::Repository, &repo),
        ] {
            let _live = listen(holder.path());
            let resolved = resolve_from(
                vec![
                    rung(&xdg, Rung::XdgRuntime),
                    rung(&run, Rung::Run),
                    rung(&repo, Rung::Repository),
                ],
                Role::Runner,
            )
            .expect("one live candidate resolves");

            assert_eq!(resolved.rung, expected);
            assert_eq!(resolved.path, holder.path());
            assert_eq!(resolved.walked.len(), 3);
            drop(_live);
            let _ = fs::remove_file(path_in(holder.path(), CONTROL_SOCKET_FILE));
        }
    }

    #[test]
    fn an_explicit_path_never_falls_back_to_a_live_candidate() {
        let explicit = TempDir::new().unwrap();
        let live = TempDir::new().unwrap();
        let _listener = listen(live.path());

        let resolved = resolve_from(
            candidates_from(Some(explicit.path().to_path_buf()), None),
            Role::Runner,
        )
        .expect("explicit resolves");

        assert_eq!(resolved.rung, Rung::Explicit);
        assert_eq!(resolved.path, explicit.path());
        // The diagnosis names the configured path only — never the ladder it
        // did not walk.
        assert_eq!(resolved.walked, vec![explicit.path().to_path_buf()]);
    }

    #[test]
    fn two_live_candidates_are_refused_and_both_are_named() {
        let xdg = TempDir::new().unwrap();
        let run = TempDir::new().unwrap();
        let _first = listen(xdg.path());
        let _second = listen(run.path());

        let refusal = resolve_from(
            vec![rung(&xdg, Rung::XdgRuntime), rung(&run, Rung::Run)],
            Role::Runner,
        )
        .expect_err("two controllers are an ambiguity, not a choice");

        match &refusal {
            SocketDirError::Ambiguous { live } => {
                assert_eq!(
                    live,
                    &vec![xdg.path().to_path_buf(), run.path().to_path_buf()]
                );
            }
            other => panic!("expected an ambiguity refusal, got {:?}", other),
        }
        // The refusal names the variable that resolves it.
        assert!(refusal.to_string().contains(SOCKET_DIR_VAR));
    }

    /// The env contract is data both sides read, exactly as the declaration
    /// keys are: `test/fixtures/runner_connect_env.txt`. Here the ladder's own
    /// constants and rung order are asserted against it; the static gate
    /// asserts the same file against every other SDK.
    #[test]
    fn the_pinned_environment_contract_is_what_the_ladder_reads() {
        const FIXTURE: &str = include_str!("../../../test/fixtures/runner_connect_env.txt");

        let rows: Vec<Vec<&str>> = FIXTURE
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(|line| line.split_whitespace().collect())
            .collect();

        let variables: Vec<&str> = rows
            .iter()
            .filter(|row| row[0] == "variable")
            .map(|row| row[1])
            .collect();
        assert!(variables.contains(&SOCKET_DIR_VAR));
        assert!(variables.contains(&XDG_RUNTIME_VAR));
        assert!(
            variables.contains(&crate::transport::config::QUIC_ADDRESSES_VAR),
            "the QUIC list variable is part of the same contract"
        );

        let retired: Vec<&str> = rows
            .iter()
            .filter(|row| row[0] == "retired")
            .map(|row| row[1])
            .collect();
        assert_eq!(
            retired,
            vec![crate::transport::config::RETIRED_QUIC_ADDRESS_VAR]
        );

        let ladder: Vec<(&str, &str)> = rows
            .iter()
            .filter(|row| row[0] == "ladder")
            .map(|row| (row[2], row[3]))
            .collect();
        assert_eq!(
            ladder,
            vec![
                ("explicit", "$FLEZHA_SOCKET_FILES_PATH"),
                ("xdg_runtime", "$XDG_RUNTIME_DIR/flezha"),
                ("run", RUN_CANDIDATE),
                ("repository", REPOSITORY_CANDIDATE),
            ],
            "the ladder and test/fixtures/runner_connect_env.txt disagree — \
             teach the fixture, then every SDK"
        );

        // The rungs the ladder actually offers, in the order it offers them.
        assert_eq!(
            candidates_from(None, Some(PathBuf::from("/tmp")))
                .iter()
                .map(|(rung, _)| rung.label())
                .collect::<Vec<_>>(),
            vec![ladder[1].1, ladder[2].1, ladder[3].1]
        );
        assert_eq!(
            candidates_from(Some(PathBuf::from("/tmp")), None)[0]
                .0
                .label(),
            ladder[0].1
        );
    }

    #[test]
    fn an_unset_runtime_dir_falls_through_to_the_later_rungs() {
        let ladder = candidates_from(None, None);

        assert_eq!(
            ladder.iter().map(|(r, _)| *r).collect::<Vec<_>>(),
            vec![Rung::Run, Rung::Repository]
        );

        let with_runtime = candidates_from(None, Some(PathBuf::from("/tmp/runtime-42")));
        assert_eq!(
            with_runtime.iter().map(|(r, _)| *r).collect::<Vec<_>>(),
            vec![Rung::XdgRuntime, Rung::Run, Rung::Repository]
        );
        assert!(with_runtime[0].1.ends_with("flezha"));
    }

    #[test]
    fn a_relative_candidate_is_reported_absolutely() {
        let ladder = candidates_from(None, None);
        let (_, repository) = ladder
            .last()
            .expect("the repository rung is always offered");

        assert!(repository.is_absolute(), "{}", repository.display());
        assert!(repository.ends_with(REPOSITORY_CANDIDATE));

        let explicit = candidates_from(Some(PathBuf::from("relative/sockets")), None);
        assert!(explicit[0].1.is_absolute());
    }

    #[test]
    fn nothing_live_reports_the_candidate_with_the_most_to_say() {
        let empty = TempDir::new().unwrap();
        let holder = TempDir::new().unwrap();
        // A socket file nobody listens on: a controller that died hard.
        fs::write(path_in(holder.path(), CONTROL_SOCKET_FILE), b"").unwrap();

        let resolved = resolve_from(
            vec![rung(&empty, Rung::Run), rung(&holder, Rung::Repository)],
            Role::Runner,
        )
        .expect("a dead controller still resolves a path to report");

        assert_eq!(resolved.path, holder.path());
    }

    #[test]
    fn the_controller_takes_the_first_candidate_it_can_create() {
        let root = TempDir::new().unwrap();
        let unwritable = PathBuf::from("/proc/flezha-cannot-create-this");
        let wanted = root.path().join("sockets");

        let resolved = resolve_from(
            vec![
                (Rung::XdgRuntime, unwritable),
                (Rung::Run, wanted.clone()),
                (Rung::Repository, root.path().join("fallback")),
            ],
            Role::Controller,
        )
        .expect("a creatable candidate resolves");

        assert_eq!(resolved.rung, Rung::Run);
        assert!(wanted.is_dir());
    }

    #[test]
    fn a_controller_steps_over_a_directory_another_controller_is_listening_in() {
        let taken = TempDir::new().unwrap();
        let free = TempDir::new().unwrap();
        let _incumbent = listen(taken.path());

        let resolved = resolve_from(
            vec![rung(&taken, Rung::XdgRuntime), rung(&free, Rung::Run)],
            Role::Controller,
        )
        .expect("the next candidate is creatable");

        assert_eq!(
            resolved.path,
            free.path(),
            "taking the occupied directory would wipe a live controller's sockets"
        );
    }

    #[test]
    fn a_recorded_directory_settles_an_ambiguity_a_first_connect_would_refuse() {
        let first = TempDir::new().unwrap();
        let second = TempDir::new().unwrap();
        let _a = listen(first.path());
        let _b = listen(second.path());

        note_connected(&path_in(second.path(), CONTROL_SOCKET_FILE));

        let resolved = resolve_from(
            vec![rung(&first, Rung::XdgRuntime), rung(&second, Rung::Run)],
            Role::Runner,
        )
        .expect("a reconnect returns to the controller holding the bindings");

        assert_eq!(resolved.path, second.path());

        // Leave the process-wide slot as the rest of the binary expects it.
        LAST_GOOD_DIR.forget();
    }

    // A 48-byte socket directory, the most the Elixir test harness mints under
    // `/tmp`, and a runner id at the width a late-scheduled test module draws.
    const PEER_DIR: &str = "/tmp/flezha_sockets/native_reject_peer@127.0.0.1";
    const LONG_ID: &str = "schema_evolution_e2e_419362_660179359383";

    #[test]
    fn a_long_runner_id_composes_a_data_socket_a_peer_directory_can_bind() {
        let dir = PathBuf::from(PEER_DIR);
        fs::create_dir_all(&dir).expect("the peer directory");

        let path =
            stream_socket_path_in(&dir, LONG_ID, "secondary").expect("the name fits the wall");

        assert!(
            path.len() <= 107,
            "{} is {} bytes; sun_path holds 107 and a NUL",
            path,
            path.len()
        );

        let _ = fs::remove_file(&path);
        std::os::unix::net::UnixListener::bind(&path)
            .unwrap_or_else(|e| panic!("the controller binds {}: {}", path, e));
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn the_id_field_is_shortened_by_the_id_alone_and_never_by_the_directory() {
        let short = PathBuf::from("/run/flezha");
        let long = PathBuf::from(PEER_DIR);

        let in_short = stream_socket_path_in(&short, LONG_ID, "secondary").expect("fits");
        let in_long = stream_socket_path_in(&long, LONG_ID, "secondary").expect("fits");

        // The controller and the runner resolve their directories separately;
        // two file names for one runner would never meet.
        assert_eq!(
            Path::new(&in_short).file_name(),
            Path::new(&in_long).file_name()
        );
    }

    #[test]
    fn a_shortened_id_is_the_ids_own_prefix_and_its_digest() {
        let field = steps_id(LONG_ID);

        assert_eq!(field.len(), STREAM_SOCKET_ID_MAX);
        // The digest is SHA-256 over the runner id's bytes, so it is
        // reproducible outside this repository: `printf %s <id> | sha256sum`.
        assert_eq!(field, "schema_evol~d33017db4b61");
        assert_eq!(steps_id(LONG_ID), field, "the same id gives the same field");

        // The literal every other end of this name is pinned to;
        // `Flezha.Socket.SupportTest` asserts the same string from the
        // controller's side.
        assert_eq!(
            stream_socket_path_in(Path::new(PEER_DIR), LONG_ID, "secondary").expect("fits"),
            format!(
                "{}/secondary_stream_schema_evol~d33017db4b61.sock",
                PEER_DIR
            )
        );
    }

    #[test]
    fn an_id_within_the_ceiling_stays_readable() {
        // The longest id `Identificator.unique_id/1` mints in the default
        // namespace: four numbers of four digits each.
        let id = "default_1000100010001000";
        assert_eq!(id.len(), STREAM_SOCKET_ID_MAX);
        assert_eq!(steps_id(id), id);
        assert_eq!(steps_id("default_1_2_3_4"), "default_1_2_3_4");
    }

    #[test]
    fn a_directory_that_cannot_hold_a_data_socket_is_refused_by_name_and_length() {
        let dir = PathBuf::from(format!("/tmp/{}", "d".repeat(80)));

        let refusal = stream_socket_path_in(&dir, "default_1_2_3_4", "secondary")
            .expect_err("a directory this long leaves no room for a socket name");

        let text = refusal.to_string();
        assert!(
            text.contains("secondary_stream_default_1_2_3_4.sock"),
            "{}",
            text
        );
        assert!(text.contains("123 bytes"), "{}", text);
        assert!(text.contains("107"), "{}", text);
    }

    #[test]
    fn the_socket_predicate_recognises_the_names_this_module_creates() {
        assert!(is_flezha_socket(CONTROL_SOCKET_FILE));
        for stream_type in ["primary", "secondary"] {
            for id in ["runner-1", &steps_id(LONG_ID)] {
                let name = format!(
                    "{}{}{}{}",
                    stream_type, STREAM_SOCKET_INFIX, id, SOCKET_SUFFIX
                );
                assert!(is_flezha_socket(&name), "{}", name);
            }
        }

        // A directory a user named through FLEZHA_SOCKET_FILES_PATH may
        // hold anything else, and none of it is ours.
        for theirs in [
            "user-notes.txt",
            "postgres.sock",
            "runner_control_channel.sock.bak",
            "primary_stream_runner-1.sock.lock",
        ] {
            assert!(!is_flezha_socket(theirs), "{}", theirs);
        }
    }
}
