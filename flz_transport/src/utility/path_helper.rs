//! Where the controller and its runners meet: the UDS socket-directory ladder,
//! the probe that says why a connect failed, and the socket names inside the
//! resolved directory. One implementation, shared by every SDK — Rust, Java and
//! Node reach it through `flz_core`, Elixir through `flz_nif`.

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::{env, fmt, fs};

use crate::transport::config::{self, TransportType};
use crate::utility::last_good::LastGood;

/// Set, this directory is used outright — no later candidate is tried.
pub const SOCKET_DIR_VAR: &str = "FLEZHA_SOCKET_FILES_PATH";
/// Per-user runtime directory every desktop and systemd session already sets.
pub const XDG_RUNTIME_VAR: &str = "XDG_RUNTIME_DIR";
/// System-wide runtime directory, for a controller running as a service.
pub const RUN_CANDIDATE: &str = "/run/flezha";
/// The repository fallback, so the development loop needs no configuration.
pub const REPOSITORY_CANDIDATE: &str = "app_data/sockets";

const CONTROL_SOCKET_FILE: &str = "runner_control_channel.sock";

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
    /// How the rung is named to an operator — the source, not the value.
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
    /// the operator did not choose.
    Ambiguous { live: Vec<PathBuf> },
    /// No candidate could be created — the controller has nowhere to listen.
    Uncreatable {
        walked: Vec<PathBuf>,
        reason: String,
    },
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
        }
    }
}

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
    fn holds_a_socket(&self) -> bool {
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
    // controller the operator did not name is worse than one that will not
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
    Ok(path_in(
        &resolve_socket_dir(Role::Runner, None)?.path,
        CONTROL_SOCKET_FILE,
    ))
}

/// A per-runner data-socket path, in the directory the control channel
/// actually reached — never re-resolved, so data and control cannot land in
/// two different directories.
pub fn stream_socket_path(runner_id: &str, stream_type: String) -> Result<String, SocketDirError> {
    let dir = match LAST_GOOD_DIR.get() {
        Some(dir) => dir,
        None => resolve_socket_dir(Role::Runner, None)?.path,
    };

    Ok(path_in(
        &dir,
        &format!("{}_stream_{}.sock", stream_type, runner_id),
    ))
}

/// Record the directory a control channel connected through, named by the
/// control socket it reached.
pub fn note_connected(control_socket: &str) {
    if let Some(dir) = Path::new(control_socket).parent() {
        LAST_GOOD_DIR.note(dir.to_path_buf());
    }
}

/// Resolve and probe in one step — the single implementation behind the
/// runner's pre-retry diagnosis and `flz runner check`.
pub fn diagnose(explicit: Option<&str>) -> Result<Diagnosis, SocketDirError> {
    let dir = resolve_socket_dir(Role::Runner, explicit)?;
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
        TransportType::Uds => match diagnose(socket_dir) {
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
        Err(refusal) => return refusal,
    };

    let mut resolved: Vec<String> = Vec::new();
    let mut unresolved: Vec<String> = Vec::new();
    for entry in &entries {
        match entry.resolve() {
            Ok(addrs) => resolved.extend(addrs.iter().map(|a| a.to_string())),
            Err(e) => unresolved.push(format!("{}: {}", entry, e)),
        }
    }

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
    let socket = PathBuf::from(path_in(dir, CONTROL_SOCKET_FILE));

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

fn env_dir(key: &str) -> Option<PathBuf> {
    match env::var(key) {
        Ok(value) => {
            let trimmed = value.trim_end_matches('/');
            if trimmed.is_empty() {
                None
            } else {
                Some(PathBuf::from(trimmed))
            }
        }
        Err(_) => None,
    }
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

fn path_in(dir: &Path, file: &str) -> String {
    format!("{}/{}", dir.display(), file)
}

fn join_paths(paths: &[PathBuf]) -> String {
    paths
        .iter()
        .map(|p| p.display().to_string())
        .collect::<Vec<_>>()
        .join(", ")
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
    use std::os::unix::net::UnixListener;

    use tempfile::TempDir;

    use super::*;

    /// A live controller: a bound, listening control socket in `dir`.
    fn listen(dir: &Path) -> UnixListener {
        UnixListener::bind(path_in(dir, CONTROL_SOCKET_FILE)).expect("bind")
    }

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
    fn the_probe_tells_the_four_situations_apart() {
        let missing = TempDir::new().unwrap().path().join("never-created");
        assert_eq!(probe(&missing), Reach::DirectoryMissing);

        let empty = TempDir::new().unwrap();
        assert_eq!(probe(empty.path()), Reach::SocketMissing);

        let stale = TempDir::new().unwrap();
        fs::write(path_in(stale.path(), CONTROL_SOCKET_FILE), b"").unwrap();
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
}
