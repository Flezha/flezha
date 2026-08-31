//! What `flz reset` may clear, where each root lives, and what probing it found.

use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use flz_transport::utility::socket_dir::{self, Role};
use flz_transport::utility::socket_probe::{diagnose, Diagnosis, Reach};

use super::Flags;

#[derive(Clone, Copy)]
pub(super) enum ClearMode {
    /// In scope on every run.
    Default,
    /// In scope only when the operator passes this flag, which is also how the
    /// root is reported when they did not. The table names the flag and the
    /// field that carries it together, so the two cannot drift apart.
    OptIn(&'static str, fn(&Flags) -> bool),
}

/// Where a root's path comes from — and therefore which of its entries this
/// command may unlink. The Controller resolves its data root and its socket
/// directory from different sources, so reset does too, and the socket root is
/// name-scoped for exactly that reason: the ladder may answer with a directory
/// an operator named and shares.
#[derive(Clone, Copy)]
pub(super) enum Source {
    /// A subdirectory of the `app_data/` root this command was given. Every
    /// entry in it is this command's.
    AppData(&'static str),
    /// The shared socket-directory ladder — the one implementation the
    /// Controller, every runner and `flz runner check` reach. Its answer is
    /// frequently not under `app_data/` at all, so only the socket files
    /// Flezha itself creates are unlinked there.
    SocketLadder,
}

impl Source {
    pub(super) fn covers(self, file_name: &OsStr) -> bool {
        match self {
            Source::AppData(_) => true,
            Source::SocketLadder => file_name.to_str().is_some_and(socket_dir::is_flezha_socket),
        }
    }
}

/// What the socket root is called when it has no path: the ladder was not
/// asked, because a run that was not asked about sockets must not establish a
/// directory, or it was asked and refused.
const UNRESOLVED_SOCKET_ROOT: &str = "socket directory";

// Update this table when a new durable runtime store lands under
// `app_data/`. Order is the order of output.
const CLEARABLE_ROOTS: &[(Source, ClearMode)] = &[
    (Source::AppData("state"), ClearMode::Default),
    (Source::AppData("process_hub"), ClearMode::Default),
    (
        Source::AppData("certs"),
        ClearMode::OptIn("--include-certs", |f| f.include_certs),
    ),
    (
        Source::SocketLadder,
        ClearMode::OptIn("--include-sockets", |f| f.include_sockets),
    ),
];

pub(super) enum Action {
    Clear,
    Keep(String),
}

/// One inspected root: what the plan decided, and what the run found there.
pub(super) struct Root {
    /// `None` where the ladder was not asked, or could not answer.
    pub(super) path: Option<PathBuf>,
    /// The rung that produced the path. `None` where the path was joined onto
    /// the data root and its source is not in question.
    pub(super) rung: Option<&'static str>,
    pub(super) source: Source,
    pub(super) action: Action,
    pub(super) files: u64,
    pub(super) bytes: u64,
}

impl Root {
    pub(super) fn planned(source: Source, path: Option<PathBuf>, action: Action) -> Self {
        Root {
            path,
            rung: None,
            source,
            action,
            files: 0,
            bytes: 0,
        }
    }

    /// The root as `--json` names it.
    pub(super) fn name(&self) -> String {
        match &self.path {
            Some(path) => path.display().to_string(),
            None => UNRESOLVED_SOCKET_ROOT.to_string(),
        }
    }

    /// How the root reads to an operator. The socket root names the rung
    /// beside its path, so a run that cleared a runtime directory instead of
    /// `app_data/sockets/` shows the ladder answering rather than the command
    /// going wrong.
    pub(super) fn label(&self) -> String {
        match (&self.path, self.rung) {
            (Some(path), Some(rung)) => format!("{}/ (from {})", path.display(), rung),
            (Some(path), None) => format!("{}/", path.display()),
            (None, _) => self.name(),
        }
    }
}

pub(super) fn build_plan(flags: &Flags) -> Vec<Root> {
    CLEARABLE_ROOTS
        .iter()
        .map(
            |&(source, mode)| match (source, out_of_scope(mode, flags)) {
                (Source::AppData(name), kept) => Root::planned(
                    source,
                    Some(flags.app_data_root.join(name)),
                    kept.unwrap_or(Action::Clear),
                ),
                // Asking the ladder establishes a directory the way a Controller
                // boot does, so a run that was not asked about sockets does not
                // ask it at all.
                (Source::SocketLadder, Some(kept)) => Root::planned(source, None, kept),
                (Source::SocketLadder, None) => plan_socket_root(),
            },
        )
        .collect()
}

/// The action for a root the operator did not ask for; `None` when it is in
/// scope.
fn out_of_scope(mode: ClearMode, flags: &Flags) -> Option<Action> {
    match mode {
        ClearMode::Default => None,
        ClearMode::OptIn(_, included) if included(flags) => None,
        ClearMode::OptIn(flag, _) => Some(Action::Keep(format!("use {} to clear", flag))),
    }
}

/// The socket directory the Controller this reset serves would use, and what
/// probing it found.
///
/// `Role::Controller` because reset acts on that Controller's own runtime
/// state: the ladder's controller walk steps past a directory a peer is
/// listening in and answers with the next rung — this Controller's own — where
/// the runner walk would have preferred the one that answers, which under
/// colocation is the peer's.
fn plan_socket_root() -> Root {
    // Resolve and probe in one step, the way the runner's pre-retry diagnosis
    // and `flz runner check` do — asked as the Controller, and never as two
    // calls that could name two directories.
    let Diagnosis { dir, reach } = match diagnose(Role::Controller, None) {
        Ok(diagnosis) => diagnosis,
        // Not knowing which directory holds the sockets is not an I/O error on
        // a path this command was asked to clear: report it and let the
        // remaining roots proceed.
        Err(refusal) => {
            return Root::planned(
                Source::SocketLadder,
                None,
                Action::Keep(refusal.to_string()),
            )
        }
    };

    // A control socket that answers is a Controller serving here — under
    // colocation, a peer, whose durable state is not under this reset's
    // `app_data/` root and whose sockets are not this reset's to unlink.
    let action = match &reach {
        Reach::Reachable => Action::Keep(format!(
            "{}; a serving controller's socket is not this reset's to unlink",
            reach
        )),
        // Not this user's to unlink, or a socket answering something odd:
        // reported and skipped rather than forced.
        Reach::AccessDenied { .. } | Reach::Unreachable(_) => {
            Action::Keep(format!("{}; {}", reach, reach.fix()))
        }
        // Absent, socketless, or a socket nobody is listening on — the last is
        // precisely what this command exists to clear, and the first two clear
        // as a no-op.
        Reach::DirectoryMissing | Reach::SocketMissing | Reach::StaleSocket => Action::Clear,
    };

    Root {
        rung: Some(dir.rung.label()),
        ..Root::planned(Source::SocketLadder, Some(dir.path), action)
    }
}

pub(super) fn preflight_guard(plan: &[Root], app_data_root: &Path) -> Result<(), String> {
    let root_c = canonicalize_or_self(app_data_root);
    let mut cleared_paths: Vec<PathBuf> = Vec::new();
    for p in plan {
        if !matches!(p.action, Action::Clear) {
            continue;
        }
        let Some(path) = &p.path else { continue };
        let candidate_c = canonicalize_or_self(path);
        if candidate_c == root_c {
            return Err(format!(
                "error: refusing to delete configured app_data root itself ({}); --app-data-root may be malformed",
                path.display()
            ));
        }
        if cleared_paths.iter().any(|prev| prev == &candidate_c) {
            return Err(format!(
                "error: refusing to clear the same path twice ({}); --app-data-root may be malformed",
                path.display()
            ));
        }
        cleared_paths.push(candidate_c);
    }
    Ok(())
}

pub(super) fn canonicalize_or_self(p: &Path) -> PathBuf {
    fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_socket_source_covers_only_the_files_flezha_creates() {
        for ours in [
            "runner_control_channel.sock",
            "primary_stream_runner-7.sock",
            "secondary_stream_runner-7.sock",
        ] {
            assert!(Source::SocketLadder.covers(OsStr::new(ours)), "{}", ours);
        }
        for theirs in ["notes.txt", "other.sock", "runner_control_channel.sock.bak"] {
            assert!(
                !Source::SocketLadder.covers(OsStr::new(theirs)),
                "{}",
                theirs
            );
            // The data roots are subdirectories this command owns, so every
            // entry there is in scope.
            assert!(Source::AppData("state").covers(OsStr::new(theirs)));
        }
    }

    #[test]
    fn preflight_guard_refuses_when_root_equals_candidate() {
        // Set up: app_data_root canonicalises to one of its candidate
        // subdir paths (via a self-symlink loop) — should refuse.
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let base =
                std::env::temp_dir().join(format!("flz-reset-preflight-{}", std::process::id()));
            let _ = fs::remove_dir_all(&base);
            fs::create_dir_all(&base).unwrap();
            // app_data_root = base, candidate "state" -> symlink back to base
            symlink(&base, base.join("state")).unwrap();

            let plan = vec![Root::planned(
                Source::AppData("state"),
                Some(base.join("state")),
                Action::Clear,
            )];
            let err = preflight_guard(&plan, &base).unwrap_err();
            assert!(err.contains("refusing"));
            let _ = fs::remove_dir_all(&base);
        }
    }
}
