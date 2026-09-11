//! What `flz reset` may clear, where each root lives, and what probing it found.

use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use flz_transport::utility::socket_dir::{self, Role};
use flz_transport::utility::socket_probe::{diagnose, Diagnosis, Reach};

use super::{Flags, INCLUDE_CERTS, INCLUDE_DLQ, INCLUDE_SCHEDULES, INCLUDE_SOCKETS};

#[derive(Clone, Copy)]
pub(super) enum ClearMode {
    /// In scope on every run.
    Default,
    /// In scope only when the operator passes this flag, which is also how the
    /// root is reported when they did not. The table names the flag and the
    /// field that carries it together, so the two cannot drift apart.
    OptIn(&'static str, fn(&Flags) -> bool),
    /// Never in scope, whatever the flags. Still reported, so a run names every
    /// root it left behind.
    Never,
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
    /// A subdirectory of the directory this command runs in, for the row whose
    /// default is not under the data root: the log file's, `logs/flezha.log`.
    /// The data root does not move it, for the Controller or for this command.
    /// None of its entries is this command's: a directory named relative to
    /// wherever the command runs may hold anything.
    WorkingDir(&'static str),
    /// The shared socket-directory ladder — the one implementation the
    /// Controller, every runner and `flz runner check` reach. Its answer is
    /// frequently not under `app_data/` at all, so only the socket files
    /// Flezha itself creates are unlinked there.
    SocketLadder,
}

impl Source {
    /// The subtree name this source is declared under in
    /// `test/fixtures/layout_rows.txt`. Only the tests read it: the run itself
    /// names a root by its path, not by its row.
    #[cfg(test)]
    fn row_name(self) -> &'static str {
        match self {
            Source::AppData(name) | Source::WorkingDir(name) => name,
            Source::SocketLadder => "sockets",
        }
    }

    pub(super) fn covers(self, file_name: &OsStr) -> bool {
        match self {
            Source::AppData(_) => true,
            Source::WorkingDir(_) => false,
            Source::SocketLadder => file_name.to_str().is_some_and(socket_dir::is_flezha_socket),
        }
    }
}

/// What the socket root is called when it has no path: the ladder was not
/// asked, because a run that was not asked about sockets must not establish a
/// directory, or it was asked and refused.
const UNRESOLVED_SOCKET_ROOT: &str = "socket directory";

// Every row of the layout and what `flz reset` does with it, in the order it
// reports them. This is the shell's half of one declaration:
// `test/fixtures/layout_rows.txt` carries the whole subtree list with each
// row's reset mode, `Flezha.Layout.Table` is the controller's half, and
// `agrees_with_the_shared_layout_rows` below holds this table equal to the
// fixture so the two cannot drift.
const ROOTS: &[(Source, ClearMode)] = &[
    (Source::AppData("state"), ClearMode::Default),
    (Source::AppData("process_hub"), ClearMode::Default),
    (
        Source::AppData("certs"),
        ClearMode::OptIn(INCLUDE_CERTS.long, |f| f.include_certs),
    ),
    (
        Source::AppData("dlq"),
        ClearMode::OptIn(INCLUDE_DLQ.long, |f| f.include_dlq),
    ),
    (
        Source::AppData("schedules"),
        ClearMode::OptIn(INCLUDE_SCHEDULES.long, |f| f.include_schedules),
    ),
    (
        Source::SocketLadder,
        ClearMode::OptIn(INCLUDE_SOCKETS.long, |f| f.include_sockets),
    ),
    // An operator's tokens, the audit trail, installed plugins and the logs
    // outlive a reset: an audit trail one command can erase is a weaker one.
    (Source::AppData("auth"), ClearMode::Never),
    (Source::AppData("audit"), ClearMode::Never),
    (Source::AppData("plugins"), ClearMode::Never),
    (Source::WorkingDir("logs"), ClearMode::Never),
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
    ROOTS
        .iter()
        .map(
            |&(source, mode)| match (source, out_of_scope(mode, flags)) {
                (Source::AppData(name), kept) => Root::planned(
                    source,
                    Some(flags.app_data_root.join(name)),
                    kept.unwrap_or(Action::Clear),
                ),
                (Source::WorkingDir(name), kept) => Root::planned(
                    source,
                    Some(PathBuf::from(name)),
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
        ClearMode::Never => Some(Action::Keep("not reset's to clear".into())),
    }
}

/// The socket directory this fleet's sockets are in, and what probing it
/// found.
///
/// `Role::Runner` because that is the question: which directory holds them.
/// The controller walk answers a different one — where could a controller
/// establish a directory — and it answers it by **creating** the rung it
/// settles on and by stepping past a rung a peer is listening in. Neither is
/// this command's to do: it may not establish a directory it was not given,
/// and the rung a controller answers in is the rung it must report on.
fn plan_socket_root() -> Root {
    // Resolve and probe in one step, the way the runner's pre-retry diagnosis
    // and `flz runner check` do, and never as two calls that could name two
    // directories.
    let Diagnosis { dir, reach } = match diagnose(Role::Runner, None) {
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
            // entry there is in scope; one named relative to wherever it runs
            // is not its to empty.
            assert!(Source::AppData("state").covers(OsStr::new(theirs)));
            assert!(!Source::WorkingDir("logs").covers(OsStr::new(theirs)));
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

    /// The shell and the controller declare one layout. This reads the shared
    /// fixture and holds `ROOTS` equal to it: every row, with its mode, in the
    /// fixture's order.
    #[test]
    fn agrees_with_the_shared_layout_rows() {
        let fixture = include_str!("../../../test/fixtures/layout_rows.txt");

        let declared: Vec<(&str, &str)> = fixture
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(|line| {
                let (name, mode) = line.split_once('|').expect("row is `<name>|<reset mode>`");
                (name, mode)
            })
            .collect();
        assert!(
            declared.len() >= 10,
            "fixture read as {} rows",
            declared.len()
        );

        let table: Vec<(&str, &str)> = ROOTS
            .iter()
            .map(|(source, mode)| {
                let word = match mode {
                    ClearMode::Default => "default",
                    ClearMode::OptIn(_, _) => "flag",
                    ClearMode::Never => "never",
                };
                (source.row_name(), word)
            })
            .collect();

        assert_eq!(
            table, declared,
            "reset's rows disagree with the layout table"
        );
    }

    fn plain_flags() -> Flags {
        Flags {
            yes: true,
            dry_run: false,
            include_certs: false,
            include_dlq: false,
            include_schedules: false,
            include_sockets: false,
            json: false,
            probe_timeout_ms: 0,
            app_data_root: PathBuf::from("/nonexistent/app_data"),
        }
    }

    fn planned<'a>(plan: &'a [Root], path: &Path) -> &'a Root {
        plan.iter()
            .find(|r| r.path.as_deref() == Some(path))
            .unwrap_or_else(|| panic!("{} is kept and the report does not name it", path.display()))
    }

    /// Where a run reports each row no flag reaches. The log file's default,
    /// `logs/flezha.log`, is not under the data root, so its row is not either.
    fn never_cleared(flags: &Flags) -> [PathBuf; 4] {
        let root = &flags.app_data_root;
        [
            root.join("auth"),
            root.join("audit"),
            root.join("plugins"),
            PathBuf::from("logs"),
        ]
    }

    /// Parked records and schedules are runtime data an operator resetting a
    /// box usually wants gone, and data someone may have kept deliberately:
    /// reset reaches each only through its own flag.
    #[test]
    fn the_dlq_and_the_schedules_are_cleared_only_by_their_own_flags() {
        for (name, flag) in [
            ("dlq", "--include-dlq"),
            ("schedules", "--include-schedules"),
        ] {
            let mode = ROOTS
                .iter()
                .find(|(source, _)| matches!(source, Source::AppData(n) if *n == name))
                .map(|&(_, mode)| mode)
                .unwrap_or_else(|| panic!("{} is a row reset cannot reach", name));
            assert!(
                matches!(mode, ClearMode::OptIn(f, _) if f == flag),
                "{} is not cleared by {}",
                name,
                flag
            );
        }
    }

    #[test]
    fn a_plain_reset_names_every_store_it_keeps() {
        let flags = plain_flags();
        let plan = build_plan(&flags);
        for (name, reason) in [
            ("dlq", "use --include-dlq to clear"),
            ("schedules", "use --include-schedules to clear"),
        ] {
            match &planned(&plan, &flags.app_data_root.join(name)).action {
                Action::Keep(said) => assert_eq!(said, reason),
                Action::Clear => panic!("a plain reset would clear {}", name),
            }
        }
    }

    /// The stores no flag reaches are reported too, so a run names everything
    /// it left behind rather than only what it could have cleared.
    #[test]
    fn a_plain_reset_names_the_stores_that_are_not_its_to_clear() {
        let flags = plain_flags();
        let plan = build_plan(&flags);
        for path in never_cleared(&flags) {
            match &planned(&plan, &path).action {
                Action::Keep(said) => assert_eq!(said, "not reset's to clear"),
                Action::Clear => panic!("a reset would clear {}", path.display()),
            }
        }
    }

    /// Every flag `--all` sets, sockets aside: asking the ladder would make
    /// this a case about the machine it runs on.
    #[test]
    fn the_include_flags_bring_the_dlq_and_the_schedules_into_scope() {
        let flags = Flags {
            include_certs: true,
            include_dlq: true,
            include_schedules: true,
            ..plain_flags()
        };
        let plan = build_plan(&flags);
        for name in ["certs", "dlq", "schedules"] {
            assert!(
                matches!(
                    planned(&plan, &flags.app_data_root.join(name)).action,
                    Action::Clear
                ),
                "{} is kept although its flag was given",
                name
            );
        }
        for path in never_cleared(&flags) {
            assert!(
                matches!(planned(&plan, &path).action, Action::Keep(_)),
                "{} is never reset's to clear, whatever the flags",
                path.display()
            );
        }
    }
}
