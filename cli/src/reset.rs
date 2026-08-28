use std::ffi::OsStr;
use std::io::{self, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process::{exit, Command, Stdio};
use std::time::{Duration, Instant};
use std::{env, fs};

use flz_transport::utility::path_helper::{self, Diagnosis, Reach, Role};

use crate::utils::{app_data_root, get_flezha_command};

#[derive(Clone, Copy)]
enum ClearMode {
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
enum Source {
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
    fn covers(self, file_name: &OsStr) -> bool {
        match self {
            Source::AppData(_) => true,
            Source::SocketLadder => file_name
                .to_str()
                .is_some_and(path_helper::is_flezha_socket),
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

pub(crate) const DEFAULT_PROBE_TIMEOUT_MS: u64 = 1_000;

#[cfg_attr(test, derive(Debug))]
struct Flags {
    yes: bool,
    dry_run: bool,
    include_certs: bool,
    include_sockets: bool,
    json: bool,
    probe_timeout_ms: u64,
    app_data_root: PathBuf,
}

enum Action {
    Clear,
    Keep(String),
}

/// One inspected root: what the plan decided, and what the run found there.
struct Root {
    /// `None` where the ladder was not asked, or could not answer.
    path: Option<PathBuf>,
    /// The rung that produced the path. `None` where the path was joined onto
    /// the data root and its source is not in question.
    rung: Option<&'static str>,
    source: Source,
    action: Action,
    files: u64,
    bytes: u64,
}

impl Root {
    fn planned(source: Source, path: Option<PathBuf>, action: Action) -> Self {
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
    fn name(&self) -> String {
        match &self.path {
            Some(path) => path.display().to_string(),
            None => UNRESOLVED_SOCKET_ROOT.to_string(),
        }
    }

    /// How the root reads to an operator. The socket root names the rung
    /// beside its path, so a run that cleared a runtime directory instead of
    /// `app_data/sockets/` shows the ladder answering rather than the command
    /// going wrong.
    fn label(&self) -> String {
        match (&self.path, self.rung) {
            (Some(path), Some(rung)) => format!("{}/ (from {})", path.display(), rung),
            (Some(path), None) => format!("{}/", path.display()),
            (None, _) => self.name(),
        }
    }
}

pub fn handle_reset_command(args: &[String]) {
    for arg in args {
        if arg == "--help" || arg == "-h" || arg == "help" {
            print_reset_help();
            return;
        }
    }

    let flags = match parse_flags(args) {
        Ok(f) => f,
        Err((code, msg)) => {
            eprintln!("{}", msg);
            exit(code);
        }
    };

    // Live-node guard: refuse if the Controller is up. It asks about THIS
    // Controller's node and is deliberately not widened to cover a colocated
    // peer: a peer is a different node with its own durable state, and that
    // state is not this reset's to protect. What a peer shares is the socket
    // directory, and that is guarded in `plan_socket_root`, where it is
    // touched.
    if controller_is_up(flags.probe_timeout_ms) {
        eprintln!(
            "flz reset refuses to run while the controller is up. Stop it first with: flz controller stop"
        );
        exit(4);
    }

    let started = Instant::now();
    let mut roots = build_plan(&flags);

    // Pre-flight: refuse if any planned clearance points at the configured
    // root itself, or at the same path as another planned root.
    if let Err(msg) = preflight_guard(&roots, &flags.app_data_root) {
        eprintln!("{}", msg);
        exit(1);
    }

    let apply = flags.yes && !flags.dry_run;
    let dry_run = !apply;

    let mut first_error: Option<(PathBuf, io::Error)> = None;

    for root in &mut roots {
        if !matches!(root.action, Action::Clear) {
            continue;
        }
        if first_error.is_some() {
            // After the first error we stop; the remaining roots are still
            // recorded so the audit output is complete.
            root.action = Action::Keep("not reached: earlier error".into());
            continue;
        }
        // A clearable root always has a path: the planner keeps every root it
        // could not resolve.
        let Some(path) = root.path.clone() else {
            continue;
        };

        let (result, files, bytes) =
            measure_and_clear(&path, root.source, &flags.app_data_root, apply);
        root.files = files;
        root.bytes = bytes;
        if let Err(e) = result {
            first_error = Some((path, e));
        }
    }

    let duration_ms = started.elapsed().as_millis() as u64;

    if flags.json {
        print_json(&roots, duration_ms, dry_run);
    } else {
        print_human(&roots, duration_ms, dry_run);
    }

    if let Some((path, err)) = first_error {
        eprintln!("error: {} ({})", path.display(), err);
        // Distinguish "before any action" (3) from "partial" (2): if at least
        // one Clear root completed (non-zero files removed OR walk succeeded
        // and apply ran), treat as partial.
        let any_cleared = apply
            && roots
                .iter()
                .any(|r| matches!(r.action, Action::Clear) && (r.files > 0 || r.bytes > 0));
        if any_cleared {
            exit(2);
        } else {
            exit(3);
        }
    }

    if !apply {
        // Plan-only mode. `--dry-run` exits 0; bare invocation exits 1.
        if flags.dry_run {
            exit(0);
        } else {
            eprintln!(
                "dry-run mode (no --yes given); pass --yes to apply or --dry-run to preview without an error code"
            );
            exit(1);
        }
    }

    exit(0);
}

fn parse_flags(args: &[String]) -> Result<Flags, (i32, String)> {
    let mut yes = false;
    let mut dry_run = false;
    let mut include_certs = false;
    let mut include_sockets = false;
    let mut all = false;
    let mut json = false;
    let mut probe_timeout_ms = DEFAULT_PROBE_TIMEOUT_MS;

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        match arg.as_str() {
            "--yes" => yes = true,
            "--dry-run" => dry_run = true,
            "--include-certs" => include_certs = true,
            "--include-sockets" => include_sockets = true,
            "--all" => all = true,
            "--json" => json = true,
            "--probe-timeout-ms" => {
                if i + 1 >= args.len() {
                    return Err((
                        1,
                        "error: --probe-timeout-ms requires a value (milliseconds)".into(),
                    ));
                }
                probe_timeout_ms = args[i + 1].parse().map_err(|_| {
                    (
                        1,
                        format!("error: invalid --probe-timeout-ms value '{}'", args[i + 1]),
                    )
                })?;
                i += 1;
            }
            "--app-data-root" => {
                if i + 1 >= args.len() {
                    return Err((1, "error: --app-data-root requires a path".into()));
                }
                i += 1; // value consumed by app_data_root() below
            }
            other if other.starts_with("--probe-timeout-ms=") => {
                let v = &other["--probe-timeout-ms=".len()..];
                probe_timeout_ms = v.parse().map_err(|_| {
                    (
                        1,
                        format!("error: invalid --probe-timeout-ms value '{}'", v),
                    )
                })?;
            }
            other if other.starts_with("--app-data-root=") => { /* value picked up below */ }
            other => {
                return Err((1, format!("error: unknown argument '{}'", other)));
            }
        }
        i += 1;
    }

    if yes && dry_run {
        return Err((
            1,
            "error: --yes and --dry-run are mutually exclusive".into(),
        ));
    }

    if all {
        include_certs = true;
        include_sockets = true;
    }

    Ok(Flags {
        yes,
        dry_run,
        include_certs,
        include_sockets,
        json,
        probe_timeout_ms,
        app_data_root: app_data_root(args),
    })
}

fn build_plan(flags: &Flags) -> Vec<Root> {
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
    let Diagnosis { dir, reach } = match path_helper::diagnose(Role::Controller, None) {
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

fn preflight_guard(plan: &[Root], app_data_root: &Path) -> Result<(), String> {
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

fn canonicalize_or_self(p: &Path) -> PathBuf {
    fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
}

fn assert_safe_to_unlink(
    entry: &Path,
    app_data_root: &Path,
    cleared_root: &Path,
) -> Result<(), String> {
    let entry_c = canonicalize_or_self(entry);
    let root_c = canonicalize_or_self(app_data_root);
    let cleared_c = canonicalize_or_self(cleared_root);
    if entry_c == root_c {
        return Err(format!(
            "refusing to unlink configured app_data root itself: {}",
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

// What this command does to one root: measure the entries the plan covers, and
// unlink them when applying. One entry list serves both, so the preview cannot
// describe anything but what the apply would remove. The measurement stands
// even when the unlink fails partway — it says what the plan covered, and the
// exit code reads it.
fn measure_and_clear(
    root: &Path,
    source: Source,
    app_data_root: &Path,
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
        unlink_all(&entries, app_data_root, root)
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
// summary that says "0 files" over two unlinked sockets tells the operator the
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

fn unlink_all(entries: &[PathBuf], app_data_root: &Path, root: &Path) -> io::Result<()> {
    for entry_path in entries {
        if let Err(msg) = assert_safe_to_unlink(entry_path, app_data_root, root) {
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

// Live-node probe.
//
// In production, runs `flzvm rpc 'Flezha.Service.CliManager.controller_status("text")'`
// with a configurable timeout and treats exit-0 with non-empty stdout as
// "up" — any other outcome (no node, timeout, distribution failure) is
// "not up" so the reset proceeds.
//
// `FLEZHA_RESET_PROBE_OVERRIDE` (test-only knob) short-circuits the probe:
//   "up"   → return true
//   "down" → return false
// Shared live-node probe (also `flz auth rebootstrap`'s guard): a successful
// controller_status RPC within the timeout means the node is up. The
// FLEZHA_RESET_PROBE_OVERRIDE env hook serves every caller's tests.
pub(crate) fn controller_is_up(probe_timeout_ms: u64) -> bool {
    if let Ok(v) = env::var("FLEZHA_RESET_PROBE_OVERRIDE") {
        match v.as_str() {
            "up" => return true,
            "down" => return false,
            _ => {}
        }
    }

    let command = get_flezha_command();
    let rpc_command = "Flezha.Service.CliManager.controller_status(\"text\")";

    let mut child = match Command::new(&command)
        .arg("rpc")
        .arg(rpc_command)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(c) => c,
        Err(_) => return false,
    };

    let timeout = Duration::from_millis(probe_timeout_ms);
    let started = Instant::now();

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    return false;
                }
                let mut out = String::new();
                if let Some(mut stdout) = child.stdout.take() {
                    let _ = std::io::Read::read_to_string(&mut stdout, &mut out);
                }
                return !out.trim().is_empty();
            }
            Ok(None) => {
                if started.elapsed() >= timeout {
                    let _ = child.kill();
                    let _ = child.wait();
                    return false;
                }
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(_) => return false,
        }
    }
}

fn print_human(roots: &[Root], duration_ms: u64, dry_run: bool) {
    let stdout = io::stdout();
    let mut h = stdout.lock();

    let cleared_verb = if dry_run { "would clear" } else { "cleared" };
    let kept_verb = if dry_run { "would keep" } else { "kept" };

    for r in roots {
        match &r.action {
            Action::Clear => {
                let _ = writeln!(
                    h,
                    "{:11} {:48} ({} files, {})",
                    cleared_verb,
                    r.label(),
                    r.files,
                    format_bytes(r.bytes)
                );
            }
            Action::Keep(reason) => {
                let _ = writeln!(h, "{:11} {:48} ({})", kept_verb, r.label(), reason);
            }
        }
    }
    let _ = writeln!(h, "reset complete in {} ms", duration_ms);
}

fn print_json(roots: &[Root], duration_ms: u64, dry_run: bool) {
    let mut cleared: Vec<String> = Vec::new();
    let mut kept: Vec<String> = Vec::new();

    for r in roots {
        // The rung rides beside the pinned fields, never in place of one: only
        // the socket root has a source in question, so only it carries it.
        let rung = match r.rung {
            Some(rung) => format!(",\"rung\":{}", json_str(rung)),
            None => String::new(),
        };
        let root = json_str(&r.name());

        match &r.action {
            Action::Clear => cleared.push(format!(
                "{{\"root\":{},\"files\":{},\"bytes\":{}{}}}",
                root, r.files, r.bytes, rung
            )),
            Action::Keep(reason) => kept.push(format!(
                "{{\"root\":{},\"reason\":{}{}}}",
                root,
                json_str(reason),
                rung
            )),
        }
    }

    let body = format!(
        "{{\"cleared\":[{}],\"kept\":[{}],\"duration_ms\":{},\"dry_run\":{}}}",
        cleared.join(","),
        kept.join(","),
        duration_ms,
        dry_run
    );

    let stdout = io::stdout();
    let mut h = stdout.lock();
    let _ = writeln!(h, "{}", body);
}

fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn format_bytes(n: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;

    if n >= GIB {
        format!("{:.1} GiB", n as f64 / GIB as f64)
    } else if n >= MIB {
        format!("{:.1} MiB", n as f64 / MIB as f64)
    } else if n >= KIB {
        format!("{:.1} KiB", n as f64 / KIB as f64)
    } else {
        format!("{} B", n)
    }
}

pub fn print_reset_help() {
    println!("flz reset - Clear Controller durable runtime data on disk");
    println!();
    println!("USAGE:");
    println!("    flz reset [OPTIONS]");
    println!();
    println!("DESCRIPTION:");
    println!("    Clears the contents of the Controller's durable on-disk state so the");
    println!("    next boot starts from a clean slate. Refuses to run while the");
    println!("    Controller is up; stop it first with `flz controller stop`.");
    println!();
    println!("    By default clears:");
    println!("        app_data/state/         (per-Entity WAL/snapshot/outbox)");
    println!("        app_data/process_hub/   (ProcessHub durable registry)");
    println!();
    println!("    Opt-in clears (off by default):");
    println!("        app_data/certs/         (--include-certs)");
    println!("        <socket directory>      (--include-sockets)");
    println!();
    println!("    The socket directory is resolved through the same ladder the Controller");
    println!("    and its runners walk -- $FLEZHA_SOCKET_FILES_PATH, else");
    println!("    $XDG_RUNTIME_DIR/flezha, else /run/flezha, else app_data/sockets -- so it");
    println!("    is frequently not under app_data/ at all. Only the socket files Flezha");
    println!("    itself creates are unlinked there, and a directory a Controller is still");
    println!("    serving in is kept.");
    println!();
    println!("OPTIONS:");
    println!("    --yes                     Apply the reset. REQUIRED for any destructive action.");
    println!(
        "    --dry-run                 Print the plan without touching the filesystem (exit 0)."
    );
    println!("    --include-certs           Also clear app_data/certs/");
    println!(
        "    --include-sockets         Also clear Flezha's sockets in the resolved socket directory"
    );
    println!("    --all                     Equivalent to --include-certs --include-sockets");
    println!("    --app-data-root <PATH>    Override the `app_data/` root (else FLEZHA_APP_DATA_ROOT or ./app_data)");
    println!(
        "    --probe-timeout-ms <N>    Live-node probe timeout (default {})",
        DEFAULT_PROBE_TIMEOUT_MS
    );
    println!("    --json                    Machine-readable output");
    println!("    --help, -h, help          Show this help message");
    println!();
    println!("EXIT CODES:");
    println!("    0   Success (apply succeeded, or --dry-run preview)");
    println!("    1   Invalid usage (missing --yes without --dry-run, conflicting flags,");
    println!("        malformed --app-data-root, refusal to delete the root itself)");
    println!("    2   Partial success (some roots cleared, then a filesystem error)");
    println!("    3   I/O error before any action");
    println!("    4   Refused: Controller is up");
    println!();
    println!("EXAMPLES:");
    println!("    flz reset                          # plan only, exit 1");
    println!("    flz reset --dry-run                # plan only, exit 0");
    println!("    flz reset --yes                    # default scope (state, process_hub)");
    println!("    flz reset --yes --all              # also clear certs and sockets");
    println!("    flz reset --yes --json             # machine-readable output");
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::*;

    fn args(vs: &[&str]) -> Vec<String> {
        vs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parse_flags_yes_and_dry_run_conflict() {
        let err = parse_flags(&args(&["--yes", "--dry-run"])).unwrap_err();
        assert_eq!(err.0, 1);
        assert!(err.1.contains("mutually exclusive"));
    }

    #[test]
    fn parse_flags_unknown_arg_rejected() {
        let err = parse_flags(&args(&["--nope"])).unwrap_err();
        assert_eq!(err.0, 1);
        assert!(err.1.contains("unknown argument"));
    }

    #[test]
    fn parse_flags_all_implies_includes() {
        let f = parse_flags(&args(&["--yes", "--all"])).unwrap();
        assert!(f.include_certs);
        assert!(f.include_sockets);
    }

    #[test]
    fn parse_flags_probe_timeout_value() {
        let f = parse_flags(&args(&["--probe-timeout-ms", "250"])).unwrap();
        assert_eq!(f.probe_timeout_ms, 250);
        let f = parse_flags(&args(&["--probe-timeout-ms=400"])).unwrap();
        assert_eq!(f.probe_timeout_ms, 400);
    }

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
    fn the_socket_root_measures_and_clears_the_same_entries() {
        let tmp = std::env::temp_dir().join(format!("flz-reset-sockets-{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();
        // Real socket inodes: a socket is neither a regular file nor a
        // directory, and the count has to see it anyway.
        for name in ["runner_control_channel.sock", "primary_stream_r1.sock"] {
            drop(std::os::unix::net::UnixListener::bind(tmp.join(name)).unwrap());
        }
        std::fs::write(tmp.join("operator-notes.txt"), vec![0u8; 99]).unwrap();

        let (result, files, _bytes) =
            measure_and_clear(&tmp, Source::SocketLadder, &tmp.join("app_data"), APPLY);
        assert!(result.is_ok());
        assert_eq!(files, 2, "the count is of what the unlink removed");

        assert!(!tmp.join("runner_control_channel.sock").exists());
        assert!(!tmp.join("primary_stream_r1.sock").exists());
        assert!(tmp.join("operator-notes.txt").exists());
        assert!(tmp.is_dir(), "the directory itself is never removed");

        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn format_bytes_units() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.0 KiB");
        assert_eq!(format_bytes(1024 * 1024), "1.0 MiB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GiB");
    }

    #[test]
    fn json_str_escapes_quotes_and_backslashes() {
        assert_eq!(json_str("a\"b"), "\"a\\\"b\"");
        assert_eq!(json_str("a\\b"), "\"a\\\\b\"");
        assert_eq!(json_str("plain"), "\"plain\"");
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
