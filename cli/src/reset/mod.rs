//! `flz reset` — clear the Controller's durable runtime data on disk.

mod clear;
mod plan;
mod report;

use std::io;
use std::path::PathBuf;
use std::process::exit;
use std::time::Instant;

use clear::measure_and_clear;
use plan::{build_plan, opt_in_flags, preflight_guard, Action, Root};
use report::{print_human, print_json};

use crate::args::{app_data_root, OptSpec, Reading, APP_DATA_ROOT, JSON_SWITCH, OUTPUT};
use crate::rpc::{controller_is_up, DEFAULT_PROBE_TIMEOUT_MS};

pub(crate) const YES: OptSpec = OptSpec::switch("--yes", "--yes");
pub(crate) const DRY_RUN: OptSpec = OptSpec::switch("--dry-run", "--dry-run");
const INCLUDE_CERTS: OptSpec = OptSpec::switch("--include-certs", "--include-certs");
const INCLUDE_DLQ: OptSpec = OptSpec::switch("--include-dlq", "--include-dlq");
const INCLUDE_SCHEDULES: OptSpec = OptSpec::switch("--include-schedules", "--include-schedules");
const INCLUDE_AUTH: OptSpec = OptSpec::switch("--include-auth", "--include-auth");
const INCLUDE_AUDIT: OptSpec = OptSpec::switch("--include-audit", "--include-audit");
const INCLUDE_PLUGINS: OptSpec = OptSpec::switch("--include-plugins", "--include-plugins");
const INCLUDE_SOCKETS: OptSpec = OptSpec::switch("--include-sockets", "--include-sockets");
const ALL: OptSpec = OptSpec::switch("--all", "--all");
const PROBE_TIMEOUT_MS: OptSpec =
    OptSpec::number("--probe-timeout-ms", "--probe-timeout-ms", "<ms>");

/// Everything `flz reset` reads. It is one of the two verbs that refuse a word
/// they do not recognise: this one deletes state, so a typed word it cannot act
/// on means the run about to happen is not the run that was asked for.
pub(crate) const OPTIONS: &[OptSpec] = &[
    YES,
    DRY_RUN,
    INCLUDE_CERTS,
    INCLUDE_DLQ,
    INCLUDE_SCHEDULES,
    INCLUDE_AUTH,
    INCLUDE_AUDIT,
    INCLUDE_PLUGINS,
    INCLUDE_SOCKETS,
    ALL,
    OUTPUT,
    JSON_SWITCH,
    PROBE_TIMEOUT_MS,
    APP_DATA_ROOT,
];

/// `--yes` applies and `--dry-run` previews; a run that was asked for both was
/// asked for neither.
pub(crate) const CONFLICTS: &[(&str, &str)] = &[(YES.long, DRY_RUN.long)];

#[cfg_attr(test, derive(Debug))]
struct Flags {
    yes: bool,
    dry_run: bool,
    /// The long spelling of every opt-in flag in effect.
    included: Vec<&'static str>,
    json: bool,
    probe_timeout_ms: u64,
    app_data_root: PathBuf,
}

/// The flags the walk read, in the shape the plan below wants them. `--all` is
/// every flag a row of the plan opts in by.
fn flags(read: &Reading) -> Flags {
    let all = read.switch(ALL.long);

    Flags {
        yes: read.switch(YES.long),
        dry_run: read.switch(DRY_RUN.long),
        included: opt_in_flags()
            .filter(|flag| all || read.switch(flag))
            .collect(),
        json: read.output_format() == "json",
        probe_timeout_ms: read
            .num::<u64>(PROBE_TIMEOUT_MS.long)
            .unwrap_or(DEFAULT_PROBE_TIMEOUT_MS),
        app_data_root: app_data_root(read),
    }
}

pub(crate) fn handle_reset_command(read: &Reading) {
    let flags = flags(read);

    // A preview changes nothing, so a live controller has nothing to be
    // protected from; `--yes` with it is refused before this runs.
    if !flags.dry_run {
        refuse_if_controller_is_up(&flags);
    }

    let started = Instant::now();
    let mut roots = build_plan(&flags);

    // Pre-flight: refuse if any planned clearance points at the configured
    // root itself, at the same path as another planned root, or onto, into or
    // around a root the run keeps.
    if let Err(msg) = preflight_guard(&roots, &flags.app_data_root) {
        eprintln!("{}", msg);
        exit(1);
    }

    let apply = flags.yes && !flags.dry_run;
    let first_error = run_plan(&mut roots, &flags, apply);
    let duration_ms = started.elapsed().as_millis() as u64;

    if flags.json {
        print_json(&roots, duration_ms, !apply);
    } else {
        print_human(&roots, duration_ms, !apply);
    }

    exit(verdict(&roots, &flags, apply, first_error));
}

/// The guard asks about THIS Controller's node and is deliberately not widened
/// to cover a colocated peer: a peer is a different node with its own durable
/// state, and that state is not this reset's to protect. What a peer shares is
/// the socket directory, and that is guarded in `plan_socket_root`, where it is
/// touched.
fn refuse_if_controller_is_up(flags: &Flags) {
    if controller_is_up(flags.probe_timeout_ms) {
        eprintln!(
            "flz reset refuses to run while the controller is up. Stop it first with: flz controller stop"
        );
        exit(4);
    }
}

/// Measure — and, when applying, clear — every root the plan marked. Answers
/// the first I/O error, which stops the run; the roots after it are still
/// recorded so the audit output is complete.
fn run_plan(roots: &mut [Root], flags: &Flags, apply: bool) -> Option<(PathBuf, io::Error)> {
    let mut first_error: Option<(PathBuf, io::Error)> = None;

    for root in roots.iter_mut() {
        if !matches!(root.action, Action::Clear) {
            continue;
        }
        if first_error.is_some() {
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

    first_error
}

/// The run's exit code, and the last thing it says.
fn verdict(
    roots: &[Root],
    flags: &Flags,
    apply: bool,
    first_error: Option<(PathBuf, io::Error)>,
) -> i32 {
    if let Some((path, err)) = first_error {
        eprintln!("error: {} ({})", path.display(), err);
        // Distinguish "before any action" (3) from "partial" (2): if at least
        // one Clear root completed (non-zero files removed OR walk succeeded
        // and apply ran), treat as partial.
        let any_cleared = apply
            && roots
                .iter()
                .any(|r| matches!(r.action, Action::Clear) && (r.files > 0 || r.bytes > 0));
        return if any_cleared { 2 } else { 3 };
    }

    // Plan-only mode. `--dry-run` exits 0; bare invocation exits 1.
    if !apply && !flags.dry_run {
        eprintln!(
            "dry-run mode (no --yes given); pass --yes to apply or --dry-run to preview without an error code"
        );
        return 1;
    }

    0
}
