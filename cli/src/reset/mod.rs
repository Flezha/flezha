//! `flz reset` — clear the Controller's durable runtime data on disk.

mod clear;
mod plan;
mod report;

use std::io;
use std::path::PathBuf;
use std::process::exit;
use std::time::Instant;

use clear::measure_and_clear;
use plan::{build_plan, preflight_guard, Action, Root};
use report::{print_human, print_json};

use crate::rpc::{controller_is_up, DEFAULT_PROBE_TIMEOUT_MS};
use crate::{args as cli_args, help};

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

pub fn handle_reset_command(args: &[String]) {
    if args
        .iter()
        .any(|a| a == "--help" || a == "-h" || a == "help")
    {
        help::render(help::RESET);
        return;
    }

    let flags = match parse_flags(args) {
        Ok(f) => f,
        Err((code, msg)) => {
            eprintln!("{}", msg);
            exit(code);
        }
    };

    refuse_if_controller_is_up(&flags);

    let started = Instant::now();
    let mut roots = build_plan(&flags);

    // Pre-flight: refuse if any planned clearance points at the configured
    // root itself, or at the same path as another planned root.
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

/// The live-node probe's deadline. Named because the walk below has to know
/// which word the reader above consumed.
const PROBE_TIMEOUT_MS: &str = "--probe-timeout-ms";

/// A boolean switch and the field it sets.
type Switch = (&'static str, fn(&mut Flags));

/// The boolean switches, and the field each one sets.
const SWITCHES: &[Switch] = &[
    ("--yes", |f| f.yes = true),
    ("--dry-run", |f| f.dry_run = true),
    ("--include-certs", |f| f.include_certs = true),
    ("--include-sockets", |f| f.include_sockets = true),
    ("--all", |f| {
        f.include_certs = true;
        f.include_sockets = true;
    }),
];

/// The walk stays a walk: an extractor reads the flags it is asked about and
/// says nothing about the rest, and refusing an argument nobody declared is
/// this command's whole safety story.
fn parse_flags(args: &[String]) -> Result<Flags, (i32, String)> {
    let mut flags = Flags {
        yes: false,
        dry_run: false,
        include_certs: false,
        include_sockets: false,
        // The output format is the CLI's one reader's answer here too, so
        // `--json` and `--output json` cannot mean different things.
        json: cli_args::parse_output_format(args).map_err(|refusal| (1, refusal))? == "json",
        probe_timeout_ms: cli_args::number(args, PROBE_TIMEOUT_MS, PROBE_TIMEOUT_MS)
            .map_err(|refusal| (1, refusal))?
            .unwrap_or(DEFAULT_PROBE_TIMEOUT_MS),
        app_data_root: cli_args::app_data_root(args),
    };

    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_str();

        if let Some((_, set)) = SWITCHES.iter().find(|(name, _)| *name == arg) {
            set(&mut flags);
        } else if arg == "--app-data-root"
            || arg == "--output"
            || arg == "-o"
            || arg == PROBE_TIMEOUT_MS
        {
            // The readers in the `Flags` literal above answer these, and
            // refuse a missing or unreadable value there. The walk only steps
            // over the word they consumed.
            i += 1;
        } else if !read_above(arg) {
            return Err((1, format!("error: unknown argument '{}'", arg)));
        }

        i += 1;
    }

    if flags.yes && flags.dry_run {
        return Err((
            1,
            "error: --yes and --dry-run are mutually exclusive".into(),
        ));
    }

    Ok(flags)
}

/// The arguments a reader above has already answered. The walk refuses
/// everything it does not recognise, so it has to know these are not nobody's.
fn read_above(arg: &str) -> bool {
    arg == "--json"
        || arg.starts_with("--output=")
        || arg.starts_with("-o=")
        || arg.starts_with("--app-data-root=")
        || arg.starts_with(&format!("{}=", PROBE_TIMEOUT_MS))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_flags_yes_and_dry_run_conflict() {
        let err = parse_flags(&cli_args::owned(&["--yes", "--dry-run"])).unwrap_err();
        assert_eq!(err.0, 1);
        assert!(err.1.contains("mutually exclusive"));
    }

    #[test]
    fn parse_flags_unknown_arg_rejected() {
        let err = parse_flags(&cli_args::owned(&["--nope"])).unwrap_err();
        assert_eq!(err.0, 1);
        assert!(err.1.contains("unknown argument"));
    }

    #[test]
    fn parse_flags_all_implies_includes() {
        let f = parse_flags(&cli_args::owned(&["--yes", "--all"])).unwrap();
        assert!(f.include_certs);
        assert!(f.include_sockets);
    }

    #[test]
    fn parse_flags_probe_timeout_value() {
        let f = parse_flags(&cli_args::owned(&["--probe-timeout-ms", "250"])).unwrap();
        assert_eq!(f.probe_timeout_ms, 250);
        let f = parse_flags(&cli_args::owned(&["--probe-timeout-ms=400"])).unwrap();
        assert_eq!(f.probe_timeout_ms, 400);
    }
}
