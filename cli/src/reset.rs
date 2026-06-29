use std::io::{self, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process::{exit, Command, Stdio};
use std::time::{Duration, Instant};
use std::{env, fs};

use crate::utils::{app_data_root, get_flezha_command};

#[derive(Clone, Copy)]
enum ClearMode {
    Default,
    OptIn(&'static str),
}

// Update this table when a new durable runtime store lands under
// `app_data/`. Order is the order of output.
const CLEARABLE_ROOTS: &[(&str, ClearMode)] = &[
    ("state", ClearMode::Default),
    ("process_hub", ClearMode::Default),
    ("certs", ClearMode::OptIn("--include-certs")),
    ("sockets", ClearMode::OptIn("--include-sockets")),
];

const DEFAULT_PROBE_TIMEOUT_MS: u64 = 1_000;

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
    Keep(&'static str),
}

struct PlannedRoot {
    path: PathBuf,
    action: Action,
}

struct RootOutcome {
    path: PathBuf,
    action: Action,
    files: u64,
    bytes: u64,
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

    // Live-node guard: refuse if the Controller is up.
    if controller_is_up(flags.probe_timeout_ms) {
        eprintln!(
            "flz reset refuses to run while the controller is up. Stop it first with: flz controller stop"
        );
        exit(4);
    }

    let started = Instant::now();
    let plan = build_plan(&flags);

    // Pre-flight: refuse if any planned clearance points at the configured
    // root itself, or at the same path as another planned root.
    if let Err(msg) = preflight_guard(&plan, &flags.app_data_root) {
        eprintln!("{}", msg);
        exit(1);
    }

    let apply = flags.yes && !flags.dry_run;
    let dry_run = !apply;

    let mut outcomes: Vec<RootOutcome> = Vec::with_capacity(plan.len());
    let mut first_error: Option<(PathBuf, io::Error)> = None;

    for planned in plan {
        match planned.action {
            Action::Keep(reason) => {
                outcomes.push(RootOutcome {
                    path: planned.path,
                    action: Action::Keep(reason),
                    files: 0,
                    bytes: 0,
                });
            }
            Action::Clear => {
                if first_error.is_some() {
                    // After the first error we stop; but we still record the
                    // remaining roots so the audit output is complete. Mark
                    // them as if untouched (0 files, 0 bytes, Keep with
                    // explanatory reason).
                    outcomes.push(RootOutcome {
                        path: planned.path,
                        action: Action::Keep("not reached: earlier error"),
                        files: 0,
                        bytes: 0,
                    });
                    continue;
                }

                let stats = match walk_for_stats(&planned.path) {
                    Ok(s) => s,
                    Err(e) => {
                        first_error = Some((planned.path.clone(), e));
                        outcomes.push(RootOutcome {
                            path: planned.path,
                            action: Action::Clear,
                            files: 0,
                            bytes: 0,
                        });
                        continue;
                    }
                };

                if apply {
                    if let Err(e) = clear_root(&planned.path, &flags.app_data_root) {
                        first_error = Some((planned.path.clone(), e));
                        outcomes.push(RootOutcome {
                            path: planned.path,
                            action: Action::Clear,
                            files: stats.0,
                            bytes: stats.1,
                        });
                        continue;
                    }
                }

                outcomes.push(RootOutcome {
                    path: planned.path,
                    action: Action::Clear,
                    files: stats.0,
                    bytes: stats.1,
                });
            }
        }
    }

    let duration_ms = started.elapsed().as_millis() as u64;

    if flags.json {
        print_json(&outcomes, duration_ms, dry_run);
    } else {
        print_human(&outcomes, duration_ms, dry_run);
    }

    if let Some((path, err)) = first_error {
        eprintln!("error: {} ({})", path.display(), err);
        // Distinguish "before any action" (3) from "partial" (2): if at least
        // one Clear root completed (non-zero files removed OR walk succeeded
        // and apply ran), treat as partial.
        let any_cleared = apply
            && outcomes
                .iter()
                .any(|o| matches!(o.action, Action::Clear) && (o.files > 0 || o.bytes > 0));
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

fn build_plan(flags: &Flags) -> Vec<PlannedRoot> {
    CLEARABLE_ROOTS
        .iter()
        .map(|(name, mode)| {
            let path = flags.app_data_root.join(name);
            let action = match mode {
                ClearMode::Default => Action::Clear,
                ClearMode::OptIn(flag_name) => {
                    let enabled = match *flag_name {
                        "--include-certs" => flags.include_certs,
                        "--include-sockets" => flags.include_sockets,
                        _ => false,
                    };
                    if enabled {
                        Action::Clear
                    } else {
                        Action::Keep(match *flag_name {
                            "--include-certs" => "use --include-certs to clear",
                            "--include-sockets" => "use --include-sockets to clear",
                            _ => "opt-in",
                        })
                    }
                }
            };
            let _ = name;
            PlannedRoot { path, action }
        })
        .collect()
}

fn preflight_guard(plan: &[PlannedRoot], app_data_root: &Path) -> Result<(), String> {
    let root_c = canonicalize_or_self(app_data_root);
    let mut cleared_paths: Vec<PathBuf> = Vec::new();
    for p in plan {
        if !matches!(p.action, Action::Clear) {
            continue;
        }
        let candidate_c = canonicalize_or_self(&p.path);
        if candidate_c == root_c {
            return Err(format!(
                "error: refusing to delete configured app_data root itself ({}); --app-data-root may be malformed",
                p.path.display()
            ));
        }
        if cleared_paths.iter().any(|prev| prev == &candidate_c) {
            return Err(format!(
                "error: refusing to clear the same path twice ({}); --app-data-root may be malformed",
                p.path.display()
            ));
        }
        cleared_paths.push(candidate_c);
    }
    Ok(())
}

fn canonicalize_or_self(p: &Path) -> PathBuf {
    fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
}

// Public-in-crate for unit tests.
pub(crate) fn assert_safe_to_unlink(
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

// Recursive walk that counts files (regular files + symlinks) and accumulates
// byte counts. A missing path returns (0, 0). Other errors propagate.
pub(crate) fn walk_for_stats(path: &Path) -> io::Result<(u64, u64)> {
    let mut files: u64 = 0;
    let mut bytes: u64 = 0;
    let mut stack: Vec<PathBuf> = vec![path.to_path_buf()];

    while let Some(p) = stack.pop() {
        let md = match fs::symlink_metadata(&p) {
            Ok(m) => m,
            Err(e) if e.kind() == ErrorKind::NotFound && p == path => {
                return Ok((0, 0));
            }
            Err(e) => return Err(e),
        };

        if md.file_type().is_symlink() {
            files += 1;
            bytes += md.len();
        } else if md.is_file() {
            files += 1;
            bytes += md.len();
        } else if md.is_dir() {
            // Do not count the directory itself; recurse into its entries.
            let read = fs::read_dir(&p);
            match read {
                Ok(iter) => {
                    for entry in iter {
                        let entry = entry?;
                        stack.push(entry.path());
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    Ok((files, bytes))
}

fn clear_root(path: &Path, app_data_root: &Path) -> io::Result<()> {
    let iter = match fs::read_dir(path) {
        Ok(it) => it,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };

    for entry in iter {
        let entry = entry?;
        let entry_path = entry.path();

        if let Err(msg) = assert_safe_to_unlink(&entry_path, app_data_root, path) {
            return Err(io::Error::new(ErrorKind::Other, msg));
        }

        let ft = entry.file_type()?;
        if ft.is_dir() {
            fs::remove_dir_all(&entry_path)?;
        } else {
            fs::remove_file(&entry_path)?;
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
fn controller_is_up(probe_timeout_ms: u64) -> bool {
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

fn print_human(outcomes: &[RootOutcome], duration_ms: u64, dry_run: bool) {
    let stdout = io::stdout();
    let mut h = stdout.lock();

    let cleared_verb = if dry_run { "would clear" } else { "cleared" };
    let kept_verb = if dry_run { "would keep" } else { "kept" };

    for o in outcomes {
        let display_path = format!("{}/", o.path.display());
        match &o.action {
            Action::Clear => {
                let _ = writeln!(
                    h,
                    "{:11} {:48} ({} files, {})",
                    cleared_verb,
                    display_path,
                    o.files,
                    format_bytes(o.bytes)
                );
            }
            Action::Keep(reason) => {
                let _ = writeln!(h, "{:11} {:48} ({})", kept_verb, display_path, reason);
            }
        }
    }
    let _ = writeln!(h, "reset complete in {} ms", duration_ms);
}

fn print_json(outcomes: &[RootOutcome], duration_ms: u64, dry_run: bool) {
    let mut cleared: Vec<String> = Vec::new();
    let mut kept: Vec<String> = Vec::new();

    for o in outcomes {
        match &o.action {
            Action::Clear => cleared.push(format!(
                "{{\"root\":{},\"files\":{},\"bytes\":{}}}",
                json_str(&format!("{}", o.path.display())),
                o.files,
                o.bytes
            )),
            Action::Keep(reason) => kept.push(format!(
                "{{\"root\":{},\"reason\":{}}}",
                json_str(&format!("{}", o.path.display())),
                json_str(reason)
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
    println!("        app_data/state/         (per-Segment WAL/snapshot/outbox)");
    println!("        app_data/process_hub/   (ProcessHub durable registry)");
    println!();
    println!("    Opt-in clears (off by default):");
    println!("        app_data/certs/         (--include-certs)");
    println!("        app_data/sockets/       (--include-sockets)");
    println!();
    println!("OPTIONS:");
    println!("    --yes                     Apply the reset. REQUIRED for any destructive action.");
    println!(
        "    --dry-run                 Print the plan without touching the filesystem (exit 0)."
    );
    println!("    --include-certs           Also clear app_data/certs/");
    println!("    --include-sockets         Also clear app_data/sockets/");
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

    #[test]
    fn walk_for_stats_handles_missing_path() {
        let p = std::env::temp_dir().join(format!("flz-reset-missing-{}", std::process::id()));
        let _ = fs::remove_dir_all(&p);
        assert_eq!(walk_for_stats(&p).unwrap(), (0, 0));
    }

    #[test]
    fn walk_for_stats_counts_files_and_bytes() {
        let tmp = std::env::temp_dir().join(format!("flz-reset-walk-{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();
        fs::create_dir_all(tmp.join("nested")).unwrap();
        std::fs::write(tmp.join("a.bin"), vec![0u8; 10]).unwrap();
        std::fs::write(tmp.join("nested/b.bin"), vec![0u8; 25]).unwrap();
        let (files, bytes) = walk_for_stats(&tmp).unwrap();
        assert_eq!(files, 2);
        assert_eq!(bytes, 35);
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

            let plan = vec![PlannedRoot {
                path: base.join("state"),
                action: Action::Clear,
            }];
            let err = preflight_guard(&plan, &base).unwrap_err();
            assert!(err.contains("refusing"));
            let _ = fs::remove_dir_all(&base);
        }
    }
}
