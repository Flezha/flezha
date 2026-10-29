mod common;

use std::fs;
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use common::{Reply, Shim};
use tempfile::TempDir;

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

const CONTROL_SOCKET: &str = "runner_control_channel.sock";
const STREAM_SOCKET: &str = "primary_stream_runner-1.sock";
/// A file in the socket directory that is not Flezha's to unlink.
const USER_FILE: &str = "user-notes.txt";

/// Every subtree of the data root the layout declares. The socket directory is
/// the ladder's, and is populated on its own.
const DATA_ROOTS: &[&str] = &[
    "state",
    "process_hub",
    "certs",
    "dlq",
    "schedules",
    "auth",
    "audit",
    "plugins",
];

/// The subtrees of the data root a plain run keeps, each with the flag that
/// clears it. `--all` sets every one.
const OPT_IN: &[(&str, &str)] = &[
    ("certs", "--include-certs"),
    ("dlq", "--include-dlq"),
    ("schedules", "--include-schedules"),
    ("auth", "--include-auth"),
    ("audit", "--include-audit"),
    ("plugins", "--include-plugins"),
];

/// The reason a root no flag reaches is reported kept for. The log directory
/// is the one such root.
const NOT_RESETS: &str = "not reset's to clear";

/// The log file's default, relative to the directory the command runs in.
const LOG_FILE: &str = "storage/logs/flezha.log";

/// How a run names the log file's directory: the default storage root's, which
/// `--storage-root` does not move.
const LOG_DIR: &str = "./storage/logs";

/// A controller that died hard: a socket file nothing is listening on. It has
/// to be a real bound socket — a regular file with the same name probes
/// `Unreachable` (ENOTSOCK), not stale.
fn stale_control_socket(dir: &Path) {
    fs::create_dir_all(dir).unwrap();
    let path = dir.join(CONTROL_SOCKET);
    drop(UnixListener::bind(&path).expect("bind"));
    await_refused(&path);
}

/// Dropping a `UnixListener` does not always kill it at once. A process a
/// sibling test forks between the `bind` and the `drop` inherits the listener
/// fd — `CLOEXEC` closes at `exec`, not at `fork` — so the socket this process
/// closed keeps accepting until that child execs. Without this wait the
/// command under test is occasionally handed a controller that answers, and
/// then keeps the socket directory it was asked to clear (KI-84).
fn await_refused(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while UnixStream::connect(path).is_ok() {
        assert!(
            Instant::now() < deadline,
            "{} still accepts connections: the fixture has not established a dead controller",
            path.display()
        );
        // A yield, not a sleep: the window closes as soon as the inheriting
        // child reaches `exec`, and nothing here is waiting on a duration.
        std::thread::yield_now();
    }
}

/// A socket directory as a controller leaves it, plus a file that is not ours.
fn populate_socket_dir(dir: &Path) {
    stale_control_socket(dir);
    drop(UnixListener::bind(dir.join(STREAM_SOCKET)).expect("bind"));
    fs::write(dir.join(USER_FILE), vec![0u8; 5]).unwrap();
}

/// `storage` is the root the command is given; `root` is `app/` under it,
/// where every store sits.
struct Fixture {
    _tmp: TempDir,
    storage: PathBuf,
    root: PathBuf,
}

impl Fixture {
    fn populated() -> Self {
        let tmp = TempDir::new().expect("tempdir");
        let storage = tmp.path().to_path_buf();
        let root = storage.join("app");
        for name in DATA_ROOTS {
            let sub = root.join(name);
            fs::create_dir_all(sub.join("nested")).unwrap();
            fs::write(sub.join("a.bin"), vec![0u8; 7]).unwrap();
            fs::write(sub.join("nested/b.bin"), vec![0u8; 13]).unwrap();
        }
        populate_socket_dir(&root.join("sockets"));
        Fixture {
            _tmp: tmp,
            storage,
            root,
        }
    }

    fn empty() -> Self {
        let tmp = TempDir::new().expect("tempdir");
        let storage = tmp.path().to_path_buf();
        let root = storage.join("app");
        fs::create_dir_all(&root).unwrap();
        Fixture {
            _tmp: tmp,
            storage,
            root,
        }
    }

    /// The socket root the ladder resolves for these cases: the explicit rung,
    /// pointed at the fixture's own directory, so a case about the roots is
    /// not also a case about which rung answered.
    fn socket_dir(&self) -> PathBuf {
        self.root.join("sockets")
    }

    fn cmd(&self) -> Command {
        let mut c = Command::new(FLZ_BIN);
        c.arg("reset")
            .arg("--storage-root")
            .arg(&self.storage)
            .env("FLEZHA_RESET_PROBE_OVERRIDE", "down")
            .env("FLEZHA_SOCKET_FILES_PATH", self.socket_dir());
        c
    }

    /// The same run with the explicit rung unset, so the ladder walks and the
    /// per-user runtime directory answers.
    fn ladder_cmd(&self, xdg: &Path) -> Command {
        let mut c = self.cmd();
        c.env_remove("FLEZHA_SOCKET_FILES_PATH")
            .env("XDG_RUNTIME_DIR", xdg);
        c
    }
}

fn dir_is_empty(p: &Path) -> bool {
    fs::read_dir(p).unwrap().next().is_none()
}

fn stdout_of(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn stderr_of(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn default_reset_clears_state_and_process_hub_and_keeps_every_other_root() {
    let f = Fixture::populated();
    let out = f.cmd().arg("--yes").output().unwrap();
    assert!(out.status.success(), "stderr: {}", stderr_of(&out));
    assert!(dir_is_empty(&f.root.join("state")));
    assert!(dir_is_empty(&f.root.join("process_hub")));
    for (name, _) in OPT_IN {
        assert!(!dir_is_empty(&f.root.join(name)), "{} needs its flag", name);
    }
    assert!(f.socket_dir().join(CONTROL_SOCKET).exists());
    // Root inode preserved.
    assert!(f.root.exists());
}

/// The report line naming `path` says it was kept, for `reason`.
fn assert_kept(text: &str, path: &str, reason: &str) {
    let label = format!("{}/", path);
    let line = text
        .lines()
        .find(|line| line.split_whitespace().nth(1) == Some(label.as_str()))
        .unwrap_or_else(|| panic!("{} is kept and not named:\n{}", label, text));
    assert!(
        line.starts_with("kept") && line.ends_with(&format!("({})", reason)),
        "{}",
        line
    );
}

/// `--all` reaches every root under the data root, the caller's tokens, the
/// audit trail and the installed plugins included. The log directory is named
/// relative to wherever the command runs, so the run owns a working directory
/// with a log in it.
#[test]
fn all_flag_clears_every_root() {
    let f = Fixture::populated();
    let cwd = TempDir::new().unwrap();
    let log = cwd.path().join(LOG_FILE);
    fs::create_dir_all(log.parent().unwrap()).unwrap();
    fs::write(&log, "a controller's log").unwrap();

    let out = f
        .cmd()
        .current_dir(cwd.path())
        .arg("--yes")
        .arg("--all")
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", stderr_of(&out));
    for name in DATA_ROOTS {
        assert!(dir_is_empty(&f.root.join(name)), "{} should be empty", name);
    }
    assert!(log.exists(), "the log directory is not reset's to clear");
    assert_kept(&stdout_of(&out), LOG_DIR, NOT_RESETS);
    assert!(!f.socket_dir().join(CONTROL_SOCKET).exists());
    assert!(!f.socket_dir().join(STREAM_SOCKET).exists());
}

#[test]
fn each_store_flag_clears_its_own_store_and_no_other() {
    for (cleared, flag) in OPT_IN {
        let f = Fixture::populated();
        let out = f.cmd().arg("--yes").arg(flag).output().unwrap();
        assert!(out.status.success(), "{}: {}", flag, stderr_of(&out));
        assert!(
            dir_is_empty(&f.root.join(cleared)),
            "{} left {}",
            flag,
            cleared
        );
        for (other, _) in OPT_IN.iter().filter(|(name, _)| name != cleared) {
            assert!(
                !dir_is_empty(&f.root.join(other)),
                "{} took {}",
                flag,
                other
            );
        }
    }
}

/// Every path a plain run keeps, with its reason. The log directory is named
/// where the log file's default puts it, `./storage/logs`, which the storage
/// root does not move.
fn kept_by_a_plain_run(f: &Fixture) -> Vec<(String, String)> {
    let mut kept: Vec<(String, String)> = OPT_IN
        .iter()
        .map(|(name, flag)| {
            let path = f.root.join(name).display().to_string();
            (path, format!("use {} to clear", flag))
        })
        .collect();
    kept.push((LOG_DIR.to_string(), NOT_RESETS.to_string()));
    kept
}

/// A plain run names every root it leaves behind and why, the ones it will
/// never clear included, so the report says what a reset did not reach.
#[test]
fn a_plain_reset_names_every_root_it_keeps() {
    let f = Fixture::populated();
    let out = f.cmd().arg("--yes").output().unwrap();
    assert!(out.status.success(), "stderr: {}", stderr_of(&out));
    let text = stdout_of(&out);

    for (path, reason) in kept_by_a_plain_run(&f) {
        assert_kept(&text, &path, &reason);
    }
}

#[test]
fn the_json_report_names_every_root_it_keeps() {
    let f = Fixture::populated();
    let out = f.cmd().arg("--dry-run").arg("--json").output().unwrap();
    assert!(out.status.success(), "stderr: {}", stderr_of(&out));
    let text = stdout_of(&out);

    for (path, reason) in kept_by_a_plain_run(&f) {
        let entry = format!("{{\"root\":\"{}\",\"reason\":\"{}\"}}", path, reason);
        assert!(text.contains(&entry), "{} is not in {}", entry, text);
    }
}

#[test]
fn unrelated_files_in_the_socket_directory_survive() {
    let f = Fixture::populated();
    let out = f.cmd().arg("--yes").arg("--all").output().unwrap();
    assert!(out.status.success(), "{}", stdout_of(&out));

    assert!(!f.socket_dir().join(CONTROL_SOCKET).exists());
    assert!(!f.socket_dir().join(STREAM_SOCKET).exists());
    assert!(
        f.socket_dir().join(USER_FILE).exists(),
        "a directory a caller named is not this command's to empty"
    );
    assert!(f.socket_dir().is_dir(), "the directory itself stays");
}

#[test]
fn idempotent_second_run_reports_zero() {
    let f = Fixture::populated();
    f.cmd().arg("--yes").output().unwrap();

    let out = f.cmd().arg("--yes").arg("--json").output().unwrap();
    assert!(out.status.success());
    let stdout = stdout_of(&out);
    // After the first run, walked stats for state/process_hub are zero.
    assert!(
        stdout.contains("\"files\":0,\"bytes\":0"),
        "expected zero-stats entries; got: {}",
        stdout
    );
}

#[test]
fn missing_root_treated_as_success() {
    let f = Fixture::empty();
    // No state/ etc. exist.
    let out = f.cmd().arg("--yes").output().unwrap();
    assert!(out.status.success(), "stderr: {}", stderr_of(&out));
}

#[test]
fn dry_run_does_not_modify_filesystem_and_exits_zero() {
    let f = Fixture::populated();
    let pre = f.root.join("state/a.bin");
    let pre_meta = fs::metadata(&pre).unwrap();

    let out = f.cmd().arg("--dry-run").output().unwrap();
    assert!(out.status.success());
    let post_meta = fs::metadata(&pre).unwrap();
    assert_eq!(pre_meta.len(), post_meta.len());
    assert!(pre.exists());

    let stdout = stdout_of(&out);
    assert!(stdout.contains("would clear"));
}

#[test]
fn bare_invocation_is_plan_only_exit_two() {
    let f = Fixture::populated();
    let out = f.cmd().output().unwrap();
    assert_eq!(out.status.code(), Some(2));
    let stdout = stdout_of(&out);
    assert!(stdout.contains("would clear"));
    assert!(stderr_of(&out).contains("(reset.confirmation_required)"));
    // No modifications.
    assert!(f.root.join("state/a.bin").exists());
}

#[test]
fn yes_and_dry_run_together_is_usage_error() {
    let f = Fixture::populated();
    let out = f.cmd().arg("--yes").arg("--dry-run").output().unwrap();
    assert_eq!(out.status.code(), Some(2));
    let stderr = stderr_of(&out);
    assert!(
        stderr.contains("(usage.options_conflict)"),
        "stderr: {}",
        stderr
    );
    // No modifications.
    assert!(f.root.join("state/a.bin").exists());
}

#[test]
fn path_equality_guard_rejects_symlink_loop() {
    let f = Fixture::empty();
    // Create a symlink loop: app/state -> app itself.
    std::os::unix::fs::symlink(&f.root, f.root.join("state")).unwrap();
    let out = f.cmd().arg("--yes").output().unwrap();
    assert_eq!(out.status.code(), Some(2));
    let stderr = stderr_of(&out);
    assert!(
        stderr.contains("(reset.path_refused)"),
        "stderr: {}",
        stderr
    );
}

#[test]
fn bind_mount_root_inode_unchanged_after_reset() {
    let f = Fixture::populated();
    let ino_before = fs::metadata(&f.root).unwrap().ino();
    let out = f.cmd().arg("--yes").output().unwrap();
    assert!(out.status.success());
    let ino_after = fs::metadata(&f.root).unwrap().ino();
    assert_eq!(ino_before, ino_after);
}

#[test]
fn probe_up_refuses_with_exit_6() {
    let f = Fixture::populated();
    let mut cmd = Command::new(FLZ_BIN);
    cmd.arg("reset")
        .arg("--storage-root")
        .arg(&f.storage)
        .arg("--yes")
        .env("FLEZHA_RESET_PROBE_OVERRIDE", "up");
    let out = cmd.output().unwrap();
    assert_eq!(out.status.code(), Some(6));
    let stderr = stderr_of(&out);
    assert!(
        stderr.contains("(reset.controller_running)")
            && stderr.contains("hint: flz controller stop"),
        "stderr: {}",
        stderr
    );
    // No modifications.
    assert!(f.root.join("state/a.bin").exists());
}

/// The probe is a `Controller.boot_await` request with no wait over the
/// control socket: a controller that answers it, ready or still starting,
/// refuses the reset, and nothing is touched.
#[test]
fn a_controller_that_answers_the_probe_refuses_the_reset() {
    for reply in [Reply::ok(), Reply::exit(4)] {
        let f = Fixture::populated();
        let shim = Shim::new(FLZ_BIN, "#!/bin/sh\nexit 1\n").serving({
            let reply = std::sync::Mutex::new(Some(reply));
            move |_, _| reply.lock().unwrap().take().unwrap_or(Reply::Refuse)
        });
        let out = shim
            .cmd()
            .args(["reset", "--yes", "--storage-root"])
            .arg(&f.storage)
            .output()
            .unwrap();

        assert_eq!(out.status.code(), Some(6), "{}", stderr_of(&out));
        assert!(stderr_of(&out).contains("(reset.controller_running)"));
        let probes = shim.requests();
        assert_eq!(probes.len(), 1, "{probes:?}");
        assert_eq!(probes[0]["name"], "Controller.boot_await");
        assert_eq!(probes[0]["args"], serde_json::json!([0]));
        assert!(f.root.join("state/a.bin").exists());
    }
}

/// A controller that closes without answering, or accepts the probe and stays
/// silent past the probe's timeout, is not up; nor is a socket nothing listens
/// on. Each run proceeds.
#[test]
fn a_probe_without_an_answer_proceeds() {
    let silent = Reply::Hold(std::sync::mpsc::channel().0);
    for reply in [Reply::Hangup, silent] {
        let f = Fixture::populated();
        let shim = Shim::new(FLZ_BIN, "#!/bin/sh\nexit 1\n").serving({
            let reply = std::sync::Mutex::new(Some(reply));
            move |_, _| reply.lock().unwrap().take().unwrap_or(Reply::Refuse)
        });
        let out = shim
            .cmd()
            .args(["reset", "--yes", "--storage-root"])
            .arg(&f.storage)
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(0), "{}", stderr_of(&out));
        assert_eq!(shim.requests().len(), 1);
    }

    let f = Fixture::populated();
    let out = f
        .cmd()
        .arg("--yes")
        .env_remove("FLEZHA_RESET_PROBE_OVERRIDE")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0), "{}", stderr_of(&out));
}

#[test]
fn a_dry_run_previews_while_the_controller_is_up() {
    let f = Fixture::populated();
    let mut cmd = f.cmd();
    cmd.arg("--dry-run")
        .arg("--json")
        .env("FLEZHA_RESET_PROBE_OVERRIDE", "up");
    let out = cmd.output().unwrap();

    assert_eq!(out.status.code(), Some(0), "{}", stderr_of(&out));
    let text = stdout_of(&out);
    assert!(text.contains("\"dry_run\":true"), "{}", text);
    assert!(f.root.join("state/a.bin").exists());
}

#[test]
fn dry_run_plan_matches_apply_counts() {
    let f = Fixture::populated();
    let dry = f
        .cmd()
        .arg("--dry-run")
        .arg("--all")
        .arg("--json")
        .output()
        .unwrap();
    assert!(dry.status.success());
    let dry_stdout = stdout_of(&dry);

    let apply = f
        .cmd()
        .arg("--yes")
        .arg("--all")
        .arg("--json")
        .output()
        .unwrap();
    assert!(apply.status.success());
    let apply_stdout = stdout_of(&apply);

    let dry_cleared = extract_cleared(&dry_stdout);
    let apply_cleared = extract_cleared(&apply_stdout);
    assert_eq!(dry_cleared, apply_cleared);
}

fn extract_cleared(json: &str) -> String {
    let start = json.find("\"cleared\":").expect("cleared key");
    let after = &json[start..];
    let end = after.find("],").expect("cleared end") + 1;
    after[..end].to_string()
}

#[test]
fn human_output_includes_summary_line() {
    let f = Fixture::populated();
    let out = f.cmd().arg("--yes").output().unwrap();
    assert!(out.status.success());
    let stdout = stdout_of(&out);
    assert!(stdout.contains("cleared"));
    assert!(stdout.contains("kept"));
    assert!(stdout.contains("reset complete in"));
    assert!(stdout.contains("use --include-certs to clear"));
}

// --- the ladder decides which socket directory is cleared -----------------

#[test]
fn the_per_user_runtime_directory_is_cleared_not_storage_sockets() {
    let f = Fixture::populated();
    let xdg = TempDir::new().unwrap();
    let live = xdg.path().join("flezha");
    populate_socket_dir(&live);

    let out = f
        .ladder_cmd(xdg.path())
        .arg("--yes")
        .arg("--include-sockets")
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", stdout_of(&out));

    assert!(
        !live.join(CONTROL_SOCKET).exists(),
        "the directory in use is what gets cleared"
    );
    assert!(
        f.socket_dir().join(CONTROL_SOCKET).exists(),
        "storage/app/sockets/ is not the socket root when a later rung answered"
    );

    let text = stdout_of(&out);
    assert!(text.contains(&live.display().to_string()), "{}", text);
    assert!(
        !text.contains(&f.socket_dir().display().to_string()),
        "{}",
        text
    );
}

#[test]
fn the_explicit_variable_wins_over_every_later_rung() {
    let f = Fixture::populated();
    let xdg = TempDir::new().unwrap();
    let later = xdg.path().join("flezha");
    populate_socket_dir(&later);

    // `cmd()` sets the explicit variable to the fixture's own socket dir.
    let out = f
        .cmd()
        .env("XDG_RUNTIME_DIR", xdg.path())
        .arg("--yes")
        .arg("--include-sockets")
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", stdout_of(&out));

    assert!(!f.socket_dir().join(CONTROL_SOCKET).exists());
    assert!(
        later.join(CONTROL_SOCKET).exists(),
        "only the named directory is cleared"
    );
    assert!(stdout_of(&out).contains("$FLEZHA_SOCKET_FILES_PATH"));
}

#[test]
fn storage_root_does_not_move_the_socket_directory() {
    let f = Fixture::populated();
    let xdg = TempDir::new().unwrap();
    let live = xdg.path().join("flezha");
    populate_socket_dir(&live);

    let out = f
        .ladder_cmd(xdg.path())
        .arg("--yes")
        .arg("--include-sockets")
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", stdout_of(&out));

    // The stores moved with the flag; the socket root did not.
    assert!(dir_is_empty(&f.root.join("state")));
    assert!(dir_is_empty(&f.root.join("process_hub")));
    assert!(!live.join(CONTROL_SOCKET).exists());
    assert!(f.socket_dir().join(CONTROL_SOCKET).exists());
}

#[test]
fn a_resolution_refusal_keeps_the_socket_root_and_clears_the_rest() {
    let f = Fixture::populated();
    // Two rungs are serving and nothing says which is meant: the ladder
    // refuses rather than choosing a controller for the caller, and the
    // refusal names both. The repository rung is relative to the working
    // directory, which is what makes the second one placeable.
    let xdg = TempDir::new().unwrap();
    let cwd = TempDir::new().unwrap();
    let first = xdg.path().join("flezha");
    let second = cwd.path().join("storage/app/sockets");
    fs::create_dir_all(&first).unwrap();
    fs::create_dir_all(&second).unwrap();
    let _peers = [
        UnixListener::bind(first.join(CONTROL_SOCKET)).expect("bind"),
        UnixListener::bind(second.join(CONTROL_SOCKET)).expect("bind"),
    ];

    let out = f
        .ladder_cmd(xdg.path())
        .current_dir(cwd.path())
        .arg("--yes")
        .arg("--include-sockets")
        .output()
        .unwrap();

    assert!(
        out.status.success(),
        "not knowing which directory holds the sockets does not change the exit code: {}",
        stderr_of(&out)
    );
    assert!(dir_is_empty(&f.root.join("state")));
    assert!(dir_is_empty(&f.root.join("process_hub")));

    let text = stdout_of(&out);
    assert!(text.contains("kept"), "{}", text);
    assert!(
        text.contains("more than one controller is listening"),
        "{}",
        text
    );
    for named in [&first, &second] {
        assert!(text.contains(&named.display().to_string()), "{}", text);
    }
    assert!(first.join(CONTROL_SOCKET).exists());
    assert!(second.join(CONTROL_SOCKET).exists());
}

/// A socket root that resolves but cannot be read is the same outcome as one
/// that cannot be resolved: reported, skipped, and the remaining roots still
/// cleared. It is not a resolution refusal — the run does not try to establish
/// the directory in order to find that out.
#[test]
fn an_unreadable_socket_root_is_reported_and_the_rest_is_cleared() {
    let f = Fixture::populated();
    // Nothing is a directory below a character device.
    let out = f
        .cmd()
        .env("FLEZHA_SOCKET_FILES_PATH", "/dev/null/nowhere")
        .arg("--yes")
        .arg("--include-sockets")
        .output()
        .unwrap();

    assert!(out.status.success(), "{}", stderr_of(&out));
    assert!(dir_is_empty(&f.root.join("state")));
    assert!(dir_is_empty(&f.root.join("process_hub")));

    let text = stdout_of(&out);
    assert!(text.contains("kept"), "{}", text);
    assert!(text.contains("/dev/null/nowhere"), "{}", text);
}

#[test]
fn a_serving_controller_keeps_the_socket_root_and_the_rest_is_cleared() {
    let f = Fixture::populated();
    // A peer controller sharing this runtime directory, in place of the
    // fixture's stale socket. This controller's own node is down, so the
    // live-node guard passes — a peer is a different node.
    let control = f.socket_dir().join(CONTROL_SOCKET);
    fs::remove_file(&control).unwrap();
    let _peer = UnixListener::bind(&control).expect("bind");

    let out = f.cmd().arg("--yes").arg("--all").output().unwrap();

    assert_eq!(out.status.code(), Some(0), "{}", stderr_of(&out));
    assert!(
        f.socket_dir().join(CONTROL_SOCKET).exists(),
        "a serving controller's socket is never unlinked"
    );
    assert!(dir_is_empty(&f.root.join("state")));
    assert!(dir_is_empty(&f.root.join("process_hub")));

    let text = stdout_of(&out);
    assert!(text.contains("controller reachable"), "{}", text);
    assert!(
        text.contains("$FLEZHA_SOCKET_FILES_PATH"),
        "the rung is named: {}",
        text
    );
    assert!(
        text.contains(&f.socket_dir().display().to_string()),
        "{}",
        text
    );
}

#[test]
fn a_stale_socket_is_cleared_not_kept() {
    let f = Fixture::populated();
    let out = f.cmd().arg("--yes").arg("--all").output().unwrap();
    assert!(out.status.success(), "{}", stdout_of(&out));
    assert!(!f.socket_dir().join(CONTROL_SOCKET).exists());
}

#[test]
fn a_directory_this_user_cannot_enter_is_reported_not_forced() {
    // Another user's directory, on every Linux box these tests do not run as
    // root on.
    let denied = Path::new("/proc/1/fd");
    if fs::read_dir(denied).is_ok() {
        return; // running as root: there is no boundary to observe
    }

    let f = Fixture::populated();
    let out = f
        .cmd()
        .env("FLEZHA_SOCKET_FILES_PATH", denied)
        .arg("--yes")
        .arg("--include-sockets")
        .output()
        .unwrap();

    assert!(out.status.success(), "{}", stderr_of(&out));
    assert!(dir_is_empty(&f.root.join("state")));
    let text = stdout_of(&out);
    assert!(text.contains("kept"), "{}", text);
}

#[test]
fn the_socket_root_names_its_rung_in_the_dry_run_preview() {
    let f = Fixture::populated();
    let xdg = TempDir::new().unwrap();
    let live = xdg.path().join("flezha");
    populate_socket_dir(&live);

    let out = f
        .ladder_cmd(xdg.path())
        .arg("--dry-run")
        .arg("--include-sockets")
        .output()
        .unwrap();
    assert!(out.status.success());

    let text = stdout_of(&out);
    assert!(text.contains("would clear"), "{}", text);
    assert!(text.contains(&live.display().to_string()), "{}", text);
    // The same words `flz runner check` prints for this rung.
    assert!(text.contains("$XDG_RUNTIME_DIR/flezha"), "{}", text);
    // Nothing was unlinked.
    assert!(live.join(CONTROL_SOCKET).exists());
}

/// The last rung of the ladder is `storage/app/sockets` relative to the working
/// directory, so a case about what the run creates has to own its cwd.
#[test]
fn no_socket_directory_is_established_by_a_run_that_clears_one() {
    let f = Fixture::populated();
    // A runtime directory with no `flezha/` in it: the rung exists, the
    // directory the ladder would name under it does not.
    let xdg = TempDir::new().unwrap();
    let cwd = TempDir::new().unwrap();

    let out = f
        .ladder_cmd(xdg.path())
        .current_dir(cwd.path())
        .arg("--dry-run")
        .arg("--include-sockets")
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", stderr_of(&out));

    assert!(
        !xdg.path().join("flezha").exists(),
        "a preview established the per-user runtime rung: {}",
        stdout_of(&out)
    );
    assert!(
        !cwd.path().join("storage").exists(),
        "a preview established the repository rung: {}",
        stdout_of(&out)
    );
}

/// A peer is serving in the first rung the ladder offers. That rung is this
/// fleet's socket directory, and stepping over it to a rung nothing has used
/// answers about the wrong directory — after creating it.
#[test]
fn a_rung_a_peer_is_listening_in_is_what_the_run_names() {
    let f = Fixture::populated();
    let xdg = TempDir::new().unwrap();
    let peer_dir = xdg.path().join("flezha");
    fs::create_dir_all(&peer_dir).unwrap();
    let _peer = UnixListener::bind(peer_dir.join(CONTROL_SOCKET)).expect("bind");
    let cwd = TempDir::new().unwrap();

    let out = f
        .ladder_cmd(xdg.path())
        .current_dir(cwd.path())
        .arg("--dry-run")
        .arg("--include-sockets")
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", stderr_of(&out));

    let text = stdout_of(&out);
    assert!(
        text.contains(&peer_dir.display().to_string()),
        "the rung the peer holds is not named: {}",
        text
    );
    assert!(text.contains("controller reachable"), "{}", text);
    assert!(
        !cwd.path().join("storage").exists(),
        "the run stepped over the peer and established a rung of its own: {}",
        text
    );
}

#[test]
fn the_json_socket_entry_carries_the_rung() {
    let f = Fixture::populated();
    let out = f
        .cmd()
        .arg("--yes")
        .arg("--all")
        .arg("--json")
        .output()
        .unwrap();
    assert!(out.status.success());

    let text = stdout_of(&out);
    assert!(
        text.contains("\"rung\":\"$FLEZHA_SOCKET_FILES_PATH\""),
        "{}",
        text
    );
    // The pinned shapes survive alongside it, and the count is of what was
    // actually unlinked: two socket inodes.
    assert!(text.contains("\"cleared\":["), "{}", text);
    assert!(
        text.contains("\"files\":2,\"bytes\":0,\"rung\""),
        "{}",
        text
    );
    assert!(text.contains("\"dry_run\":false"), "{}", text);
}
