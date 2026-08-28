use std::fs;
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

const CONTROL_SOCKET: &str = "runner_control_channel.sock";
const STREAM_SOCKET: &str = "primary_stream_runner-1.sock";
/// A file in the socket directory that is not Flezha's to unlink.
const OPERATOR_FILE: &str = "operator-notes.txt";

/// A controller that died hard: a socket file nothing is listening on. It has
/// to be a real bound socket — a regular file with the same name probes
/// `Unreachable` (ENOTSOCK), not stale.
fn stale_control_socket(dir: &Path) {
    fs::create_dir_all(dir).unwrap();
    drop(UnixListener::bind(dir.join(CONTROL_SOCKET)).expect("bind"));
}

/// A socket directory as a controller leaves it, plus a file that is not ours.
fn populate_socket_dir(dir: &Path) {
    stale_control_socket(dir);
    drop(UnixListener::bind(dir.join(STREAM_SOCKET)).expect("bind"));
    fs::write(dir.join(OPERATOR_FILE), vec![0u8; 5]).unwrap();
}

struct Fixture {
    _tmp: TempDir,
    root: PathBuf,
}

impl Fixture {
    fn populated() -> Self {
        let tmp = TempDir::new().expect("tempdir");
        let root = tmp.path().to_path_buf();
        for name in ["state", "process_hub", "certs"] {
            let sub = root.join(name);
            fs::create_dir_all(sub.join("nested")).unwrap();
            fs::write(sub.join("a.bin"), vec![0u8; 7]).unwrap();
            fs::write(sub.join("nested/b.bin"), vec![0u8; 13]).unwrap();
        }
        populate_socket_dir(&root.join("sockets"));
        Fixture { _tmp: tmp, root }
    }

    fn empty() -> Self {
        let tmp = TempDir::new().expect("tempdir");
        let root = tmp.path().to_path_buf();
        Fixture { _tmp: tmp, root }
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
            .arg("--app-data-root")
            .arg(&self.root)
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
fn default_reset_clears_state_and_process_hub_keeps_certs_and_sockets() {
    let f = Fixture::populated();
    let out = f.cmd().arg("--yes").output().unwrap();
    assert!(out.status.success(), "stderr: {}", stderr_of(&out));
    assert!(dir_is_empty(&f.root.join("state")));
    assert!(dir_is_empty(&f.root.join("process_hub")));
    assert!(!dir_is_empty(&f.root.join("certs")));
    assert!(f.socket_dir().join(CONTROL_SOCKET).exists());
    // Root inode preserved.
    assert!(f.root.exists());
}

#[test]
fn all_flag_clears_every_root() {
    let f = Fixture::populated();
    let out = f.cmd().arg("--yes").arg("--all").output().unwrap();
    assert!(out.status.success(), "{}", stdout_of(&out));
    for name in ["state", "process_hub", "certs"] {
        assert!(dir_is_empty(&f.root.join(name)), "{} should be empty", name);
    }
    assert!(!f.socket_dir().join(CONTROL_SOCKET).exists());
    assert!(!f.socket_dir().join(STREAM_SOCKET).exists());
}

#[test]
fn unrelated_files_in_the_socket_directory_survive() {
    let f = Fixture::populated();
    let out = f.cmd().arg("--yes").arg("--all").output().unwrap();
    assert!(out.status.success(), "{}", stdout_of(&out));

    assert!(!f.socket_dir().join(CONTROL_SOCKET).exists());
    assert!(!f.socket_dir().join(STREAM_SOCKET).exists());
    assert!(
        f.socket_dir().join(OPERATOR_FILE).exists(),
        "a directory an operator named is not this command's to empty"
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
fn bare_invocation_is_plan_only_exit_one() {
    let f = Fixture::populated();
    let out = f.cmd().output().unwrap();
    assert_eq!(out.status.code(), Some(1));
    let stdout = stdout_of(&out);
    assert!(stdout.contains("would clear"));
    // No modifications.
    assert!(f.root.join("state/a.bin").exists());
}

#[test]
fn yes_and_dry_run_together_is_usage_error() {
    let f = Fixture::populated();
    let out = f.cmd().arg("--yes").arg("--dry-run").output().unwrap();
    assert_eq!(out.status.code(), Some(1));
    let stderr = stderr_of(&out);
    assert!(stderr.contains("mutually exclusive"));
    // No modifications.
    assert!(f.root.join("state/a.bin").exists());
}

#[test]
fn path_equality_guard_rejects_symlink_loop() {
    let f = Fixture::empty();
    // Create a symlink loop: app_data_root/state -> app_data_root itself.
    std::os::unix::fs::symlink(&f.root, f.root.join("state")).unwrap();
    let out = f.cmd().arg("--yes").output().unwrap();
    assert_eq!(out.status.code(), Some(1));
    let stderr = stderr_of(&out);
    assert!(stderr.contains("refusing"));
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
fn probe_up_refuses_with_exit_4() {
    let f = Fixture::populated();
    let mut cmd = Command::new(FLZ_BIN);
    cmd.arg("reset")
        .arg("--app-data-root")
        .arg(&f.root)
        .arg("--yes")
        .env("FLEZHA_RESET_PROBE_OVERRIDE", "up");
    let out = cmd.output().unwrap();
    assert_eq!(out.status.code(), Some(4));
    let stderr = stderr_of(&out);
    assert!(
        stderr.contains("refuses to run while the controller is up"),
        "stderr: {}",
        stderr
    );
    // No modifications.
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
fn the_per_user_runtime_directory_is_cleared_not_app_data_sockets() {
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
        "app_data/sockets/ is not the socket root when a later rung answered"
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
fn app_data_root_does_not_move_the_socket_directory() {
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

    // The three `app_data/` roots moved with the flag; the socket root did not.
    assert!(dir_is_empty(&f.root.join("state")));
    assert!(dir_is_empty(&f.root.join("process_hub")));
    assert!(!live.join(CONTROL_SOCKET).exists());
    assert!(f.socket_dir().join(CONTROL_SOCKET).exists());
}

#[test]
fn a_resolution_refusal_keeps_the_socket_root_and_clears_the_rest() {
    let f = Fixture::populated();
    // No directory can be created below a character device.
    let out = f
        .cmd()
        .env("FLEZHA_SOCKET_FILES_PATH", "/dev/null/nowhere")
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
        text.contains("no socket directory could be created"),
        "{}",
        text
    );
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
