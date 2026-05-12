use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

struct Fixture {
    _tmp: TempDir,
    root: PathBuf,
}

impl Fixture {
    fn populated() -> Self {
        let tmp = TempDir::new().expect("tempdir");
        let root = tmp.path().to_path_buf();
        for name in ["state", "process_hub", "certs", "sockets"] {
            let sub = root.join(name);
            fs::create_dir_all(sub.join("nested")).unwrap();
            fs::write(sub.join("a.bin"), vec![0u8; 7]).unwrap();
            fs::write(sub.join("nested/b.bin"), vec![0u8; 13]).unwrap();
        }
        Fixture { _tmp: tmp, root }
    }

    fn empty() -> Self {
        let tmp = TempDir::new().expect("tempdir");
        let root = tmp.path().to_path_buf();
        Fixture { _tmp: tmp, root }
    }

    fn cmd(&self) -> Command {
        let mut c = Command::new(FLZ_BIN);
        c.arg("reset")
            .arg("--app-data-root")
            .arg(&self.root)
            .env("FLEZHA_RESET_PROBE_OVERRIDE", "down");
        c
    }
}

fn dir_is_empty(p: &Path) -> bool {
    fs::read_dir(p).unwrap().next().is_none()
}

#[test]
fn default_reset_clears_state_and_process_hub_keeps_certs_and_sockets() {
    let f = Fixture::populated();
    let out = f.cmd().arg("--yes").output().unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(dir_is_empty(&f.root.join("state")));
    assert!(dir_is_empty(&f.root.join("process_hub")));
    assert!(!dir_is_empty(&f.root.join("certs")));
    assert!(!dir_is_empty(&f.root.join("sockets")));
    // Root inode preserved.
    assert!(f.root.exists());
}

#[test]
fn all_flag_clears_every_root() {
    let f = Fixture::populated();
    let out = f.cmd().arg("--yes").arg("--all").output().unwrap();
    assert!(out.status.success());
    for name in ["state", "process_hub", "certs", "sockets"] {
        assert!(dir_is_empty(&f.root.join(name)), "{} should be empty", name);
    }
}

#[test]
fn idempotent_second_run_reports_zero() {
    let f = Fixture::populated();
    f.cmd().arg("--yes").output().unwrap();

    let out = f.cmd().arg("--yes").arg("--json").output().unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
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
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
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

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("would clear"));
}

#[test]
fn bare_invocation_is_plan_only_exit_one() {
    let f = Fixture::populated();
    let out = f.cmd().output().unwrap();
    assert_eq!(out.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("would clear"));
    // No modifications.
    assert!(f.root.join("state/a.bin").exists());
}

#[test]
fn yes_and_dry_run_together_is_usage_error() {
    let f = Fixture::populated();
    let out = f.cmd().arg("--yes").arg("--dry-run").output().unwrap();
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
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
    let stderr = String::from_utf8_lossy(&out.stderr);
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
    let stderr = String::from_utf8_lossy(&out.stderr);
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
    let dry = f.cmd().arg("--dry-run").arg("--json").output().unwrap();
    assert!(dry.status.success());
    let dry_stdout = String::from_utf8_lossy(&dry.stdout).into_owned();

    let apply = f.cmd().arg("--yes").arg("--json").output().unwrap();
    assert!(apply.status.success());
    let apply_stdout = String::from_utf8_lossy(&apply.stdout).into_owned();

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
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("cleared"));
    assert!(stdout.contains("kept"));
    assert!(stdout.contains("reset complete in"));
    assert!(stdout.contains("use --include-certs to clear"));
}
