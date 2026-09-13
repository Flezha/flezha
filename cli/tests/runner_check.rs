//! `flz runner check` — the runner's own probe, from a shell.

use std::os::unix::net::UnixListener;
use std::process::{Command, Output};

use flz_transport::utility::socket_probe::{probe, Reach};
use tempfile::TempDir;

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

fn check_in(dir: &std::path::Path) -> Output {
    Command::new(FLZ_BIN)
        .args(["runner", "check"])
        .env("FLEZHA_SOCKET_FILES_PATH", dir)
        .env_remove("FLEZHA_TRANSPORT")
        .output()
        .expect("flz runner check runs")
}

#[test]
fn a_reachable_controller_is_reported_and_exits_zero() {
    let dir = TempDir::new().unwrap();
    let _controller = UnixListener::bind(dir.path().join("runner_control_channel.sock")).unwrap();

    let output = check_in(dir.path());
    let text = String::from_utf8_lossy(&output.stdout).to_string();

    assert!(output.status.success(), "{}", text);
    assert!(
        text.contains(&dir.path().display().to_string()),
        "the resolved absolute directory is named: {}",
        text
    );
    assert!(text.contains("controller reachable"), "{}", text);
    assert!(
        !text
            .lines()
            .any(|line| line.trim_start().starts_with("fix")),
        "a controller that answered has nothing to fix: {}",
        text
    );
}

#[test]
fn an_unreachable_controller_exits_non_zero_with_the_runners_own_diagnosis() {
    let dir = TempDir::new().unwrap();

    let output = check_in(dir.path());
    let text = String::from_utf8_lossy(&output.stdout).to_string();

    assert!(!output.status.success(), "{}", text);
    assert!(text.contains("control socket absent"), "{}", text);
    assert!(
        text.contains(&Reach::SocketMissing.fix().unwrap()),
        "{}",
        text
    );
}

#[test]
fn the_access_denied_output_is_the_diagnosis_a_runner_would_print() {
    // A directory this process cannot traverse can only be another user's;
    // `/proc/1/fd` is one on every Linux box the tests do not run as root on.
    let denied = std::path::Path::new("/proc/1/fd");
    let verdict = probe(denied);
    if !matches!(verdict, Reach::AccessDenied { .. }) {
        // Running as root, or not on Linux: there is no boundary to observe.
        return;
    }

    let output = check_in(denied);
    let text = String::from_utf8_lossy(&output.stdout).to_string();

    assert!(!output.status.success(), "{}", text);
    assert!(text.contains("access denied"), "{}", text);
    // Same probe, same words — the operator's check and the runner's log agree.
    assert!(text.contains(&verdict.fix().unwrap()), "{}", text);
    assert!(text.contains("FLEZHA_SOCKET_DIR_GROUP"), "{}", text);
}
