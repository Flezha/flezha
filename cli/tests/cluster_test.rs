//! Integration tests for the `flz cluster` command surface.
//!
//! These tests exercise the actual `flz` binary's dispatch and arg-handling
//! paths — the help text, the missing-arg error paths, and the subcommand
//! routing. They deliberately do not require a running Flezha agent: the
//! agent-side cluster-snapshot/restore flow is covered by
//! `test/cluster/snapshot_e2e_test.exs` (in-BEAM) and
//! `test/system/cluster_snapshot_restore_test.exs` (two-node CST).
//!
//! Together: the CLI tests verify dispatch + error UX from the binary's
//! argv → the Elixir RPC string is built correctly. The Elixir tests
//! verify the RPC entry point and the snapshot/restore mechanics. Full
//! binary-against-real-agent end-to-end is operational and out of scope
//! for unit/integration tests.

use std::process::{Command, Output};

fn flz_binary() -> std::path::PathBuf {
    let mut path = std::env::current_exe().expect("current_exe");
    // target/debug/deps/flz-XXXXX → target/debug/flz
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    path.push("flz");
    path
}

fn run_flz(args: &[&str]) -> Output {
    let bin = flz_binary();
    Command::new(&bin)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("failed to spawn {:?}: {}", bin, e))
}

#[test]
fn flz_help_lists_cluster_command() {
    let out = run_flz(&["help"]);
    let stdout = String::from_utf8_lossy(&out.stdout);

    assert!(out.status.success(), "flz help exited non-zero");
    assert!(
        stdout.contains("cluster"),
        "top-level help should mention `cluster` — got:\n{}",
        stdout
    );
    assert!(
        stdout.contains("docs/cluster-snapshot.md"),
        "top-level help should reference the docs path — got:\n{}",
        stdout
    );
}

#[test]
fn flz_cluster_help_lists_snapshot_and_restore() {
    let out = run_flz(&["cluster", "help"]);
    let stdout = String::from_utf8_lossy(&out.stdout);

    assert!(out.status.success(), "flz cluster help exited non-zero");
    assert!(
        stdout.contains("snapshot"),
        "cluster help should list `snapshot` — got:\n{}",
        stdout
    );
    assert!(
        stdout.contains("restore"),
        "cluster help should list `restore` — got:\n{}",
        stdout
    );
    assert!(
        stdout.contains("--peer-timeout-ms"),
        "cluster help should describe --peer-timeout-ms — got:\n{}",
        stdout
    );
    assert!(
        stdout.contains("--strict"),
        "cluster help should describe --strict — got:\n{}",
        stdout
    );
    assert!(
        stdout.contains("--warn-if-older-than"),
        "cluster help should describe --warn-if-older-than — got:\n{}",
        stdout
    );
}

#[test]
fn flz_cluster_with_no_subcommand_errors_with_usage() {
    let out = run_flz(&["cluster"]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let stdout = String::from_utf8_lossy(&out.stdout);

    assert!(
        !out.status.success(),
        "expected non-zero exit when no subcommand given; got success.\nstdout:\n{}\nstderr:\n{}",
        stdout,
        stderr
    );
    assert!(
        stdout.contains("snapshot") || stderr.contains("snapshot"),
        "usage output should mention `snapshot` subcommand"
    );
}

#[test]
fn flz_cluster_unknown_subcommand_errors() {
    let out = run_flz(&["cluster", "obliterate"]);
    let stderr = String::from_utf8_lossy(&out.stderr);

    assert!(
        !out.status.success(),
        "expected non-zero exit for unknown subcommand"
    );
    assert!(
        stderr.contains("Invalid cluster subcommand")
            || stderr.contains("obliterate")
            || stderr.contains("Use"),
        "stderr should explain the bad subcommand — got:\n{}",
        stderr
    );
}

#[test]
fn flz_cluster_snapshot_without_file_errors_with_usage() {
    let out = run_flz(&["cluster", "snapshot"]);
    let stderr = String::from_utf8_lossy(&out.stderr);

    assert!(
        !out.status.success(),
        "expected non-zero exit when <file> arg missing"
    );
    assert!(
        stderr.contains("Usage: flz cluster snapshot")
            || stderr.contains("Invalid number of arguments"),
        "stderr should print usage — got:\n{}",
        stderr
    );
}

#[test]
fn flz_cluster_restore_without_file_errors_with_usage() {
    let out = run_flz(&["cluster", "restore"]);
    let stderr = String::from_utf8_lossy(&out.stderr);

    assert!(
        !out.status.success(),
        "expected non-zero exit when <file> arg missing"
    );
    assert!(
        stderr.contains("Usage: flz cluster restore")
            || stderr.contains("Invalid number of arguments"),
        "stderr should print usage — got:\n{}",
        stderr
    );
}
