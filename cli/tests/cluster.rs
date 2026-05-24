use std::process::Command;

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

#[test]
fn cluster_help_lists_status_subcommand() {
    let out = Command::new(FLZ_BIN)
        .args(["cluster", "help"])
        .output()
        .unwrap();

    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("status"),
        "cluster help must document the status subcommand"
    );
    assert!(stdout.contains("cluster"));
}

#[test]
fn cluster_unknown_subcommand_errors() {
    let out = Command::new(FLZ_BIN)
        .args(["cluster", "bogus"])
        .output()
        .unwrap();

    assert!(
        !out.status.success(),
        "an unknown cluster subcommand must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("Unknown cluster subcommand"));
}

#[test]
fn cluster_with_no_subcommand_errors() {
    let out = Command::new(FLZ_BIN).args(["cluster"]).output().unwrap();
    assert!(
        !out.status.success(),
        "cluster with no subcommand must exit non-zero"
    );
}
