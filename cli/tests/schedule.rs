use std::process::Command;

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

#[test]
fn schedule_help_lists_all_subcommands() {
    let out = Command::new(FLZ_BIN)
        .args(["schedule", "help"])
        .output()
        .unwrap();

    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    for verb in ["add", "list", "remove", "run"] {
        assert!(
            stdout.contains(verb),
            "schedule help must document the {} subcommand",
            verb
        );
    }
}

#[test]
fn schedule_unknown_subcommand_errors() {
    let out = Command::new(FLZ_BIN)
        .args(["schedule", "bogus"])
        .output()
        .unwrap();

    assert!(
        !out.status.success(),
        "an unknown schedule subcommand must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("Unknown schedule command"));
}

#[test]
fn schedule_with_no_subcommand_errors() {
    let out = Command::new(FLZ_BIN).args(["schedule"]).output().unwrap();
    assert!(
        !out.status.success(),
        "schedule with no subcommand must exit non-zero"
    );
}

#[test]
fn schedule_add_requires_cron_tz_and_factory() {
    for missing in [
        vec![
            "schedule",
            "add",
            "digest",
            "--tz",
            "Etc/UTC",
            "--factory",
            "f",
        ],
        vec![
            "schedule",
            "add",
            "digest",
            "--cron",
            "0 8 * * *",
            "--factory",
            "f",
        ],
        vec![
            "schedule",
            "add",
            "digest",
            "--cron",
            "0 8 * * *",
            "--tz",
            "Etc/UTC",
        ],
        vec!["schedule", "add"],
    ] {
        let out = Command::new(FLZ_BIN).args(&missing).output().unwrap();
        assert!(
            !out.status.success(),
            "schedule add with missing required args must exit non-zero: {:?}",
            missing
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("Usage: flz schedule add"));
    }
}
