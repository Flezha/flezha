//! `flz schedule add` — the arguments it will not run without.

use std::process::Command;

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

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
