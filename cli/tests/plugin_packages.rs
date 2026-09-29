//! `flz plugin install`, `uninstall` and `sync` change the plugin root, never a
//! running controller: each starts one `flzvm eval`, no `rpc`, and the
//! credential a caller typed reaches neither. An upgrade is an install of a
//! newer package and a package's details are its row in `flz plugin list
//! --installed`, so neither subcommand exists, and asking for one starts
//! nothing.
//!
//! One test, run in order: shims copied and executed from parallel threads of
//! one process race on a file still open for writing ("text file busy").

mod common;

use std::fs;
use std::process::Output;

use common::Shim;

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

/// A `flzvm` that appends every invocation, one line each, to the file named in
/// `FLZ_SHIM_LOG`, and succeeds.
const RECORDING: &str = "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$FLZ_SHIM_LOG\"\nexit 0\n";

fn invocations(argv: &[&str]) -> (Output, Vec<String>) {
    let shim = Shim::new(FLZ_BIN, RECORDING);
    let log = shim.flzvm_path().with_file_name("invocations.log");
    let out = shim
        .cmd()
        .args(argv)
        .env("FLZ_SHIM_LOG", &log)
        .output()
        .expect("flz runs");
    let calls = fs::read_to_string(&log)
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect();
    (out, calls)
}

#[test]
fn the_verbs_that_place_packages_run_one_eval_and_the_removed_ones_nothing() {
    for argv in [
        &["plugin", "install", "./p.fpz", "--token", "t"][..],
        &["plugin", "uninstall", "p", "--token", "t"][..],
        &["plugin", "sync", "--token", "t"][..],
    ] {
        let (out, calls) = invocations(argv);
        let line = argv.join(" ");

        assert!(out.status.success(), "flz {} failed: {:?}", line, out);
        assert_eq!(calls.len(), 1, "flz {} started {:?}", line, calls);
        assert!(
            calls[0].starts_with("eval "),
            "flz {} ran {}",
            line,
            calls[0]
        );
        assert!(
            !calls[0].contains("token"),
            "flz {} passed a token: {}",
            line,
            calls[0]
        );
    }

    for argv in [
        &["plugin", "upgrade", "p", "./p.fpz"][..],
        &["plugin", "info", "p"][..],
    ] {
        let (out, calls) = invocations(argv);
        let line = argv.join(" ");

        assert!(!out.status.success(), "flz {} succeeded", line);
        assert!(calls.is_empty(), "flz {} started {:?}", line, calls);
    }
}
