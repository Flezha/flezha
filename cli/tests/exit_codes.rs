//! What `flz` exits with when the `flzvm` it spawns refuses.
//!
//! Every verb here delegates to the sibling `flzvm` binary. A verb that
//! answers 0 after that spawn failed is reporting a success it did not earn,
//! and a script cannot tell it from a success (KI-79).

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Command;

use tempfile::TempDir;

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

/// A `flz` whose `flzvm` always refuses. `flz` resolves `flzvm` beside its own
/// executable, so the shim has to sit next to a *copy* of the binary rather
/// than on `PATH`. Exit 7 is chosen because no `flz` dialect produces it from
/// anything but a propagated child code.
struct Refusing {
    _tmp: TempDir,
    flz: PathBuf,
}

impl Refusing {
    fn new() -> Self {
        let tmp = TempDir::new().expect("tempdir");
        let flz = tmp.path().join("flz");
        fs::copy(FLZ_BIN, &flz).expect("copy the flz under test");
        let shim = tmp.path().join("flzvm");
        fs::write(&shim, "#!/bin/sh\nprintf 'flzvm: refused\\n' >&2\nexit 7\n")
            .expect("write shim");
        fs::set_permissions(&shim, fs::Permissions::from_mode(0o755)).expect("chmod shim");
        Refusing { _tmp: tmp, flz }
    }

    fn code(&self, args: &[&str]) -> i32 {
        let out = Command::new(&self.flz)
            .args(args)
            .output()
            .expect("flz runs");
        out.status
            .code()
            .expect("flz exits rather than being signalled")
    }

    /// Every mismatching case, so one run names all of them.
    fn wrong(&self, cases: &[(&[&str], i32)]) -> Vec<String> {
        cases
            .iter()
            .filter_map(|(args, want)| {
                let got = self.code(args);
                (got != *want)
                    .then(|| format!("flz {}: exit {} (want {})", args.join(" "), got, want))
            })
            .collect()
    }
}

/// The five verbs of KI-79. Their `match status` had no `exit()` in either
/// failure arm, so `main` returned normally and the process exited 0 — while
/// `operative send`, six lines away, exited 1 from the identical match.
fn no_refusal_is_reported_as_success(cli: &Refusing) -> Vec<String> {
    cli.wrong(&[
        (&["operative", "stop", "op1"], 1),
        (&["operative", "rewind", "op1", "--to-iteration", "3"], 1),
        (&["operative", "replay", "a/b/c", "--from", "5"], 1),
        (&["operative", "state", "op1"], 1),
        (&["controller", "start"], 1),
    ])
}

/// One verb per surviving exit-code dialect, so a later refactor of the 34
/// spawn sites cannot quietly move an operator-visible code.
fn every_dialect_keeps_its_code(cli: &Refusing) -> Vec<String> {
    cli.wrong(&[
        // The reference arm KI-79's fix was copied from.
        (&["operative", "send", "op1", "--text", "hi"], 1),
        // 17 sites: diagnose, then exit 1.
        (&["operative", "list"], 1),
        // 5 sites: propagate the child's code.
        (&["boot", "status"], 7),
        // 2 sites: exit 3, silently.
        (&["controller", "status"], 3),
        // 3 sites: diagnose the spawn, else propagate the child's code.
        (&["audit", "list"], 7),
    ])
}

/// One `#[test]`, two phases: the fixture execs a *copy* of the binary, and a
/// copy written while a sibling thread forks is `ExecutableFileBusy` in the
/// child that inherited the write handle. One thread, one copy, no race.
#[test]
fn every_verb_answers_a_refused_flzvm_with_the_code_it_promises() {
    let cli = Refusing::new();

    let mut wrong = no_refusal_is_reported_as_success(&cli);
    wrong.extend(every_dialect_keeps_its_code(&cli));

    assert!(
        wrong.is_empty(),
        "flzvm refused with exit 7 and:\n{}",
        wrong.join("\n")
    );
}
