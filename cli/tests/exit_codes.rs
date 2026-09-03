//! What `flz` exits with when the `flzvm` it spawns refuses.
//!
//! Every verb here delegates to the sibling `flzvm` binary. A verb that
//! answers 0 after that spawn failed is reporting a success it did not earn,
//! and a script cannot tell it from a success (KI-79).

mod common;

use common::Shim;

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

/// A `flz` whose `flzvm` always refuses. Exit 7 is chosen because no `flz`
/// dialect produces it from anything but a propagated child code.
struct Refusing {
    shim: Shim,
}

impl Refusing {
    fn new() -> Self {
        Refusing {
            shim: Shim::new(
                FLZ_BIN,
                "#!/bin/sh\nprintf 'flzvm: refused\\n' >&2\nexit 7\n",
            ),
        }
    }

    fn code(&self, args: &[&str]) -> i32 {
        self.shim
            .run(args)
            .status
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
