//! Every help screen `flz` prints, byte for byte.
//!
//! The text is hand-aligned and internally inconsistent — `flz help`'s `runner`
//! row sits two columns left of its siblings, `flz audit help`'s `stats` row one
//! — so it cannot be re-derived "correctly" without changing what an operator
//! sees. The golden files are captured from the binary, not written by hand, and
//! they are what proves a rendering change is deliberate.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

/// Screen name (its golden file's stem) and the invocation that prints it.
const SCREENS: &[(&str, &[&str])] = &[
    ("flz", &["help"]),
    ("audit", &["audit", "help"]),
    ("audit-list", &["audit", "list", "--help"]),
    ("audit-read", &["audit", "read", "--help"]),
    ("audit-tail", &["audit", "tail", "--help"]),
    ("audit-stats", &["audit", "stats", "--help"]),
    ("audit-rotate", &["audit", "rotate", "--help"]),
    ("auth", &["auth", "help"]),
    ("boot", &["boot", "help"]),
    ("cluster", &["cluster", "help"]),
    ("controller", &["controller", "help"]),
    ("controller-start", &["controller", "start", "--help"]),
    ("controller-stop", &["controller", "stop", "--help"]),
    ("controller-status", &["controller", "status", "--help"]),
    ("controller-mem-top", &["controller", "mem-top", "--help"]),
    ("dlq", &["dlq", "help"]),
    ("namespace", &["namespace", "help"]),
    ("observe", &["observe", "help"]),
    ("observe-sources", &["observe", "sources", "--help"]),
    ("observe-sinks", &["observe", "sinks", "--help"]),
    ("observe-tail", &["observe", "tail", "--help"]),
    ("observe-enable", &["observe", "enable", "--help"]),
    ("observe-disable", &["observe", "disable", "--help"]),
    ("operative", &["operative", "help"]),
    ("plugin", &["plugin", "help"]),
    ("reset", &["reset", "help"]),
    ("role", &["role", "help"]),
    ("runner", &["runner", "help"]),
    ("schedule", &["schedule", "help"]),
    ("state", &["state", "help"]),
    ("token", &["token", "help"]),
];

fn golden_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden")
}

#[test]
fn every_help_screen_matches_its_golden_capture() {
    let mut wrong: Vec<String> = Vec::new();

    for (name, args) in SCREENS {
        let out = Command::new(FLZ_BIN)
            .args(*args)
            .output()
            .expect("flz runs");
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        let want = fs::read_to_string(golden_dir().join(format!("{}.txt", name)))
            .unwrap_or_else(|e| panic!("golden file for '{}': {}", name, e));

        if !out.status.success() {
            wrong.push(format!(
                "flz {}: exit {:?} (a help screen exits 0)",
                args.join(" "),
                out.status.code()
            ));
        }
        if stdout != want {
            wrong.push(format!(
                "flz {} ({}.txt):\n{}",
                args.join(" "),
                name,
                first_difference(&want, &stdout)
            ));
        }
    }

    assert!(
        wrong.is_empty(),
        "{} help screen(s) no longer match their capture:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

/// A screen with no golden file would silently stop being checked, and a golden
/// file no screen prints is a screen that was deleted without anyone saying so.
#[test]
fn the_golden_files_and_the_screens_are_the_same_set() {
    let mut on_disk: Vec<String> = fs::read_dir(golden_dir())
        .expect("golden dir")
        .map(|e| {
            e.expect("entry")
                .file_name()
                .to_string_lossy()
                .trim_end_matches(".txt")
                .to_string()
        })
        .collect();
    on_disk.sort();

    let mut declared: Vec<String> = SCREENS.iter().map(|(n, _)| n.to_string()).collect();
    declared.sort();

    assert_eq!(declared, on_disk);
}

/// The first differing line, with its neighbours — a whole-screen diff buries
/// a one-column move.
fn first_difference(want: &str, got: &str) -> String {
    let (w, g): (Vec<&str>, Vec<&str>) = (want.lines().collect(), got.lines().collect());
    for i in 0..w.len().max(g.len()) {
        let (a, b) = (w.get(i).copied(), g.get(i).copied());
        if a != b {
            return format!(
                "  line {}\n  want: {:?}\n  got:  {:?}",
                i + 1,
                a.unwrap_or("<end of screen>"),
                b.unwrap_or("<end of screen>")
            );
        }
    }
    "  (identical lines; trailing newline differs)".to_string()
}
