//! Where an option stands among the words does not change what it means.
//!
//! An option is read wherever the operator writes it, and the positionals are
//! the words that remain — so a flag written BEFORE the subject of the command
//! is a flag and not the subject. `flz operative stop --token t op1` used to
//! send `--token` as the operative id and drop the credential entirely; every
//! case below is the same invocation written both ways, and the values carry
//! the three characters an argument list has historically lost.
//!
//! The evidence is the RPC expression itself, so the fixture is a `flzvm` that
//! echoes what it was handed instead of running it.

mod common;

use common::Shim;

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

/// A `flzvm` that prints each argument on its own line and succeeds, so the
/// expression `flz` built is readable on stdout.
const ECHOING: &str = "#!/bin/sh\nprintf '%s\\n' \"$@\"\nexit 0\n";

const CASES: &[(&[&str], &str)] = &[
    // An option written before the positionals reads as an option, and the
    // words that remain are the positionals. `flz operative stop --token t
    // op1` used to send `--token` as the operative id and drop the credential
    // entirely; every verb below is the same invocation written both ways.
    (
        &["operative", "stop", "--token", "t", "op1"],
        r#"Flezha.Service.CliManager.operative_stop("", "op1", [token: "t"])"#,
    ),
    (
        &["operative", "stop", "op1", "--token", "t"],
        r#"Flezha.Service.CliManager.operative_stop("", "op1", [token: "t"])"#,
    ),
    (
        &["operative", "stop", "-n", "prod", "op1"],
        r#"Flezha.Service.CliManager.operative_stop("prod", "op1", [])"#,
    ),
    (
        &["operative", "state", "--namespace=prod", "op1"],
        r#"Flezha.Service.CliManager.cell_state("prod", "op1", [])"#,
    ),
    (
        &["state", "get", "--output", "json", "a/b/c"],
        r#"Flezha.Service.CliManager.state_get("a/b/c", "json", [])"#,
    ),
    (
        &["plugin", "install", "--token", "t", "/tmp/p.fpz"],
        r#"Flezha.Service.CliManager.install_plugin("/tmp/p.fpz", [token: "t"])"#,
    ),
    (
        &["token", "issue", "--ttl", "60", "p", "ns", "viewer"],
        r#"Flezha.Service.CliManager.authz_render_token_issue("p", "ns", "viewer", [ttl: 60])"#,
    ),
    // A switch and the credential read the same on either side of the operative.
    (
        &["operative", "listen", "--input", "--token", "t", "op1"],
        r#"Flezha.Service.CliManager.operative_listen("", "op1", "input", "text", [duration_ms: 30000, token: "t"])"#,
    ),
    (
        &["operative", "listen", "op1", "--token", "t", "--input"],
        r#"Flezha.Service.CliManager.operative_listen("", "op1", "input", "text", [duration_ms: 30000, token: "t"])"#,
    ),
    // A value carrying a quote, a backslash and a non-ASCII letter survives
    // whichever side of the positionals it was written on.
    (
        &[
            "operative",
            "send",
            "--text",
            "a\"b\\cä",
            "-n",
            "prod",
            "op1",
        ],
        r#"Flezha.Service.CliManager.operative_send("prod", "op1", "a\"b\\cä", "", [])"#,
    ),
    (
        &[
            "operative",
            "send",
            "op1",
            "-n",
            "prod",
            "--text",
            "a\"b\\cä",
        ],
        r#"Flezha.Service.CliManager.operative_send("prod", "op1", "a\"b\\cä", "", [])"#,
    ),
    (
        &["audit", "read", "--source", "x\"y", "f.log"],
        r#"Flezha.Service.CliManager.audit_log_render_read("f.log", "text", [source: "x\"y"])"#,
    ),
];

#[test]
fn an_option_reads_the_same_wherever_it_is_written() {
    let shim = Shim::new(FLZ_BIN, ECHOING);
    let mut wrong: Vec<String> = Vec::new();

    for (argv, want) in CASES {
        let out = shim.run(argv);
        let got = String::from_utf8_lossy(&out.stdout);

        if !got.lines().any(|line| line == *want) {
            wrong.push(format!(
                "flz {}\n     got  {}\n     want {}",
                argv.join(" "),
                got.trim().replace('\n', " | "),
                want
            ));
        }
    }

    assert!(
        wrong.is_empty(),
        "{} of {} invocations built the wrong request:\n{}",
        wrong.len(),
        CASES.len(),
        wrong.join("\n")
    );
}
