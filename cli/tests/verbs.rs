//! Where an option stands among the words does not change what it means.
//!
//! An option is read wherever the caller writes it, and the positionals are
//! the words that remain — so a flag written BEFORE the subject of the command
//! is a flag and not the subject. `flz operator stop --token t op1` used to
//! send `--token` as the operator id and drop the credential entirely; every
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
    // words that remain are the positionals. `flz operator stop --token t
    // op1` used to send `--token` as the operator id and drop the credential
    // entirely; every verb below is the same invocation written both ways.
    (
        &["operator", "stop", "--token", "t", "op1"],
        r#"Flezha.Service.CliManager.operator_stop("", "op1", [token: "t"])"#,
    ),
    (
        &["operator", "stop", "op1", "--token", "t"],
        r#"Flezha.Service.CliManager.operator_stop("", "op1", [token: "t"])"#,
    ),
    (
        &["operator", "stop", "-n", "prod", "op1"],
        r#"Flezha.Service.CliManager.operator_stop("prod", "op1", [])"#,
    ),
    (
        &["operator", "state", "--namespace=prod", "op1"],
        r#"Flezha.Service.CliManager.cell_state("prod", "op1", [])"#,
    ),
    (
        &["state", "get", "--output", "json", "a/b/c"],
        r#"Flezha.Service.CliManager.state_get("a/b/c", [output: "json"])"#,
    ),
    // A verb that reaches no controller carries no token, wherever it is written.
    (
        &["plugin", "install", "--token", "t", "/tmp/p.fpz"],
        r#"Flezha.Service.Cli.Plugin.eval("install", ["/tmp/p.fpz"], [])"#,
    ),
    (
        &["token", "issue", "--ttl", "60", "p", "ns", "viewer"],
        r#"Flezha.Service.CliManager.authz_render_token_issue("p", "ns", "viewer", [ttl: 60])"#,
    ),
    // A switch and the credential read the same on either side of the operator.
    (
        &["operator", "listen", "--input", "--token", "t", "op1"],
        r#"Flezha.Service.CliManager.operator_listen("", "op1", "input", [duration_ms: 30000, token: "t"])"#,
    ),
    (
        &["operator", "listen", "op1", "--token", "t", "--input"],
        r#"Flezha.Service.CliManager.operator_listen("", "op1", "input", [duration_ms: 30000, token: "t"])"#,
    ),
    // A value carrying a quote, a backslash and a non-ASCII letter survives
    // whichever side of the positionals it was written on.
    (
        &[
            "operator",
            "send",
            "--text",
            "a\"b\\cä",
            "-n",
            "prod",
            "op1",
        ],
        r#"Flezha.Service.CliManager.operator_send("prod", "op1", "a\"b\\cä", "", [])"#,
    ),
    (
        &[
            "operator",
            "send",
            "op1",
            "-n",
            "prod",
            "--text",
            "a\"b\\cä",
        ],
        r#"Flezha.Service.CliManager.operator_send("prod", "op1", "a\"b\\cä", "", [])"#,
    ),
    (
        &["audit", "read", "--source", "x\"y", "f.log"],
        r#"Flezha.Service.CliManager.audit_log_render_read("f.log", [source: "x\"y"])"#,
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
