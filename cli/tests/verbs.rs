//! Where an option stands among the words does not change what it means.
//!
//! An option is read wherever the caller writes it, and the positionals are
//! the words that remain — so a flag written BEFORE the subject of the command
//! is a flag and not the subject. `flz relay stop --token t op1` used to
//! send `--token` as the relay id and drop the credential entirely; every
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
    // words that remain are the positionals. `flz relay stop --token t
    // op1` used to send `--token` as the relay id and drop the credential
    // entirely; every verb below is the same invocation written both ways.
    (
        &["relay", "stop", "--token", "t", "op1"],
        r#"Flezha.Service.Cli.call("Relay.stop", "[\"default\",\"op1\"]", [token: IO.read(:eof)])"#,
    ),
    (
        &["relay", "stop", "op1", "--token", "t"],
        r#"Flezha.Service.Cli.call("Relay.stop", "[\"default\",\"op1\"]", [token: IO.read(:eof)])"#,
    ),
    (
        &["relay", "stop", "-n", "prod", "op1"],
        r#"Flezha.Service.Cli.call("Relay.stop", "[\"prod\",\"op1\"]", [])"#,
    ),
    (
        &["relay", "state", "--namespace=prod", "op1"],
        r#"Flezha.Service.Cli.call("Relay.cell_state", "[\"prod\",\"op1\"]", [])"#,
    ),
    (
        &["state", "get", "--output", "json", "a/b/c"],
        r#"Flezha.Service.Cli.call("Steps.state_get", "[\"a/b/c\"]", [output: "json"])"#,
    ),
    // A verb that reaches no controller carries no token, wherever it is written.
    (
        &["plugin", "install", "--token", "t", "/tmp/p.fpz"],
        r#"Flezha.Plugin.Installer.Command.eval("install", ["/tmp/p.fpz"], [])"#,
    ),
    (
        &["token", "issue", "--ttl", "60", "p", "ns", "viewer"],
        r#"Flezha.Service.Cli.call("Namespace.issue_token", "[\"p\",\"ns\",\"viewer\",{\"ttl\":60}]", [])"#,
    ),
    // A switch and the credential read the same on either side of the relay.
    (
        &["relay", "listen", "--input", "--token", "t", "op1"],
        r#"Flezha.Service.Cli.call("Relay.listen", "[\"default\",\"op1\",\"input\",{\"window_ms\":30000}]", [token: IO.read(:eof)])"#,
    ),
    (
        &["relay", "listen", "op1", "--token", "t", "--input"],
        r#"Flezha.Service.Cli.call("Relay.listen", "[\"default\",\"op1\",\"input\",{\"window_ms\":30000}]", [token: IO.read(:eof)])"#,
    ),
    (
        &["relay", "listen", "op1", "--calls"],
        r#"Flezha.Service.Cli.call("Relay.listen", "[\"default\",\"op1\",\"calls\",{\"window_ms\":30000}]", [])"#,
    ),
    // A value carrying a quote, a backslash and a non-ASCII letter survives
    // whichever side of the positionals it was written on.
    (
        &["relay", "send", "--text", "a\"b\\cä", "-n", "prod", "op1"],
        r#"Flezha.Service.Cli.call("Relay.send", "[\"prod\",\"op1\",\"a\\\"b\\\\cä\",\"\",{}]", [])"#,
    ),
    (
        &["relay", "send", "op1", "-n", "prod", "--text", "a\"b\\cä"],
        r#"Flezha.Service.Cli.call("Relay.send", "[\"prod\",\"op1\",\"a\\\"b\\\\cä\",\"\",{}]", [])"#,
    ),
    // `call`'s deadline defaults and has no short spelling: `-t` is `--text`,
    // the payload as written.
    (
        &["relay", "call", "echo", "-t", "hello"],
        r#"Flezha.Service.Cli.call("Relay.call_channel", "[\"default\",\"echo\",\"\",\"hello\",{\"deadline_ms\":30000,\"payload\":\"text\"}]", [])"#,
    ),
    (
        &[
            "relay",
            "call",
            "--timeout",
            "100",
            "--key",
            "EURUSD",
            "prices",
            "--text",
            "x",
        ],
        r#"Flezha.Service.Cli.call("Relay.call_channel", "[\"default\",\"prices\",\"\",\"x\",{\"deadline_ms\":100,\"key\":\"EURUSD\",\"payload\":\"text\"}]", [])"#,
    ),
    // The answer channel rides beside the deadline; `--channel` stays the entry.
    (
        &[
            "relay",
            "call",
            "quoter",
            "--text",
            "EUR/USD",
            "--answer-channel",
            "quotes",
        ],
        r#"Flezha.Service.Cli.call("Relay.call_channel", "[\"default\",\"quoter\",\"\",\"EUR/USD\",{\"answer_channel\":\"quotes\",\"deadline_ms\":30000,\"payload\":\"text\"}]", [])"#,
    ),
    // Each `--arg` is one field, still text: the controller types it by the
    // channel's schema, which `coerce` asks it to read first.
    (
        &[
            "relay",
            "call",
            "orders-eu",
            "--arg",
            "pair=EUR/USD",
            "--channel",
            "place",
            "--arg",
            "amount=1000",
        ],
        r#"Flezha.Service.Cli.call("Relay.call_channel", "[\"default\",\"orders-eu\",\"place\",{\"amount\":\"1000\",\"pair\":\"EUR/USD\"},{\"deadline_ms\":30000}]", [coerce: true])"#,
    ),
    // With a relay id, `info` reads that relay's API.
    (
        &["relay", "info", "orders-eu"],
        r#"Flezha.Service.Cli.call("Relay.api", "[\"default\",\"orders-eu\"]", [])"#,
    ),
    (
        &["relay", "send", "prices", "--key", "EURUSD", "--text", "x"],
        r#"Flezha.Service.Cli.call("Relay.send", "[\"default\",\"prices\",\"x\",\"\",{\"key\":\"EURUSD\"}]", [])"#,
    ),
    (
        &["relay", "send", "orders", "--msg-id", "a1", "--text", "x"],
        r#"Flezha.Service.Cli.call("Relay.send", "[\"default\",\"orders\",\"x\",\"\",{\"msg_id\":\"a1\"}]", [])"#,
    ),
    (
        &["audit", "read", "--source", "x\"y", "f.log"],
        r#"Flezha.Service.Cli.call("AuditLog.read", "[{\"file\":\"f.log\",\"source\":\"x\\\"y\"}]", [])"#,
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

/// `--text` and `--arg` are the two ways to give a call its value: one of
/// them, never both, and an `--arg` names its field.
#[test]
fn a_call_takes_its_value_one_way() {
    let shim = Shim::new(FLZ_BIN, ECHOING);

    for (argv, code) in [
        (
            &["relay", "call", "op", "--text", "x", "--arg", "a=1"][..],
            "usage.options_conflict",
        ),
        (
            &["relay", "listen", "op1", "--input", "--calls"][..],
            "usage.options_conflict",
        ),
        (&["relay", "call", "op"][..], "usage.option_required"),
        (
            &["relay", "call", "op", "--arg", "amount"][..],
            "api.input_invalid",
        ),
    ] {
        let out = shim.run(argv);
        let err = String::from_utf8_lossy(&out.stderr);

        assert_eq!(out.status.code(), Some(2), "flz {}: {err}", argv.join(" "));
        assert!(err.contains(code), "flz {}: {err}", argv.join(" "));
        assert!(
            out.stdout.is_empty(),
            "flz {} sent a request",
            argv.join(" ")
        );
    }
}
