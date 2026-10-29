//! Where an option stands among the words does not change what it means.
//!
//! An option is read wherever the caller writes it, and the positionals are
//! the words that remain — so a flag written BEFORE the subject of the command
//! is a flag and not the subject. `flz relay stop --token t op1` used to
//! send `--token` as the relay id and drop the credential entirely; every
//! case below is the same invocation written both ways, and the values carry
//! the three characters an argument list has historically lost.
//!
//! The evidence is the request itself, so the fixture is a socket that records
//! what it was sent, and a `flzvm` that echoes what it was handed, instead of
//! running either.

mod common;

use common::Shim;

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

const CASES: &[(&[&str], &str)] = &[
    // An option written before the positionals reads as an option, and the
    // words that remain are the positionals. `flz relay stop --token t
    // op1` used to send `--token` as the relay id and drop the credential
    // entirely; every verb below is the same invocation written both ways.
    (
        &["relay", "stop", "--token", "t", "op1"],
        r#"Relay.stop ["default","op1"] {"token":"t"}"#,
    ),
    (
        &["relay", "stop", "op1", "--token", "t"],
        r#"Relay.stop ["default","op1"] {"token":"t"}"#,
    ),
    (
        &["relay", "stop", "-n", "prod", "op1"],
        r#"Relay.stop ["prod","op1"] {}"#,
    ),
    (
        &["relay", "state", "--namespace=prod", "op1"],
        r#"Relay.cell_state ["prod","op1"] {}"#,
    ),
    (
        &["state", "get", "--output", "json", "a/b/c"],
        r#"Steps.state_get ["a/b/c"] {"output":"json"}"#,
    ),
    // A verb that reaches no controller carries no token, wherever it is written.
    (
        &["plugin", "install", "--token", "t", "/tmp/p.fpz"],
        r#"Flezha.Plugin.Installer.Command.eval("install", ["/tmp/p.fpz"], [])"#,
    ),
    (
        &["token", "issue", "--ttl", "60", "p", "ns", "viewer"],
        r#"Namespace.issue_token ["p","ns","viewer",{"ttl":60}] {}"#,
    ),
    // A switch and the credential read the same on either side of the relay.
    (
        &["relay", "listen", "--input", "--token", "t", "op1"],
        r#"Relay.listen ["default","op1","input",{"window_ms":30000}] {"token":"t"}"#,
    ),
    (
        &["relay", "listen", "op1", "--token", "t", "--input"],
        r#"Relay.listen ["default","op1","input",{"window_ms":30000}] {"token":"t"}"#,
    ),
    (
        &["relay", "listen", "op1", "--calls"],
        r#"Relay.listen ["default","op1","calls",{"window_ms":30000}] {}"#,
    ),
    // A value carrying a quote, a backslash and a non-ASCII letter survives
    // whichever side of the positionals it was written on.
    (
        &["relay", "send", "--text", "a\"b\\cä", "-n", "prod", "op1"],
        r#"Relay.send ["prod","op1","a\"b\\cä",null,{}] {}"#,
    ),
    (
        &["relay", "send", "op1", "-n", "prod", "--text", "a\"b\\cä"],
        r#"Relay.send ["prod","op1","a\"b\\cä",null,{}] {}"#,
    ),
    // `call` sends a deadline only with `--timeout`, so the controller's
    // default holds without it; `-t` is `--text`, the payload as written.
    (
        &["relay", "call", "echo", "-t", "hello"],
        r#"Relay.call_channel ["default","echo","","hello",{"payload":"text"}] {}"#,
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
        r#"Relay.call_channel ["default","prices","","x",{"deadline_ms":100,"key":"EURUSD","payload":"text"}] {}"#,
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
        r#"Relay.call_channel ["default","quoter","","EUR/USD",{"answer_channel":"quotes","payload":"text"}] {}"#,
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
        r#"Relay.call_channel ["default","orders-eu","place",{"amount":"1000","pair":"EUR/USD"},{}] {"coerce":true}"#,
    ),
    // With a relay id, `info` reads that relay's API.
    (
        &["relay", "info", "orders-eu"],
        r#"Relay.api ["default","orders-eu"] {}"#,
    ),
    (
        &["relay", "send", "prices", "--key", "EURUSD", "--text", "x"],
        r#"Relay.send ["default","prices","x",null,{"key":"EURUSD"}] {}"#,
    ),
    (
        &["relay", "send", "orders", "--msg-id", "a1", "--text", "x"],
        r#"Relay.send ["default","orders","x",null,{"msg_id":"a1"}] {}"#,
    ),
    (
        &["audit", "read", "--source", "x\"y", "f.log"],
        r#"Logs.read [{"file":"f.log","filters":{"source":"x\"y"},"kind":"audit"}] {}"#,
    ),
];

#[test]
fn an_option_reads_the_same_wherever_it_is_written() {
    let shim = Shim::recording(FLZ_BIN);
    let mut wrong: Vec<String> = Vec::new();

    for (argv, want) in CASES {
        let out = shim.run(argv);
        let got = String::from_utf8_lossy(&out.stdout);
        let doors = shim.doors();

        // A verb that reaches no controller writes its call whole on the
        // `flzvm` command line; every other verb sends it over the socket.
        let reached = if want.starts_with("Flezha.") {
            doors.is_empty() && got.lines().any(|line| line == *want)
        } else {
            doors == [*want]
        };
        if !reached {
            wrong.push(format!(
                "flz {}\n     got  {} {:?}\n     want {}",
                argv.join(" "),
                got.trim().replace('\n', " | "),
                doors,
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
/// them, never both, and an `--arg` names its field. A zero `--timeout` is
/// refused before anything is sent.
#[test]
fn a_call_takes_its_value_one_way() {
    let shim = Shim::recording(FLZ_BIN);

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
            &["relay", "call", "op", "-t", "x", "--timeout", "0"][..],
            "usage.invalid_number",
        ),
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
            shim.requests().is_empty(),
            "flz {} sent a request",
            argv.join(" ")
        );
    }
}
