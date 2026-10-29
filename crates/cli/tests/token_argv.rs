//! The caller's token never rides a command line, which any local user can
//! read in `/proc/<pid>/cmdline`, nor a child's stdin: it travels in the
//! request frame alone, over the owner-only control socket, and no child is
//! started for a verb that reaches a controller. The one-time bootstrap token
//! rides neither command line: `flz auth bootstrap` reads it from its own
//! stdin. With no controller, `flz mcp` hands each `flzvm eval` child its own
//! line and nothing else, so no child can take a protocol line meant for the
//! server.

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::os::fd::{FromRawFd, OwnedFd};
use std::process::Stdio;
use std::time::{Duration, Instant};

use common::{output_fed, Reply, Shim};
use serde_json::{json, Value};

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");
const TOKEN: &str = "flzt_kb716_must_not_reach_argv";
const BOOTSTRAP: &str = "kb802bootstraptokenmustnotreachargv";
const TOOLS_LIST: &str = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#;
const NEXT_LINE: &str = r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#;

/// A flzvm that records its argv beside itself, replies, then records all of
/// its stdin: an inherited stdin hands it whatever the caller writes next.
const RECORDER: &str = "#!/bin/sh
dir=\"$(dirname \"$0\")\"
printf '%s\\n' \"$@\" >> \"$dir/argv\"
echo '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}'
cat >> \"$dir/stdin\"
";

fn seen(shim: &Shim, file: &str) -> String {
    std::fs::read_to_string(shim.flzvm_path().with_file_name(file)).unwrap_or_default()
}

/// The recorder beside `flz`, and a socket that accepts every request.
fn serving() -> Shim {
    Shim::new(FLZ_BIN, RECORDER).serving(|_, _| Reply::ok())
}

/// The one request a run sent, after checking it started no child.
fn the_request(shim: &Shim) -> Value {
    assert_eq!(seen(shim, "argv"), "", "a child was started");
    let mut requests = shim.requests();
    assert_eq!(requests.len(), 1, "{requests:?}");
    requests.remove(0)
}

#[test]
fn a_verb_sends_the_token_in_its_request_alone() {
    let shim = serving();
    let out = shim
        .cmd()
        .args(["dlq", "list"])
        .env("FLEZHA_TOKEN", TOKEN)
        .output()
        .expect("flz runs");
    assert!(out.status.success(), "{:?}", out);

    let request = the_request(&shim);
    assert_eq!(request["name"], "Dlq.list");
    assert_eq!(request["args"], json!([null, null]));
    assert_eq!(request["opts"], json!({ "token": TOKEN }));
}

/// A verb's arguments can be as private as a token: a call's payload travels
/// in the request too, never on a command line.
#[test]
fn a_verb_sends_its_arguments_in_its_request() {
    let shim = serving();
    let out = shim.run(&["relay", "call", "op", "--text", "kb802 payload ä"]);
    assert!(out.status.success(), "{:?}", out);

    let request = the_request(&shim);
    assert_eq!(request["name"], "Relay.call_channel");
    assert_eq!(
        request["args"],
        json!(["default", "op", "", "kb802 payload ä", { "payload": "text" }])
    );
}

#[test]
fn bootstrap_sends_its_token_in_its_request() {
    let shim = serving();
    let mut cmd = shim.cmd();
    cmd.args(["auth", "bootstrap"]).env("FLEZHA_TOKEN", TOKEN);
    let out = output_fed(cmd, &format!("  {BOOTSTRAP}\n"));
    assert!(out.status.success(), "{:?}", out);

    let request = the_request(&shim);
    assert_eq!(request["name"], "Namespace.bootstrap");
    assert_eq!(request["args"], json!([BOOTSTRAP]));
    assert_eq!(request["opts"], json!({ "token": TOKEN }));
}

/// The token is never typed on `flz`'s own command line either: a word there
/// is refused, and so is an empty stdin, before any child starts.
#[test]
fn bootstrap_takes_its_token_from_stdin_only() {
    let shim = serving();

    for (argv, input, code) in [
        (
            &["auth", "bootstrap", BOOTSTRAP][..],
            "",
            "usage.unexpected_argument",
        ),
        (&["auth", "bootstrap"][..], "\n", "usage.missing_argument"),
        (&["auth", "bootstrap"][..], "", "usage.missing_argument"),
    ] {
        let mut cmd = shim.cmd();
        cmd.args(argv);
        let out = output_fed(cmd, input);
        let err = String::from_utf8_lossy(&out.stderr);

        assert_eq!(out.status.code(), Some(2), "flz {}: {err}", argv.join(" "));
        assert!(err.contains(code), "flz {}: {err}", argv.join(" "));
    }
    assert_eq!(
        seen(&shim, "argv"),
        "",
        "a refused bootstrap started a child"
    );
    assert!(
        shim.requests().is_empty(),
        "a refused bootstrap sent a request"
    );
}

/// A terminal on stdin is refused at once rather than read from in silence.
#[test]
fn bootstrap_refuses_a_terminal_on_stdin() {
    let shim = Shim::new(FLZ_BIN, RECORDER);
    let (mut controller, mut terminal) = (-1, -1);
    // SAFETY: `openpty` writes only the two descriptors it opens.
    let opened = unsafe {
        libc::openpty(
            &mut controller,
            &mut terminal,
            std::ptr::null_mut(),
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    assert_eq!(opened, 0, "openpty");
    // SAFETY: both descriptors were just opened, and nothing else owns them.
    let (_controller, terminal) = unsafe {
        (
            OwnedFd::from_raw_fd(controller),
            OwnedFd::from_raw_fd(terminal),
        )
    };

    let mut child = shim
        .cmd()
        .args(["auth", "bootstrap"])
        .stdin(Stdio::from(terminal))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("flz runs");
    // A guard against a wait on the terminal, not a measure of anything.
    let deadline = Instant::now() + Duration::from_secs(5);
    while child.try_wait().expect("wait").is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    let _ = child.kill();
    let out = child.wait_with_output().expect("flz ends");
    let err = String::from_utf8_lossy(&out.stderr);

    assert_eq!(out.status.code(), Some(2), "{err}");
    assert!(err.contains("usage.missing_argument"), "{err}");
    assert_eq!(
        seen(&shim, "argv"),
        "",
        "a refused bootstrap started a child"
    );
}

#[test]
fn flz_mcp_sends_the_token_in_its_request_alone() {
    let reply = r#"{"jsonrpc":"2.0","id":1,"result":{}}"#;
    let shim = Shim::new(FLZ_BIN, RECORDER).serving(move |_, _| Reply::Answer {
        stdout: format!("{reply}\n"),
        stderr: String::new(),
        code: 0,
    });
    let mut child = shim
        .cmd()
        .arg("mcp")
        .env("FLEZHA_TOKEN", TOKEN)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("flz mcp runs");
    let mut stdin = child.stdin.take().expect("stdin");
    writeln!(stdin, "{TOOLS_LIST}").unwrap();
    drop(stdin);
    let out = child.wait_with_output().expect("flz mcp ends");

    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), reply);
    let request = the_request(&shim);
    assert_eq!(request["entry"], "mcp");
    assert_eq!(request["line"], TOOLS_LIST);
    assert_eq!(request["opts"], json!({ "token": TOKEN }));
}

/// With no controller, the second line is written only once the first is
/// answered, while that first child still reads its stdin: an inherited stdin
/// would take it. Each child reads its own line and no other.
#[test]
fn flz_mcp_keeps_every_protocol_line_from_its_children() {
    let shim = Shim::new(FLZ_BIN, RECORDER);
    let mut child = shim
        .cmd()
        .arg("mcp")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("flz mcp runs");
    let mut stdin = child.stdin.take().expect("stdin");
    let mut stdout = BufReader::new(child.stdout.take().expect("stdout"));

    writeln!(stdin, "{TOOLS_LIST}").unwrap();
    let mut first = String::new();
    stdout.read_line(&mut first).unwrap();
    assert!(first.starts_with('{'), "first reply: {first}");

    writeln!(stdin, "{NEXT_LINE}").unwrap();
    drop(stdin);
    let mut rest = String::new();
    stdout.read_to_string(&mut rest).unwrap();
    child.wait().expect("flz mcp ends");

    assert_eq!(rest.lines().count(), 1, "second reply: {rest}");
    assert_eq!(
        seen(&shim, "argv")
            .matches("Flezha.Mcp.Stdio.offline")
            .count(),
        2
    );
    assert_eq!(seen(&shim, "stdin"), format!("{TOOLS_LIST}\n{NEXT_LINE}\n"));
}
