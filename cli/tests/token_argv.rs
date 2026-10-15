//! The caller's token never rides a `flzvm` command line, which any local user
//! can read in `/proc/<pid>/cmdline`: it reaches the child on its stdin, and
//! `flz mcp` hands its own stdin to no child, so no child can take a protocol
//! line meant for the server.

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::process::Stdio;

use common::Shim;

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");
const TOKEN: &str = "flzt_kb716_must_not_reach_argv";
const TOOLS_LIST: &str = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#;

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

#[test]
fn a_verb_hands_the_token_over_on_stdin_not_on_the_command_line() {
    let shim = Shim::new(FLZ_BIN, RECORDER);
    let out = shim
        .cmd()
        .args(["dlq", "list"])
        .env("FLEZHA_TOKEN", TOKEN)
        .output()
        .expect("flz runs");
    assert!(out.status.success(), "{:?}", out);

    let argv = seen(&shim, "argv");
    assert!(
        argv.contains("Flezha.Service.Cli.call"),
        "the shim was called: {argv}"
    );
    assert!(!argv.contains(TOKEN), "token on argv: {argv}");
    assert_eq!(seen(&shim, "stdin"), TOKEN);
}

#[test]
fn flz_mcp_hands_the_token_over_on_stdin_not_on_the_command_line() {
    let shim = Shim::new(FLZ_BIN, RECORDER);
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
    child.wait_with_output().expect("flz mcp ends");

    let argv = seen(&shim, "argv");
    assert!(
        argv.contains("Flezha.Mcp.Stdio.answer"),
        "the shim was called: {argv}"
    );
    assert!(!argv.contains(TOKEN), "token on argv: {argv}");
    assert_eq!(seen(&shim, "stdin"), TOKEN);
}

/// The second line is written only once the first is answered, while that
/// first child still reads its stdin: an inherited stdin would take it.
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

    writeln!(stdin, "{TOOLS_LIST}").unwrap();
    drop(stdin);
    let mut rest = String::new();
    stdout.read_to_string(&mut rest).unwrap();
    child.wait().expect("flz mcp ends");

    assert_eq!(rest.lines().count(), 1, "second reply: {rest}");
    assert_eq!(
        seen(&shim, "argv")
            .matches("Flezha.Mcp.Stdio.answer")
            .count(),
        2
    );
    assert_eq!(seen(&shim, "stdin"), "");
}
