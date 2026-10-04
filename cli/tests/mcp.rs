//! `flz mcp`'s own part of the protocol: which `flzvm` answers a line, that
//! stdout carries replies only, and the answers it gives itself when no child
//! can.

mod common;

use std::io::Write;
use std::process::{Output, Stdio};

use common::Shim;
use serde_json::Value;

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

const REQUEST: &str = r#"{"jsonrpc":"2.0","id":7,"method":"ping"}"#;
const NOTIFICATION: &str = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;

/// A controller that cannot be reached, and a VM of its own that logs a line
/// at boot, replies, and shows on stderr the expression it was handed.
const OFFLINE: &str = "#!/bin/sh
case \"$1\" in
rpc) echo '--rpc-eval : RPC failed with reason :noconnection' >&2; exit 1 ;;
eval) echo 'a boot log line'; echo '{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{}}'; echo \"$2\" >&2 ;;
esac
";

/// A child that fails without replying.
const FAILING: &str = "#!/bin/sh\necho 'it broke' >&2\nexit 1\n";

fn serve(shim: &Shim, lines: &[&str]) -> Output {
    let mut child = shim
        .cmd()
        .arg("mcp")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("flz mcp runs");

    let mut stdin = child.stdin.take().expect("stdin");
    for line in lines {
        writeln!(stdin, "{}", line).expect("write a line");
    }
    drop(stdin);

    child
        .wait_with_output()
        .expect("flz mcp ends with its input")
}

fn replies(out: &Output) -> Vec<Value> {
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).expect("every stdout line is JSON"))
        .collect()
}

#[test]
fn an_unreachable_controller_hands_the_line_to_a_vm_of_its_own() {
    let shim = Shim::new(FLZ_BIN, OFFLINE);
    let out = serve(&shim, &[REQUEST]);
    let stderr = String::from_utf8_lossy(&out.stderr);

    assert!(out.status.success());
    assert_eq!(
        replies(&out),
        vec![serde_json::json!({"jsonrpc": "2.0", "id": 7, "result": {}})]
    );
    assert!(stderr.contains("a boot log line"), "{}", stderr);
    assert!(stderr.contains("Flezha.Mcp.Stdio.offline("), "{}", stderr);
    assert!(!stderr.contains("noconnection"), "{}", stderr);
}

#[test]
fn a_child_that_fails_without_replying_still_answers_the_request() {
    let shim = Shim::new(FLZ_BIN, FAILING);
    let out = serve(&shim, &[REQUEST, NOTIFICATION]);

    let answered = replies(&out);
    assert_eq!(answered.len(), 1, "a notification is answered with nothing");
    assert_eq!(answered[0]["id"], 7);
    assert_eq!(answered[0]["error"]["code"], -32_603);
    assert!(String::from_utf8_lossy(&out.stderr).contains("it broke"));

    let missing = serve(&Shim::without_flzvm(FLZ_BIN), &[REQUEST]);
    assert_eq!(replies(&missing)[0]["error"]["code"], -32_603);
}

/// A line that is not UTF-8 is handed on like any other, and the lines after
/// it, and after a request that failed, are still answered.
#[test]
fn a_bad_line_does_not_end_the_session() {
    let shim = Shim::new(FLZ_BIN, FAILING);
    let mut child = shim
        .cmd()
        .arg("mcp")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("flz mcp runs");

    let mut stdin = child.stdin.take().expect("stdin");
    stdin
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"\xff\"}\n")
        .expect("write");
    writeln!(stdin, r#"{{"jsonrpc":"2.0","id":2,"method":"ping"}}"#).expect("write");
    drop(stdin);

    let out = child
        .wait_with_output()
        .expect("flz mcp ends with its input");
    let ids: Vec<Value> = replies(&out)
        .iter()
        .map(|reply| reply["id"].clone())
        .collect();
    assert_eq!(ids, vec![Value::from(1), Value::from(2)]);
}

#[test]
fn a_request_past_the_argument_limit_is_refused_without_a_child() {
    let shim = Shim::new(FLZ_BIN, FAILING);
    let large = format!(
        r#"{{"jsonrpc":"2.0","id":"big","method":"ping","params":{{"pad":"{}"}}}}"#,
        "x".repeat(130 * 1024)
    );
    let out = serve(&shim, &[&large]);

    let answered = replies(&out);
    assert_eq!(answered[0]["id"], "big");
    assert_eq!(answered[0]["error"]["code"], -32_600);
    assert!(!String::from_utf8_lossy(&out.stderr).contains("it broke"));
}
