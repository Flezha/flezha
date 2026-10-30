//! `flz mcp`'s own part of the protocol: who answers a line (the controller
//! over its socket, or a `flzvm` of its own when none can be reached), that
//! stdout carries replies only, and the answers it gives itself when neither
//! can.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use common::{Reply, Shim, ECHOING};
use serde_json::Value;

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

const REQUEST: &str = r#"{"jsonrpc":"2.0","id":7,"method":"tools/list"}"#;
const NOTIFICATION: &str = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;

/// With no controller to reach, a VM of its own that shows on stderr the
/// expression it was handed and the line its stdin carried, then logs a line
/// at boot and replies. Its stderr is written in full first: `flz` passes the
/// boot line to the same stderr, and would split a copy still being written.
const OFFLINE: &str = "#!/bin/sh
echo \"$2\" >&2; cat >&2; echo 'a boot log line'; echo '{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{}}'
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
    assert!(
        stderr.contains("arguments = IO.read(:line); Flezha.Mcp.Stdio.offline(arguments, "),
        "{}",
        stderr
    );
    assert!(stderr.contains(&format!("{REQUEST}\n")), "{}", stderr);
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
    writeln!(stdin, r#"{{"jsonrpc":"2.0","id":2,"method":"tools/list"}}"#).expect("write");
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

/// A request rides the child's stdin, so one past the 128 KiB a command-line
/// argument may hold is handed on whole.
#[test]
fn a_request_past_the_argument_limit_is_handed_on() {
    let shim = Shim::new(FLZ_BIN, OFFLINE);
    let large = format!(
        r#"{{"jsonrpc":"2.0","id":7,"method":"ping","params":{{"pad":"{}"}}}}"#,
        "x".repeat(130 * 1024)
    );
    let out = serve(&shim, &[&large]);

    assert_eq!(replies(&out)[0]["id"], 7);
    assert!(String::from_utf8_lossy(&out.stderr).contains(&format!("{large}\n")));
}

/// With a controller serving, the line is its request and its answer the
/// reply: no VM is started.
#[test]
fn a_running_controller_answers_and_no_vm_starts() {
    let reply = r#"{"jsonrpc":"2.0","id":7,"result":{"tools":[]}}"#;
    let shim = Shim::new(FLZ_BIN, ECHOING).serving(move |_, _| Reply::Answer {
        stdout: format!("{reply}\n"),
        stderr: String::new(),
        code: 0,
    });
    let out = serve(&shim, &[REQUEST]);

    assert_eq!(
        replies(&out),
        vec![serde_json::from_str::<Value>(reply).unwrap()]
    );
    assert_eq!(shim.starts(), Vec::<String>::new());
    let requests = shim.requests();
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert_eq!(requests[0]["entry"], "mcp");
    assert_eq!(requests[0]["line"], REQUEST);
}

/// A call the controller is still answering is cancelled by closing its
/// connection, which ends the call there, and it is never answered.
#[test]
fn a_cancelled_call_closes_its_connection() {
    let (closed, closes) = mpsc::channel();
    let closed = std::sync::Mutex::new(Some(closed));
    let shim = Shim::new(FLZ_BIN, ECHOING).serving(move |_, _| {
        closed
            .lock()
            .unwrap()
            .take()
            .map_or(Reply::ok(), Reply::Hold)
    });
    let mut mcp = spawn_mcp(&shim);
    let mut stdin = mcp.stdin.take().expect("stdin");
    writeln!(stdin, "{}", CALL).expect("write");
    assert!(
        within_guard(|| !shim.requests().is_empty()),
        "the call reached the controller"
    );
    writeln!(stdin, "{}", CANCEL).expect("write");

    let ended = closes.recv_timeout(Duration::from_secs(5));
    drop(stdin);
    let out = mcp.wait_with_output().expect("flz mcp ends with its input");
    assert!(
        ended.is_ok(),
        "the cancelled call's connection is still open"
    );
    assert!(replies(&out).is_empty(), "{:?}", replies(&out));
    assert_eq!(shim.starts(), Vec::<String>::new());
}

/// A controller that hangs on a `tools/call`, recording the hung pid beside
/// itself, and answers anything else at once.
const HANGS_ON_CALL: &str = "#!/bin/sh
read -r line
case \"$line\" in
*tools/call*) echo $$ > \"$(dirname \"$0\")/hung.pid\"; exec sleep 30 ;;
*) echo '{\"jsonrpc\":\"2.0\",\"id\":3,\"result\":{}}' ;;
esac
";

const CALL: &str = r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"relay_list","arguments":{}}}"#;
const PING: &str = r#"{"jsonrpc":"2.0","id":3,"method":"ping"}"#;
const CANCEL: &str =
    r#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":2}}"#;

fn spawn_mcp(shim: &Shim) -> Child {
    shim.cmd()
        .arg("mcp")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("flz mcp runs")
}

/// Whether `check` holds within fifteen seconds: a guard against a hang on a
/// loaded machine, not a measure of how long anything takes.
fn within_guard(check: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !check() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    true
}

/// The pid of the child the `tools/call` hangs in, once it has started.
fn hung_pid(shim: &Shim) -> String {
    let file = shim.flzvm_path().with_file_name("hung.pid");
    let read = || std::fs::read_to_string(&file).unwrap_or_default();
    assert!(
        within_guard(|| read().ends_with('\n')),
        "the call reached the child"
    );
    read().trim().to_string()
}

fn alive(pid: &str) -> bool {
    kill("-0", pid)
}

/// Whether `signal` reached `pid`.
fn kill(signal: &str, pid: &str) -> bool {
    Command::new("kill")
        .args([signal, pid])
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

#[test]
fn a_ping_is_answered_while_a_call_hangs() {
    let shim = Shim::new(FLZ_BIN, HANGS_ON_CALL);
    let mut mcp = spawn_mcp(&shim);
    let mut stdin = mcp.stdin.take().expect("stdin");
    writeln!(stdin, "{}", CALL).expect("write");
    let pid = hung_pid(&shim);
    writeln!(stdin, "{}", PING).expect("write");

    let stdout = mcp.stdout.take().expect("stdout");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            let _ = tx.send(line);
        }
    });

    let reply = rx.recv_timeout(Duration::from_secs(5));
    kill("-TERM", &mcp.id().to_string());
    let _ = mcp.wait();
    kill("-KILL", &pid);
    let reply = reply.expect("the ping is answered while the call hangs");
    assert!(reply.contains(r#""id":3"#), "{reply}");
}

/// SIGTERM is how an MCP client stops a server that does not exit when its
/// input closes; the child it was waiting on goes with it.
#[test]
fn a_terminated_flz_mcp_leaves_no_child_behind() {
    let shim = Shim::new(FLZ_BIN, HANGS_ON_CALL);
    let mut mcp = spawn_mcp(&shim);
    writeln!(mcp.stdin.as_mut().expect("stdin"), "{}", CALL).expect("write");
    let pid = hung_pid(&shim);

    kill("-TERM", &mcp.id().to_string());
    let _ = mcp.wait();

    let left = alive(&pid);
    kill("-KILL", &pid);
    assert!(!left, "the flzvm child {pid} outlived flz mcp");
}

/// A cancelled call's child is killed, and the call is never answered: MCP
/// has a receiver send nothing for a request it was told to cancel.
#[test]
fn a_cancelled_call_is_killed_and_never_answered() {
    let shim = Shim::new(FLZ_BIN, HANGS_ON_CALL);
    let mut mcp = spawn_mcp(&shim);
    let mut stdin = mcp.stdin.take().expect("stdin");
    writeln!(stdin, "{}", CALL).expect("write");
    let pid = hung_pid(&shim);
    writeln!(stdin, "{}", CANCEL).expect("write");

    let killed = within_guard(|| !alive(&pid));
    kill("-KILL", &pid);
    drop(stdin);
    let out = mcp.wait_with_output().expect("flz mcp ends with its input");
    assert!(killed, "the cancelled call's child {pid} still runs");
    assert!(replies(&out).is_empty(), "{:?}", replies(&out));
}

/// A `flz mcp` killed outright, with no chance to stop its child, still takes
/// that child with it.
#[test]
fn a_killed_flz_mcp_leaves_no_child_behind() {
    let shim = Shim::new(FLZ_BIN, HANGS_ON_CALL);
    let mut mcp = spawn_mcp(&shim);
    writeln!(mcp.stdin.as_mut().expect("stdin"), "{}", CALL).expect("write");
    let pid = hung_pid(&shim);

    let _ = mcp.kill();
    let _ = mcp.wait();

    let gone = within_guard(|| !alive(&pid));
    kill("-KILL", &pid);
    assert!(gone, "the flzvm child {pid} outlived a killed flz mcp");
}
