//! `flz mcp` — the MCP server an AI assistant starts, on stdin and stdout.
//!
//! It holds no MCP of its own. Each line is one JSON-RPC message, answered by
//! `Flezha.Mcp.Stdio` the way every verb is answered: on the controller through
//! `flzvm rpc`, as the caller's token, or, while no controller can be reached,
//! in a VM of its own through `flzvm eval`, which serves the knowledge tools and
//! refuses the operations. Each line asks again, so a controller started later
//! is found without restarting the assistant.

use std::io::{self, BufRead, Write};

use flz_transport::utility::socket_dir;
use serde_json::{json, Value};

use crate::args::Reading;
use crate::rpc::{self, text, Out, Request};

/// The longest request passed on. It rides one argument of the child's
/// command line, which Linux caps at 128 KiB.
const MAX_EXPRESSION: usize = 120 * 1024;

pub(crate) fn serve(read: &Reading) {
    let token = read.token();
    let socket = socket_dir::control_socket_path().unwrap_or_default();

    // Bytes, not `lines()`: one line that is not UTF-8 must not end the
    // session. It reaches the core as text the parser then refuses.
    for bytes in io::stdin().lock().split(b'\n').map_while(Result::ok) {
        let line = String::from_utf8_lossy(&bytes);
        if !line.trim().is_empty() {
            answer(&line, token, &socket);
        }
    }
}

fn answer(line: &str, token: Option<&str>, socket: &str) {
    let online = rpc::request(&[], token, false, |list| {
        format!("Flezha.Mcp.Stdio.answer({}, {})", text(line), list)
    });
    let offline = Request::bare(format!(
        "Flezha.Mcp.Stdio.offline({}, {})",
        text(line),
        text(socket)
    ));

    if online.expression().len().max(offline.expression().len()) > MAX_EXPRESSION {
        return refuse(line, -32_600, "Request too large");
    }

    // Stdout is the protocol's alone: a line the child printed that is not a
    // reply, such as a boot-time log line, goes to stderr, where a client
    // keeps a server's log.
    let mut replied = false;
    let mut pass = |line: &str| {
        let reply = line.starts_with('{');
        if reply {
            replied = true;
        } else {
            eprintln!("{}", line);
        }
        reply
    };

    let answered = match rpc::child("rpc", &online, Out::Filter(&mut pass)) {
        Ok(answer) if answer.unreachable => rpc::child("eval", &offline, Out::Filter(&mut pass)),
        answered => answered,
    };

    // A child that failed before replying has said why on stderr; the client
    // still gets an answer to its request rather than waiting for one.
    if !replied && !matches!(answered, Ok(ref answer) if answer.status.success()) {
        refuse(line, -32_603, "Internal error");
    }
}

/// A JSON-RPC error for `line`: under its id when it is a request, under a
/// null id when it cannot be read, and nothing for a notification.
fn refuse(line: &str, code: i64, message: &str) {
    let id = match serde_json::from_str::<Value>(line) {
        Ok(request) => request.get("id").cloned(),
        Err(_) => Some(Value::Null),
    };

    if let Some(id) = id {
        let error =
            json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}});
        let mut out = io::stdout().lock();
        let _ = writeln!(out, "{}", error);
        let _ = out.flush();
    }
}
