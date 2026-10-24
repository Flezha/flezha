//! The fixture `flz`'s integration tests share: a copy of the binary under
//! test with a `flzvm` shim beside it, and a stand-in for the controller's
//! control socket in a socket directory of its own.
//!
//! `flz` resolves `flzvm` next to its own executable rather than on `PATH`, so
//! standing in for `flzvm` means copying `flz` somewhere writable first. Every
//! run is pointed at the fixture's socket directory, so no test reaches a
//! controller running on the machine. What the shim and the socket answer is
//! each test's own.

#![allow(dead_code)]

use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::sync::{mpsc, Arc, Mutex};
use std::{fs, thread};

use flz_transport::service::payloader;
use serde_json::{json, Value};
use tempfile::TempDir;

/// A `flzvm` that records each start beside itself, prints each argument on
/// its own line, then its whole stdin, and succeeds, so what `flz` handed it is
/// readable on stdout.
pub const ECHOING: &str = "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$(dirname \"$0\")/flzvm.starts\"\nprintf '%s\\n' \"$@\"\ncat\nexit 0\n";

/// The stdin a run over every verb gives each one: `flz auth bootstrap` reads
/// its token from it, and no other verb reads it.
pub const BOOTSTRAP_INPUT: &str = "bootstrap-token\n";

/// A request as one line: the door name, its arguments and its options as
/// JSON, or `mcp`, the line and its options.
pub fn door(request: &Value) -> String {
    match request["entry"].as_str() {
        Some("mcp") => format!("mcp {} {}", request["line"], request["opts"]),
        _ => format!(
            "{} {} {}",
            request["name"].as_str().unwrap_or("<no name>"),
            request["args"],
            request["opts"]
        ),
    }
}

/// Run `cmd` with `input` as its whole stdin.
pub fn output_fed(mut cmd: Command, input: &str) -> Output {
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("flz runs");
    // A run that exits before reading closes the pipe; its output says why.
    let _ = child
        .stdin
        .take()
        .expect("stdin")
        .write_all(input.as_bytes());
    child.wait_with_output().expect("flz ends")
}

/// What the stand-in socket does with one request.
pub enum Reply {
    /// Accept, write `stdout` then `stderr`, and exit with `code`.
    Answer {
        stdout: String,
        stderr: String,
        code: i32,
    },
    /// Read the request and answer nothing, as a controller that does not know it.
    Silent,
    /// Accept, then close without an exit code.
    Hangup,
    /// Close without accepting: a controller that is not serving yet.
    Refuse,
    /// Accept, then hold the connection until `flz` closes it, and say so.
    Hold(mpsc::Sender<()>),
}

impl Reply {
    /// Accepted, nothing written, exit 0.
    pub fn ok() -> Reply {
        Reply::exit(0)
    }

    pub fn exit(code: i32) -> Reply {
        Reply::Answer {
            stdout: String::new(),
            stderr: String::new(),
            code,
        }
    }
}

/// How the stand-in answers its `n`th request (from 0).
pub type Script = Box<dyn Fn(usize, &Value) -> Reply + Send + Sync>;

pub struct Shim {
    tmp: TempDir,
    flz: PathBuf,
    flzvm: PathBuf,
    sockets: PathBuf,
    requests: Arc<Mutex<Vec<Value>>>,
}

impl Shim {
    /// `script` is the whole `flzvm` shim, `#!` line included.
    pub fn new(flz_bin: &str, script: &str) -> Self {
        Shim::beside(flz_bin, Some(script))
    }

    /// A `flz` with NO sibling at all: the checkout where the release was
    /// never built, or the directory it was not built in.
    pub fn without_flzvm(flz_bin: &str) -> Self {
        Shim::beside(flz_bin, None)
    }

    /// The echoing `flzvm`, and a socket that accepts every request and
    /// answers exit 0.
    pub fn recording(flz_bin: &str) -> Self {
        Shim::new(flz_bin, ECHOING).serving(|_, _| Reply::ok())
    }

    fn beside(flz_bin: &str, script: Option<&str>) -> Self {
        let tmp = TempDir::new().expect("tempdir");
        let flz = tmp.path().join("flz");
        // A link opens no write handle, so a test thread forking meanwhile
        // cannot hold one across this binary's exec (`Text file busy`).
        fs::hard_link(flz_bin, &flz)
            .or_else(|_| fs::copy(flz_bin, &flz).map(drop))
            .expect("place the flz under test");
        let flzvm = tmp.path().join("flzvm");
        if let Some(script) = script {
            fs::write(&flzvm, script).expect("write shim");
            fs::set_permissions(&flzvm, fs::Permissions::from_mode(0o755)).expect("chmod shim");
        }
        let sockets = tmp.path().join("s");
        fs::create_dir(&sockets).expect("socket dir");
        Shim {
            tmp,
            flz,
            flzvm,
            sockets,
            requests: Arc::default(),
        }
    }

    /// Listen on the control socket, answering each request as `script` says.
    pub fn serving(self, script: impl Fn(usize, &Value) -> Reply + Send + Sync + 'static) -> Self {
        let listener =
            UnixListener::bind(self.sockets.join("runner_control_channel.sock")).expect("bind");
        let requests = self.requests.clone();
        let script: Script = Box::new(script);
        thread::spawn(move || serve(listener, script, requests));
        self
    }

    /// The requests the socket has received since the last call, oldest first.
    pub fn requests(&self) -> Vec<Value> {
        std::mem::take(&mut *self.requests.lock().expect("requests"))
    }

    /// `requests`, each as `door` writes it.
    pub fn doors(&self) -> Vec<String> {
        self.requests().iter().map(door).collect()
    }

    /// Every `flzvm` start an `ECHOING` shim recorded, as its arguments.
    pub fn starts(&self) -> Vec<String> {
        fs::read_to_string(self.tmp.path().join("flzvm.starts"))
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }

    /// Where this `flz` looks for its sibling, and so what a refusal that
    /// names the path it could not run has to say.
    pub fn flzvm_path(&self) -> &PathBuf {
        &self.flzvm
    }

    /// Where this `flz` looks for the controller's control socket.
    pub fn socket_path(&self) -> String {
        self.sockets
            .join("runner_control_channel.sock")
            .display()
            .to_string()
    }

    /// The copied `flz`, pointed at the fixture's socket directory, for a
    /// caller that adds environment of its own.
    pub fn cmd(&self) -> Command {
        let mut cmd = Command::new(&self.flz);
        cmd.env("FLEZHA_SOCKET_FILES_PATH", &self.sockets);
        cmd
    }

    pub fn run(&self, args: &[&str]) -> Output {
        self.cmd().args(args).output().expect("flz runs")
    }
}

fn serve(listener: UnixListener, script: Script, requests: Arc<Mutex<Vec<Value>>>) {
    for (n, stream) in listener.incoming().enumerate() {
        let Ok(mut stream) = stream else { continue };
        let Ok((_, body, _)) = payloader::extract_payload(&mut stream) else {
            continue;
        };
        let request: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        let reply = script(n, &request);
        requests.lock().expect("requests").push(request);
        answer(stream, reply);
    }
}

fn answer(mut stream: UnixStream, reply: Reply) {
    if let Reply::Silent = reply {
        // Held open until `flz` gives up and closes it.
        thread::spawn(move || {
            let _ = stream.read_to_end(&mut Vec::new());
        });
        return;
    }
    let mut frame = |id: &str, bytes: &[u8]| {
        let _ = stream.write_all(&payloader::build_without_metadata(id, bytes));
    };
    match reply {
        Reply::Answer {
            stdout,
            stderr,
            code,
        } => {
            frame("cli_accepted", br#"{"protocol":1}"#);
            if !stdout.is_empty() {
                frame("cli_stdout", stdout.as_bytes());
            }
            if !stderr.is_empty() {
                frame("cli_stderr", stderr.as_bytes());
            }
            frame("cli_exit", json!({ "code": code }).to_string().as_bytes());
        }
        Reply::Hangup => frame("cli_accepted", br#"{"protocol":1}"#),
        Reply::Hold(closed) => {
            frame("cli_accepted", br#"{"protocol":1}"#);
            thread::spawn(move || {
                let _ = stream.read_to_end(&mut Vec::new());
                let _ = closed.send(());
            });
        }
        Reply::Refuse | Reply::Silent => {}
    }
}
