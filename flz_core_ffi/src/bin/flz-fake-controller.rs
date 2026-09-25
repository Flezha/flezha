//! Drive one SDK runner through connect, a class start, inputs, outputs and
//! shutdown, with no controller and no Elixir.
//!
//! This is an SDK repository's smoke tier: give it the command that starts the
//! runner, and take its exit code. It launches the runner against a socket
//! directory of its own, holds the runner's standard input open for the run,
//! and prints what the runner wrote when the run fails. It holds no protocol of
//! its own: every frame is
//! [`flz_core_ffi::fake_controller`], the module the crate's own ABI scenario
//! drives, so a tier that passes here and a tier that passes in this tree
//! cannot be measuring two different wires.

use std::fs::File;
use std::path::PathBuf;
use std::process::{Child, Command, ExitCode, Stdio};
use std::time::Duration;

use flz_core_ffi::fake_controller::Scenario;
use flz_transport::utility::socket_dir::SOCKET_DIR_VAR;

const USAGE: &str = "\
flz-fake-controller: drive one runner through a smoke run

  --socket-dir <dir>      where the control and data sockets live; the runner
                          must read the same directory (FLEZHA_SOCKET_FILES_PATH)
                          [default with a runner command: a directory of its own]
  --factory <id>          the class to start
  --operator <id>        what to call the started operator [default: the factory id]
  --transport <uds>       the transport the runner connects over [default: uds]
  --channel <name>        the entry channel inputs are fed to [default: the first declared]
  --inputs <file>         one input payload per line
  --input <payload>       one input payload; repeatable, fed after `--inputs`
  --runner-id <id>        the session token that names the data sockets [default: flz-smoke]
  --expect-outputs <n>    how many outputs the run must see [default: the input count]
  --timeout-ms <ms>       the ceiling on every wait [default: 30000]
  -h, --help              this text
  -- <command> [args]     the runner to launch against the socket directory, with
                          its standard input held open for the run and its output
                          printed if the run fails
";

fn main() -> ExitCode {
    let Invocation { scenario, runner } = match parse(std::env::args().skip(1)) {
        Ok(Some(invocation)) => invocation,
        Ok(None) => {
            print!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(refusal) => {
            eprintln!("flz-fake-controller: {refusal}");
            eprint!("\n{USAGE}");
            return ExitCode::FAILURE;
        }
    };

    // Every step as it is awaited: a tier that fails in CI is read from its
    // log, and "awaiting the runner's connect frame" followed by silence says
    // which end stopped.
    let report = |step: &str| println!("flz-fake-controller: {step}");
    let launched = match runner.as_deref().map(|command| launch(command, &scenario)) {
        None => None,
        Some(Ok(launched)) => Some(launched),
        Some(Err(refusal)) => {
            eprintln!("flz-fake-controller: {refusal}");
            return ExitCode::FAILURE;
        }
    };
    let outcome = scenario.run(&report);
    let log = launched.map(|launched| launched.stop(&scenario));
    match outcome {
        Ok(()) => {
            println!("flz-fake-controller: the run completed");
            ExitCode::SUCCESS
        }
        Err(refusal) => {
            eprintln!("flz-fake-controller: {refusal}");
            if let Some(log) = log {
                eprintln!("flz-fake-controller: the runner's own output:");
                for line in log.lines() {
                    eprintln!("  {line}");
                }
            }
            ExitCode::FAILURE
        }
    }
}

/// A runner this binary started, and what it holds for the run's length.
struct Launched {
    child: Child,
    /// The runner ends at end of stream on its standard input, so the pipe is
    /// held open until the run is over.
    stdin: Option<std::process::ChildStdin>,
    /// A socket directory this binary made, and so removes.
    owned_dir: bool,
}

/// Start `command` against the scenario's socket directory, over UDS.
fn launch(command: &[String], scenario: &Scenario) -> Result<Launched, String> {
    let owned_dir = !scenario.socket_dir.exists();
    std::fs::create_dir_all(&scenario.socket_dir).map_err(|e| {
        format!(
            "the socket directory {} could not be made: {e}",
            scenario.socket_dir.display()
        )
    })?;
    let log = File::create(runner_log(scenario))
        .map_err(|e| format!("the runner's log could not be opened: {e}"))?;
    let stderr = log
        .try_clone()
        .map_err(|e| format!("the runner's log could not be opened: {e}"))?;
    let mut child = Command::new(&command[0])
        .args(&command[1..])
        .env(SOCKET_DIR_VAR, &scenario.socket_dir)
        .env("FLEZHA_TRANSPORT", "uds")
        .stdin(Stdio::piped())
        .stdout(log)
        .stderr(stderr)
        .spawn()
        .map_err(|e| {
            format!(
                "the runner `{}` could not be started: {e}",
                command.join(" ")
            )
        })?;
    Ok(Launched {
        stdin: child.stdin.take(),
        child,
        owned_dir,
    })
}

impl Launched {
    /// End the runner and hand back what it wrote. The run has already shut
    /// it down when it passed, so this closes its input and reaps it; a run
    /// that failed may have left it waiting, so it is killed.
    fn stop(mut self, scenario: &Scenario) -> String {
        drop(self.stdin.take());
        let _ = self.child.kill();
        let _ = self.child.wait();
        let log = std::fs::read_to_string(runner_log(scenario)).unwrap_or_default();
        if self.owned_dir {
            let _ = std::fs::remove_dir_all(&scenario.socket_dir);
        }
        log
    }
}

fn runner_log(scenario: &Scenario) -> PathBuf {
    scenario.socket_dir.join("runner.log")
}

/// What one command line asks for: the run, and the runner command after `--`
/// when one is given.
struct Invocation {
    scenario: Scenario,
    runner: Option<Vec<String>>,
}

/// `Ok(None)` is `--help`. Every refusal names the option it is about.
fn parse(args: impl Iterator<Item = String>) -> Result<Option<Invocation>, String> {
    let mut socket_dir: Option<PathBuf> = None;
    let mut factory: Option<String> = None;
    let mut operator: Option<String> = None;
    let mut channel: Option<String> = None;
    let mut inputs_file: Option<PathBuf> = None;
    let mut inline_inputs: Vec<Vec<u8>> = Vec::new();
    let mut runner: Option<Vec<String>> = None;
    let mut runner_id = "flz-smoke".to_string();
    let mut expect_outputs: Option<usize> = None;
    let mut timeout_ms: u64 = 30_000;

    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        let mut value = || {
            args.next()
                .ok_or_else(|| format!("`{arg}` needs a value after it"))
        };
        match arg.as_str() {
            "-h" | "--help" => return Ok(None),
            "--" => {
                let command: Vec<String> = args.by_ref().collect();
                if command.is_empty() {
                    return Err("`--` needs the runner command after it".to_string());
                }
                runner = Some(command);
            }
            "--socket-dir" => socket_dir = Some(PathBuf::from(value()?)),
            "--factory" => factory = Some(value()?),
            "--operator" => operator = Some(value()?),
            "--channel" => channel = Some(value()?),
            "--inputs" => inputs_file = Some(PathBuf::from(value()?)),
            "--input" => inline_inputs.push(value()?.into_bytes()),
            "--runner-id" => runner_id = value()?,
            "--transport" => transport(&value()?)?,
            "--expect-outputs" => {
                let raw = value()?;
                expect_outputs = Some(
                    raw.parse()
                        .map_err(|_| format!("`--expect-outputs` wants a count, not `{raw}`"))?,
                );
            }
            "--timeout-ms" => {
                let raw = value()?;
                timeout_ms = raw
                    .parse()
                    .map_err(|_| format!("`--timeout-ms` wants milliseconds, not `{raw}`"))?;
            }
            other => return Err(format!("unknown option `{other}`")),
        }
    }

    let socket_dir = match (socket_dir, &runner) {
        (Some(dir), _) => dir,
        (None, Some(_)) => std::env::temp_dir().join(format!("flz-smoke-{}", std::process::id())),
        (None, None) => {
            return Err("`--socket-dir` is required unless a runner command is given".into())
        }
    };
    let factory_id = factory.ok_or("`--factory` is required")?;
    if runner_id.len() > flz_transport::utility::socket_dir::stream_socket_id_max() {
        return Err(format!(
            "`--runner-id` is {} bytes; the data socket name holds {}",
            runner_id.len(),
            flz_transport::utility::socket_dir::stream_socket_id_max()
        ));
    }
    let mut inputs: Vec<Vec<u8>> = match &inputs_file {
        None => Vec::new(),
        Some(path) => std::fs::read_to_string(path)
            .map_err(|e| format!("`--inputs` {} could not be read: {e}", path.display()))?
            .lines()
            .map(|line| line.as_bytes().to_vec())
            .collect(),
    };
    inputs.extend(inline_inputs);

    Ok(Some(Invocation {
        scenario: Scenario {
            operator_id: operator.unwrap_or_else(|| factory_id.clone()),
            expect_outputs: expect_outputs.unwrap_or(inputs.len()),
            socket_dir,
            runner_id,
            factory_id,
            channel,
            inputs,
            timeout: Duration::from_millis(timeout_ms),
        },
        runner,
    }))
}

/// The transport the runner is expected to connect over. QUIC is refused by
/// name rather than silently ignored: its peer needs this repository's test
/// certificates, which an SDK repository does not have, so a tier that asked
/// for it would connect over UDS and report a pass for the wrong wire.
fn transport(named: &str) -> Result<(), String> {
    match named {
        "uds" => Ok(()),
        "quic" => Err(
            "`--transport quic` is not available: a QUIC peer needs this repository's \
             test certificates, so the smoke tier runs over UDS"
                .to_string(),
        ),
        other => Err(format!("`--transport` takes `uds`, not `{other}`")),
    }
}
