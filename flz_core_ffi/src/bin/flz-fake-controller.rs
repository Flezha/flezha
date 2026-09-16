//! Drive one SDK runner through connect, a class start, inputs, outputs and
//! shutdown, with no controller and no Elixir.
//!
//! This is an SDK repository's smoke tier: start it on a socket directory,
//! launch the runner against the same directory, and take its exit code. It
//! holds no protocol of its own: every frame is
//! [`flz_core_ffi::fake_controller`], the module the crate's own ABI scenario
//! drives, so a tier that passes here and a tier that passes in this tree
//! cannot be measuring two different wires.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use flz_core_ffi::fake_controller::Scenario;

const USAGE: &str = "\
flz-fake-controller: drive one runner through a smoke run

  --socket-dir <dir>      where the control and data sockets live; the runner
                          must read the same directory (FLEZHA_SOCKET_FILES_PATH)
  --factory <id>          the class to start
  --operative <id>        what to call the started operative [default: the factory id]
  --transport <uds>       the transport the runner connects over [default: uds]
  --channel <name>        the entry channel inputs are fed to [default: the first declared]
  --inputs <file>         one input payload per line; omit to feed nothing
  --runner-id <id>        the session token that names the data sockets [default: flz-smoke]
  --expect-outputs <n>    how many outputs the run must see [default: the input count]
  --timeout-ms <ms>       the ceiling on every wait [default: 30000]
  -h, --help              this text
";

fn main() -> ExitCode {
    let scenario = match parse(std::env::args().skip(1)) {
        Ok(Some(scenario)) => scenario,
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
    match scenario.run(&report) {
        Ok(()) => {
            println!("flz-fake-controller: the run completed");
            ExitCode::SUCCESS
        }
        Err(refusal) => {
            eprintln!("flz-fake-controller: {refusal}");
            ExitCode::FAILURE
        }
    }
}

/// `Ok(None)` is `--help`. Every refusal names the option it is about.
fn parse(args: impl Iterator<Item = String>) -> Result<Option<Scenario>, String> {
    let mut socket_dir: Option<PathBuf> = None;
    let mut factory: Option<String> = None;
    let mut operative: Option<String> = None;
    let mut channel: Option<String> = None;
    let mut inputs_file: Option<PathBuf> = None;
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
            "--socket-dir" => socket_dir = Some(PathBuf::from(value()?)),
            "--factory" => factory = Some(value()?),
            "--operative" => operative = Some(value()?),
            "--channel" => channel = Some(value()?),
            "--inputs" => inputs_file = Some(PathBuf::from(value()?)),
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

    let socket_dir = socket_dir.ok_or("`--socket-dir` is required")?;
    let factory_id = factory.ok_or("`--factory` is required")?;
    if runner_id.len() > flz_transport::utility::socket_dir::stream_socket_id_max() {
        return Err(format!(
            "`--runner-id` is {} bytes; the data socket name holds {}",
            runner_id.len(),
            flz_transport::utility::socket_dir::stream_socket_id_max()
        ));
    }
    let inputs = match &inputs_file {
        None => Vec::new(),
        Some(path) => std::fs::read_to_string(path)
            .map_err(|e| format!("`--inputs` {} could not be read: {e}", path.display()))?
            .lines()
            .map(|line| line.as_bytes().to_vec())
            .collect(),
    };

    Ok(Some(Scenario {
        operative_id: operative.unwrap_or_else(|| factory_id.clone()),
        expect_outputs: expect_outputs.unwrap_or(inputs.len()),
        socket_dir,
        runner_id,
        factory_id,
        channel,
        inputs,
        timeout: Duration::from_millis(timeout_ms),
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
