use std::process::exit;

use crate::args::{self, Spelling};
use crate::dispatch::{self, Command};
use crate::{help, rpc};

pub(crate) const TABLE: Command = Command {
    help: help::BOOT,
    verbs: &[("status", handle_status), ("await", handle_await)],
    help_verbs: dispatch::HELP_VERBS,
    missing: Some("Error: Invalid number of arguments. See `flz boot help`."),
    unknown: "Error: Invalid argument '{}'. Use 'status', 'await', or 'help'.",
    unknown_prints_help: false,
};

fn handle_status(args: &[String]) {
    rpc::run("boot_status", &[], &[], args, rpc::PROPAGATE);
}

const DEFAULT_TIMEOUT_MS: u64 = 60_000;

fn handle_await(args: &[String]) {
    let timeout_ms = read_timeout_ms(&args[1..]).unwrap_or_else(|refusal| {
        eprintln!("{}", refusal);
        exit(1);
    });

    rpc::run(
        "boot_await",
        &[timeout_ms.to_string()],
        &[],
        args,
        rpc::PROPAGATE,
    );
}

/// The wait in milliseconds, from the operator's seconds. `extract_param` reads
/// the value in every spelling and refuses a flag written with none; the walk is
/// what refuses a word this verb cannot act on at all, because answering that
/// with the default wait is a silence, not a wait. The credential is the one
/// other word every verb accepts.
fn read_timeout_ms(args: &[String]) -> Result<u64, String> {
    let (long, short) = ("--timeout", "-t");

    let mut i = 0;
    while i < args.len() {
        let spelling = match args::resolve_spelling(&args[i], long, short) {
            Spelling::Other => args::resolve_spelling(&args[i], "--token", "--token"),
            known => known,
        };

        match spelling {
            Spelling::Attached(_) => {}
            Spelling::Separated => i += 1,
            Spelling::Other => return Err(format!("Error: unexpected argument '{}'", args[i])),
        }
        i += 1;
    }

    Ok(args::number::<u64>(args, long, short)?
        .map_or(DEFAULT_TIMEOUT_MS, |secs| secs.saturating_mul(1000)))
}
