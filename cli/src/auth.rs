use std::fs;
use std::io::Write;
use std::process::exit;

use crate::dispatch::{self, Command};
use crate::rpc::{self, controller_is_up, text, DEFAULT_PROBE_TIMEOUT_MS};
use crate::{args, help};

pub(crate) const TABLE: Command = Command {
    help: help::AUTH,
    verbs: &[
        ("bootstrap", handle_bootstrap),
        ("whoami", handle_whoami),
        ("rebootstrap", handle_rebootstrap),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: None,
    unknown: "Error: Invalid argument '{}'. Use 'bootstrap', 'whoami', 'rebootstrap', or 'help'.",
    unknown_prints_help: false,
};

fn handle_bootstrap(args: &[String]) {
    if args.len() < 2 {
        eprintln!(
            "Usage: flz auth bootstrap <token> (the token path is logged at controller boot)"
        );
        exit(1);
    }

    rpc::run(
        "authz_render_bootstrap",
        &[text(&args[1])],
        &[],
        &args[2..],
        rpc::PROPAGATE,
    );
}

fn handle_whoami(args: &[String]) {
    rpc::token_only(args, "authz_render_whoami");
}

// Break-glass recovery for a lost admin@_system: OFFLINE by design — it
// requires the controller to be stopped and filesystem access to
// `app_data/auth/` (the same trust the original bootstrap assumes). It only
// writes the REBOOTSTRAP marker; the next controller boot consumes it,
// invalidates the stale admin bindings/tokens, and re-enters the ordinary
// bootstrap flow with a fresh token.
fn handle_rebootstrap(args: &[String]) {
    if controller_is_up(DEFAULT_PROBE_TIMEOUT_MS) {
        eprintln!(
            "flz auth rebootstrap refuses to run while the controller is up. \
             Stop it first with: flz controller stop"
        );
        exit(1);
    }

    let auth_dir = match std::env::var("FLEZHA_AUTHZ_DIR") {
        Ok(dir) if !dir.is_empty() => std::path::PathBuf::from(dir),
        _ => args::app_data_root(&args[1..]).join("auth"),
    };

    if !auth_dir.is_dir() {
        eprintln!(
            "Error: auth directory {} does not exist — nothing to recover.",
            auth_dir.display()
        );
        exit(1);
    }

    let marker = auth_dir.join("REBOOTSTRAP");

    match fs::File::create(&marker).and_then(|mut f| f.write_all(b"rebootstrap requested\n")) {
        Ok(()) => {
            println!("Recovery marker written to {}.", marker.display());
            println!(
                "Start the controller: it will invalidate the previous admin@_system binding \
                 and its _system admin tokens, then write a fresh bootstrap.token to claim."
            );
        }
        Err(e) => {
            eprintln!("Error: could not write {}: {}", marker.display(), e);
            exit(1);
        }
    }
}
