use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::exit;

use crate::args::{app_data_root, Positional, Reading, APP_DATA_ROOT};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, controller_is_up, text, DEFAULT_PROBE_TIMEOUT_MS};

pub(crate) const TABLE: Command = Command {
    path: "flz auth",
    help: help::AUTH,
    verbs: &[
        Verb::new("bootstrap", handle_bootstrap).taking(&[Positional::required("<token>")]),
        Verb::new("whoami", |read| {
            rpc::token_only(read, "authz_render_whoami")
        }),
        Verb::new("rebootstrap", handle_rebootstrap).reading(&[APP_DATA_ROOT]),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Missing::Screen,
    unknown_prints_help: false,
};

fn handle_bootstrap(read: &Reading) {
    rpc::run(
        "authz_render_bootstrap",
        &[text(read.pos("<token>"))],
        &[],
        read.token(),
        rpc::PROPAGATE,
    );
}

// Break-glass recovery for a lost admin@_system: OFFLINE by design — it
// requires the controller to be stopped and filesystem access to
// `app_data/auth/` (the same trust the original bootstrap assumes). It only
// writes the REBOOTSTRAP marker; the next controller boot consumes it,
// invalidates the stale admin bindings/tokens, and re-enters the ordinary
// bootstrap flow with a fresh token.
fn handle_rebootstrap(read: &Reading) {
    if controller_is_up(DEFAULT_PROBE_TIMEOUT_MS) {
        eprintln!(
            "flz auth rebootstrap refuses to run while the controller is up. \
             Stop it first with: flz controller stop"
        );
        exit(1);
    }

    let auth_dir = match std::env::var("FLEZHA_AUTHZ_DIR") {
        Ok(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => app_data_root(read).join("auth"),
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
