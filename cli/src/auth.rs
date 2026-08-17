use std::fs;
use std::io::Write;
use std::process::exit;

use crate::utils::{app_data_root, run_rpc, token_opts_fragment};

pub fn handle_auth_command(args: &[String]) {
    if args.is_empty() {
        print_auth_help();
        exit(1);
    }

    match args[0].as_str() {
        "bootstrap" => handle_bootstrap(args),
        "whoami" => handle_whoami(args),
        "rebootstrap" => handle_rebootstrap(args),
        "help" | "--help" | "-h" => print_auth_help(),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'bootstrap', 'whoami', 'rebootstrap', or 'help'.",
                args[0]
            );
            exit(1);
        }
    }
}

fn handle_bootstrap(args: &[String]) {
    if args.len() < 2 {
        eprintln!(
            "Usage: flz auth bootstrap <token> (the token path is logged at controller boot)"
        );
        exit(1);
    }

    run_rpc(format!(
        "Flezha.Service.CliManager.authz_render_bootstrap(\"{}\")",
        crate::utils::escape_rpc_arg(&args[1])
    ));
}

fn handle_whoami(args: &[String]) {
    run_rpc(format!(
        "Flezha.Service.CliManager.authz_render_whoami({})",
        token_opts_fragment(&args[1..])
    ));
}

// Break-glass recovery for a lost admin@_system: OFFLINE by design — it
// requires the controller to be stopped and filesystem access to
// `app_data/auth/` (the same trust the original bootstrap assumes). It only
// writes the REBOOTSTRAP marker; the next controller boot consumes it,
// invalidates the stale admin bindings/tokens, and re-enters the ordinary
// bootstrap flow with a fresh token.
fn handle_rebootstrap(args: &[String]) {
    if crate::reset::controller_is_up(crate::reset::DEFAULT_PROBE_TIMEOUT_MS) {
        eprintln!(
            "flz auth rebootstrap refuses to run while the controller is up. \
             Stop it first with: flz controller stop"
        );
        exit(1);
    }

    let auth_dir = match std::env::var("FLEZHA_AUTHZ_DIR") {
        Ok(dir) if !dir.is_empty() => std::path::PathBuf::from(dir),
        _ => app_data_root(&args[1..]).join("auth"),
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

fn print_auth_help() {
    println!("flz auth - Authentication bootstrap and identity");
    println!();
    println!("USAGE:");
    println!("    flz auth <COMMAND>");
    println!();
    println!("COMMANDS:");
    println!("    bootstrap <token>   Claim the one-time bootstrap token as admin@_system");
    println!("    whoami              Show the identity the current token resolves to");
    println!("    rebootstrap         OFFLINE break-glass: reset a lost admin@_system");
    println!(
        "                        (controller must be stopped; writes app_data/auth/REBOOTSTRAP)"
    );
    println!("    help                Show this help message");
    println!();
    println!("Identity for gated verbs rides `--token <token>` or the FLEZHA_TOKEN env var.");
}
