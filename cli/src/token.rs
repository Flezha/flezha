use std::process::exit;

use crate::utils::{escape_rpc_arg, extract_param, run_rpc, token_opts_fragment};

pub fn handle_token_command(args: &[String]) {
    if args.is_empty() {
        print_token_help();
        exit(1);
    }

    match args[0].as_str() {
        "issue" => handle_issue(args),
        "revoke" => handle_revoke(args),
        "list" => handle_list(args),
        "help" | "--help" | "-h" => print_token_help(),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'issue', 'revoke', 'list', or 'help'.",
                args[0]
            );
            exit(1);
        }
    }
}

fn handle_issue(args: &[String]) {
    if args.len() < 4 {
        eprintln!(
            "Usage: flz token issue <principal> <namespace> <viewer|operator|admin|runner> \
             [--ttl <seconds>] [--token <admin-token>]"
        );
        exit(1);
    }

    let ttl = match extract_param(&args[4..], "--ttl", "--ttl") {
        Some(t) => match t.parse::<u64>() {
            Ok(n) if n > 0 => n.to_string(),
            _ => {
                eprintln!("Error: --ttl requires a positive integer (seconds)");
                exit(1);
            }
        },
        // Default CLI-token TTL (config `:token_ttl_default_seconds`).
        None => "86400".to_string(),
    };

    run_rpc(format!(
        "Flezha.Service.CliManager.authz_render_token_issue(\"{}\", \"{}\", \"{}\", {}, {})",
        escape_rpc_arg(&args[1]),
        escape_rpc_arg(&args[2]),
        escape_rpc_arg(&args[3]),
        ttl,
        token_opts_fragment(&args[4..]),
    ));
}

fn handle_revoke(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Usage: flz token revoke <token-id> [--token <admin-token>]");
        exit(1);
    }

    run_rpc(format!(
        "Flezha.Service.CliManager.authz_render_token_revoke(\"{}\", {})",
        escape_rpc_arg(&args[1]),
        token_opts_fragment(&args[2..]),
    ));
}

fn handle_list(args: &[String]) {
    run_rpc(format!(
        "Flezha.Service.CliManager.authz_render_token_list({})",
        token_opts_fragment(&args[1..]),
    ));
}

fn print_token_help() {
    println!("flz token - Scoped-token management");
    println!();
    println!("USAGE:");
    println!("    flz token <COMMAND>");
    println!();
    println!("COMMANDS:");
    println!("    issue <principal> <namespace> <role>   Issue a token (printed once)");
    println!("        [--ttl <seconds>]");
    println!("    revoke <token-id>                      Revoke by id (prefix of the token hash)");
    println!("    list                                   List tokens visible to the caller");
    println!("    help                                   Show this help message");
    println!();
    println!("Identity for these verbs rides `--token <token>` or the FLEZHA_TOKEN env var.");
}
