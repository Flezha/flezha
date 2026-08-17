use std::process::exit;

use crate::utils::{escape_rpc_arg, run_rpc, token_opts_fragment};

pub fn handle_role_command(args: &[String]) {
    if args.is_empty() {
        print_role_help();
        exit(1);
    }

    match args[0].as_str() {
        "bind" => handle_bind(args),
        "unbind" => handle_unbind(args),
        "list" => handle_list(args),
        "help" | "--help" | "-h" => print_role_help(),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'bind', 'unbind', 'list', or 'help'.",
                args[0]
            );
            exit(1);
        }
    }
}

fn handle_bind(args: &[String]) {
    if args.len() < 4 {
        eprintln!(
            "Usage: flz role bind <principal> <viewer|operator|admin|runner> <namespace> \
             [--token <admin-token>]"
        );
        exit(1);
    }

    run_rpc(format!(
        "Flezha.Service.CliManager.authz_render_role_bind(\"{}\", \"{}\", \"{}\", {})",
        escape_rpc_arg(&args[1]),
        escape_rpc_arg(&args[2]),
        escape_rpc_arg(&args[3]),
        token_opts_fragment(&args[4..]),
    ));
}

fn handle_unbind(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Usage: flz role unbind <principal> [--token <admin-token>]");
        exit(1);
    }

    run_rpc(format!(
        "Flezha.Service.CliManager.authz_render_role_unbind(\"{}\", {})",
        escape_rpc_arg(&args[1]),
        token_opts_fragment(&args[2..]),
    ));
}

fn handle_list(args: &[String]) {
    run_rpc(format!(
        "Flezha.Service.CliManager.authz_render_role_list({})",
        token_opts_fragment(&args[1..]),
    ));
}

fn print_role_help() {
    println!("flz role - Principal → role bindings");
    println!();
    println!("USAGE:");
    println!("    flz role <COMMAND>");
    println!();
    println!("COMMANDS:");
    println!("    bind <principal> <role> <namespace>   Bind a principal to a role in a namespace");
    println!("    unbind <principal>                    Remove a principal's binding");
    println!("    list                                  List bindings visible to the caller");
    println!("    help                                  Show this help message");
    println!();
    println!("Identity for these verbs rides `--token <token>` or the FLEZHA_TOKEN env var.");
}
