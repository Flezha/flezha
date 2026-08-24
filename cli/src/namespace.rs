use std::process::{exit, Command};

use crate::utils::{
    escape_rpc_arg, extract_x_flags, get_flezha_command, get_output_format, run_rpc,
    serialize_flags, token_opts_fragment,
};

pub fn handle_namespace_command(args: &[String]) {
    if args.is_empty() {
        eprintln!("Error: Invalid number of arguments. See help for more information.");
        exit(1);
    }

    match args[0].as_str() {
        "list" => handle_list(args),
        "create" => handle_create(args),
        "delete" => handle_delete(args),
        "show" => handle_show(args),
        "set-quota" => handle_set_quota(args),
        "help" | "--help" | "-h" => print_namespace_help(),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'list', 'create', 'delete', 'show', 'set-quota', or 'help'.",
                args[0]
            );
            exit(1);
        }
    }
}

fn handle_list(args: &[String]) {
    let output_format = get_output_format(&args[1..]);

    let command = get_flezha_command();
    let rpc_command = format!(
        "Flezha.Service.CliManager.list_namespaces(\"{}\")",
        output_format
    );

    // Only show progress message for text output
    if output_format == "text" {
        println!("Fetching namespaces...");
    }

    let status = Command::new(command)
        .arg("rpc")
        .arg(rpc_command)
        .status()
        .expect("Failed to execute command");

    if !status.success() {
        eprintln!("Error: Command execution failed.");
        exit(1);
    }
}

// Quota overrides ride the repeatable `--X<field>=<value>` flags
// (e.g. `--Xmax_runners=4 --Xmax_state_bytes=1073741824`), serialized to the
// same JSON object shape every other `--X` surface uses.
fn quotas_json_literal(args: &[String]) -> String {
    let flags = extract_x_flags(args);
    serde_json::Value::String(serialize_flags(&flags)).to_string()
}

fn handle_create(args: &[String]) {
    if args.len() < 2 {
        eprintln!(
            "Usage: flz namespace create <name> [--X<quota>=<value> ...] [--token <admin-token>]"
        );
        exit(1);
    }

    run_rpc(format!(
        "Flezha.Service.CliManager.authz_render_namespace_create(\"{}\", {}, {})",
        escape_rpc_arg(&args[1]),
        quotas_json_literal(&args[2..]),
        token_opts_fragment(&args[2..]),
    ));
}

fn handle_delete(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Usage: flz namespace delete <name> [--token <admin-token>]");
        exit(1);
    }

    run_rpc(format!(
        "Flezha.Service.CliManager.authz_render_namespace_delete(\"{}\", {})",
        escape_rpc_arg(&args[1]),
        token_opts_fragment(&args[2..]),
    ));
}

fn handle_show(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Usage: flz namespace show <name> [--token <token>]");
        exit(1);
    }

    run_rpc(format!(
        "Flezha.Service.CliManager.authz_render_namespace_show(\"{}\", {})",
        escape_rpc_arg(&args[1]),
        token_opts_fragment(&args[2..]),
    ));
}

fn handle_set_quota(args: &[String]) {
    if args.len() < 2 {
        eprintln!(
            "Usage: flz namespace set-quota <name> --X<quota>=<value> [...] [--token <admin-token>]"
        );
        exit(1);
    }

    run_rpc(format!(
        "Flezha.Service.CliManager.authz_render_namespace_set_quota(\"{}\", {}, {})",
        escape_rpc_arg(&args[1]),
        quotas_json_literal(&args[2..]),
        token_opts_fragment(&args[2..]),
    ));
}

fn print_namespace_help() {
    println!("flz namespace - Namespace management commands");
    println!();
    println!("USAGE:");
    println!("    flz namespace <COMMAND>");
    println!();
    println!("COMMANDS:");
    println!("    list                                 List running namespaces");
    println!("    create <name> [--X<quota>=<value>]   Create a namespace in the auth catalog");
    println!("    delete <name>                        Delete a namespace from the auth catalog");
    println!("    show <name>                          Show a namespace's quotas");
    println!("    set-quota <name> --X<quota>=<value>  Update quota overrides");
    println!("    help                                 Show this help message");
    println!();
    println!("Quota fields: max_operatives, max_entities_per_operative, max_mailbox_bytes_total,");
    println!("              max_message_rate_per_second, max_state_bytes, max_runners");
    println!();
    println!("Identity for gated verbs rides `--token <token>` or the FLEZHA_TOKEN env var.");
}
