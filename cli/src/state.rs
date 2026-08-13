use std::process::exit;

use crate::utils::{get_output_format, run_rpc};

pub fn handle_state_command(args: &[String]) {
    if args.is_empty() {
        eprintln!("Error: Invalid number of arguments. See `flz state help`.");
        exit(1);
    }

    match args[0].as_str() {
        "get" => handle_read(
            args,
            "get",
            "<operative/procedure[/entity_key]>",
            "state_get",
        ),
        "list" => handle_read(args, "list", "<operative_class>", "state_list"),
        "at" => handle_at(args),
        "rotate" => handle_entity_verb(args, "rotate", "state_rotate"),
        "retention" => handle_retention(args),
        "delete" => handle_entity_verb(args, "delete", "state_delete"),
        "outbox" => handle_entity_verb(args, "outbox", "state_outbox"),
        "journal" => handle_read(args, "journal", "<operative/instance>", "state_journal"),
        "inbox" => handle_read(args, "inbox", "<operative/instance>", "state_inbox"),
        "help" | "--help" | "-h" => print_state_help(),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'get', 'list', 'at', 'rotate', 'retention', 'delete', 'outbox', 'journal', 'inbox', or 'help'.",
                args[0]
            );
            exit(1);
        }
    }
}

fn handle_at(args: &[String]) {
    if args.len() < 3 {
        eprintln!("Usage: flz state at <operative/procedure/entity_key> <seq | iso8601-timestamp> [--output text|json]");
        exit(1);
    }

    run_rpc(format!(
        "Flezha.Service.CliManager.state_at(\"{}\", \"{}\", \"{}\")",
        &args[1],
        &args[2],
        get_output_format(&args[3..]),
    ));
}

fn handle_retention(args: &[String]) {
    if args.len() < 3 {
        eprintln!("Usage: flz state retention <operative_class> <hours>");
        exit(1);
    }

    run_rpc(format!(
        "Flezha.Service.CliManager.state_retention(\"{}\", \"{}\")",
        &args[1], &args[2],
    ));
}

/// The Entity-scoped write verbs — `rotate`, `delete`, `outbox` — take one
/// Entity path and no output format; they differ only in the CliManager
/// function they call.
fn handle_entity_verb(args: &[String], verb: &str, rpc_fun: &str) {
    if args.len() < 2 {
        eprintln!("Usage: flz state {verb} <operative/procedure/entity_key>");
        exit(1);
    }

    run_rpc(format!(
        "Flezha.Service.CliManager.{}(\"{}\")",
        rpc_fun, &args[1],
    ));
}

/// The read verbs — `list`, `journal`, `inbox` — take one scope argument plus an
/// output format; they differ only in that argument's name and the CliManager
/// function they call.
fn handle_read(args: &[String], verb: &str, scope: &str, rpc_fun: &str) {
    if args.len() < 2 {
        eprintln!("Usage: flz state {verb} {scope} [--output text|json]");
        exit(1);
    }

    run_rpc(format!(
        "Flezha.Service.CliManager.{}(\"{}\", \"{}\")",
        rpc_fun,
        &args[1],
        get_output_format(&args[2..]),
    ));
}

fn print_state_help() {
    println!("flz state - Per-Entity state inspection and time-travel queries");
    println!();
    println!("USAGE:");
    println!("    flz state <SUBCOMMAND>");
    println!();
    println!("SUBCOMMANDS:");
    println!(
        "    get <operative/procedure[/key]>          Current in-memory state of a live Entity, with its change_seq"
    );
    println!("    list <operative>                         List Entities under an Operative class");
    println!("    at <operative/procedure/key> <seq|ts>    Query state at a point in time");
    println!("    rotate <operative/procedure/key>         Force a compaction snapshot");
    println!("    retention <operative> <hours>            Set time-travel retention");
    println!(
        "    delete <operative/procedure/key>         Hard-delete an Entity's persisted state"
    );
    println!(
        "    outbox <operative/procedure/key>         Effectively-once outbox status (lag, cursor, watermark)"
    );
    println!(
        "    journal <operative/instance>             Workflow instance proc-journal (committed procs, iterations, park)"
    );
    println!(
        "    inbox <operative/instance>               Workflow instance durable inbox (offsets, undelivered tail)"
    );
    println!();
    println!("EXAMPLES:");
    println!("    flz state get MarketOperative/OhlcProcedure/M1");
    println!("    flz state get MarketOperative/OhlcProcedure --output json");
    println!("    flz state list MarketOperative");
    println!("    flz state at MarketOperative/OhlcProcedure/M1 12345");
    println!("    flz state at MarketOperative/OhlcProcedure/M1 2026-05-01T12:00:00Z");
    println!("    flz state rotate MarketOperative/OhlcProcedure/M1");
    println!("    flz state retention MarketOperative 6");
    println!("    flz state delete MarketOperative/OhlcProcedure/M1");
    println!("    flz state outbox MarketOperative/OhlcProcedure/M1");
    println!("    flz state journal etl-job-42");
    println!("    flz state inbox chat-session-7");
}
