use std::process::exit;

use crate::utils::{get_output_format, run_rpc};

pub fn handle_state_command(args: &[String]) {
    if args.is_empty() {
        eprintln!("Error: Invalid number of arguments. See `flz state help`.");
        exit(1);
    }

    match args[0].as_str() {
        "list" => handle_list(args),
        "at" => handle_at(args),
        "rotate" => handle_segment_verb(args, "rotate", "state_rotate"),
        "retention" => handle_retention(args),
        "delete" => handle_segment_verb(args, "delete", "state_delete"),
        "outbox" => handle_segment_verb(args, "outbox", "state_outbox"),
        "journal" => handle_instance_read(args, "journal", "state_journal"),
        "inbox" => handle_instance_read(args, "inbox", "state_inbox"),
        "help" | "--help" | "-h" => print_state_help(),
        _ => {
            eprintln!(
                "Error: Invalid argument '{}'. Use 'list', 'at', 'rotate', 'retention', 'delete', 'outbox', 'journal', 'inbox', or 'help'.",
                args[0]
            );
            exit(1);
        }
    }
}

fn handle_list(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Usage: flz state list <operative_class> [--output text|json]");
        exit(1);
    }

    run_rpc(format!(
        "Flezha.Service.CliManager.state_list(\"{}\", \"{}\")",
        &args[1],
        get_output_format(&args[2..]),
    ));
}

fn handle_at(args: &[String]) {
    if args.len() < 3 {
        eprintln!("Usage: flz state at <operative/operation/segment_key> <seq | iso8601-timestamp> [--output text|json]");
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

/// The Segment-scoped write verbs — `rotate`, `delete`, `outbox` — take one
/// Segment path and no output format; they differ only in the CliManager
/// function they call.
fn handle_segment_verb(args: &[String], verb: &str, rpc_fun: &str) {
    if args.len() < 2 {
        eprintln!("Usage: flz state {verb} <operative/operation/segment_key>");
        exit(1);
    }

    run_rpc(format!(
        "Flezha.Service.CliManager.{}(\"{}\")",
        rpc_fun, &args[1],
    ));
}

/// The instance-scoped read verbs — `journal` and `inbox` — differ only in the
/// CliManager function they call.
fn handle_instance_read(args: &[String], verb: &str, rpc_fun: &str) {
    if args.len() < 2 {
        eprintln!("Usage: flz state {verb} <operative/instance> [--output text|json]");
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
    println!("flz state - Per-Segment state inspection and time-travel queries");
    println!();
    println!("USAGE:");
    println!("    flz state <SUBCOMMAND>");
    println!();
    println!("SUBCOMMANDS:");
    println!(
        "    list <operative>                          List Segments under an Operative class"
    );
    println!("    at <operative/operation/key> <seq|ts>         Query state at a point in time");
    println!("    rotate <operative/operation/key>              Force a compaction snapshot");
    println!("    retention <operative> <hours>             Set time-travel retention");
    println!(
        "    delete <operative/operation/key>              Hard-delete a Segment's persisted state"
    );
    println!(
        "    outbox <operative/operation/key>              Effectively-once outbox status (lag, cursor, watermark)"
    );
    println!(
        "    journal <operative/instance>              Bounded instance op-journal (committed ops, iterations, park)"
    );
    println!(
        "    inbox <operative/instance>                Bounded instance durable inbox (offsets, undelivered tail)"
    );
    println!();
    println!("EXAMPLES:");
    println!("    flz state list MarketOperative");
    println!("    flz state at MarketOperative/OhlcOperation/M1 12345");
    println!("    flz state at MarketOperative/OhlcOperation/M1 2026-05-01T12:00:00Z");
    println!("    flz state rotate MarketOperative/OhlcOperation/M1");
    println!("    flz state retention MarketOperative 6");
    println!("    flz state delete MarketOperative/OhlcOperation/M1");
    println!("    flz state outbox MarketOperative/OhlcOperation/M1");
    println!("    flz state journal etl-job-42");
    println!("    flz state inbox chat-session-7");
}
