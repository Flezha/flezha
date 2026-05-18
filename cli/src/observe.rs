use std::process::{exit, Command};

use crate::utils::get_flezha_command;

pub fn handle_observe_command(args: &[String]) {
    if args.is_empty() {
        print_observe_help();
        exit(1);
    }

    match args[0].as_str() {
        "sources" => handle_sources(&args[1..]),
        "sinks" => handle_sinks(&args[1..]),
        "tail" => handle_tail(&args[1..]),
        "enable" => handle_enable(&args[1..]),
        "disable" => handle_disable(&args[1..]),
        "help" | "--help" | "-h" => print_observe_help(),
        other => {
            eprintln!(
                "Error: Unknown observe subcommand '{}'. Use sources, sinks, tail, enable, disable.",
                other
            );
            exit(1);
        }
    }
}

fn want_json(args: &[String]) -> bool {
    args.iter().any(|a| a == "--json" || a == "-o=json")
}

fn handle_sources(args: &[String]) {
    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_sources_help();
        return;
    }

    let format = if want_json(args) { "json" } else { "text" };

    let rpc = format!(
        "Flezha.Service.CliManager.observability_render_sources(\"{}\")",
        format
    );

    run_rpc(&rpc);
}

fn handle_sinks(args: &[String]) {
    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_sinks_help();
        return;
    }

    let format = if want_json(args) { "json" } else { "text" };

    let rpc = format!(
        "Flezha.Service.CliManager.observability_render_sinks(\"{}\")",
        format
    );

    run_rpc(&rpc);
}

fn handle_tail(args: &[String]) {
    if args.is_empty() || args.iter().any(|a| a == "--help" || a == "-h") {
        print_tail_help();
        return;
    }

    let source = &args[0];
    let format = if want_json(&args[1..]) {
        "json"
    } else {
        "text"
    };

    let duration_ms = extract_duration_ms(&args[1..]).unwrap_or(30_000);

    let rpc = format!(
        "Flezha.Service.CliManager.observability_render_tail(\"{}\", \"{}\", [duration_ms: {}])",
        source, format, duration_ms
    );

    run_rpc(&rpc);
}

fn extract_duration_ms(args: &[String]) -> Option<u64> {
    let mut iter = args.iter();
    while let Some(a) = iter.next() {
        if a == "--duration-ms" {
            if let Some(v) = iter.next() {
                if let Ok(n) = v.parse::<u64>() {
                    return Some(n);
                }
            }
        } else if let Some(v) = a.strip_prefix("--duration-ms=") {
            if let Ok(n) = v.parse::<u64>() {
                return Some(n);
            }
        }
    }
    None
}

fn handle_enable(args: &[String]) {
    if args.is_empty() || args.iter().any(|a| a == "--help" || a == "-h") {
        print_enable_help();
        return;
    }

    let source = &args[0];

    let rpc = format!(
        "Flezha.Service.CliManager.observability_render_enable(\"{}\")",
        source
    );

    run_rpc(&rpc);
}

fn handle_disable(args: &[String]) {
    if args.is_empty() || args.iter().any(|a| a == "--help" || a == "-h") {
        print_disable_help();
        return;
    }

    let source = &args[0];

    let rpc = format!(
        "Flezha.Service.CliManager.observability_render_disable(\"{}\")",
        source
    );

    run_rpc(&rpc);
}

fn run_rpc(rpc: &str) {
    let command = get_flezha_command();
    let status = Command::new(command).arg("rpc").arg(rpc).status();

    match status {
        Ok(s) if s.success() => {}
        Ok(s) => exit(s.code().unwrap_or(3)),
        Err(e) => {
            eprintln!("error: failed to spawn rpc: {}", e);
            exit(3);
        }
    }
}

fn print_observe_help() {
    println!("flz observe - Observability foundation inspection");
    println!();
    println!("USAGE:");
    println!("    flz observe <COMMAND> [OPTIONS]");
    println!();
    println!("COMMANDS:");
    println!("    sources    List registered sources and their status / cardinality budget");
    println!("    sinks      List registered sinks and their overflow policy / watermark");
    println!("    tail       Stream live signals from a source via the PubSub sink");
    println!("    enable     Enable a source (transitions its registry status to :active)");
    println!("    disable    Disable a source (transitions to :disabled — Bus drops emits)");
}

fn print_sources_help() {
    println!("flz observe sources [--json]");
    println!();
    println!("Print one row per registered source (name, status, kinds, cardinality budget).");
}

fn print_sinks_help() {
    println!("flz observe sinks [--json]");
    println!();
    println!("Print one row per registered sink (name, module, overflow, watermark).");
}

fn print_tail_help() {
    println!("flz observe tail <SOURCE> [--json] [--duration-ms N]");
    println!();
    println!("Stream live signals from SOURCE via the PubSub sink. Requires");
    println!(":pubsub_sink_enabled to be true in :observability config.");
    println!();
    println!("OPTIONS:");
    println!("    --json              Emit NDJSON (one JSON object per signal per line)");
    println!("    --duration-ms <N>   Stream for at most N milliseconds (default 30000)");
}

fn print_enable_help() {
    println!("flz observe enable <SOURCE>");
}

fn print_disable_help() {
    println!("flz observe disable <SOURCE>");
}
