use std::process::{exit, Command};

use crate::utils::get_flezha_command;

pub fn handle_audit_command(args: &[String]) {
    if args.is_empty() {
        print_audit_help();
        exit(1);
    }

    match args[0].as_str() {
        "list" => handle_list(&args[1..]),
        "read" => handle_read(&args[1..]),
        "tail" => handle_tail(&args[1..]),
        "stats" => handle_stats(&args[1..]),
        "rotate" => handle_rotate(&args[1..]),
        "help" | "--help" | "-h" => print_audit_help(),
        other => {
            eprintln!(
                "Error: Unknown audit subcommand '{}'. Use list, read, tail, stats, rotate.",
                other
            );
            exit(1);
        }
    }
}

fn want_json(args: &[String]) -> bool {
    args.iter().any(|a| a == "--json" || a == "-o=json")
}

fn extract_named(args: &[String], name: &str) -> Option<String> {
    let prefix = format!("--{}=", name);
    let flag = format!("--{}", name);
    let mut iter = args.iter();
    while let Some(a) = iter.next() {
        if a == &flag {
            if let Some(v) = iter.next() {
                return Some(v.clone());
            }
        } else if let Some(v) = a.strip_prefix(&prefix) {
            return Some(v.to_string());
        }
    }
    None
}

fn extract_u64(args: &[String], name: &str) -> Option<u64> {
    extract_named(args, name).and_then(|v| v.parse::<u64>().ok())
}

fn extract_i64(args: &[String], name: &str) -> Option<i64> {
    extract_named(args, name).and_then(|v| v.parse::<i64>().ok())
}

fn handle_list(args: &[String]) {
    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!("flz audit list [--json]");
        return;
    }

    let format = if want_json(args) { "json" } else { "text" };

    let rpc = format!(
        "Flezha.Service.CliManager.audit_log_render_list(\"{}\")",
        format
    );

    run_rpc(&rpc);
}

fn handle_read(args: &[String]) {
    if args.is_empty() || args.iter().any(|a| a == "--help" || a == "-h") {
        print_read_help();
        return;
    }

    let file = &args[0];
    let format = if want_json(&args[1..]) { "json" } else { "text" };

    let mut opts = String::new();

    if let Some(source) = extract_named(&args[1..], "source") {
        opts.push_str(&format!("source: \"{}\", ", source));
    }
    if let Some(name) = extract_named(&args[1..], "name") {
        opts.push_str(&format!("name: \"{}\", ", name));
    }
    if let Some(since) = extract_i64(&args[1..], "since") {
        opts.push_str(&format!("since_us: {}, ", since));
    }
    if let Some(until) = extract_i64(&args[1..], "until") {
        opts.push_str(&format!("until_us: {}, ", until));
    }
    if let Some(limit) = extract_u64(&args[1..], "limit") {
        opts.push_str(&format!("limit: {}, ", limit));
    }

    let opts_kw = if opts.is_empty() {
        "[]".to_string()
    } else {
        format!("[{}]", opts.trim_end_matches(", "))
    };

    let rpc = format!(
        "Flezha.Service.CliManager.audit_log_render_read(\"{}\", \"{}\", {})",
        file, format, opts_kw
    );

    run_rpc(&rpc);
}

fn handle_tail(args: &[String]) {
    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!("flz audit tail [--json] [--duration-ms N]");
        return;
    }

    let format = if want_json(args) { "json" } else { "text" };
    let duration_ms = extract_u64(args, "duration-ms").unwrap_or(30_000);

    let rpc = format!(
        "Flezha.Service.CliManager.audit_log_render_tail(\"{}\", [duration_ms: {}])",
        format, duration_ms
    );

    run_rpc(&rpc);
}

fn handle_stats(args: &[String]) {
    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!("flz audit stats [--json]");
        return;
    }

    let format = if want_json(args) { "json" } else { "text" };

    let rpc = format!(
        "Flezha.Service.CliManager.audit_log_render_stats(\"{}\")",
        format
    );

    run_rpc(&rpc);
}

fn handle_rotate(args: &[String]) {
    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!("flz audit rotate [--json]");
        return;
    }

    let format = if want_json(args) { "json" } else { "text" };

    let rpc = format!(
        "Flezha.Service.CliManager.audit_log_render_rotate(\"{}\")",
        format
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

fn print_audit_help() {
    println!("flz audit - Durable audit log inspection");
    println!();
    println!("USAGE:");
    println!("    flz audit <COMMAND> [OPTIONS]");
    println!();
    println!("COMMANDS:");
    println!("    list      List the on-disk catalog of rotated files");
    println!("    read      Read records from a specific file with optional filters");
    println!("    tail      Stream live audit records until a duration elapses");
    println!("    stats    Show operational snapshot (file path, size, totals, drops)");
    println!("    rotate    Force an immediate rotation of the active file");
    println!();
    println!("OPTIONS:");
    println!("    --json    Emit machine-readable output (NDJSON for tail/read)");
}

fn print_read_help() {
    println!("flz audit read <FILE> [OPTIONS]");
    println!();
    println!("OPTIONS:");
    println!("    --json              Emit NDJSON");
    println!("    --source <S>        Match records whose source equals S");
    println!("    --name <N>          Match records whose name equals N");
    println!("    --since <TS_US>     Match records with ts_us >= TS_US (monotonic)");
    println!("    --until <TS_US>     Match records with ts_us <= TS_US");
    println!("    --limit <N>         Return at most N records (default 1000)");
}
