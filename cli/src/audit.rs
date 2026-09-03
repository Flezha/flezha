use std::str::FromStr;

use crate::dispatch::{self, Command};
use crate::{args, help, rpc};

pub(crate) const TABLE: Command = Command {
    help: help::AUDIT,
    verbs: &[
        ("list", handle_list),
        ("read", handle_read),
        ("tail", handle_tail),
        ("stats", handle_stats),
        ("rotate", handle_rotate),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: None,
    unknown: "Error: Unknown audit subcommand '{}'. Use list, read, tail, stats, rotate.",
    unknown_prints_help: false,
};

fn read_named(args: &[String], name: &str) -> Option<String> {
    let key = format!("--{}", name);
    args::or_refuse(args::extract_param(args, &key, &key))
}

/// The audit options spell their long key from the option's name; the parse
/// and its refusal are the shared reader's.
fn read_number<T: FromStr>(args: &[String], name: &str) -> Option<T> {
    let key = format!("--{}", name);
    args::or_refuse(args::number(args, &key, &key))
}

fn handle_list(args: &[String]) {
    if dispatch::subcommand_help(&args[1..], help::AUDIT_LIST, dispatch::HELP_FLAGS) {
        return;
    }

    rpc::run(
        format!(
            "Flezha.Service.CliManager.audit_log_render_list(\"{}\")",
            args::output_format(&args[1..])
        ),
        rpc::DIAGNOSED,
    );
}

fn handle_read(args: &[String]) {
    let given = &args[1..];
    if given.is_empty() || given.iter().any(|a| a == "--help" || a == "-h") {
        help::render(help::AUDIT_READ);
        return;
    }

    let file = &given[0];
    let rest = &given[1..];
    let mut opts = String::new();

    if let Some(source) = read_named(rest, "source") {
        opts.push_str(&format!("source: \"{}\", ", source));
    }
    if let Some(name) = read_named(rest, "name") {
        opts.push_str(&format!("name: \"{}\", ", name));
    }
    if let Some(since) = read_number::<i64>(rest, "since") {
        opts.push_str(&format!("since_us: {}, ", since));
    }
    if let Some(until) = read_number::<i64>(rest, "until") {
        opts.push_str(&format!("until_us: {}, ", until));
    }
    if let Some(limit) = read_number::<u64>(rest, "limit") {
        opts.push_str(&format!("limit: {}, ", limit));
    }

    let opts_kw = if opts.is_empty() {
        "[]".to_string()
    } else {
        format!("[{}]", opts.trim_end_matches(", "))
    };

    rpc::run(
        format!(
            "Flezha.Service.CliManager.audit_log_render_read(\"{}\", \"{}\", {})",
            file,
            args::output_format(rest),
            opts_kw
        ),
        rpc::DIAGNOSED,
    );
}

fn handle_tail(args: &[String]) {
    if dispatch::subcommand_help(&args[1..], help::AUDIT_TAIL, dispatch::HELP_FLAGS) {
        return;
    }

    let rest = &args[1..];
    let duration_ms = read_number::<u64>(rest, "duration-ms").unwrap_or(30_000);

    rpc::run(
        format!(
            "Flezha.Service.CliManager.audit_log_render_tail(\"{}\", [duration_ms: {}])",
            args::output_format(rest),
            duration_ms
        ),
        rpc::DIAGNOSED,
    );
}

fn handle_stats(args: &[String]) {
    if dispatch::subcommand_help(&args[1..], help::AUDIT_STATS, dispatch::HELP_FLAGS) {
        return;
    }

    rpc::run(
        format!(
            "Flezha.Service.CliManager.audit_log_render_stats(\"{}\")",
            args::output_format(&args[1..])
        ),
        rpc::DIAGNOSED,
    );
}

fn handle_rotate(args: &[String]) {
    if dispatch::subcommand_help(&args[1..], help::AUDIT_ROTATE, dispatch::HELP_FLAGS) {
        return;
    }

    rpc::run(
        format!(
            "Flezha.Service.CliManager.audit_log_render_rotate(\"{}\")",
            args::output_format(&args[1..])
        ),
        rpc::DIAGNOSED,
    );
}
