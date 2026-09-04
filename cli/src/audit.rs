use std::str::FromStr;

use crate::dispatch::{self, Command};
use crate::rpc::{self, text, Opt};
use crate::{args, help};

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
        "audit_log_render_list",
        &[text(args::output_format(&args[1..]))],
        &[],
        args,
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
    let mut opts: Vec<Opt> = Vec::new();

    if let Some(source) = read_named(rest, "source") {
        opts.push(("source", text(&source)));
    }
    if let Some(name) = read_named(rest, "name") {
        opts.push(("name", text(&name)));
    }
    if let Some(since) = read_number::<i64>(rest, "since") {
        opts.push(("since_us", since.to_string()));
    }
    if let Some(until) = read_number::<i64>(rest, "until") {
        opts.push(("until_us", until.to_string()));
    }
    if let Some(limit) = read_number::<u64>(rest, "limit") {
        opts.push(("limit", limit.to_string()));
    }

    rpc::run(
        "audit_log_render_read",
        &[text(file), text(args::output_format(rest))],
        &opts,
        rest,
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
        "audit_log_render_tail",
        &[text(args::output_format(rest))],
        &[("duration_ms", duration_ms.to_string())],
        rest,
        rpc::DIAGNOSED,
    );
}

fn handle_stats(args: &[String]) {
    if dispatch::subcommand_help(&args[1..], help::AUDIT_STATS, dispatch::HELP_FLAGS) {
        return;
    }

    rpc::run(
        "audit_log_render_stats",
        &[text(args::output_format(&args[1..]))],
        &[],
        args,
        rpc::DIAGNOSED,
    );
}

fn handle_rotate(args: &[String]) {
    if dispatch::subcommand_help(&args[1..], help::AUDIT_ROTATE, dispatch::HELP_FLAGS) {
        return;
    }

    rpc::run(
        "audit_log_render_rotate",
        &[text(args::output_format(&args[1..]))],
        &[],
        args,
        rpc::DIAGNOSED,
    );
}
