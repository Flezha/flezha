use crate::args;
use crate::dispatch::{self, Command};
use crate::help::{self, Help};
use crate::rpc::{self, text};

pub(crate) const TABLE: Command = Command {
    help: help::OBSERVE,
    verbs: &[
        ("sources", |args| {
            render(args, help::OBSERVE_SOURCES, "observability_render_sources")
        }),
        ("sinks", |args| {
            render(args, help::OBSERVE_SINKS, "observability_render_sinks")
        }),
        ("tail", handle_tail),
        ("enable", |args| {
            named_source(args, help::OBSERVE_ENABLE, "observability_render_enable")
        }),
        ("disable", |args| {
            named_source(args, help::OBSERVE_DISABLE, "observability_render_disable")
        }),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: None,
    unknown: "Error: Unknown observe subcommand '{}'. Use sources, sinks, tail, enable, disable.",
    unknown_prints_help: false,
};

/// `sources` and `sinks`: a format, and nothing else.
fn render(args: &[String], screen: Help, rpc_fun: &str) {
    let given = &args[1..];
    if dispatch::subcommand_help(given, screen, dispatch::HELP_FLAGS) {
        return;
    }

    rpc::run(
        rpc_fun,
        &[text(args::output_format(given))],
        &[],
        given,
        rpc::DIAGNOSED,
    );
}

/// `enable` and `disable`: one source name, and nothing else.
fn named_source(args: &[String], screen: Help, rpc_fun: &str) {
    let given = &args[1..];
    if given.is_empty() || dispatch::subcommand_help(given, screen, dispatch::HELP_FLAGS) {
        if given.is_empty() {
            help::render(screen);
        }
        return;
    }

    rpc::run(rpc_fun, &[text(&given[0])], &[], given, rpc::DIAGNOSED);
}

fn handle_tail(args: &[String]) {
    let given = &args[1..];
    if given.is_empty() {
        help::render(help::OBSERVE_TAIL);
        return;
    }
    if dispatch::subcommand_help(given, help::OBSERVE_TAIL, dispatch::HELP_FLAGS) {
        return;
    }

    let source = &given[0];
    let rest = &given[1..];
    let duration_ms =
        args::or_refuse(args::number(rest, "--duration-ms", "--duration-ms")).unwrap_or(30_000u64);

    rpc::run(
        "observability_render_tail",
        &[text(source), text(args::output_format(rest))],
        &[("duration_ms", duration_ms.to_string())],
        rest,
        rpc::DIAGNOSED,
    );
}
