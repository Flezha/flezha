mod args;
mod audit;
mod auth;
mod boot;
mod cluster;
mod controller;
mod dispatch;
mod dlq;
mod help;
mod namespace;
mod observe;
mod operative;
mod plugin;
mod reset;
mod role;
mod rpc;
mod runner;
mod schedule;
mod state;
mod token;

use std::env;

use dispatch::Command;

/// `flz` itself is a command table like the sixteen below it: same bare-invocation
/// rule, same help spellings, same refusal.
const FLZ: Command = Command {
    help: help::FLZ,
    verbs: &[
        ("controller", |args| {
            dispatch::run(&controller::TABLE, &args[1..])
        }),
        ("cluster", |args| dispatch::run(&cluster::TABLE, &args[1..])),
        ("operative", |args| {
            dispatch::run(&operative::TABLE, &args[1..])
        }),
        ("namespace", |args| {
            dispatch::run(&namespace::TABLE, &args[1..])
        }),
        ("runner", |args| dispatch::run(&runner::TABLE, &args[1..])),
        ("plugin", |args| dispatch::run(&plugin::TABLE, &args[1..])),
        ("boot", |args| dispatch::run(&boot::TABLE, &args[1..])),
        ("state", |args| dispatch::run(&state::TABLE, &args[1..])),
        ("reset", |args| reset::handle_reset_command(&args[1..])),
        ("observe", |args| dispatch::run(&observe::TABLE, &args[1..])),
        ("audit", |args| dispatch::run(&audit::TABLE, &args[1..])),
        ("dlq", |args| dispatch::run(&dlq::TABLE, &args[1..])),
        ("schedule", |args| {
            dispatch::run(&schedule::TABLE, &args[1..])
        }),
        ("auth", |args| dispatch::run(&auth::TABLE, &args[1..])),
        ("token", |args| dispatch::run(&token::TABLE, &args[1..])),
        ("role", |args| dispatch::run(&role::TABLE, &args[1..])),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: None,
    unknown: "Error: Unknown command '{}'",
    unknown_prints_help: true,
};

fn main() {
    let args: Vec<String> = env::args().collect();
    dispatch::run(&FLZ, &args[1..]);
}
