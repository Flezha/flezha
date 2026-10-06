mod args;
mod audit;
mod auth;
mod boot;
mod cluster;
mod controller;
mod dispatch;
mod dlq;
mod help;
mod mcp;
mod namespace;
mod observe;
mod plugin;
mod relay;
mod reset;
mod role;
mod rpc;
mod runner;
mod schedule;
mod settings;
mod state;
mod template;
mod token;

use std::env;

use dispatch::{Command, Missing, Verb};

/// `flz` itself is a command table like the seventeen below it: same bare-invocation
/// rule, same help spellings, same refusal. Sixteen of its rows are tables of
/// their own; `reset`, `mcp` and `help`, which prints the catalogue under
/// `--json`, are the verbs the top level runs itself.
const FLZ: Command = Command {
    path: "flz",
    help: help::FLZ,
    verbs: &[
        Verb::table("controller", &controller::TABLE),
        Verb::table("cluster", &cluster::TABLE),
        Verb::table("relay", &relay::TABLE),
        Verb::table("namespace", &namespace::TABLE),
        Verb::table("runner", &runner::TABLE),
        Verb::table("plugin", &plugin::TABLE),
        Verb::table("boot", &boot::TABLE),
        Verb::table("state", &state::TABLE),
        Verb::new("reset", reset::handle_reset_command)
            .reading(reset::OPTIONS)
            .refusing(reset::CONFLICTS)
            .strict()
            .screen(help::RESET),
        Verb::new("mcp", mcp::serve).strict().screen(help::MCP),
        Verb::table("template", &template::TABLE),
        Verb::table("observe", &observe::TABLE),
        Verb::table("audit", &audit::TABLE),
        Verb::table("dlq", &dlq::TABLE),
        Verb::table("schedule", &schedule::TABLE),
        Verb::table("auth", &auth::TABLE),
        Verb::table("token", &token::TABLE),
        Verb::table("role", &role::TABLE),
        Verb::new("help", help::handle_help),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Missing::Screen,
    unknown_prints_help: true,
};

fn main() {
    let args: Vec<String> = env::args().collect();
    rpc::set_json(args::asks_json(&args[1..]));
    dispatch::run(&FLZ, &args[1..]);
}
