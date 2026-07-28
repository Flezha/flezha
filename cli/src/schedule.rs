use crate::utils::{extract_param, get_json, get_namespace, run_rpc};
use std::process::exit;

pub fn handle_schedule_command(args: &[String]) {
    if args.is_empty() {
        print_schedule_help();
        exit(1);
    }

    match args[0].as_str() {
        "add" => handle_add(&args[1..]),
        "list" => run_rpc("Flezha.Service.CliManager.schedule_list()".to_string()),
        "remove" => handle_named("remove", &args[1..]),
        "run" => handle_named("run", &args[1..]),
        "help" | "--help" | "-h" => print_schedule_help(),
        other => {
            eprintln!(
                "Error: Unknown schedule command '{}'. Valid: add, list, remove, run, help",
                other
            );
            exit(1);
        }
    }
}

fn handle_add(args: &[String]) {
    let usage = "Usage: flz schedule add <name> --cron \"<expr>\" --tz <iana> --factory <factory_id> [--namespace <ns> | -n <ns>] [--init_values <json> | -iv <json>] [--kind bounded]";

    let name = match args.first() {
        Some(name) if !name.starts_with('-') => name.clone(),
        _ => {
            eprintln!("Error: missing schedule name.\n {}", usage);
            exit(1);
        }
    };

    let cron = require_param(args, "--cron", usage);
    let tz = require_param(args, "--tz", usage);
    let factory = require_param(args, "--factory", usage);
    let kind = extract_param(args, "--kind", "--kind").unwrap_or_else(|| "bounded".to_string());
    let namespace = get_namespace(args);
    let init_values = get_json(args, "--init_values", "-iv");
    let escaped_init_values = init_values.replace('"', "\\\"");

    run_rpc(format!(
        "Flezha.Service.CliManager.schedule_add(\"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\")",
        name, cron, tz, factory, namespace, escaped_init_values, kind,
    ));
}

fn handle_named(verb: &str, args: &[String]) {
    let name = match args.first() {
        Some(name) => name.clone(),
        None => {
            eprintln!("Usage: flz schedule {} <name>", verb);
            exit(1);
        }
    };

    run_rpc(format!(
        "Flezha.Service.CliManager.schedule_{}(\"{}\")",
        verb, name
    ));
}

fn require_param(args: &[String], key: &str, usage: &str) -> String {
    match extract_param(args, key, key) {
        Some(value) if !value.is_empty() => value,
        _ => {
            eprintln!("Error: {} is required.\n {}", key, usage);
            exit(1);
        }
    }
}

fn print_schedule_help() {
    println!("flz schedule - cron-scheduled bounded operative spawns");
    println!();
    println!("USAGE:");
    println!("    flz schedule <SUBCOMMAND>");
    println!();
    println!("SUBCOMMANDS:");
    println!("    add <name> --cron \"<expr>\" --tz <iana> --factory <factory_id>");
    println!("        [--namespace <ns>] [--init_values <json>] [--kind bounded]");
    println!("                 Add a durable schedule (bounded operatives only)");
    println!("    list         List schedules with their next fire time");
    println!("    remove <name>  Remove a schedule");
    println!("    run <name>     Fire one instance immediately with the stored config");
    println!("    help         Show this help message");
    println!();
    println!("EXAMPLES:");
    println!("    flz schedule add digest --cron \"0 8 * * *\" --tz Europe/Tallinn --factory ai_digest");
    println!("    flz schedule list");
    println!("    flz schedule run digest");
}
