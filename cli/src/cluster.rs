use crate::utils::{extract_param, get_flezha_command, get_namespace};
use std::process::{exit, Command};

pub fn handle_cluster_command(args: &[String]) {
    if args.is_empty() {
        print_cluster_help();
        exit(1);
    }

    match args[0].as_str() {
        "snapshot" => handle_snapshot(args),
        "restore" => handle_restore(args),
        "help" | "--help" | "-h" => print_cluster_help(),
        _ => {
            eprintln!(
                "Error: Invalid cluster subcommand '{}'. Use 'snapshot', 'restore', or 'help'.",
                args[0]
            );
            print_cluster_help();
            exit(1);
        }
    }
}

fn handle_snapshot(args: &[String]) {
    let example_usage =
        "Usage: flz cluster snapshot <file> [--peer-timeout-ms N] [--strict]";

    if args.len() < 2 {
        eprintln!("Error: Invalid number of arguments.\n {}", example_usage);
        exit(1);
    }

    let file = &args[1];
    let peer_timeout = extract_param(&args[2..], "--peer-timeout-ms", "--peer-timeout-ms");
    let strict = args.iter().any(|a| a == "--strict");

    let opts_json = build_snapshot_opts_json(peer_timeout.as_deref(), strict);
    let escaped_opts = opts_json.replace('\"', "\\\"");

    let rpc_command = format!(
        "Flezha.Service.CliManager.cluster_snapshot(\"{}\", \"{}\")",
        file, escaped_opts,
    );

    let command = get_flezha_command();
    let status = Command::new(command)
        .arg("rpc")
        .arg(rpc_command)
        .status()
        .expect("Failed to execute command");

    if !status.success() {
        eprintln!("Error: snapshot command failed");
        exit(status.code().unwrap_or(1));
    }
}

fn handle_restore(args: &[String]) {
    let example_usage = "Usage: flz cluster restore <file> [--warn-if-older-than DURATION] [--namespace NS] [--peer-timeout-ms N]";

    if args.len() < 2 {
        eprintln!("Error: Invalid number of arguments.\n {}", example_usage);
        exit(1);
    }

    let file = &args[1];
    let warn_age = extract_param(&args[2..], "--warn-if-older-than", "--warn-if-older-than");
    let namespace = get_namespace(&args[2..]);
    let peer_timeout = extract_param(&args[2..], "--peer-timeout-ms", "--peer-timeout-ms");

    let opts_json = build_restore_opts_json(
        peer_timeout.as_deref(),
        warn_age.as_deref(),
        if namespace.is_empty() {
            None
        } else {
            Some(namespace.as_str())
        },
    );
    let escaped_opts = opts_json.replace('\"', "\\\"");

    let rpc_command = format!(
        "Flezha.Service.CliManager.cluster_restore(\"{}\", \"{}\")",
        file, escaped_opts,
    );

    let command = get_flezha_command();
    let status = Command::new(command)
        .arg("rpc")
        .arg(rpc_command)
        .status()
        .expect("Failed to execute command");

    if !status.success() {
        eprintln!("Error: restore command failed");
        exit(status.code().unwrap_or(1));
    }
}

fn build_snapshot_opts_json(peer_timeout: Option<&str>, strict: bool) -> String {
    let mut entries: Vec<String> = Vec::new();

    if let Some(t) = peer_timeout {
        entries.push(format!("\"peer_timeout_ms\": \"{}\"", t));
    }

    if strict {
        entries.push("\"strict\": true".to_string());
    }

    format!("{{{}}}", entries.join(", "))
}

fn build_restore_opts_json(
    peer_timeout: Option<&str>,
    warn_age: Option<&str>,
    namespace: Option<&str>,
) -> String {
    let mut entries: Vec<String> = Vec::new();

    if let Some(t) = peer_timeout {
        entries.push(format!("\"peer_timeout_ms\": \"{}\"", t));
    }

    if let Some(d) = warn_age {
        entries.push(format!("\"warn_if_older_than\": \"{}\"", d));
    }

    if let Some(ns) = namespace {
        entries.push(format!("\"namespace\": \"{}\"", ns));
    }

    format!("{{{}}}", entries.join(", "))
}

fn print_cluster_help() {
    println!("flz cluster - Cluster snapshot/restore commands");
    println!();
    println!("USAGE:");
    println!("    flz cluster <COMMAND>");
    println!();
    println!("COMMANDS:");
    println!("    snapshot   Capture a cluster-wide snapshot to a file");
    println!("    restore    Restore a cluster from a snapshot file");
    println!("    help       Show this help message");
    println!();
    println!("snapshot options:");
    println!("    --peer-timeout-ms <N>   Per-peer RPC timeout in ms (default 5000)");
    println!("    --strict                Abort on any peer failure (default best-effort)");
    println!();
    println!("restore options:");
    println!("    --warn-if-older-than <DURATION>");
    println!("                            Log WARN if snapshot is older than the threshold.");
    println!("                            Accepts Ns, Nm, Nh, Nd suffixes (e.g. 24h, 7d).");
    println!("    --namespace <NS>        Restore only entries in NS (default all)");
    println!("    --peer-timeout-ms <N>   Per-peer apply RPC timeout in ms (default 5000)");
    println!();
    println!("See docs/cluster-snapshot.md for the operator flow.");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_opts_empty_when_no_flags() {
        let json = build_snapshot_opts_json(None, false);
        assert_eq!(json, "{}");
    }

    #[test]
    fn snapshot_opts_includes_strict_and_timeout() {
        let json = build_snapshot_opts_json(Some("3000"), true);
        assert!(json.contains("\"peer_timeout_ms\": \"3000\""));
        assert!(json.contains("\"strict\": true"));
    }

    #[test]
    fn restore_opts_includes_namespace_and_warn_age() {
        let json = build_restore_opts_json(None, Some("24h"), Some("market"));
        assert!(json.contains("\"warn_if_older_than\": \"24h\""));
        assert!(json.contains("\"namespace\": \"market\""));
    }

    #[test]
    fn restore_opts_omits_unset_fields() {
        let json = build_restore_opts_json(None, None, None);
        assert_eq!(json, "{}");
    }
}
