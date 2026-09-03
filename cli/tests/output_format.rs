//! One question, one answer: `--json` and `--output json` are two spellings of
//! the same request, and every verb that renders must mean the same thing by
//! both (KI-80). The same holds of an option's four forms — `--long=value`,
//! `--long value`, `-s=value`, `-s value`. `flz plugin list` additionally has
//! to stop letting the format flag choose *which* collection it lists
//! (KI-148).
//!
//! The evidence is the RPC expression itself, so the fixture is a `flzvm` that
//! echoes what it was handed instead of running it.

mod common;

use std::process::Output;

use common::Shim;

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

/// A `flzvm` that prints each argument on its own line and succeeds, so the
/// expression `flz` built is readable on stdout.
const ECHOING: &str = "#!/bin/sh\nprintf '%s\\n' \"$@\"\nexit 0\n";

/// An `app_data` root nothing can be under, so the `flz reset` cases plan over
/// an empty tree. They pass `--dry-run` as well: nothing is ever cleared.
const NO_ROOT: &str = "--app-data-root=/nonexistent/flz-output-format-test";

enum Want {
    /// The `CliManager` call the verb must reach.
    Rpc(&'static str),
    /// A verb that renders locally: this text on stdout, no RPC.
    Local(&'static str),
    /// A refusal saying this, and no RPC at all.
    Refused(&'static str),
}

use Want::{Local, Refused, Rpc};

const CASES: &[(&[&str], Want)] = &[
    // `flz cluster status` — the `--output` dialect, in all four forms of the
    // value plus the switch.
    (
        &["cluster", "status"],
        Rpc("Flezha.Service.CliManager.cluster_status(\"\", \"text\")"),
    ),
    (
        &["cluster", "status", "--json"],
        Rpc("Flezha.Service.CliManager.cluster_status(\"\", \"json\")"),
    ),
    (
        &["cluster", "status", "--output", "json"],
        Rpc("Flezha.Service.CliManager.cluster_status(\"\", \"json\")"),
    ),
    (
        &["cluster", "status", "--output=json"],
        Rpc("Flezha.Service.CliManager.cluster_status(\"\", \"json\")"),
    ),
    (
        &["cluster", "status", "-o", "json"],
        Rpc("Flezha.Service.CliManager.cluster_status(\"\", \"json\")"),
    ),
    (
        &["cluster", "status", "-o=json"],
        Rpc("Flezha.Service.CliManager.cluster_status(\"\", \"json\")"),
    ),
    // A spelling nobody recognises is refused, not rendered as text.
    (
        &["cluster", "status", "--output", "yaml"],
        Refused("unknown output format"),
    ),
    // `flz runner list` and `flz operative list` share `cluster status`'s
    // reader, so the switch has to reach them too.
    (
        &["runner", "list", "--json"],
        Rpc("Flezha.Service.CliManager.list_runners(\"\", \"json\")"),
    ),
    (
        &["operative", "list", "--output", "json"],
        Rpc("Flezha.Service.CliManager.list_operatives(\"\", \"json\")"),
    ),
    // `flz audit` — the `--json` dialect.
    (
        &["audit", "list"],
        Rpc("Flezha.Service.CliManager.audit_log_render_list(\"text\")"),
    ),
    (
        &["audit", "list", "--json"],
        Rpc("Flezha.Service.CliManager.audit_log_render_list(\"json\")"),
    ),
    (
        &["audit", "list", "--output", "json"],
        Rpc("Flezha.Service.CliManager.audit_log_render_list(\"json\")"),
    ),
    (
        &["audit", "tail", "--json"],
        Rpc("Flezha.Service.CliManager.audit_log_render_tail(\"json\", [duration_ms: 30000])"),
    ),
    (
        &["audit", "tail", "--output", "json"],
        Rpc("Flezha.Service.CliManager.audit_log_render_tail(\"json\", [duration_ms: 30000])"),
    ),
    (
        &["audit", "stats", "-o", "json"],
        Rpc("Flezha.Service.CliManager.audit_log_render_stats(\"json\")"),
    ),
    // `flz controller` — the predicate that was inlined twice.
    (
        &["controller", "status", "--json"],
        Rpc("Flezha.Service.CliManager.controller_status(\"json\")"),
    ),
    (
        &["controller", "status", "--output", "json"],
        Rpc("Flezha.Service.CliManager.controller_status(\"json\")"),
    ),
    (
        &["controller", "mem-top", "--json"],
        Rpc("Flezha.Service.CliManager.controller_mem_top(\"json\")"),
    ),
    (
        &["controller", "mem-top", "--output", "json"],
        Rpc("Flezha.Service.CliManager.controller_mem_top(\"json\")"),
    ),
    // `flz observe` — the second byte-identical copy of that predicate.
    (
        &["observe", "sources", "--json"],
        Rpc("Flezha.Service.CliManager.observability_render_sources(\"json\")"),
    ),
    (
        &["observe", "sources", "--output", "json"],
        Rpc("Flezha.Service.CliManager.observability_render_sources(\"json\")"),
    ),
    (
        &["observe", "tail", "beam_vm", "--output", "json"],
        Rpc(
            "Flezha.Service.CliManager.observability_render_tail(\"beam_vm\", \"json\", [duration_ms: 30000])",
        ),
    ),
    // `flz namespace list` and `flz state` — the `--output` dialect again.
    (
        &["namespace", "list", "--json"],
        Rpc("Flezha.Service.CliManager.list_namespaces(\"json\")"),
    ),
    (
        &["namespace", "list", "--output", "json"],
        Rpc("Flezha.Service.CliManager.list_namespaces(\"json\")"),
    ),
    (
        &["state", "list", "MarketOperative", "--json"],
        Rpc("Flezha.Service.CliManager.state_list(\"MarketOperative\", \"json\", [])"),
    ),
    (
        &["state", "list", "MarketOperative", "--output", "json"],
        Rpc("Flezha.Service.CliManager.state_list(\"MarketOperative\", \"json\", [])"),
    ),
    // `flz plugin list` — the format flag renders; `--installed` chooses the
    // collection. Neither may do the other's job.
    (
        &["plugin", "list"],
        Rpc("Flezha.Service.CliManager.list_plugins()"),
    ),
    (
        &["plugin", "list", "--installed"],
        Rpc("Flezha.Service.CliManager.list_installed_plugins(\"text\")"),
    ),
    (
        &["plugin", "list", "--installed", "--json"],
        Rpc("Flezha.Service.CliManager.list_installed_plugins(\"json\")"),
    ),
    (
        &["plugin", "list", "--installed", "--output", "json"],
        Rpc("Flezha.Service.CliManager.list_installed_plugins(\"json\")"),
    ),
    // `flz plugin enable` passes its options on rather than rendering them,
    // and reads them through the second scanner. An option's four spellings
    // are one option there too: a short flag written with `=` is what the
    // operator typed.
    (
        &["plugin", "enable", "p", "prov", "--output=json"],
        Rpc(
            r#"Flezha.Service.CliManager.enable_plugin("p", "prov", "{}", "{\"output\": \"json\", \"namespace\": \"default\"}")"#,
        ),
    ),
    (
        &["plugin", "enable", "p", "prov", "-o=json"],
        Rpc(
            r#"Flezha.Service.CliManager.enable_plugin("p", "prov", "{}", "{\"output\": \"json\", \"namespace\": \"default\"}")"#,
        ),
    ),
    // The provider registry has no JSON rendering, so asking for one is
    // refused rather than answered with a different query.
    (&["plugin", "list", "--json"], Refused("--installed")),
    (
        &["plugin", "list", "--output", "json"],
        Refused("--installed"),
    ),
    // `flz reset` renders its own report, and reads the same two spellings.
    (
        &["reset", "--dry-run", "--json", NO_ROOT],
        Local("\"cleared\":["),
    ),
    (
        &["reset", "--dry-run", "--output", "json", NO_ROOT],
        Local("\"cleared\":["),
    ),
    (
        &["reset", "--dry-run", "--output", "yaml", NO_ROOT],
        Refused("unknown output format"),
    ),
];

/// The one `CliManager` call the run reached, as the shim echoed it.
fn rpc_expression(stdout: &str) -> Option<&str> {
    stdout
        .lines()
        .find(|l| l.starts_with("Flezha.Service.CliManager."))
}

fn run(cli: &Shim, argv: &[&str]) -> Output {
    cli.cmd()
        .args(argv)
        // `flz reset` probes for a live controller before doing anything, and
        // the echoing shim would answer that probe with "up".
        .env("FLEZHA_RESET_PROBE_OVERRIDE", "down")
        .output()
        .expect("flz runs")
}

/// One `#[test]`, one fixture: the copy is written once and every case runs
/// against it, so no thread forks while that write handle is still open.
#[test]
fn both_spellings_of_the_output_format_mean_the_same_thing() {
    let cli = Shim::new(FLZ_BIN, ECHOING);
    let mut wrong: Vec<String> = Vec::new();

    for (argv, want) in CASES {
        let out = run(&cli, argv);
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();
        let reached = rpc_expression(&stdout);
        let invocation = format!("flz {}", argv.join(" "));

        match want {
            Rpc(expected) => {
                if reached != Some(*expected) {
                    wrong.push(format!(
                        "{}\n     got  {}\n     want {}",
                        invocation,
                        reached.unwrap_or("<no RPC>"),
                        expected
                    ));
                }
            }
            Local(needle) => {
                if !stdout.contains(needle) {
                    wrong.push(format!(
                        "{}\n     got  {:?}\n     want stdout containing {:?}",
                        invocation,
                        stdout.trim(),
                        needle
                    ));
                }
            }
            Refused(needle) => {
                if out.status.success() || !stderr.contains(needle) || reached.is_some() {
                    wrong.push(format!(
                        "{}\n     got  exit {:?}, stderr {:?}, RPC {:?}\n     want a refusal saying {:?} and no RPC",
                        invocation,
                        out.status.code(),
                        stderr.trim(),
                        reached,
                        needle
                    ));
                }
            }
        }
    }

    assert!(
        wrong.is_empty(),
        "{} of {} invocations answered the wrong thing:\n{}",
        wrong.len(),
        CASES.len(),
        wrong.join("\n")
    );
}
