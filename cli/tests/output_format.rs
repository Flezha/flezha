//! One question, one answer: `--json` and `--output json` are two spellings of
//! the same request, and every verb that renders must mean the same thing by
//! both (KI-80). The same holds of an option's four forms — `--long=value`,
//! `--long value`, `-s=value`, `-s value`. `flz plugin list` additionally has
//! to stop letting the format flag choose *which* collection it lists
//! (KI-148), and a plugin setting has to mean the same thing written
//! `Xkey=value` as written `--Xkey=value` (KI-167). An option written with no
//! value at all is a fourth question of the same kind, and its answer is a
//! refusal naming the flag rather than the default (KI-173). A value the
//! reader cannot parse is the fifth, and takes the same answer (KI-176) — and
//! the request an invocation builds may not depend on the process that built
//! it (KI-177).
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
        Rpc("Flezha.Service.CliManager.cluster_status(\"\", \"text\", [])"),
    ),
    (
        &["cluster", "status", "--json"],
        Rpc("Flezha.Service.CliManager.cluster_status(\"\", \"json\", [])"),
    ),
    (
        &["cluster", "status", "--output", "json"],
        Rpc("Flezha.Service.CliManager.cluster_status(\"\", \"json\", [])"),
    ),
    (
        &["cluster", "status", "--output=json"],
        Rpc("Flezha.Service.CliManager.cluster_status(\"\", \"json\", [])"),
    ),
    (
        &["cluster", "status", "-o", "json"],
        Rpc("Flezha.Service.CliManager.cluster_status(\"\", \"json\", [])"),
    ),
    (
        &["cluster", "status", "-o=json"],
        Rpc("Flezha.Service.CliManager.cluster_status(\"\", \"json\", [])"),
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
        Rpc("Flezha.Service.CliManager.list_runners(\"\", \"json\", [])"),
    ),
    (
        &["operative", "list", "--output", "json"],
        Rpc("Flezha.Service.CliManager.list_operatives(\"\", \"json\", [])"),
    ),
    // `flz audit` — the `--json` dialect.
    (
        &["audit", "list"],
        Rpc("Flezha.Service.CliManager.audit_log_render_list(\"text\", [])"),
    ),
    (
        &["audit", "list", "--json"],
        Rpc("Flezha.Service.CliManager.audit_log_render_list(\"json\", [])"),
    ),
    (
        &["audit", "list", "--output", "json"],
        Rpc("Flezha.Service.CliManager.audit_log_render_list(\"json\", [])"),
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
        Rpc("Flezha.Service.CliManager.audit_log_render_stats(\"json\", [])"),
    ),
    // `flz controller` — the predicate that was inlined twice.
    (
        &["controller", "status", "--json"],
        Rpc("Flezha.Service.CliManager.controller_status(\"json\", [])"),
    ),
    (
        &["controller", "status", "--output", "json"],
        Rpc("Flezha.Service.CliManager.controller_status(\"json\", [])"),
    ),
    (
        &["controller", "mem-top", "--json"],
        Rpc("Flezha.Service.CliManager.controller_mem_top(\"json\", [])"),
    ),
    (
        &["controller", "mem-top", "--output", "json"],
        Rpc("Flezha.Service.CliManager.controller_mem_top(\"json\", [])"),
    ),
    // `flz observe` — the second byte-identical copy of that predicate.
    (
        &["observe", "sources", "--json"],
        Rpc("Flezha.Service.CliManager.observability_render_sources(\"json\", [])"),
    ),
    (
        &["observe", "sources", "--output", "json"],
        Rpc("Flezha.Service.CliManager.observability_render_sources(\"json\", [])"),
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
        Rpc("Flezha.Service.CliManager.list_namespaces(\"json\", [])"),
    ),
    (
        &["namespace", "list", "--output", "json"],
        Rpc("Flezha.Service.CliManager.list_namespaces(\"json\", [])"),
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
        Rpc("Flezha.Service.CliManager.list_plugins([])"),
    ),
    (
        &["plugin", "list", "--installed"],
        Rpc("Flezha.Service.CliManager.list_installed_plugins(\"text\", [])"),
    ),
    (
        &["plugin", "list", "--installed", "--json"],
        Rpc("Flezha.Service.CliManager.list_installed_plugins(\"json\", [])"),
    ),
    (
        &["plugin", "list", "--installed", "--output", "json"],
        Rpc("Flezha.Service.CliManager.list_installed_plugins(\"json\", [])"),
    ),
    // `flz plugin enable` passes its options on rather than rendering them,
    // and reads them through the second scanner. An option's four spellings
    // are one option there too: a short flag written with `=` is what the
    // operator typed.
    (
        &["plugin", "enable", "p", "prov", "--output=json"],
        Rpc(
            r#"Flezha.Service.CliManager.enable_plugin("p", "prov", "{}", "{\"output\": \"json\", \"namespace\": \"default\"}", [])"#,
        ),
    ),
    (
        &["plugin", "enable", "p", "prov", "-o=json"],
        Rpc(
            r#"Flezha.Service.CliManager.enable_plugin("p", "prov", "{}", "{\"output\": \"json\", \"namespace\": \"default\"}", [])"#,
        ),
    ),
    // A plugin setting is one setting in both its spellings, and the bare one
    // is what `flz help` and the enable usage line teach (KI-167).
    (
        &[
            "plugin",
            "enable",
            "p",
            "prov",
            "Xhosts=kafka:9092",
            "Xtopic=my_topic",
        ],
        Rpc(
            r#"Flezha.Service.CliManager.enable_plugin("p", "prov", "{\"hosts\": \"kafka:9092\", \"topic\": \"my_topic\"}", "{\"namespace\": \"default\"}", [])"#,
        ),
    ),
    (
        &[
            "plugin",
            "enable",
            "p",
            "prov",
            "--Xhosts=kafka:9092",
            "--Xtopic=my_topic",
        ],
        Rpc(
            r#"Flezha.Service.CliManager.enable_plugin("p", "prov", "{\"hosts\": \"kafka:9092\", \"topic\": \"my_topic\"}", "{\"namespace\": \"default\"}", [])"#,
        ),
    ),
    // The provider registry has no JSON rendering, so asking for one is
    // refused rather than answered with a different query.
    (&["plugin", "list", "--json"], Refused("--installed")),
    (
        &["plugin", "list", "--output", "json"],
        Refused("--installed"),
    ),
    // `flz boot await` takes one option, and it is one option in all four of
    // its spellings (KI-169). The wait is 90 s so a reader that quietly fell
    // back to the 60 s default fails the row instead of passing it by
    // coincidence.
    (
        &["boot", "await"],
        Rpc("Flezha.Service.CliManager.boot_await(60000, [])"),
    ),
    (
        &["boot", "await", "--timeout", "90"],
        Rpc("Flezha.Service.CliManager.boot_await(90000, [])"),
    ),
    (
        &["boot", "await", "--timeout=90"],
        Rpc("Flezha.Service.CliManager.boot_await(90000, [])"),
    ),
    (
        &["boot", "await", "-t", "90"],
        Rpc("Flezha.Service.CliManager.boot_await(90000, [])"),
    ),
    (
        &["boot", "await", "-t=90"],
        Rpc("Flezha.Service.CliManager.boot_await(90000, [])"),
    ),
    // Reading every spelling may not cost the refusals the same reader gives:
    // a word it cannot act on, and a flag with no value, are still refused by
    // name rather than answered with the default wait.
    (
        &["boot", "await", "--nope"],
        Refused("unknown argument '--nope'"),
    ),
    (
        &["boot", "await", "--timeout"],
        Refused("--timeout requires a value"),
    ),
    (
        &["boot", "await", "--timeout", "abc"],
        Refused("invalid --timeout value 'abc'"),
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
    // An option written with NO VALUE is a request the reader cannot answer,
    // and answering it with the default is the silence KI-173 filed. Each
    // trailing flag is paired with the invocation that omits it, so a reader
    // that simply refused everything could not pass either row.
    (
        &["token", "issue", "p", "ns", "viewer"],
        Rpc(
            "Flezha.Service.CliManager.authz_render_token_issue(\"p\", \"ns\", \"viewer\", [ttl: 86400])",
        ),
    ),
    (
        &["token", "issue", "p", "ns", "viewer", "--ttl", "60"],
        Rpc(
            "Flezha.Service.CliManager.authz_render_token_issue(\"p\", \"ns\", \"viewer\", [ttl: 60])",
        ),
    ),
    (
        &["token", "issue", "p", "ns", "viewer", "--ttl"],
        Refused("--ttl requires a value"),
    ),
    (
        &["operative", "scale", "g", "2", "--timeout"],
        Refused("--timeout requires a value"),
    ),
    // The identity readers: a default that is another namespace, or another
    // caller, is worse than a default that is another number.
    (
        &["cluster", "status", "--namespace"],
        Refused("--namespace requires a value"),
    ),
    (&["token", "list", "--token"], Refused("--token requires a value")),
    (
        &["operative", "start", "op", "fac", "--kind"],
        Refused("--kind requires a value"),
    ),
    (
        &["controller", "start", "--mode"],
        Refused("--mode requires a value"),
    ),
    (
        &["audit", "tail", "--duration-ms"],
        Refused("--duration-ms requires a value"),
    ),
    // `flz reset` reads its root and its format through the same scanner, so
    // one case here covers both.
    (
        &["reset", "--dry-run", "--app-data-root"],
        Refused("--app-data-root requires a value"),
    ),
    // The switch spelling may not carry the reader past the refusal: `--json`
    // answered the format before `--output` was ever read.
    (
        &["cluster", "status", "--json", "--output"],
        Refused("--output requires a value"),
    ),
    // A value the operator wrote and left empty IS a value: it is not refused,
    // and `json`'s own rule for an empty value still answers `{}`.
    (
        &["operative", "start", "op", "fac", "-iv", ""],
        Rpc(
            "Flezha.Service.CliManager.operative_start(\"\", \"op\", \"fac\", \"{}\", \"{}\", [])",
        ),
    ),
    // A NUMBER the reader cannot parse is the same request as one nobody
    // wrote, and `.parse().ok()` made them the same answer (KI-176). Each bad
    // value is paired with the good value and the omitted form, so a reader
    // that refused everything fails the triple instead of passing a third of
    // it.
    (
        &["audit", "read", "f"],
        Rpc("Flezha.Service.CliManager.audit_log_render_read(\"f\", \"text\", [])"),
    ),
    (
        &["audit", "read", "f", "--limit", "5"],
        Rpc("Flezha.Service.CliManager.audit_log_render_read(\"f\", \"text\", [limit: 5])"),
    ),
    (
        &["audit", "read", "f", "--limit", "abc"],
        Refused("invalid --limit value 'abc'"),
    ),
    (
        &["audit", "read", "f", "--since", "10", "--until", "20"],
        Rpc(
            "Flezha.Service.CliManager.audit_log_render_read(\"f\", \"text\", [since_us: 10, until_us: 20])",
        ),
    ),
    (
        &["audit", "read", "f", "--since", "abc"],
        Refused("invalid --since value 'abc'"),
    ),
    (
        &["audit", "read", "f", "--until", "abc"],
        Refused("invalid --until value 'abc'"),
    ),
    (
        &["audit", "tail", "--duration-ms", "5000"],
        Rpc("Flezha.Service.CliManager.audit_log_render_tail(\"text\", [duration_ms: 5000])"),
    ),
    (
        &["audit", "tail", "--duration-ms", "abc"],
        Refused("invalid --duration-ms value 'abc'"),
    ),
    (
        &["observe", "tail", "beam_vm", "--duration-ms", "5000"],
        Rpc(
            "Flezha.Service.CliManager.observability_render_tail(\"beam_vm\", \"text\", [duration_ms: 5000])",
        ),
    ),
    (
        &["observe", "tail", "beam_vm", "--duration-ms", "abc"],
        Refused("invalid --duration-ms value 'abc'"),
    ),
    // The two verbs that never parsed at all: they shipped the word the
    // operator typed to the controller as a JSON string.
    (
        &[
            "operative",
            "start",
            "op",
            "fac",
            "--snapshot-interval",
            "5",
            "--partitions",
            "4",
        ],
        Rpc(
            r#"Flezha.Service.CliManager.operative_start("", "op", "fac", "{}", "{\"snapshot_interval\": \"5\", \"partitions\": \"4\"}", [])"#,
        ),
    ),
    (
        &["operative", "start", "op", "fac", "--partitions", "abc"],
        Refused("invalid --partitions value 'abc'"),
    ),
    (
        &[
            "operative",
            "start",
            "op",
            "fac",
            "--snapshot-interval",
            "abc",
        ],
        Refused("invalid --snapshot-interval value 'abc'"),
    ),
    (
        &["operative", "scale", "g", "2"],
        Rpc("Flezha.Service.CliManager.operative_scale(\"\", \"g\", \"2\", \"{}\", [])"),
    ),
    (
        &["operative", "scale", "g", "2", "--timeout", "5000"],
        Rpc(
            r#"Flezha.Service.CliManager.operative_scale("", "g", "2", "{\"timeout_ms\": \"5000\"}", [])"#,
        ),
    ),
    (
        &["operative", "scale", "g", "2", "--timeout", "abc"],
        Refused("invalid --timeout value 'abc'"),
    ),
    // A number written as a POSITIONAL is the same request as one written as
    // an option, and was the one spelling KI-176's reader never saw. Until it
    // did, `flz operative scale g abc --timeout abc` refused the flag and
    // forwarded the word — one verb answering one typo two ways.
    (
        &["operative", "scale", "g", "abc"],
        Refused("invalid <partitions> value 'abc'"),
    ),
    (
        &["operative", "scale", "g", "abc", "--timeout", "abc"],
        Refused("invalid <partitions> value 'abc'"),
    ),
    (
        &["state", "retention", "MyClass", "2"],
        Rpc("Flezha.Service.CliManager.state_retention(\"MyClass\", \"2\", [])"),
    ),
    (
        &["state", "retention", "MyClass", "abc"],
        Refused("invalid <hours> value 'abc'"),
    ),
    // The five readers that already refused a bad number, each in its own
    // words. They answer the shared sentence now, so the rule cannot depend on
    // which verb was typed.
    (
        &["token", "issue", "p", "ns", "viewer", "--ttl", "abc"],
        Refused("invalid --ttl value 'abc'"),
    ),
    // A range rule is the option's own, and only the parse is shared.
    (
        &["token", "issue", "p", "ns", "viewer", "--ttl", "0"],
        Refused("--ttl requires a positive integer"),
    ),
    // The word reading: the registry has always answered an entry with no
    // expiry, and this is the spelling that reaches it.
    (
        &["token", "issue", "p", "ns", "viewer", "--ttl", "never"],
        Rpc("Flezha.Service.CliManager.authz_render_token_issue(\"p\", \"ns\", \"viewer\", [ttl: :never])"),
    ),
    (
        &["token", "issue", "runner@ns", "ns", "runner", "--ttl", "never"],
        Rpc("Flezha.Service.CliManager.authz_render_token_issue(\"runner@ns\", \"ns\", \"runner\", [ttl: :never])"),
    ),
    // The binding an application needs: the role comes before the namespace,
    // which is the order the controller reads them in.
    (
        &["role", "bind", "runner@ns", "app", "ns"],
        Rpc("Flezha.Service.CliManager.authz_render_role_bind(\"runner@ns\", \"app\", \"ns\", [])"),
    ),
    (
        &["controller", "stop", "--timeout-ms", "abc"],
        Refused("invalid --timeout-ms value 'abc'"),
    ),
    (
        &["operative", "rewind", "op", "--to-iteration", "abc"],
        Refused("invalid --to-iteration value 'abc'"),
    ),
    (
        &["reset", "--dry-run", "--probe-timeout-ms", "abc", NO_ROOT],
        Refused("invalid --probe-timeout-ms value 'abc'"),
    ),
    // `flz operative start` carries `--plugins` and `--kind` in the options
    // list, and only when written; the two verbs whose credential once rode a
    // JSON document or nowhere at all now end with the same list as every
    // other.
    (
        &["operative", "start", "op", "fac", "--kind", "workflow", "--plugins", "[]"],
        Rpc(r#"Flezha.Service.CliManager.operative_start("", "op", "fac", "{}", "{}", [plugins: "[]", kind: "workflow"])"#),
    ),
    (
        &["plugin", "upgrade", "p", "/x.fpz", "--token", "t"],
        Rpc(r#"Flezha.Service.CliManager.upgrade_plugin("p", "/x.fpz", [token: "t"])"#),
    ),
    (
        &["operative", "scale", "g", "2", "--token", "t"],
        Rpc(r#"Flezha.Service.CliManager.operative_scale("", "g", "2", "{}", [token: "t"])"#),
    ),
    (
        &["audit", "stats", "--token", "t"],
        Rpc(r#"Flezha.Service.CliManager.audit_log_render_stats("text", [token: "t"])"#),
    ),
    // `flz schedule` — `add`'s flags travel as one keyword list in which an
    // absent flag is absent, `--plugins` stays the text the operator wrote
    // whatever it parses as, and `run` carries the token like its siblings.
    (
        &["schedule", "run", "digest"],
        Rpc("Flezha.Service.CliManager.schedule_run(\"digest\", [])"),
    ),
    (
        &["schedule", "run", "digest", "--token", "t"],
        Rpc("Flezha.Service.CliManager.schedule_run(\"digest\", [token: \"t\"])"),
    ),
    // A quote escaped inside the JSON stays escaped inside the Elixir literal.
    (
        &[
            "schedule",
            "add",
            "digest",
            "--cron",
            "0 8 * * *",
            "--tz",
            "Etc/UTC",
            "--factory",
            "f",
            "-iv",
            r#"{"a":"x\"y"}"#,
        ],
        Rpc(
            r#"Flezha.Service.CliManager.schedule_add("digest", [cron: "0 8 * * *", tz: "Etc/UTC", factory: "f", init_values: "{\"a\":\"x\\\"y\"}"])"#,
        ),
    ),
    (
        &["schedule", "add", "digest", "--cron", "0 8 * * *", "--tz", "Etc/UTC", "--factory", "f"],
        Rpc(r#"Flezha.Service.CliManager.schedule_add("digest", [cron: "0 8 * * *", tz: "Etc/UTC", factory: "f"])"#),
    ),
    (
        &["schedule", "add", "digest", "--cron", "0 8 * * *", "--tz", "Etc/UTC", "--factory", "f", "-n", "prod", "-iv", r#"{"a":1}"#, "--kind", "workflow", "-p", r#"[{"id":"p","provider":"x","subscriptions":["on_output"]}]"#, "--token", "t"],
        Rpc(r#"Flezha.Service.CliManager.schedule_add("digest", [cron: "0 8 * * *", tz: "Etc/UTC", factory: "f", namespace: "prod", init_values: "{\"a\":1}", kind: "workflow", plugins: "[{\"id\":\"p\",\"provider\":\"x\",\"subscriptions\":[\"on_output\"]}]", token: "t"])"#),
    ),
];

/// One invocation per request builder in `cli/src`, each run with `--token t`
/// appended: the credential rides the options list every request ends with,
/// whatever else the verb carries, so no verb can be gated on the controller
/// and token-less on the wire.
const TOKEN_BEARERS: &[&[&str]] = &[
    &["audit", "list"],
    &["audit", "read", "f"],
    &["audit", "tail"],
    &["audit", "stats"],
    &["audit", "rotate"],
    &["auth", "bootstrap", "tok"],
    &["auth", "whoami"],
    &["boot", "status"],
    &["boot", "await"],
    &["cluster", "status"],
    &["controller", "status"],
    &["controller", "mem-top"],
    &["controller", "stop"],
    &["dlq", "list"],
    &["dlq", "show", "a/b/c"],
    &["dlq", "replay", "a/b/c"],
    &["dlq", "drop", "a/b/c"],
    &["namespace", "list"],
    &["namespace", "create", "ns"],
    &["namespace", "delete", "ns"],
    &["namespace", "show", "ns"],
    &["namespace", "set-quota", "ns", "--Xmax_runners=1"],
    &["observe", "sources"],
    &["observe", "sinks"],
    &["observe", "tail", "beam_vm"],
    &["observe", "enable", "s"],
    &["observe", "disable", "s"],
    &["operative", "start", "op", "fac"],
    &["operative", "scale", "g", "2"],
    &["operative", "stop", "op"],
    &["operative", "state", "op"],
    &["operative", "list"],
    &["operative", "info"],
    &["operative", "io_listen", "input", "op"],
    &["operative", "send", "op", "--text", "hi"],
    &["operative", "rewind", "op", "--to-iteration", "1"],
    &["operative", "replay", "a/b/c", "--from", "1"],
    &["plugin", "install", "/x.fpz"],
    &["plugin", "upgrade", "p", "/x.fpz"],
    &["plugin", "uninstall", "p"],
    &["plugin", "info", "p"],
    &["plugin", "enable", "p", "prov"],
    &["plugin", "disable", "p"],
    &["plugin", "help", "prov"],
    &["plugin", "list"],
    &["plugin", "list", "--installed"],
    &["role", "bind", "p", "viewer", "ns"],
    &["role", "unbind", "p"],
    &["role", "list"],
    &["runner", "list"],
    &[
        "schedule",
        "add",
        "d",
        "--cron",
        "* * * * *",
        "--tz",
        "Etc/UTC",
        "--factory",
        "f",
    ],
    &["schedule", "list"],
    &["schedule", "remove", "d"],
    &["schedule", "run", "d"],
    &["state", "get", "a/b"],
    &["state", "list", "C"],
    &["state", "at", "a/b/c", "5"],
    &["state", "rotate", "a/b/c"],
    &["state", "retention", "C", "2"],
    &["state", "delete", "a/b/c"],
    &["state", "outbox", "a/b/c"],
    &["state", "journal", "i"],
    &["state", "inbox", "i"],
    &["token", "issue", "p", "ns", "viewer"],
    &["token", "revoke", "id"],
    &["token", "list"],
];

/// One invocation, thirty-two runs: `operative start` built its options from a
/// `HashMap`, and `RandomState` seeds itself per process, so the key order was
/// a property of the run rather than of what the operator typed (KI-177). One
/// flip is enough to fail this; on the defect it flipped within two runs.
const REPEATED: &[&str] = &[
    "operative",
    "start",
    "op",
    "fac",
    "--snapshot-interval",
    "5",
    "--partitions",
    "4",
];
const REPEATS: usize = 32;

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

    wrong.extend(the_same_invocation_answers_the_same_way_every_run(&cli));
    wrong.extend(every_request_ends_with_the_token_it_was_given(&cli));

    assert!(
        wrong.is_empty(),
        "{} of {} invocations answered the wrong thing:\n{}",
        wrong.len(),
        CASES.len() + 1 + TOKEN_BEARERS.len(),
        wrong.join("\n")
    );
}

/// The same words must build the same request. A row asserting one exact
/// expression would fail about half the time on the defect, and a coin flip
/// proves nothing; repetition is what makes the answer decidable.
fn the_same_invocation_answers_the_same_way_every_run(cli: &Shim) -> Vec<String> {
    let expression = |out: Output| {
        rpc_expression(&String::from_utf8_lossy(&out.stdout))
            .unwrap_or("<no RPC>")
            .to_string()
    };

    let first = expression(run(cli, REPEATED));

    for run_number in 2..=REPEATS {
        let again = expression(run(cli, REPEATED));
        if again != first {
            return vec![format!(
                "flz {}\n     run 1   {}\n     run {}  {}",
                REPEATED.join(" "),
                first,
                run_number,
                again
            )];
        }
    }

    Vec::new()
}

/// Every builder appends the credential to the options list its request ends
/// with; a verb that dropped it, or carried it anywhere else, fails by name.
fn every_request_ends_with_the_token_it_was_given(cli: &Shim) -> Vec<String> {
    let mut wrong = Vec::new();

    for argv in TOKEN_BEARERS {
        let mut with_token: Vec<&str> = argv.to_vec();
        with_token.extend(["--token", "t"]);
        let out = run(cli, &with_token);
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();

        match rpc_expression(&stdout) {
            Some(expr) if expr.ends_with("token: \"t\"])") => {}
            reached => wrong.push(format!(
                "flz {}\n     got  {}\n     want a request ending with `token: \"t\"])`",
                with_token.join(" "),
                reached.unwrap_or("<no RPC>")
            )),
        }
    }

    wrong
}
