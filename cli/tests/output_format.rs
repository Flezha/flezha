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
    /// The door call the verb must reach.
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
        Rpc(r#"Flezha.Service.Cli.call("Controller.cluster_status", "[\"default\"]", [])"#),
    ),
    (
        &["cluster", "status", "--json"],
        Rpc(
            r#"Flezha.Service.Cli.call("Controller.cluster_status", "[\"default\"]", [output: "json"])"#,
        ),
    ),
    (
        &["cluster", "status", "--output", "json"],
        Rpc(
            r#"Flezha.Service.Cli.call("Controller.cluster_status", "[\"default\"]", [output: "json"])"#,
        ),
    ),
    (
        &["cluster", "status", "--output=json"],
        Rpc(
            r#"Flezha.Service.Cli.call("Controller.cluster_status", "[\"default\"]", [output: "json"])"#,
        ),
    ),
    (
        &["cluster", "status", "-o", "json"],
        Rpc(
            r#"Flezha.Service.Cli.call("Controller.cluster_status", "[\"default\"]", [output: "json"])"#,
        ),
    ),
    (
        &["cluster", "status", "-o=json"],
        Rpc(
            r#"Flezha.Service.Cli.call("Controller.cluster_status", "[\"default\"]", [output: "json"])"#,
        ),
    ),
    // A spelling nobody recognises is refused, not rendered as text.
    (
        &["cluster", "status", "--output", "yaml"],
        Refused("unknown output format"),
    ),
    // `flz runner list` and `flz relay list` share `cluster status`'s
    // reader, so the switch has to reach them too.
    (
        &["runner", "list", "--json"],
        Rpc(r#"Flezha.Service.Cli.call("Fleet.runners", "[\"default\"]", [output: "json"])"#),
    ),
    (
        &["relay", "list", "--output", "json"],
        Rpc(r#"Flezha.Service.Cli.call("Relay.list", "[\"default\"]", [output: "json"])"#),
    ),
    // `flz relay listen`: output by default, the window always carried.
    (
        &["relay", "listen", "op"],
        Rpc(
            r#"Flezha.Service.Cli.call("Relay.listen", "[\"default\",\"op\",\"output\",{\"window_ms\":30000}]", [])"#,
        ),
    ),
    (
        &[
            "relay",
            "listen",
            "op",
            "--input",
            "--json",
            "--duration-ms",
            "500",
            "-n",
            "prod",
        ],
        Rpc(
            r#"Flezha.Service.Cli.call("Relay.listen", "[\"prod\",\"op\",\"input\",{\"window_ms\":500}]", [output: "json"])"#,
        ),
    ),
    (
        &["relay", "listen", "op", "--duration", "5"],
        Refused("unknown option '--duration'"),
    ),
    (
        &["relay", "listen", "op", "extra"],
        Refused("unexpected argument 'extra'"),
    ),
    // `--await` rides the options list only when written.
    (
        &["relay", "send", "op", "--text", "hi"],
        Rpc(r#"Flezha.Service.Cli.call("Relay.send", "[\"default\",\"op\",\"hi\",\"\",{}]", [])"#),
    ),
    (
        &["relay", "send", "op", "--text", "hi", "--await", "2000"],
        Rpc(
            r#"Flezha.Service.Cli.call("Relay.send", "[\"default\",\"op\",\"hi\",\"\",{}]", [await_ms: 2000])"#,
        ),
    ),
    // `flz audit` — the `--json` dialect.
    (
        &["audit", "list"],
        Rpc(r#"Flezha.Service.Cli.call("AuditLog.list_files", "[]", [])"#),
    ),
    (
        &["audit", "list", "--json"],
        Rpc(r#"Flezha.Service.Cli.call("AuditLog.list_files", "[]", [output: "json"])"#),
    ),
    (
        &["audit", "list", "--output", "json"],
        Rpc(r#"Flezha.Service.Cli.call("AuditLog.list_files", "[]", [output: "json"])"#),
    ),
    (
        &["audit", "tail", "--json"],
        Rpc(
            r#"Flezha.Service.Cli.call("AuditLog.tail", "[]", [duration_ms: 30000, output: "json"])"#,
        ),
    ),
    (
        &["audit", "tail", "--output", "json"],
        Rpc(
            r#"Flezha.Service.Cli.call("AuditLog.tail", "[]", [duration_ms: 30000, output: "json"])"#,
        ),
    ),
    (
        &["audit", "stats", "-o", "json"],
        Rpc(r#"Flezha.Service.Cli.call("AuditLog.stats", "[]", [output: "json"])"#),
    ),
    // `flz controller` — the predicate that was inlined twice.
    (
        &["controller", "status", "--json"],
        Rpc(r#"Flezha.Service.Cli.call("Controller.status", "[]", [output: "json"])"#),
    ),
    (
        &["controller", "status", "--output", "json"],
        Rpc(r#"Flezha.Service.Cli.call("Controller.status", "[]", [output: "json"])"#),
    ),
    (
        &["controller", "mem-top", "--json"],
        Rpc(r#"Flezha.Service.Cli.call("Controller.mem_top", "[]", [output: "json"])"#),
    ),
    (
        &["controller", "mem-top", "--output", "json"],
        Rpc(r#"Flezha.Service.Cli.call("Controller.mem_top", "[]", [output: "json"])"#),
    ),
    // `flz observe` — the second byte-identical copy of that predicate.
    (
        &["observe", "sources", "--json"],
        Rpc(r#"Flezha.Service.Cli.call("Observability.list_sources", "[]", [output: "json"])"#),
    ),
    (
        &["observe", "sources", "--output", "json"],
        Rpc(r#"Flezha.Service.Cli.call("Observability.list_sources", "[]", [output: "json"])"#),
    ),
    (
        &["observe", "tail", "beam_vm", "--output", "json"],
        Rpc(
            r#"Flezha.Service.Cli.call("Observability.tail", "[\"beam_vm\"]", [duration_ms: 30000, output: "json"])"#,
        ),
    ),
    // `flz namespace list` and `flz state` — the `--output` dialect again.
    (
        &["namespace", "list", "--json"],
        Rpc(r#"Flezha.Service.Cli.call("Relay.list_namespaces", "[]", [output: "json"])"#),
    ),
    // An acting verb takes the format too: it rides the list, and only the
    // form of a failure changes.
    (
        &["namespace", "delete", "ghost", "--json"],
        Rpc(r#"Flezha.Service.Cli.call("Namespace.delete", "[\"ghost\"]", [output: "json"])"#),
    ),
    (
        &[
            "schedule",
            "add",
            "s1",
            "--cron",
            "* * * * *",
            "--tz",
            "UTC",
            "--factory",
            "f",
            "--json",
        ],
        Rpc(
            r#"Flezha.Service.Cli.call("Schedule.add", "[{\"cron\":\"* * * * *\",\"factory_id\":\"f\",\"name\":\"s1\",\"timezone\":\"UTC\"}]", [output: "json"])"#,
        ),
    ),
    (
        &["namespace", "list", "--output", "json"],
        Rpc(r#"Flezha.Service.Cli.call("Relay.list_namespaces", "[]", [output: "json"])"#),
    ),
    (
        &["state", "list", "MarketRelay", "--json"],
        Rpc(r#"Flezha.Service.Cli.call("Steps.cells", "[\"MarketRelay\"]", [output: "json"])"#),
    ),
    (
        &["state", "list", "MarketRelay", "--output", "json"],
        Rpc(r#"Flezha.Service.Cli.call("Steps.cells", "[\"MarketRelay\"]", [output: "json"])"#),
    ),
    // `flz plugin list` — the format flag renders; `--installed` and
    // `--providers` choose the collection. Neither may do the other's job.
    (
        &["plugin", "list"],
        Rpc(r#"Flezha.Service.Cli.call("PluginManager.enabled", "[\"default\"]", [])"#),
    ),
    (
        &["plugin", "list", "--namespace", "n", "--json"],
        Rpc(r#"Flezha.Service.Cli.call("PluginManager.enabled", "[\"n\"]", [output: "json"])"#),
    ),
    (
        &["plugin", "list", "--providers"],
        Rpc(r#"Flezha.Service.Cli.call("Fleet.providers", "[]", [])"#),
    ),
    (
        &["plugin", "list", "--installed"],
        Rpc(r#"Flezha.Service.Cli.call("Fleet.installed_plugins", "[]", [])"#),
    ),
    (
        &["plugin", "list", "--installed", "--json"],
        Rpc(r#"Flezha.Service.Cli.call("Fleet.installed_plugins", "[]", [output: "json"])"#),
    ),
    (
        &["plugin", "list", "--installed", "--output", "json"],
        Rpc(r#"Flezha.Service.Cli.call("Fleet.installed_plugins", "[]", [output: "json"])"#),
    ),
    // `flz plugin enable` passes its options on rather than rendering them,
    // and reads them through the second scanner. An option's four spellings
    // are one option there too: a short flag written with `=` is what the
    // caller typed.
    (
        &["plugin", "enable", "p", "prov", "--output=json"],
        Rpc(
            r#"Flezha.Service.Cli.call("PluginManager.enable", "[\"p\",{\"namespace\":\"default\",\"provider\":\"prov\"}]", [output: "json"])"#,
        ),
    ),
    (
        &["plugin", "enable", "p", "prov", "-o=json"],
        Rpc(
            r#"Flezha.Service.Cli.call("PluginManager.enable", "[\"p\",{\"namespace\":\"default\",\"provider\":\"prov\"}]", [output: "json"])"#,
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
            r#"Flezha.Service.Cli.call("PluginManager.enable", "[\"p\",{\"namespace\":\"default\",\"opts\":{\"hosts\":\"kafka:9092\",\"topic\":\"my_topic\"},\"provider\":\"prov\"}]", [])"#,
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
            r#"Flezha.Service.Cli.call("PluginManager.enable", "[\"p\",{\"namespace\":\"default\",\"opts\":{\"hosts\":\"kafka:9092\",\"topic\":\"my_topic\"},\"provider\":\"prov\"}]", [])"#,
        ),
    ),
    // The provider registry answers as JSON like every other operation, and
    // the switch still chooses which collection is listed.
    (
        &["plugin", "list", "--providers", "--json"],
        Rpc(r#"Flezha.Service.Cli.call("Fleet.providers", "[]", [output: "json"])"#),
    ),
    (
        &["plugin", "list", "--providers", "--output", "json"],
        Rpc(r#"Flezha.Service.Cli.call("Fleet.providers", "[]", [output: "json"])"#),
    ),
    // The verbs that name a plugin carry its plain options on as one
    // document; `bind` and `update` carry its settings beside them.
    (
        &[
            "plugin",
            "bind",
            "trades",
            "--relay_id",
            "a",
            "--hook=on_output",
            "--channel",
            "ticks",
        ],
        Rpc(
            r#"Flezha.Service.Cli.call("PluginManager.bind", "[\"trades\",{\"channel\":\"ticks\",\"hooks\":[{\"name\":\"on_output\",\"opts\":{}}],\"namespace\":\"default\",\"relay_id\":\"a\"}]", [])"#,
        ),
    ),
    (
        &["plugin", "unbind", "trades", "--relay_id", "a", "-n", "n"],
        Rpc(
            r#"Flezha.Service.Cli.call("PluginManager.unbind", "[\"trades\",{\"namespace\":\"n\",\"relay_id\":\"a\"}]", [])"#,
        ),
    ),
    (
        &["plugin", "update", "trades", "Xsubscription_items=[\"a\"]"],
        Rpc(
            r#"Flezha.Service.Cli.call("PluginManager.update", "[\"trades\",{\"namespace\":\"default\",\"opts\":{\"subscription_items\":[\"a\"]}}]", [])"#,
        ),
    ),
    (
        &["plugin", "disable", "trades"],
        Rpc(
            r#"Flezha.Service.Cli.call("PluginManager.disable", "[\"trades\",{\"namespace\":\"default\"}]", [])"#,
        ),
    ),
    (
        &["plugin", "enable", "trades", "relay_websocket", "--single"],
        Rpc(
            r#"Flezha.Service.Cli.call("PluginManager.enable", "[\"trades\",{\"namespace\":\"default\",\"provider\":\"relay_websocket\",\"single\":true}]", [])"#,
        ),
    ),
    // `flz boot await` takes one option, and it is one option in all four of
    // its spellings (KI-169). The wait is 90 s so a reader that quietly fell
    // back to the 60 s default fails the row instead of passing it by
    // coincidence.
    (
        &["boot", "await"],
        Rpc(r#"Flezha.Service.Cli.call("Controller.boot_await", "[60000]", [])"#),
    ),
    (
        &["boot", "await", "--timeout", "90"],
        Rpc(r#"Flezha.Service.Cli.call("Controller.boot_await", "[90000]", [])"#),
    ),
    (
        &["boot", "await", "--timeout=90"],
        Rpc(r#"Flezha.Service.Cli.call("Controller.boot_await", "[90000]", [])"#),
    ),
    (
        &["boot", "await", "-t", "90"],
        Rpc(r#"Flezha.Service.Cli.call("Controller.boot_await", "[90000]", [])"#),
    ),
    (
        &["boot", "await", "-t=90"],
        Rpc(r#"Flezha.Service.Cli.call("Controller.boot_await", "[90000]", [])"#),
    ),
    // Reading every spelling may not cost the refusals the same reader gives:
    // a word it cannot act on, and a flag with no value, are still refused by
    // name rather than answered with the default wait.
    (
        &["boot", "await", "--nope"],
        Refused("unknown option '--nope'"),
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
            r#"Flezha.Service.Cli.call("Namespace.issue_token", "[\"p\",\"ns\",\"viewer\",{}]", [])"#,
        ),
    ),
    (
        &["token", "issue", "p", "ns", "viewer", "--ttl", "60"],
        Rpc(
            r#"Flezha.Service.Cli.call("Namespace.issue_token", "[\"p\",\"ns\",\"viewer\",{\"ttl\":60}]", [])"#,
        ),
    ),
    (
        &["token", "issue", "p", "ns", "viewer", "--ttl"],
        Refused("--ttl requires a value"),
    ),
    (
        &["relay", "scale", "g", "2", "--timeout"],
        Refused("--timeout requires a value"),
    ),
    // The identity readers: a default that is another namespace, or another
    // caller, is worse than a default that is another number.
    (
        &["cluster", "status", "--namespace"],
        Refused("--namespace requires a value"),
    ),
    (
        &["token", "list", "--token"],
        Refused("--token requires a value"),
    ),
    (
        &["relay", "start", "fac", "--id"],
        Refused("--id requires a value"),
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
    // A value the caller wrote and left empty IS a value: it is not refused,
    // and `json`'s own rule for an empty value still answers `{}`.
    (
        &["relay", "start", "fac", "--init-values", ""],
        Rpc(
            r#"Flezha.Service.Cli.call("Relay.start", "[\"default\",{\"factory_id\":\"fac\",\"init_values\":{}}]", [])"#,
        ),
    ),
    // A NUMBER the reader cannot parse is the same request as one nobody
    // wrote, and `.parse().ok()` made them the same answer (KI-176). Each bad
    // value is paired with the good value and the omitted form, so a reader
    // that refused everything fails the triple instead of passing a third of
    // it.
    (
        &["audit", "read", "f"],
        Rpc(r#"Flezha.Service.Cli.call("AuditLog.read", "[{\"file\":\"f\"}]", [])"#),
    ),
    (
        &["audit", "read", "f", "--limit", "5"],
        Rpc(r#"Flezha.Service.Cli.call("AuditLog.read", "[{\"file\":\"f\",\"limit\":5}]", [])"#),
    ),
    (
        &["audit", "read", "f", "--limit", "abc"],
        Refused("invalid --limit value 'abc'"),
    ),
    (
        &["audit", "read", "f", "--since", "10", "--until", "20"],
        Rpc(
            r#"Flezha.Service.Cli.call("AuditLog.read", "[{\"file\":\"f\",\"since_us\":10,\"until_us\":20}]", [])"#,
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
        Rpc(r#"Flezha.Service.Cli.call("AuditLog.tail", "[]", [duration_ms: 5000])"#),
    ),
    (
        &["audit", "tail", "--duration-ms", "abc"],
        Refused("invalid --duration-ms value 'abc'"),
    ),
    (
        &["observe", "tail", "beam_vm", "--duration-ms", "5000"],
        Rpc(
            r#"Flezha.Service.Cli.call("Observability.tail", "[\"beam_vm\"]", [duration_ms: 5000])"#,
        ),
    ),
    (
        &["observe", "tail", "beam_vm", "--duration-ms", "abc"],
        Refused("invalid --duration-ms value 'abc'"),
    ),
    // The two verbs that never parsed at all: they shipped the word the
    // caller typed to the controller as a JSON string.
    (
        &["relay", "start", "fac", "--id", "op", "--partitions", "4"],
        Rpc(
            r#"Flezha.Service.Cli.call("Relay.start", "[\"default\",{\"factory_id\":\"fac\",\"id\":\"op\",\"partitions\":4}]", [])"#,
        ),
    ),
    (
        &["relay", "start", "fac", "--partitions", "abc"],
        Refused("invalid --partitions value 'abc'"),
    ),
    // The snapshot interval is the controller's environment, not a start's.
    (
        &["relay", "start", "echo", "--snapshot-interval", "5000"],
        Refused("unknown option '--snapshot-interval'"),
    ),
    (
        &["relay", "scale", "g", "2"],
        Rpc(r#"Flezha.Service.Cli.call("Steps.rescale", "[\"default\",\"g\",2,{}]", [])"#),
    ),
    (
        &["relay", "scale", "g", "2", "--timeout", "5000"],
        Rpc(
            r#"Flezha.Service.Cli.call("Steps.rescale", "[\"default\",\"g\",2,{\"quiesce_timeout_ms\":5000}]", [])"#,
        ),
    ),
    (
        &["relay", "scale", "g", "2", "--timeout", "abc"],
        Refused("invalid --timeout value 'abc'"),
    ),
    // A number written as a POSITIONAL is the same request as one written as
    // an option, and was the one spelling KI-176's reader never saw. Until it
    // did, `flz relay scale g abc --timeout abc` refused the flag and
    // forwarded the word — one verb answering one typo two ways.
    (
        &["relay", "scale", "g", "abc"],
        Refused("invalid <partitions> value 'abc'"),
    ),
    (
        &["relay", "scale", "g", "abc", "--timeout", "abc"],
        Refused("invalid <partitions> value 'abc'"),
    ),
    (
        &["state", "retention", "MyClass", "2"],
        Rpc(r#"Flezha.Service.Cli.call("Steps.set_retention", "[\"MyClass\",2]", [])"#),
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
        Refused("(authz.invalid_ttl)"),
    ),
    // The word reading: the registry has always answered an entry with no
    // expiry, and this is the spelling that reaches it.
    (
        &["token", "issue", "p", "ns", "viewer", "--ttl", "never"],
        Rpc(
            r#"Flezha.Service.Cli.call("Namespace.issue_token", "[\"p\",\"ns\",\"viewer\",{\"ttl\":\"never\"}]", [])"#,
        ),
    ),
    (
        &[
            "token",
            "issue",
            "runner@ns",
            "ns",
            "runner",
            "--ttl",
            "never",
        ],
        Rpc(
            r#"Flezha.Service.Cli.call("Namespace.issue_token", "[\"runner@ns\",\"ns\",\"runner\",{\"ttl\":\"never\"}]", [])"#,
        ),
    ),
    // The binding an application needs: the role comes before the namespace,
    // which is the order the controller reads them in.
    (
        &["role", "bind", "runner@ns", "app", "ns"],
        Rpc(
            r#"Flezha.Service.Cli.call("Namespace.bind_role", "[\"runner@ns\",\"app\",\"ns\"]", [])"#,
        ),
    ),
    (
        &["role", "bind", "runner@ns", "stopper", "ns"],
        Rpc(
            r#"Flezha.Service.Cli.call("Namespace.bind_role", "[\"runner@ns\",\"stopper\",\"ns\"]", [])"#,
        ),
    ),
    // The role an unbind names is optional, and the terminal always sends the
    // slot, so the controller reads principal, role and options in fixed
    // positions.
    (
        &["role", "unbind", "runner@ns", "spawner"],
        Rpc(
            r#"Flezha.Service.Cli.call("Namespace.unbind_role", "[\"runner@ns\",\"spawner\"]", [])"#,
        ),
    ),
    (
        &["role", "unbind", "runner@ns"],
        Rpc(r#"Flezha.Service.Cli.call("Namespace.unbind_role", "[\"runner@ns\",null]", [])"#),
    ),
    (
        &["controller", "stop", "--timeout-ms", "abc"],
        Refused("invalid --timeout-ms value 'abc'"),
    ),
    (
        &["relay", "rewind", "op", "--to-iteration", "abc"],
        Refused("invalid --to-iteration value 'abc'"),
    ),
    (
        &["reset", "--dry-run", "--probe-timeout-ms", "abc", NO_ROOT],
        Refused("invalid --probe-timeout-ms value 'abc'"),
    ),
    // `flz relay start` carries `--id` and `--plugins` in the options
    // list, and only when written; the two verbs whose credential once rode a
    // JSON document or nowhere at all now end with the same list as every
    // other.
    (
        &["relay", "start", "fac", "--id", "op", "--plugins", "[]"],
        Rpc(
            r#"Flezha.Service.Cli.call("Relay.start", "[\"default\",{\"factory_id\":\"fac\",\"id\":\"op\",\"plugins\":[]}]", [])"#,
        ),
    ),
    // A start without an id carries no id at all: the controller generates it.
    (
        &["relay", "start", "echo"],
        Rpc(
            r#"Flezha.Service.Cli.call("Relay.start", "[\"default\",{\"factory_id\":\"echo\"}]", [])"#,
        ),
    ),
    (
        &["relay", "start", "echo", "-i", "my_echo"],
        Rpc(
            r#"Flezha.Service.Cli.call("Relay.start", "[\"default\",{\"factory_id\":\"echo\",\"id\":\"my_echo\"}]", [])"#,
        ),
    ),
    (
        &["relay", "start", "echo", "-o", "json"],
        Rpc(
            r#"Flezha.Service.Cli.call("Relay.start", "[\"default\",{\"factory_id\":\"echo\"}]", [output: "json"])"#,
        ),
    ),
    (
        &[
            "relay",
            "start",
            "echo",
            "--json",
            "--init-values",
            r#"{"a":1}"#,
        ],
        Rpc(
            r#"Flezha.Service.Cli.call("Relay.start", "[\"default\",{\"factory_id\":\"echo\",\"init_values\":{\"a\":1}}]", [output: "json"])"#,
        ),
    ),
    // A first message rides the options list as text, with the entry channel
    // it names; a channel with no message names nothing.
    (
        &[
            "relay",
            "start",
            "order_executor",
            "--text",
            r#"{"symbol":"EUR/USD","quantity":10}"#,
            "--channel",
            "order",
        ],
        Rpc(
            r#"Flezha.Service.Cli.call("Relay.start", "[\"default\",{\"channel\":\"order\",\"factory_id\":\"order_executor\",\"text\":\"{\\\"symbol\\\":\\\"EUR/USD\\\",\\\"quantity\\\":10}\"}]", [])"#,
        ),
    ),
    (
        &["relay", "start", "echo", "-t", "hello"],
        Rpc(
            r#"Flezha.Service.Cli.call("Relay.start", "[\"default\",{\"factory_id\":\"echo\",\"text\":\"hello\"}]", [])"#,
        ),
    ),
    (
        &["relay", "start", "echo", "--channel", "order"],
        Refused("(relay.channel_without_text)"),
    ),
    (
        &["relay", "start", "my_echo", "echo"],
        Refused("unexpected argument 'echo'"),
    ),
    // The spellings the rule retired are refused, not dropped: a start that
    // ignored them would run without the values the caller wrote.
    (
        &["relay", "start", "echo", "-iv", r#"{"a":1}"#],
        Refused("unknown option '-iv'"),
    ),
    (
        &["relay", "start", "echo", "--init_values", r#"{"a":1}"#],
        Refused("unknown option '--init_values'"),
    ),
    // The kind is the factory's, announced by its runner: a written one is an
    // option neither verb declares.
    (
        &["relay", "start", "echo", "--kind", "flow"],
        Refused("unknown option '--kind'"),
    ),
    (
        &[
            "schedule",
            "add",
            "n",
            "--cron",
            "* * * * *",
            "--tz",
            "UTC",
            "--factory",
            "f",
            "--kind",
            "steps",
        ],
        Refused("unknown option '--kind'"),
    ),
    (
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
            "-iv",
            "{}",
        ],
        Refused("unknown option '-iv'"),
    ),
    (
        &["relay", "scale", "g", "2", "--token", "t"],
        Rpc(
            r#"Flezha.Service.Cli.call("Steps.rescale", "[\"default\",\"g\",2,{}]", [token: "t"])"#,
        ),
    ),
    (
        &["audit", "stats", "--token", "t"],
        Rpc(r#"Flezha.Service.Cli.call("AuditLog.stats", "[]", [token: "t"])"#),
    ),
    // `flz schedule` — `add`'s flags travel as one keyword list in which an
    // absent flag is absent, `--plugins` stays the text the caller wrote
    // whatever it parses as, and `run` carries the token like its siblings.
    (
        &["schedule", "run", "digest"],
        Rpc(r#"Flezha.Service.Cli.call("Schedule.run", "[\"digest\"]", [])"#),
    ),
    (
        &["schedule", "run", "digest", "--token", "t"],
        Rpc(r#"Flezha.Service.Cli.call("Schedule.run", "[\"digest\"]", [token: "t"])"#),
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
            "--init-values",
            r#"{"a":"x\"y"}"#,
        ],
        Rpc(
            r#"Flezha.Service.Cli.call("Schedule.add", "[{\"cron\":\"0 8 * * *\",\"factory_id\":\"f\",\"init_values\":{\"a\":\"x\\\"y\"},\"name\":\"digest\",\"timezone\":\"Etc/UTC\"}]", [])"#,
        ),
    ),
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
        ],
        Rpc(
            r#"Flezha.Service.Cli.call("Schedule.add", "[{\"cron\":\"0 8 * * *\",\"factory_id\":\"f\",\"name\":\"digest\",\"timezone\":\"Etc/UTC\"}]", [])"#,
        ),
    ),
    (
        &[
            "schedule",
            "add",
            "nightly",
            "--cron",
            "0 2 * * *",
            "--tz",
            "UTC",
            "--factory",
            "report",
            "--text",
            "hello",
            "--channel",
            "requests",
        ],
        Rpc(
            r#"Flezha.Service.Cli.call("Schedule.add", "[{\"channel\":\"requests\",\"cron\":\"0 2 * * *\",\"factory_id\":\"report\",\"name\":\"nightly\",\"text\":\"hello\",\"timezone\":\"UTC\"}]", [])"#,
        ),
    ),
    (
        &[
            "schedule",
            "add",
            "nightly",
            "--cron",
            "0 2 * * *",
            "--tz",
            "UTC",
            "--factory",
            "report",
            "--channel",
            "requests",
        ],
        Refused("(relay.channel_without_text)"),
    ),
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
            "-n",
            "prod",
            "--init-values",
            r#"{"a":1}"#,
            "-p",
            r#"[{"name":"p","provider":"x","hooks":["on_output"]}]"#,
            "--token",
            "t",
        ],
        Rpc(
            r#"Flezha.Service.Cli.call("Schedule.add", "[{\"cron\":\"0 8 * * *\",\"factory_id\":\"f\",\"init_values\":{\"a\":1},\"name\":\"digest\",\"namespace\":\"prod\",\"plugins\":\"[{\\\"hooks\\\":[\\\"on_output\\\"],\\\"name\\\":\\\"p\\\",\\\"provider\\\":\\\"x\\\"}]\",\"timezone\":\"Etc/UTC\"}]", [token: "t"])"#,
        ),
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
    &["relay", "start", "fac"],
    &["relay", "scale", "g", "2"],
    &["relay", "stop", "op"],
    &["relay", "state", "op"],
    &["relay", "list"],
    &["relay", "info"],
    &["relay", "listen", "op"],
    &["relay", "send", "op", "--text", "hi", "--await", "500"],
    &["relay", "send", "op", "--text", "hi"],
    &["relay", "rewind", "op", "--to-iteration", "1"],
    &["relay", "replay", "a/b/c", "--from", "1"],
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

/// One invocation, thirty-two runs: `relay start` built its options from a
/// `HashMap`, and `RandomState` seeds itself per process, so the key order was
/// a property of the run rather than of what the caller typed (KI-177). One
/// flip is enough to fail this; on the defect it flipped within two runs.
const REPEATED: &[&str] = &["relay", "start", "fac", "--id", "op", "--partitions", "4"];
const REPEATS: usize = 32;

/// The one door call the run reached, as the shim echoed it.
fn rpc_expression(stdout: &str) -> Option<&str> {
    stdout
        .lines()
        .find(|l| l.starts_with("Flezha.Service.Cli.call("))
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
