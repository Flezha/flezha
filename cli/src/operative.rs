use crate::args::{self, OptSpec, Positional, Reading, JSON_SWITCH, NAMESPACE, OUTPUT};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, text, Opt};
use crate::settings::serialize_flags;

const OPERATIVE_ID: &[Positional] = &[Positional::required("<operative_id>")];

const KIND: OptSpec = OptSpec::text("--kind", "--kind", "<stream|workflow>");
const INIT_VALUES: OptSpec = OptSpec::text("--init_values", "-iv", "<json>");
const PLUGINS: OptSpec = OptSpec::text("--plugins", "--plugins", "<json-array>");
const PARTITIONS: OptSpec = OptSpec::number("--partitions", "--partitions", "<N>");
const SNAPSHOT_INTERVAL: OptSpec = OptSpec::number("--snapshot-interval", "-si", "<N>");
const TIMEOUT: OptSpec = OptSpec::number("--timeout", "-t", "<ms>");
const PAYLOAD: OptSpec = OptSpec::text("--text", "-t", "<payload>").required();
const CHANNEL: OptSpec = OptSpec::text("--channel", "-c", "<ch>");
const TO_ITERATION: OptSpec = OptSpec::number("--to-iteration", "-i", "<N>").required();
const FROM: OptSpec = OptSpec::text("--from", "-f", "<seq|timestamp>").required();

pub(crate) const TABLE: Command = Command {
    path: "flz operative",
    help: help::OPERATIVE,
    verbs: &[
        Verb::new("start", handle_start)
            .taking(&[
                Positional::required("<operative_id>"),
                Positional::required("<factory_id>"),
            ])
            .reading(&[
                KIND,
                NAMESPACE,
                INIT_VALUES,
                PLUGINS,
                PARTITIONS,
                SNAPSHOT_INTERVAL,
            ]),
        Verb::new("scale", handle_scale)
            .taking(&[
                Positional::required("<group>"),
                Positional::number("<partitions>"),
            ])
            .reading(&[NAMESPACE, TIMEOUT]),
        Verb::new("stop", handle_stop)
            .taking(OPERATIVE_ID)
            .reading(&[NAMESPACE]),
        Verb::new("state", handle_state)
            .taking(OPERATIVE_ID)
            .reading(&[NAMESPACE]),
        Verb::new("list", |read| {
            rpc::scoped_read(read, "list_operatives", Some("operatives"))
        })
        .reading(&[NAMESPACE, OUTPUT, JSON_SWITCH]),
        Verb::new("info", |read| {
            rpc::scoped_read(read, "operative_info", Some("operative information"))
        })
        .reading(&[NAMESPACE, OUTPUT, JSON_SWITCH]),
        Verb::new("io_listen", handle_io_listen)
            .taking(&[
                Positional::choice("input|output", "IO type", &["input", "output"]),
                Positional::required("<operative_id>"),
            ])
            .reading(&[NAMESPACE]),
        Verb::new("send", handle_send)
            .taking(OPERATIVE_ID)
            .reading(&[PAYLOAD, CHANNEL, NAMESPACE]),
        Verb::new("rewind", handle_rewind)
            .taking(OPERATIVE_ID)
            .reading(&[TO_ITERATION, NAMESPACE]),
        Verb::new("replay", handle_replay)
            .taking(&[Positional::required("<operative/procedure/key>")])
            .reading(&[FROM, NAMESPACE]),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Missing::Refuse,
    unknown_prints_help: false,
};

/// The JSON opts object `operative_start` takes: the flags that are neither
/// the operative's identity nor its init values. Ordered, because the same
/// invocation has to build the same request every run.
fn start_opts(read: &Reading) -> String {
    let mut opts: Vec<(String, String)> = Vec::new();

    for (spec, name) in [
        (SNAPSHOT_INTERVAL, "snapshot_interval"),
        (PARTITIONS, "partitions"),
    ] {
        if let Some(count) = read.opt(spec.long) {
            opts.push((name.to_string(), count.to_string()));
        }
    }

    serialize_flags(&opts)
}

/// `--plugins` and `--kind` ride the options list, and only when written: an
/// absent flag is absent from the request and the controller applies its
/// default.
fn handle_start(read: &Reading) {
    let mut opts: Vec<Opt> = Vec::new();

    if let Some(plugins) = read.opt(PLUGINS.long) {
        opts.push(("plugins", text(&plugins_json_or_refuse(plugins))));
    }
    if let Some(kind) = read.opt(KIND.long) {
        opts.push(("kind", text(kind)));
    }

    println!("Starting operative...");

    rpc::run(
        "operative_start",
        &[
            text(read.namespace()),
            text(read.pos("<operative_id>")),
            text(read.pos("<factory_id>")),
            text(read.json(INIT_VALUES.long)),
            text(&start_opts(read)),
        ],
        &opts,
        read.token(),
        rpc::REFUSED,
    );
}

/// `flz operative scale <group> <partitions>` — rescale a partition group.
/// The group is quiesced, every Entity whose key group changes owner is moved,
/// and the new member set is started; a refusal changes nothing.
fn handle_scale(read: &Reading) {
    let group_id = read.pos("<group>");
    let partitions = read.pos("<partitions>");

    let mut opts: Vec<(String, String)> = Vec::new();
    if let Some(timeout_ms) = read.opt(TIMEOUT.long) {
        opts.push(("timeout_ms".to_string(), timeout_ms.to_string()));
    }

    println!("Scaling operative group {} to {}...", group_id, partitions);

    rpc::run(
        "operative_scale",
        &[
            text(read.namespace()),
            text(group_id),
            text(partitions),
            text(&serialize_flags(&opts)),
        ],
        &[],
        read.token(),
        rpc::REFUSED,
    );
}

fn handle_stop(read: &Reading) {
    println!("Stopping operative...");

    // operative_stop halts non-zero on error verdicts (not_found / refused).
    rpc::run(
        "operative_stop",
        &[text(read.namespace()), text(read.pos(OPERATIVE_ID[0].name))],
        &[],
        read.token(),
        rpc::UNKNOWN_STATUS.diagnosing("Failed to stop operative"),
    );

    println!("Operative stopped successfully.");
}

/// `flz operative send <operative_id> --text <payload> [--channel <ch>]` —
/// inject a payload into an operative's input stream (the SDK-facing
/// `send_io` route). Blocks for the confirming verdict so a parked or absent
/// target is reported rather than silently dropped.
fn handle_send(read: &Reading) {
    // operative_send halts non-zero on error verdicts (not_found / not_ready / refused).
    rpc::run(
        "operative_send",
        &[
            text(read.namespace()),
            text(read.pos(OPERATIVE_ID[0].name)),
            text(read.opt(PAYLOAD.long).unwrap_or_default()),
            text(read.opt(CHANNEL.long).unwrap_or_default()),
        ],
        &[],
        read.token(),
        rpc::QUIET.diagnosing("Failed to send input to operative"),
    );
}

/// `flz operative rewind <instance> --to-iteration N` — take a workflow loop back
/// to a committed iteration: the ones above it are compensated in reverse, state
/// returns to that boundary, and the epoch is bumped so the re-run's output
/// supersedes the unwound run's at a guarded sink.
fn handle_rewind(read: &Reading) {
    // operative_rewind halts non-zero on error verdicts (not_found / refused).
    rpc::run(
        "operative_rewind",
        &[
            text(read.namespace()),
            text(read.pos(OPERATIVE_ID[0].name)),
            read.opt(TO_ITERATION.long).unwrap_or_default().to_string(),
        ],
        &[],
        read.token(),
        rpc::UNKNOWN_STATUS.diagnosing("Failed to rewind operative"),
    );
}

/// `flz operative replay <operative/procedure/key> --from <seq|ts>` — re-run a
/// Entity's retained inputs from a point in the past through the current code,
/// republishing under a bumped epoch so a guarded sink overwrites the stale rows
/// by natural key. "Deploy a fix, heal the recent past."
///
/// The same transaction a late input triggers automatically; here the operator
/// picks the baseline instead of a late event.
fn handle_replay(read: &Reading) {
    // operative_replay halts non-zero on error verdicts (not_found / refused).
    rpc::run(
        "operative_replay",
        &[
            text(read.namespace()),
            text(read.pos("<operative/procedure/key>")),
            text(read.opt(FROM.long).unwrap_or_default()),
        ],
        &[],
        read.token(),
        rpc::UNKNOWN_STATUS.diagnosing("Failed to replay entity"),
    );
}

fn handle_state(read: &Reading) {
    // entity_state halts non-zero on error verdicts (not_found / refused).
    rpc::run(
        "entity_state",
        &[text(read.namespace()), text(read.pos(OPERATIVE_ID[0].name))],
        &[],
        read.token(),
        rpc::UNKNOWN_STATUS.diagnosing("Failed to get operative state"),
    );
}

fn handle_io_listen(read: &Reading) {
    let io_type = read.pos("input|output");
    let operative_id = read.pos("<operative_id>");

    println!(
        "Starting IO listener for operative '{}' ({} mode)...",
        operative_id, io_type
    );
    println!("Press Ctrl+C to stop listening");

    rpc::run(
        "operative_listen",
        &[text(read.namespace()), text(operative_id), text(io_type)],
        &[],
        read.token(),
        rpc::REFUSED,
    );
}

/// Validate the `--plugins` JSON blob at the CLI layer so parse errors are
/// surfaced with a local exit code and message, rather than round-tripping
/// an opaque RPC failure through the controller.
///
/// Accepts:
/// * `"{}"` / empty string — treated as "no plugins attached"; returns `"[]"`.
/// * A JSON array of plugin objects. Each object MUST have `id`, `provider`
///   (non-empty strings) and a non-empty `subscriptions` array. Deeper
///   validation (provider registration lookup, subscription-opts shape)
///   happens on the Elixir side where the registry is authoritative.
pub(crate) fn parse_plugins_json(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "{}" {
        return Ok("[]".to_string());
    }

    let value: serde_json::Value =
        serde_json::from_str(trimmed).map_err(|e| format!("not valid JSON — {}", e))?;

    let items = value
        .as_array()
        .ok_or_else(|| "expected a JSON array of plugin objects".to_string())?;

    for (idx, item) in items.iter().enumerate() {
        let obj = item
            .as_object()
            .ok_or_else(|| format!("plugin[{}] must be a JSON object", idx))?;

        let id = obj
            .get("id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("plugin[{}] missing non-empty string field `id`", idx))?;

        obj.get("provider")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("plugin[{}] ({}) missing `provider`", idx, id))?;

        match obj.get("subscriptions") {
            Some(serde_json::Value::Array(subs)) if !subs.is_empty() => (),
            _ => {
                return Err(format!(
                    "plugin[{}] ({}) must declare a non-empty `subscriptions` array",
                    idx, id
                ));
            }
        }
    }

    Ok(serde_json::to_string(&value).unwrap_or_else(|_| trimmed.to_string()))
}

/// `parse_plugins_json` for a handler that answers `()`: the canonical text,
/// or the one refusal every verb that takes `--plugins` prints.
pub(crate) fn plugins_json_or_refuse(raw: &str) -> String {
    args::or_refuse(
        parse_plugins_json(raw)
            .map_err(|reason| format!("Error: invalid --plugins JSON: {}", reason)),
    )
}

#[cfg(test)]
mod tests {
    use super::parse_plugins_json;

    #[test]
    fn empty_input_becomes_empty_array() {
        assert_eq!(parse_plugins_json("").unwrap(), "[]");
        assert_eq!(parse_plugins_json("{}").unwrap(), "[]");
        assert_eq!(parse_plugins_json("   ").unwrap(), "[]");
    }

    #[test]
    fn valid_sibling_spec_round_trips() {
        let input = r#"[{"id":"init_state_from_java","provider":"init_state_sibling","opts":{"source_operative_id":"market-init-state","handler_key":"fetch_init_state","request_payload":{"symbol":"XBT/USD"}},"subscriptions":[["on_operative_init",{"on_error":"empty"}]]}]"#;
        let canonical = parse_plugins_json(input).unwrap();
        assert!(canonical.contains("\"init_state_from_java\""));
        assert!(canonical.contains("\"init_state_sibling\""));
    }

    #[test]
    fn malformed_json_returns_err() {
        assert!(parse_plugins_json("not json").is_err());
        assert!(parse_plugins_json("[").is_err());
    }

    #[test]
    fn non_array_returns_err() {
        let err = parse_plugins_json(r#"{"id":"x","provider":"y"}"#).unwrap_err();
        assert!(err.contains("JSON array"));
    }

    #[test]
    fn missing_id_returns_err() {
        let err =
            parse_plugins_json(r#"[{"provider":"init_state_sibling","subscriptions":[["x",{}]]}]"#)
                .unwrap_err();
        assert!(err.contains("missing non-empty string field `id`"));
    }

    #[test]
    fn empty_subscriptions_returns_err() {
        let err = parse_plugins_json(
            r#"[{"id":"p","provider":"init_state_sibling","subscriptions":[]}]"#,
        )
        .unwrap_err();
        assert!(err.contains("non-empty `subscriptions`"));
    }
}
