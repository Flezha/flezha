use flz_transport::errors::FlezhaError;

use crate::args::{self, OptSpec, Positional, Reading, DURATION_MS, INIT_VALUES, NAMESPACE};
use crate::dispatch::{self, Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, text, Opt};
use crate::settings::serialize_flags;

const OPERATOR_ID: &[Positional] = &[Positional::required("<operator_id>")];

const ID: OptSpec = OptSpec::text("--id", "-i", "<id>");
const PLUGINS: OptSpec = OptSpec::text("--plugins", "--plugins", "<json-array>");
const PARTITIONS: OptSpec = OptSpec::number("--partitions", "--partitions", "<N>");
const SNAPSHOT_INTERVAL: OptSpec =
    OptSpec::number("--snapshot-interval", "--snapshot-interval", "<N>");
const TIMEOUT: OptSpec = OptSpec::number("--timeout", "-t", "<ms>");
const PAYLOAD: OptSpec = OptSpec::text("--text", "-t", "<payload>").required();
/// `send`'s payload spelling on `start`, where a first message is optional.
pub(crate) const TEXT: OptSpec = OptSpec::text("--text", "-t", "<payload>");
pub(crate) const CHANNEL: OptSpec = OptSpec::text("--channel", "-c", "<ch>");
const TO_ITERATION: OptSpec = OptSpec::number("--to-iteration", "--to-iteration", "<N>").required();
const FROM: OptSpec = OptSpec::text("--from", "-f", "<seq|timestamp>").required();
const INPUT: OptSpec = OptSpec::switch("--input", "--input");
const AWAIT: OptSpec = OptSpec::number("--await", "--await", "<ms>");

pub(crate) const TABLE: Command = Command {
    path: "flz operator",
    help: help::OPERATOR,
    verbs: &[
        Verb::new("start", handle_start)
            .taking(&[Positional::required("<factory_id>")])
            .reading(&[
                ID,
                NAMESPACE,
                INIT_VALUES,
                TEXT,
                CHANNEL,
                PLUGINS,
                PARTITIONS,
                SNAPSHOT_INTERVAL,
            ])
            .strict()
            .screen(help::OPERATOR_START),
        Verb::new("scale", handle_scale)
            .taking(&[
                Positional::required("<group>"),
                Positional::number("<partitions>"),
            ])
            .reading(&[NAMESPACE, TIMEOUT]),
        Verb::new("stop", handle_stop)
            .taking(OPERATOR_ID)
            .reading(&[NAMESPACE]),
        Verb::new("state", handle_state)
            .taking(OPERATOR_ID)
            .reading(&[NAMESPACE]),
        Verb::new("list", |read| {
            rpc::scoped_read(read, "list_operators", Some("operators"))
        })
        .reading(&[NAMESPACE]),
        Verb::new("info", |read| {
            rpc::scoped_read(read, "operator_info", Some("operator information"))
        })
        .reading(&[NAMESPACE]),
        Verb::new("listen", handle_listen)
            .taking(OPERATOR_ID)
            .reading(&[INPUT, DURATION_MS, NAMESPACE])
            .strict()
            .screen(help::OPERATOR_LISTEN),
        Verb::new("send", handle_send)
            .taking(OPERATOR_ID)
            .reading(&[PAYLOAD, CHANNEL, AWAIT, NAMESPACE]),
        Verb::new("rewind", handle_rewind)
            .taking(OPERATOR_ID)
            .reading(&[TO_ITERATION, NAMESPACE]),
        Verb::new("replay", handle_replay)
            .taking(&[Positional::required("<operator/tasklet/key>")])
            .reading(&[FROM, NAMESPACE]),
    ],
    help_verbs: dispatch::HELP_VERBS,
    missing: Missing::Refuse,
    unknown_prints_help: false,
};

/// The JSON opts object `operator_start` takes: the flags that are neither
/// the operator's identity nor its init values. Ordered, because the same
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

/// `--id`, `--plugins`, `--text` and `--channel` ride the options list, and
/// only when written: an absent flag is absent from the request and the
/// controller applies its default, which for the id is one it generates.
/// `--text` is the instance's first message, on the entry channel `--channel`
/// names; a channel with no text names nothing and is refused. The kind is the
/// factory's, announced by its runner, so no flag names it.
fn handle_start(read: &Reading) {
    let mut opts: Vec<Opt> = Vec::new();

    if let Some(id) = read.opt(ID.long) {
        opts.push(("id", text(id)));
    }
    if let Some(plugins) = read.opt(PLUGINS.long) {
        opts.push(("plugins", text(&plugins_json_or_refuse(plugins))));
    }
    opts.extend(first_message_opts(read));

    rpc::run(
        "operator_start",
        &[
            text(read.namespace()),
            text(read.pos("<factory_id>")),
            text(read.json(INIT_VALUES.long)),
            text(&start_opts(read)),
        ],
        &opts,
        read,
    );
}

/// The first message a `start` or a `schedule add` carries: `text` and,
/// when written, `channel`, as text; `--channel` without `--text` is refused.
pub(crate) fn first_message_opts<'a>(read: &'a Reading) -> Vec<Opt<'a>> {
    let mut opts: Vec<Opt> = Vec::new();
    let payload = read.opt(TEXT.long);
    if let Some(payload) = payload {
        opts.push(("text", text(payload)));
    }
    if let Some(channel) = read.opt(CHANNEL.long) {
        args::or_refuse(
            payload.ok_or_else(|| {
                FlezhaError::new("operator.channel_without_text", Default::default())
            }),
        );
        opts.push(("channel", text(channel)));
    }
    opts
}

/// `flz operator scale <group> <partitions>` — rescale a partition group.
/// The group is quiesced, every Cell whose key group changes owner is moved,
/// and the new member set is started; a refusal changes nothing.
fn handle_scale(read: &Reading) {
    let group_id = read.pos("<group>");
    let partitions = read.pos("<partitions>");

    let mut opts: Vec<(String, String)> = Vec::new();
    if let Some(timeout_ms) = read.opt(TIMEOUT.long) {
        opts.push(("timeout_ms".to_string(), timeout_ms.to_string()));
    }

    println!("Scaling operator group {} to {}...", group_id, partitions);

    rpc::run(
        "operator_scale",
        &[
            text(read.namespace()),
            text(group_id),
            text(partitions),
            text(&serialize_flags(&opts)),
        ],
        &[],
        read,
    );
}

fn handle_stop(read: &Reading) {
    println!("Stopping operator...");

    // operator_stop halts non-zero on error verdicts (not_found / refused).
    rpc::run(
        "operator_stop",
        &[text(read.namespace()), text(read.pos(OPERATOR_ID[0].name))],
        &[],
        read,
    );

    println!("Operator stopped successfully.");
}

/// `flz operator send <operator_id> --text <payload> [--channel <ch>]
/// [--await <ms>]`: inject a payload into an operator's input stream (the
/// SDK-facing `send_io` route). Blocks for the confirming verdict so a parked or
/// absent target is reported rather than silently dropped. `--await` rides the
/// options list only when written.
fn handle_send(read: &Reading) {
    let mut opts: Vec<Opt> = Vec::new();

    if let Some(ms) = read.opt(AWAIT.long) {
        opts.push(("await_ms", ms.to_string()));
    }

    // operator_send halts non-zero on error verdicts (not_found / not_ready / refused).
    rpc::run(
        "operator_send",
        &[
            text(read.namespace()),
            text(read.pos(OPERATOR_ID[0].name)),
            text(read.opt(PAYLOAD.long).unwrap_or_default()),
            text(read.opt(CHANNEL.long).unwrap_or_default()),
        ],
        &opts,
        read,
    );
}

/// `flz operator rewind <instance> --to-iteration N` — take a bounded loop back
/// to a committed iteration: the ones above it are compensated in reverse, state
/// returns to that boundary, and the epoch is bumped so the re-run's output
/// supersedes the unwound run's at a guarded sink.
fn handle_rewind(read: &Reading) {
    // operator_rewind halts non-zero on error verdicts (not_found / refused).
    rpc::run(
        "operator_rewind",
        &[
            text(read.namespace()),
            text(read.pos(OPERATOR_ID[0].name)),
            read.opt(TO_ITERATION.long).unwrap_or_default().to_string(),
        ],
        &[],
        read,
    );
}

/// `flz operator replay <operator/tasklet/key> --from <seq|ts>` — re-run a
/// Cell's retained inputs from a point in the past through the current code,
/// republishing under a bumped epoch so a guarded sink overwrites the stale rows
/// by natural key. "Deploy a fix, heal the recent past."
///
/// The same transaction a late input triggers automatically; here the caller
/// picks the baseline instead of a late event.
fn handle_replay(read: &Reading) {
    // operator_replay halts non-zero on error verdicts (not_found / refused).
    rpc::run(
        "operator_replay",
        &[
            text(read.namespace()),
            text(read.pos("<operator/tasklet/key>")),
            text(read.opt(FROM.long).unwrap_or_default()),
        ],
        &[],
        read,
    );
}

fn handle_state(read: &Reading) {
    // cell_state halts non-zero on error verdicts (not_found / refused).
    rpc::run(
        "cell_state",
        &[text(read.namespace()), text(read.pos(OPERATOR_ID[0].name))],
        &[],
        read,
    );
}

/// `flz operator listen <operator_id> [--input]`: print every frame the
/// operator emits, or ingests, until the window closes. Prints nothing of its
/// own, so what reaches stdout is the frames alone.
fn handle_listen(read: &Reading) {
    let direction = if read.switch(INPUT.long) {
        "input"
    } else {
        "output"
    };

    rpc::run(
        "operator_listen",
        &[
            text(read.namespace()),
            text(read.pos(OPERATOR_ID[0].name)),
            text(direction),
        ],
        &[rpc::window(read)],
        read,
    );
}

/// Validate the `--plugins` JSON blob at the CLI layer so parse errors are
/// surfaced with a local exit code and message, rather than round-tripping
/// an opaque RPC failure through the controller.
///
/// Accepts:
/// * `"{}"` / empty string — treated as "no plugins attached"; returns `"[]"`.
/// * A JSON array of plugin objects. Each object MUST have `id`, `provider`
///   (non-empty strings) and a non-empty `hooks` array. Deeper
///   validation (provider registration lookup, hook-opts shape)
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

        match obj.get("hooks") {
            Some(serde_json::Value::Array(subs)) if !subs.is_empty() => (),
            _ => {
                return Err(format!(
                    "plugin[{}] ({}) must declare a non-empty `hooks` array",
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
        parse_plugins_json(raw).map_err(|reason| {
            FlezhaError::with("usage.invalid_plugins", [("reason", reason.into())])
        }),
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
        let input = r#"[{"id":"init_state_from_java","provider":"init_state_sibling","opts":{"source_operator_id":"market-init-state","handler_key":"fetch_init_state","request_payload":{"symbol":"XBT/USD"}},"hooks":[["on_operator_init",{"on_error":"empty"}]]}]"#;
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
        let err = parse_plugins_json(r#"[{"provider":"init_state_sibling","hooks":[["x",{}]]}]"#)
            .unwrap_err();
        assert!(err.contains("missing non-empty string field `id`"));
    }

    #[test]
    fn empty_hooks_returns_err() {
        let err = parse_plugins_json(r#"[{"id":"p","provider":"init_state_sibling","hooks":[]}]"#)
            .unwrap_err();
        assert!(err.contains("non-empty `hooks`"));
    }

    /// fails when: a spec that still names its hooks `subscriptions` passes the
    /// check and reaches the controller as a plugin with no hooks.
    #[test]
    fn hooks_under_another_key_return_err() {
        let err = parse_plugins_json(
            r#"[{"id":"p","provider":"operator_stdout","subscriptions":["on_output"]}]"#,
        )
        .unwrap_err();
        assert!(err.contains("non-empty `hooks`"));
    }
}
