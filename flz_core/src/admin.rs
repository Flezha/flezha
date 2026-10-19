//! Administering the namespace this runner's connection proved.
//!
//! One request shape for every verb, because the verb is data: [`call`] ships
//! an `AdminRequest` naming a function of the controller's published consumer
//! surface and blocks for the reply. Nothing in the request names a namespace,
//! a principal or a role. The controller fills all three from the connection,
//! so a host that adds a verb adds a name and no plumbing.
//!
//! Every SDK over this kernel reaches the surface here rather than through a
//! shape of its own: `sdk/rust` calls it directly and `flz_core_ffi` exposes
//! it as the one C ABI entry the other five hosts share. The runner's [`send`]
//! is here for the same reason: its options and refusals are written once.

use std::time::Duration;

use flz_transport::errors::{ErrorModel, FlezhaError};
use serde_json::{json, Map, Value};

use crate::error::Code;
use crate::pb;

/// How long an administration call waits for the controller's answer: the
/// deadline every request over [`crate::shutdown::ask`] waits out.
pub const ADMIN_TIMEOUT: Duration = crate::shutdown::REPLY_TIMEOUT;

/// Why an administration call produced no answer, as distinct from the
/// controller's own refusal, which arrives as [`Refused`](AdminError::Refused).
#[derive(Debug, Clone, PartialEq)]
pub enum AdminError {
    /// The controller judged the call and said no: policy, quota, a name that
    /// resolves to nothing, or the effect's own failure, under its own code.
    Refused(FlezhaError),

    /// No control plane to ask on: a transport-less test or bench, or a
    /// runner that has closed.
    Unavailable,

    /// The request went out and nothing came back within [`ADMIN_TIMEOUT`].
    Unanswered,
}

impl ErrorModel for AdminError {
    fn to_error(&self) -> FlezhaError {
        match self {
            AdminError::Refused(error) => error.clone(),
            AdminError::Unavailable => Code::ControlNoControlPlane.with([]),
            AdminError::Unanswered => {
                Code::ControlTimeout.with([("seconds", ADMIN_TIMEOUT.as_secs().into())])
            }
        }
    }
}

flz_transport::model_error!(AdminError);

/// Run `verb` with `args`, a JSON array of the call's arguments other than the
/// namespace. Answers the published function's own result as JSON.
pub fn call(verb: &str, args: &str) -> Result<String, AdminError> {
    let reference_id = format!("admin-{}-{}", verb, crate::util::nonce_suffix());
    let command = pb::command_request::Command::AdminRequest(pb::AdminRequest {
        verb: verb.to_string(),
        args: args.to_string(),
    });
    let rx =
        crate::shutdown::ask(reference_id.clone(), command).map_err(|_| AdminError::Unavailable)?;

    match rx.recv_timeout(ADMIN_TIMEOUT) {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(refusal)) => Err(AdminError::Refused(refusal)),
        Err(_) => {
            crate::shutdown::drop_pending_ack(&reference_id);
            Err(AdminError::Unanswered)
        }
    }
}

/// Send `message` to the entry channel `channel` of `relay`, answering as
/// JSON. Through the controller's `Relay.send` by default, answering its
/// verdict; `msg_id` and `key` ride as its named fields. With `local`, in
/// process to a relay bound on this runner
/// ([`crate::runner::BoundRelays::send`]), answering `null`; an id or a key
/// beside it is refused before anything is sent, since only the controller
/// reads them.
pub fn send(
    relay: &str,
    message: &[u8],
    channel: &str,
    msg_id: Option<&str>,
    key: Option<&str>,
    local: bool,
) -> Result<String, AdminError> {
    let given = [("msg_id", msg_id), ("key", key)];
    if local {
        if let Some((other, _)) = given.iter().find(|(_, value)| value.is_some()) {
            return Err(AdminError::Refused(
                Code::OptionsConflict
                    .with([("option", "local".into()), ("other", (*other).into())]),
            ));
        }
        crate::runner::bound()
            .send(relay, channel, message.to_vec())
            .map_err(|e| AdminError::Refused(e.to_error()))?;
        return Ok("null".to_string());
    }
    let message = std::str::from_utf8(message).map_err(|_| {
        AdminError::Refused(Code::ArgumentNotUtf8.with([("argument", "message".into())]))
    })?;
    call("Relay.send", &send_args(relay, message, channel, &given))
}

/// Call the entry channel `channel` of `relay` with `payload` as it is,
/// through the controller's `Relay.call_channel`, answering its JSON:
/// `{"answer", "answer_codec"}`, or `{"accepted": true}` for a channel that
/// does not answer.
pub fn call_channel(relay: &str, channel: &str, payload: &[u8]) -> Result<String, AdminError> {
    call(
        "Relay.call_channel",
        &call_channel_args(relay, channel, payload),
    )
}

fn call_channel_args(relay: &str, channel: &str, payload: &[u8]) -> String {
    json!([
        relay,
        channel,
        crate::call::base64_encode(payload),
        {"payload": "base64"}
    ])
    .to_string()
}

/// `Relay.send`'s arguments: the named fields ride only when one is given,
/// so a send without them is the request it always was.
fn send_args(relay: &str, message: &str, channel: &str, given: &[(&str, Option<&str>)]) -> String {
    let mut args = vec![json!(relay), json!(message), json!(channel)];
    let fields: Map<String, Value> = given
        .iter()
        .filter_map(|(name, value)| value.map(|v| (name.to_string(), json!(v))))
        .collect();
    if !fields.is_empty() {
        args.push(Value::Object(fields));
    }
    Value::Array(args).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_channel_call_carries_its_payload_as_base64() {
        assert_eq!(
            call_channel_args("orders-eu", "place", br#"{"a":1}"#),
            r#"["orders-eu","place","eyJhIjoxfQ==",{"payload":"base64"}]"#
        );
    }

    #[test]
    fn a_send_names_its_fields_only_when_given() {
        assert_eq!(
            send_args("orders", "x", "in", &[("msg_id", None), ("key", None)]),
            r#"["orders","x","in"]"#
        );
        assert_eq!(
            send_args(
                "orders",
                "x",
                "in",
                &[("msg_id", Some("o/0/41")), ("key", Some("EURUSD"))]
            ),
            r#"["orders","x","in",{"key":"EURUSD","msg_id":"o/0/41"}]"#
        );
    }

    #[test]
    fn a_local_send_is_refused_for_an_id_a_key_or_a_relay_held_elsewhere() {
        let code =
            |result: Result<String, AdminError>| result.expect_err("refused").to_error().code;
        assert_eq!(
            code(send("orders", b"x", "in", Some("m-1"), None, true)),
            "usage.options_conflict"
        );
        assert_eq!(
            code(send("orders", b"x", "in", None, Some("EURUSD"), true)),
            "usage.options_conflict"
        );
        assert_eq!(
            code(send("elsewhere", b"x", "in", None, None, true)),
            "runtime.relay_not_local"
        );
    }
}
