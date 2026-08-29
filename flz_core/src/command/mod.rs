use std::collections::HashMap;
use std::io::Write;
use std::net::Shutdown;

use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;
use prost::Message;

use crate::command;
use crate::pb::command_request::Command;
use crate::pb::{CommandRequest, CommandResponse};
use crate::runner::RunnerNode;

mod compact;
mod credit;
mod entity_apply_state;
pub mod entity_state;
mod entity_status;
mod migration_deserialization;
mod migration_serialization;
mod operative_message;
mod operative_start;
mod operative_stop;
mod park_ack;
mod ping;
mod restore;
mod runner_assign;
mod runner_disconnect;
mod streams_connect;
mod workflow_stop;

/// One command's answer: the bytes the `CommandResponse` carries as
/// `raw_data`, under the payload id the handler names itself by. The reply
/// frame is stamped with the REQUEST's payload id; `payload_id` is the
/// handler's own name for what it answered.
pub struct Reply {
    pub payload_id: &'static str,
    pub bytes: Vec<u8>,
}

pub trait CommandAction {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        command_struct: &str,
        errors: &mut Vec<String>,
    ) -> Reply;

    /// Reply payload plus the metadata to stamp on its frame. Overridden only
    /// by commands whose answer carries more than bytes — `EntityState` stamps
    /// `flz.change_seq` for a single selected Entity. The default forwards to
    /// [`Self::handle`] with no metadata, which is what every other command
    /// wants and keeps their frames byte-identical.
    fn reply(
        &self,
        runner: &mut RunnerNode,
        command_struct: &str,
        errors: &mut Vec<String>,
    ) -> (Reply, HashMap<String, String>) {
        (self.handle(runner, command_struct, errors), HashMap::new())
    }
}

pub enum CommandActionType {
    Regular(Box<dyn CommandAction>),
    RunnerDisconnect(Box<dyn CommandAction>),
    /// Controller -> SDK reply to an SDK-initiated request
    /// (`RunnerShutdownAck`, `ProcCommitResponse`). Wake the waiter
    /// registered under the request's `reference_id` with the carried
    /// outcome. No `CommandResponse` is written — the controller sent this
    /// as its own `CommandRequest`, not in reply to one we sent.
    AckSignal(crate::shutdown::AckResult),
    /// Defensive: SDK should never receive `RunnerShutdownNotification`
    /// (it's the SDK->controller direction). Logged and dropped if a buggy
    /// or future controller ever emits one.
    NotificationIgnore,
}

/// Surface of `get_handler` so callers can distinguish a transport-edge
/// event (peer closed the socket cleanly → reconnect) from a decoded
/// command frame.
pub enum FrameOutcome {
    PeerClosed,
    Command(CommandActionType, String),
}

impl std::fmt::Debug for FrameOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameOutcome::PeerClosed => write!(f, "FrameOutcome::PeerClosed"),
            FrameOutcome::Command(_, ref_id) => {
                write!(f, "FrameOutcome::Command(<handler>, {:?})", ref_id)
            }
        }
    }
}

/// Surface of `get_handler` failure cases. The control-channel reader
/// MUST drive the existing reconnect loop on either variant and MUST NOT
/// panic. `Malformed` is for non-empty bytes that fail `prost::decode`;
/// `EmptyCommand` is for a well-formed `CommandRequest` whose `command`
/// oneof is `None`.
#[derive(Debug)]
pub enum CommandDecodeError {
    Malformed { stack: String },
    EmptyCommand,
}

pub fn get_handler(_payload_id: &str, message: &[u8]) -> Result<FrameOutcome, CommandDecodeError> {
    if message.is_empty() {
        return Ok(FrameOutcome::PeerClosed);
    }

    let wrapper_command =
        CommandRequest::decode(message).map_err(|e| CommandDecodeError::Malformed {
            stack: format!("{:?}", e),
        })?;

    let reference_id = wrapper_command.reference_id;
    let command_handler = classify(wrapper_command.command)?;
    Ok(FrameOutcome::Command(command_handler, reference_id))
}

fn regular(handler: impl CommandAction + 'static) -> CommandActionType {
    CommandActionType::Regular(Box::new(handler))
}

/// Which handler a decoded command takes. `WorkflowStop` trips a workflow
/// driver's cancellation token; `CreditWindowInit` arms the sender window and
/// acks in its `CommandResponse`, `CreditGrant` replenishes it;
/// `ParkPayloadAck` wakes the dispatch thread parked in `park::park_blocking`.
/// The variants the SDK SENDS (`RunnerShutdownNotification`, the
/// `*Response`s it answers with, `WorkflowCompleted`, `ProcCommit` /
/// `IterCommit` / `IterPark` / `UpdateFactories`, `SpawnWorkflowRequest`,
/// `CreditWindowAck`) and the retired reconnect pair (`RunnerReconnected`,
/// `ReassignRunnerId` — the SDK re-presents its stable id on every connect
/// and the controller resolves a reconnect as a binding swap) are protocol
/// mistakes on this side: dropped and logged. A controller reply to an
/// SDK-initiated request wakes the waiter registered under its
/// `reference_id`: a spawn resolves its requester's `SpawnHandle`, a
/// durability reply the workflow driver blocked at its commit barrier.
fn classify(command: Option<Command>) -> Result<CommandActionType, CommandDecodeError> {
    Ok(match command {
        Some(Command::AssignRunnerId(c)) => regular(c),
        Some(Command::ConnectSocket(c)) => regular(c),
        Some(Command::AddOperative(c)) => regular(c),
        Some(Command::RemOperative(c)) => regular(c),
        Some(Command::MigrationSerialization(c)) => regular(c),
        Some(Command::MigrationDeserialization(c)) => regular(c),
        Some(Command::EntityState(c)) => regular(c),
        Some(Command::RunnerDisconnect(c)) => CommandActionType::RunnerDisconnect(Box::new(c)),
        Some(Command::OperativeMessage(c)) => regular(c),
        Some(Command::EntityApplyState(c)) => regular(c),
        Some(Command::RestoreInitState(c)) => regular(c),
        Some(Command::CompactEntity(c)) => regular(c),
        Some(Command::EntityMailboxStatus(c)) => regular(c),
        Some(Command::Ping(c)) => regular(c),
        Some(Command::WorkflowStop(c)) => regular(c),
        Some(Command::CreditWindowInit(c)) => regular(c),
        Some(Command::CreditGrant(c)) => regular(c),
        Some(Command::ParkPayloadAck(c)) => regular(c),
        Some(Command::RunnerShutdownNotification(_))
        | Some(Command::RestoreInitStateResponse(_))
        | Some(Command::CompactEntityResponse(_))
        | Some(Command::RunnerReconnected(_))
        | Some(Command::ReassignRunnerId(_))
        | Some(Command::WorkflowCompleted(_))
        | Some(Command::CreditWindowAck(_))
        | Some(Command::ProcCommit(_))
        | Some(Command::IterCommit(_))
        | Some(Command::IterPark(_))
        | Some(Command::UpdateFactories(_))
        | Some(Command::SpawnWorkflowRequest(_)) => CommandActionType::NotificationIgnore,
        Some(Command::RunnerShutdownAck(_)) => CommandActionType::AckSignal(Ok(String::new())),
        Some(Command::SpawnWorkflowResponse(reply)) => {
            CommandActionType::AckSignal(ack_result(reply.outcome, reply.error))
        }
        Some(Command::ProcCommitResponse(crate::pb::ProcCommitResponse { error, .. }))
        | Some(Command::IterCommitResponse(crate::pb::IterCommitResponse { error, .. })) => {
            CommandActionType::AckSignal(ack_result(String::new(), error))
        }
        None => return Err(CommandDecodeError::EmptyCommand),
    })
}

/// A reply's `error` field as the waiter's ack result: empty ⇒ `Ok(outcome)`
/// (a durable journal entry, a spawn's outcome), non-empty ⇒ the error, which
/// terminates a workflow run abnormally.
fn ack_result(outcome: String, error: String) -> crate::shutdown::AckResult {
    if error.is_empty() {
        Ok(outcome)
    } else {
        Err(error)
    }
}

pub fn handle_command(
    runner: &mut RunnerNode,
    stream: &FlezhaStream,
    payload_id: &str,
    message: &[u8],
) {
    // Controller's keepalive echo (see flz_transport liveness): the inbound
    // read already re-armed the steady-state idle watchdog, and the frame
    // carries a bare PingResponse on its own payload — nothing to dispatch.
    if payload_id == flz_transport::transport::liveness::CONTROL_PING_PAYLOAD_ID {
        return;
    }
    let Some((handler_type, reference_id)) = decode_frame(payload_id, message) else {
        return;
    };

    match handler_type {
        CommandActionType::Regular(handler) => {
            let reply = command::exec_control(runner, handler, payload_id, message, reference_id);
            if let Err(e) = (&*stream).write_all(&reply) {
                eprintln!("[flezha] Failed to write command response: {}", e);
            }
        }

        CommandActionType::RunnerDisconnect(handler) => {
            let reply = command::exec_control(runner, handler, payload_id, message, reference_id);
            if let Err(e) = (&*stream).write_all(&reply) {
                eprintln!("[flezha] Failed to write disconnect response: {}", e);
            }
            finish_disconnect(stream);
        }

        CommandActionType::AckSignal(result) => {
            // Suppress unused-variable warnings for unused params.
            let _ = runner;
            crate::shutdown::signal_result(&reference_id, result);
        }

        CommandActionType::NotificationIgnore => {
            let _ = runner;
            eprintln!(
                "[flezha] Received an unexpected controller->SDK command the SDK does \
                 not handle (ref={}); ignoring.",
                reference_id
            );
        }
    }
}

/// The handler a frame decodes to, or `None` for a frame that is dropped: a
/// malformed one (the read loop drives the reconnect), a `command: None` (the
/// next frame is processed normally), or a 0-byte payload — the reader
/// (`payloader::extract_payload`) is the canonical EOF detector and emits
/// `RunnerMessage::Reconnect` on its own io::Error path, so this is a
/// defence-in-depth no-op in case one ever flows in band.
fn decode_frame(payload_id: &str, message: &[u8]) -> Option<(CommandActionType, String)> {
    match command::get_handler(payload_id, message) {
        Ok(FrameOutcome::Command(handler, reference_id)) => Some((handler, reference_id)),
        Ok(FrameOutcome::PeerClosed) => None,
        Err(CommandDecodeError::Malformed { stack }) => {
            eprintln!(
                "[flezha] control-channel frame failed to decode (stack={}); \
                 dropping and waiting for the read loop to drive reconnect.",
                stack
            );
            None
        }
        Err(CommandDecodeError::EmptyCommand) => {
            eprintln!(
                "[flezha] control-channel frame had `command: None`; dropping. \
                 The next frame on this connection will be processed normally."
            );
            None
        }
    }
}

/// After the disconnect ack is written: FIN, not abort — shutting only the
/// write half guarantees the response bytes are delivered BEFORE the peer
/// observes the close (`Shutdown::Both` raced them, and the controller could
/// deadlock its disconnect call on the missing ack); the read half drains to
/// EOF when the controller closes its side. A terminal disconnect ships its
/// state BEFORE the write half closes, because the graceful shutdown's
/// per-operative notifications ride this same control channel. Once the ack
/// and the final state are on the wire the process may go; nothing runs after
/// a terminal exit.
fn finish_disconnect(stream: &FlezhaStream) {
    let terminal = crate::shutdown::take_terminal_exit();
    if terminal.is_some() {
        crate::shutdown::run_active_graceful_shutdown();
    }
    let _ = stream.shutdown(Shutdown::Write);
    if let Some(reason) = terminal {
        crate::shutdown::exit_terminally(&reason);
    }
}

fn exec_control(
    runner: &mut RunnerNode,
    handler: Box<dyn CommandAction>,
    payload_id: &str,
    message: &[u8],
    reference_id: String,
) -> Vec<u8> {
    let mut errors: Vec<String> = vec![];
    let mut status = String::from("success");

    let message_str = std::str::from_utf8(message).unwrap_or("");
    let (reply, metadata) = handler.reply(runner, message_str, &mut errors);

    if !errors.is_empty() {
        status = "error".to_string();
    }

    let command_response: CommandResponse = CommandResponse {
        reference_id,
        status,
        errors,
        raw_data: reply.bytes,
    };

    let command_response_bytes = command_response.encode_to_vec();

    payloader::build_from_binary(payload_id, &command_response_bytes, metadata)
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;

    #[test]
    fn empty_bytes_signal_peer_closed() {
        let out = get_handler("payload", &[]).expect("empty input is not an error");
        assert!(matches!(out, FrameOutcome::PeerClosed));
    }

    #[test]
    fn truncated_prefix_returns_malformed_error() {
        // Three bytes: a valid prost varint prefix that promises more data
        // than is present. Decode MUST fail with a typed error rather
        // than panic.
        let err = get_handler("payload", &[0x08, 0x01, 0x12]).expect_err("truncation must error");
        match err {
            CommandDecodeError::Malformed { stack } => {
                assert!(
                    !stack.is_empty(),
                    "malformed-error stack must carry the prost diagnostic"
                );
            }
            other => panic!("expected Malformed, got {:?}", other),
        }
    }

    #[test]
    fn empty_command_oneof_returns_typed_error() {
        let request = CommandRequest {
            reference_id: "ref-empty".to_string(),
            command: None,
        };

        let bytes = request.encode_to_vec();
        let err = get_handler("payload", &bytes).expect_err("None command must error");
        assert!(matches!(err, CommandDecodeError::EmptyCommand));
    }

    #[test]
    fn well_formed_request_returns_command_variant() {
        let request = CommandRequest {
            reference_id: "ref-shutdown".to_string(),
            command: Some(Command::RunnerShutdownAck(crate::pb::RunnerShutdownAck {
                operative_id: "".to_string(),
            })),
        };

        let bytes = request.encode_to_vec();
        let out = get_handler("payload", &bytes).expect("well-formed request must decode");

        match out {
            FrameOutcome::Command(_, ref_id) => assert_eq!(ref_id, "ref-shutdown"),
            FrameOutcome::PeerClosed => panic!("well-formed request must not be PeerClosed"),
        }
    }

    #[test]
    fn a_spawn_reply_wakes_the_requesters_handle_with_its_outcome() {
        let reply = |outcome: &str, error: &str| {
            let bytes = CommandRequest {
                reference_id: "ref-spawn".to_string(),
                command: Some(Command::SpawnWorkflowResponse(
                    crate::pb::SpawnWorkflowResponse {
                        outcome: outcome.to_string(),
                        error: error.to_string(),
                    },
                )),
            }
            .encode_to_vec();

            match get_handler("payload", &bytes).expect("spawn reply must decode") {
                FrameOutcome::Command(CommandActionType::AckSignal(result), ref_id) => {
                    assert_eq!(ref_id, "ref-spawn");
                    result
                }
                other => panic!("spawn reply must route to AckSignal, got {:?}", other),
            }
        };

        assert_eq!(reply("spawned", ""), Ok("spawned".to_string()));
        assert_eq!(reply("delivered", ""), Ok("delivered".to_string()));
        assert_eq!(reply("refused", "quota"), Err("quota".to_string()));
    }

    #[test]
    fn no_get_handler_input_causes_panic() {
        // Final guard: catch_unwind across every error-edge input proves
        // the path no longer panics.
        let empty_command = CommandRequest {
            reference_id: "x".to_string(),
            command: None,
        }
        .encode_to_vec();

        let valid_request = CommandRequest {
            reference_id: "y".to_string(),
            command: Some(Command::RunnerShutdownAck(crate::pb::RunnerShutdownAck {
                operative_id: "".to_string(),
            })),
        }
        .encode_to_vec();

        let inputs: Vec<Vec<u8>> = vec![
            Vec::new(),
            vec![0x08, 0x01, 0x12],
            empty_command,
            valid_request,
        ];

        for input in inputs {
            let owned = input.clone();
            let result = std::panic::catch_unwind(|| {
                let _ = get_handler("payload", &owned);
            });
            assert!(
                result.is_ok(),
                "get_handler must not panic on input {:?}",
                input
            );
        }
    }
}
