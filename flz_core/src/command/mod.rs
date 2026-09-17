use std::collections::HashMap;
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

/// One command's answer: the `CommandResponse` bytes under the handler's own
/// payload id (the frame is stamped with the REQUEST's).
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

    /// The reply plus its frame metadata; overridden only by a command whose
    /// answer carries more than bytes.
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
    /// The controller's reply to an SDK-initiated request: wake the waiter
    /// under its `reference_id`; no `CommandResponse` is written.
    AckSignal(crate::shutdown::AckResult),
    /// A frame this side only ever sends; logged and dropped.
    NotificationIgnore,
}

/// A decoded command frame, or the peer's clean close.
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

/// Why a frame did not decode; the reader drives a reconnect on either and
/// never panics.
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

/// Which handler a decoded command takes. The variants this side SENDS, and
/// the reconnect pair the controller resolves as a binding swap instead, are
/// protocol mistakes here: dropped and logged. A reply to an SDK-initiated
/// request wakes the waiter under its `reference_id`.
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
        | Some(Command::SpawnWorkflowRequest(_))
        | Some(Command::AdminRequest(_)) => CommandActionType::NotificationIgnore,
        Some(Command::RunnerShutdownAck(_)) => CommandActionType::AckSignal(Ok(String::new())),
        Some(Command::SpawnWorkflowResponse(reply)) => CommandActionType::AckSignal(ack_result(
            crate::spawn::encode_ack(&reply.outcome, &reply.instance_id),
            reply.error,
        )),
        Some(Command::AdminResponse(reply)) => {
            CommandActionType::AckSignal(ack_result(reply.result, reply.error))
        }
        Some(Command::ProcCommitResponse(crate::pb::ProcCommitResponse { error, .. }))
        | Some(Command::IterCommitResponse(crate::pb::IterCommitResponse { error, .. })) => {
            CommandActionType::AckSignal(ack_result(String::new(), error))
        }
        None => return Err(CommandDecodeError::EmptyCommand),
    })
}

/// A reply's `error` field as the waiter's result: empty is `Ok(outcome)`.
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
            let written = command::exec_control(runner, handler, payload_id, message, reference_id)
                .and_then(|reply| write_reply(stream, reply));
            if let Err(e) = written {
                eprintln!("[flezha] Failed to write command response: {}", e);
            }
        }

        CommandActionType::RunnerDisconnect(handler) => {
            let written = command::exec_control(runner, handler, payload_id, message, reference_id)
                .and_then(|reply| write_reply(stream, reply));
            if let Err(e) = written {
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

/// The handler a frame decodes to, or `None` for one that is dropped: a
/// malformed frame (the read loop reconnects), `command: None`, or a 0-byte
/// payload the reader already treats as EOF.
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

/// After the disconnect ack is written: a terminal disconnect ships its
/// state first (the notifications ride this channel), then only the write
/// half closes so the ack lands before the peer sees the close.
/// A command reply on the control stream. The class refuses it rather than
/// holding it: a reply re-driven after its waiter timed out is a stale answer,
/// and the reconnect re-issues the request it belonged to.
fn write_reply(stream: &FlezhaStream, reply: Vec<u8>) -> std::io::Result<()> {
    crate::state_sink::write(crate::state_sink::Class::Control, Some(stream), reply).refused()
}

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
) -> std::io::Result<Vec<u8>> {
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
    fn a_spawn_reply_wakes_the_requesters_handle_with_its_outcome_and_id() {
        let reply = |outcome: &str, error: &str, instance_id: &str| {
            let bytes = CommandRequest {
                reference_id: "ref-spawn".to_string(),
                command: Some(Command::SpawnWorkflowResponse(
                    crate::pb::SpawnWorkflowResponse {
                        outcome: outcome.to_string(),
                        error: error.to_string(),
                        instance_id: instance_id.to_string(),
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

        assert_eq!(
            reply("spawned", "", "worker-k7m2qzxw4pd3fn2a"),
            Ok(crate::spawn::encode_ack(
                "spawned",
                "worker-k7m2qzxw4pd3fn2a"
            ))
        );
        assert_eq!(
            reply("delivered", "", "w1"),
            Ok(crate::spawn::encode_ack("delivered", "w1"))
        );
        assert_eq!(reply("refused", "quota", ""), Err("quota".to_string()));
    }

    /// Every command runs on the coordinator thread, which owns the control
    /// stream, so a panic in one ends the session without a word to the
    /// controller. The production half of this directory refuses instead.
    /// One directory, not the crate: `flz_core/src` holds more.
    #[test]
    fn no_command_can_panic_the_thread_that_runs_it() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/src/command");
        let mut scanned = 0;

        for entry in std::fs::read_dir(dir).expect("the command directory") {
            let path = entry.expect("a directory entry").path();
            let source = std::fs::read_to_string(&path).expect("a command source");
            let production = source
                .split("#[cfg(test)]\nmod ")
                .next()
                .unwrap_or_default();
            for call in [".unwrap()", ".expect(", "panic!("] {
                assert!(
                    !production.contains(call),
                    "{} calls `{call}` outside its tests",
                    path.display()
                );
            }
            scanned += 1;
        }

        assert!(
            scanned > 1,
            "the scan read {scanned} files and measured nothing"
        );
    }
}
