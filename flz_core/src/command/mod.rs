use std::collections::HashMap;
use std::net::Shutdown;
use std::sync::Arc;

use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;
use prost::Message;

use crate::pb::command_request::Command;
use crate::pb::{CommandRequest, CommandResponse};
use crate::runner::RunnerNode;
use crate::{command, ErrorModel};

pub mod cell_state;
mod cell_status;
mod compact;
mod credit;
mod effect_results;
mod migration_deserialization;
mod migration_serialization;
mod park_ack;
mod ping;
mod relay_message;
mod relay_start;
mod relay_stop;
mod restore;
mod runner_assign;
mod runner_disconnect;
mod steps_stop;
mod streams_connect;
mod tasklet_activity;

pub(crate) use credit::answer_credit;

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
        errors: &mut Vec<crate::Error>,
    ) -> Reply;

    /// The reply plus its frame metadata; overridden only by a command whose
    /// answer carries more than bytes.
    fn reply(
        &self,
        runner: &mut RunnerNode,
        command_struct: &str,
        errors: &mut Vec<crate::Error>,
    ) -> (Reply, HashMap<String, String>) {
        (self.handle(runner, command_struct, errors), HashMap::new())
    }
}

/// A read too slow for the control reader: what it needs is taken from the
/// runner there, and the answer is built and written on a worker thread, so
/// the commands queued behind it are not held up.
pub trait OffReaderAction {
    fn prepare(&self, runner: &mut RunnerNode) -> Result<OffReaderBuild, crate::Error>;
}

pub type OffReaderBuild = Box<dyn FnOnce() -> Vec<u8> + Send>;

pub enum CommandActionType {
    Regular(Box<dyn CommandAction>),
    OffReader(Box<dyn OffReaderAction>),
    /// A start of an id whose stopped predecessor's leave the host has not
    /// answered: handled once it has, so nothing of the new instance reaches
    /// the host first. The controller sends the relay nothing before the
    /// start's answer.
    AfterLeave(Arc<crate::host::PendingLeave>),
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
        Some(Command::AddRelay(c)) => match crate::host::pending_leave(&c.relay_id) {
            Some(pending) => CommandActionType::AfterLeave(pending),
            None => regular(c),
        },
        Some(Command::RemRelay(c)) => regular(c),
        Some(Command::MigrationSerialization(c)) => regular(c),
        Some(Command::MigrationDeserialization(c)) => regular(c),
        Some(Command::CellState(c)) => regular(c),
        Some(Command::RunnerDisconnect(c)) => CommandActionType::RunnerDisconnect(Box::new(c)),
        Some(Command::RelayMessage(c)) => regular(c),
        Some(Command::RestoreInitState(c)) => regular(c),
        Some(Command::CompactCell(c)) => regular(c),
        Some(Command::CellMailboxStatus(c)) => regular(c),
        Some(Command::Ping(c)) => regular(c),
        Some(Command::StepsStop(c)) => regular(c),
        Some(Command::ParkPayloadAck(c)) => regular(c),
        Some(Command::EffectResults(c)) => regular(c),
        Some(Command::TaskletActivity(c)) => CommandActionType::OffReader(Box::new(c)),
        Some(Command::RunnerShutdownNotification(_))
        | Some(Command::RestoreInitStateResponse(_))
        | Some(Command::CompactCellResponse(_))
        | Some(Command::RelayCompleted(_))
        | Some(Command::CreditWindowAck(_))
        | Some(Command::TaskCommit(_))
        | Some(Command::IterCommit(_))
        | Some(Command::IterPark(_))
        | Some(Command::UpdateFactories(_))
        | Some(Command::SpawnStepsRequest(_))
        | Some(Command::AdminRequest(_))
        | Some(Command::EffectResult(_))
        // The control reader answers a refusal itself, before the handshake
        // it ends; one arriving later has no session to refuse.
        | Some(Command::ConnectRefused(_))
        // Answered where control frames are read (`answer_credit`).
        | Some(Command::CreditWindowInit(_))
        | Some(Command::CreditGrant(_)) => CommandActionType::NotificationIgnore,
        Some(Command::RunnerShutdownAck(_)) => CommandActionType::AckSignal(Ok(String::new())),
        Some(Command::SpawnStepsResponse(reply)) => CommandActionType::AckSignal(ack_result(
            crate::spawn::encode_ack(&reply.outcome, &reply.instance_id),
            reply.error,
        )),
        Some(Command::EffectResultAck(reply)) => CommandActionType::AckSignal(ack_result(
            crate::effect_result::encode_ack(reply.first, &reply.payload),
            reply.error,
        )),
        Some(Command::AdminResponse(reply)) => {
            CommandActionType::AckSignal(ack_result(reply.result, reply.error))
        }
        Some(Command::TaskCommitResponse(crate::pb::TaskCommitResponse { error, .. }))
        | Some(Command::IterCommitResponse(crate::pb::IterCommitResponse { error, .. })) => {
            CommandActionType::AckSignal(ack_result(String::new(), error))
        }
        None => return Err(CommandDecodeError::EmptyCommand),
    })
}

/// A reply's `error` field as the waiter's result: absent is `Ok(outcome)`.
fn ack_result(outcome: String, error: Option<crate::pb::Error>) -> crate::shutdown::AckResult {
    match error {
        None => Ok(outcome),
        Some(error) => Err(error.into()),
    }
}

pub fn handle_command(
    runner: &mut RunnerNode,
    stream: &Arc<FlezhaStream>,
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
        CommandActionType::Regular(handler) => respond_with(
            stream,
            command::exec_control(runner, handler, payload_id, message, reference_id),
        ),

        CommandActionType::OffReader(action) => match action.prepare(runner) {
            Ok(build) => {
                let (stream, payload_id) = (Arc::clone(stream), payload_id.to_string());
                let spawned = std::thread::Builder::new()
                    .name("flezha-off-reader".into())
                    .spawn(move || {
                        let frame = encode_response(
                            &payload_id,
                            reference_id,
                            build(),
                            &[],
                            HashMap::new(),
                        );
                        respond_with(&stream, frame);
                    });
                if let Err(e) = spawned {
                    flz_transport::notice!(Error, "Failed to start an off-reader answer: {}", e);
                }
            }
            Err(refusal) => respond_with(
                stream,
                encode_response(
                    payload_id,
                    reference_id,
                    Vec::new(),
                    &[refusal],
                    HashMap::new(),
                ),
            ),
        },

        CommandActionType::AfterLeave(pending) => {
            if pending
                .hold((payload_id.to_string(), message.to_vec()))
                .is_some()
            {
                // Answered since it was classified: run it now.
                handle_command(runner, stream, payload_id, message);
            }
        }

        CommandActionType::RunnerDisconnect(handler) => {
            respond_with(
                stream,
                command::exec_control(runner, handler, payload_id, message, reference_id),
            );
            finish_disconnect(stream);
        }

        CommandActionType::AckSignal(result) => {
            // Suppress unused-variable warnings for unused params.
            let _ = runner;
            crate::shutdown::signal_result(&reference_id, result);
        }

        CommandActionType::NotificationIgnore => {
            let _ = runner;
            flz_transport::notice!(
                Warn,
                "Received an unexpected controller->SDK command (ref={reference_id}); ignoring."
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
            flz_transport::notice!(
                Warn,
                "control-channel frame failed to decode (stack={}); \
                 dropping and waiting for the read loop to drive reconnect.",
                stack
            );
            None
        }
        Err(CommandDecodeError::EmptyCommand) => {
            flz_transport::notice!(
                Warn,
                "control-channel frame had `command: None`; dropping. \
                 The next frame on this connection will be processed normally."
            );
            None
        }
    }
}

/// Write an encoded reply, logging a frame that could not be built or written.
fn respond_with(stream: &FlezhaStream, frame: std::io::Result<Vec<u8>>) {
    if let Err(e) = frame.and_then(|reply| write_reply(stream, reply)) {
        flz_transport::notice!(Error, "Failed to write command response: {}", e);
    }
}

/// A command reply on the control stream. The class refuses it rather than
/// holding it: a reply re-driven after its waiter timed out is a stale answer,
/// and the reconnect re-issues the request it belonged to.
fn write_reply(stream: &FlezhaStream, reply: Vec<u8>) -> std::io::Result<()> {
    crate::state_sink::write(crate::state_sink::Class::Control, Some(stream), reply).refused()
}

/// After the disconnect ack is written: a terminal disconnect ships its
/// state first (the notifications ride this channel), then only the write
/// half closes so the ack lands before the peer sees the close.
fn finish_disconnect(stream: &FlezhaStream) {
    let terminal = crate::shutdown::take_terminal_exit();
    if terminal.is_some() {
        crate::shutdown::run_active_graceful_shutdown();
    }
    let _ = stream.shutdown(Shutdown::Write);
    if let Some(reason) = terminal {
        crate::shutdown::exit(crate::shutdown::Exit::terminal(&reason));
    }
}

fn exec_control(
    runner: &mut RunnerNode,
    handler: Box<dyn CommandAction>,
    payload_id: &str,
    message: &[u8],
    reference_id: String,
) -> std::io::Result<Vec<u8>> {
    let mut errors: Vec<crate::Error> = vec![];
    let message_str = std::str::from_utf8(message).unwrap_or("");
    let (reply, metadata) = handler.reply(runner, message_str, &mut errors);
    encode_response(payload_id, reference_id, reply.bytes, &errors, metadata)
}

/// A `CommandResponse` frame under the request's payload id: `error` when any
/// refusal was answered, `success` otherwise.
fn encode_response(
    payload_id: &str,
    reference_id: String,
    raw_data: Vec<u8>,
    errors: &[crate::Error],
    metadata: HashMap<String, String>,
) -> std::io::Result<Vec<u8>> {
    let status = if errors.is_empty() {
        "success"
    } else {
        "error"
    };
    let command_response = CommandResponse {
        reference_id,
        status: status.to_string(),
        errors: errors
            .iter()
            .map(|error| crate::pb::Error::from(&error.to_error()))
            .collect(),
        raw_data,
    };
    payloader::build_from_binary(payload_id, &command_response.encode_to_vec(), metadata)
}

#[cfg(test)]
mod tests {
    use flz_transport::errors::FlezhaError;
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
                relay_id: "".to_string(),
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
        let reply = |outcome: &str, error: Option<&FlezhaError>, instance_id: &str| {
            let bytes = CommandRequest {
                reference_id: "ref-spawn".to_string(),
                command: Some(Command::SpawnStepsResponse(crate::pb::SpawnStepsResponse {
                    outcome: outcome.to_string(),
                    error: error.map(crate::pb::Error::from),
                    instance_id: instance_id.to_string(),
                })),
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
            reply("spawned", None, "worker-k7m2qzxw4pd3fn2a"),
            Ok(crate::spawn::encode_ack(
                "spawned",
                "worker-k7m2qzxw4pd3fn2a"
            ))
        );
        assert_eq!(
            reply("delivered", None, "w1"),
            Ok(crate::spawn::encode_ack("delivered", "w1"))
        );
        let quota = FlezhaError::with(
            "quota.exceeded",
            [
                ("kind", "max_relays".into()),
                ("current", 3.into()),
                ("limit", 3.into()),
            ],
        );
        assert_eq!(reply("", Some(&quota), ""), Err(quota));
    }

    /// Every command runs on the coordinator thread or, for credit, the
    /// thread that reads control frames; a panic in either ends the session
    /// without a word to the controller. The production half of this
    /// directory refuses instead.
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
