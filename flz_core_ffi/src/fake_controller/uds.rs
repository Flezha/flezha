//! The UDS half of the fake controller: the control-socket peer, and the run
//! that drives a runner over it.
//!
//! Split from its parent only because the control channel and the data
//! streams are Unix domain sockets, exactly as `FlezhaStream::Unix` is. The
//! release builds this crate for a Windows triple too, and a smoke tier over
//! UDS has nothing to offer there.

use std::io::Write;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use flz_core::pb;
use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;
use flz_transport::utility::socket_dir;
use prost::Message;

use super::{Progress, Scenario};

/// A captured runner→controller frame: its payload id and its message bytes.
pub type CapturedFrame = (String, Vec<u8>);

type Frames = Arc<(Mutex<Vec<CapturedFrame>>, Condvar)>;

/// How long [`Controller::try_await_frame`] waits when no caller says
/// otherwise. A scenario that drives a runner in another process passes its
/// own.
const DEFAULT_AWAIT: Duration = Duration::from_secs(5);

/// The payload id the Elixir controller frames a command under, and therefore
/// the one a runner's reply comes back under.
const COMMAND_PAYLOAD_ID: &str = "control_command";

/// The controller end of the control channel: accepts, captures every frame
/// the runner writes, and holds the write half so a phase can push commands.
pub struct Controller {
    accepted: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
    frames: Frames,
    writer: Arc<Mutex<Option<FlezhaStream>>>,
    await_for: Duration,
}

impl Controller {
    pub fn start(dir: &Path) -> Self {
        let path = socket_dir::control_socket_path_in(dir).expect("control socket path");
        let listener = UnixListener::bind(path).expect("bind control socket");
        Self::over(listener, DEFAULT_AWAIT)
    }

    /// The same peer over a listener the caller bound, waiting `await_for` for
    /// each frame. The binary resolves and reports its own socket path, so it
    /// binds before it builds this.
    pub fn over(listener: UnixListener, await_for: Duration) -> Self {
        let controller = Self {
            accepted: Arc::new(AtomicUsize::new(0)),
            closed: Arc::new(AtomicUsize::new(0)),
            frames: Arc::new((Mutex::new(Vec::new()), Condvar::new())),
            writer: Arc::new(Mutex::new(None)),
            await_for,
        };
        let counter = Arc::clone(&controller.accepted);
        let closed = Arc::clone(&controller.closed);
        let frames = Arc::clone(&controller.frames);
        let writer_slot = Arc::clone(&controller.writer);
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(s) = stream else { break };
                counter.fetch_add(1, Ordering::SeqCst);
                let write_half = s.try_clone().expect("clone control stream");
                *writer_slot.lock().unwrap() = Some(FlezhaStream::Unix(write_half));
                let frames = Arc::clone(&frames);
                let closed = Arc::clone(&closed);
                thread::spawn(move || capture(FlezhaStream::Unix(s), frames, closed));
            }
        });
        controller
    }

    /// How many control connections have been accepted.
    pub fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    /// Wait until every accepted control connection has ended: the runner
    /// confirming it is gone rather than merely told to go. A tier that
    /// stopped at the order would exit while the runner was still shipping
    /// state, and take the socket with it.
    pub fn await_closed(&self, wait: Duration) -> Result<(), String> {
        let open = || self.accepted.load(Ordering::SeqCst) - self.closed.load(Ordering::SeqCst);
        self.try_await_frame_within("every control connection to close", wait, |_| {
            (open() == 0).then_some(())
        })
        .map_err(|_| {
            format!(
                "timed out after {wait:?} with {} control connection(s) still open; \
                 the runner was told to shut down and has not",
                open()
            )
        })
    }

    /// Push a controller→runner command down the accepted connection. A
    /// runner in another process can be gone by the time one is written, so
    /// every answer here is reported rather than raised: the binary turns a
    /// refusal into an exit code, and the crate's own scenario puts it in a
    /// test's voice.
    pub fn try_send(
        &self,
        reference_id: &str,
        command: pb::command_request::Command,
    ) -> std::io::Result<()> {
        let request = pb::CommandRequest {
            reference_id: reference_id.to_string(),
            command: Some(command),
        };
        let frame = payloader::build_from_binary(
            COMMAND_PAYLOAD_ID,
            &request.encode_to_vec(),
            Default::default(),
        )?;
        let mut guard = self.writer.lock().unwrap();
        let stream = guard
            .as_mut()
            .ok_or_else(|| std::io::Error::other("no control connection has been accepted yet"))?;
        (&*stream).write_all(&frame)
    }

    /// Wait until `probe` finds what it is looking for among the captured
    /// frames. The refusal names the frame that was awaited and what did
    /// arrive, because "timed out" on its own tells an SDK author nothing
    /// about which end stopped.
    pub fn try_await_frame<T>(
        &self,
        what: &str,
        probe: impl Fn(&[CapturedFrame]) -> Option<T>,
    ) -> Result<T, String> {
        self.try_await_frame_within(what, self.await_for, probe)
    }

    /// The same wait against a deadline of the caller's own, for a frame whose
    /// own side already bounds how long it will be waiting for an answer.
    pub fn try_await_frame_within<T>(
        &self,
        what: &str,
        wait: Duration,
        probe: impl Fn(&[CapturedFrame]) -> Option<T>,
    ) -> Result<T, String> {
        let (captured, arrived) = &*self.frames;
        let deadline = Instant::now() + wait;
        let mut frames = captured.lock().unwrap();
        loop {
            if let Some(found) = probe(&frames) {
                return Ok(found);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(format!(
                    "timed out after {wait:?} waiting for {what}; the runner sent {}",
                    describe(&frames)
                ));
            }
            frames = arrived.wait_timeout(frames, left).unwrap().0;
        }
    }

    /// Wait for the command reply carrying `reference_id`.
    pub fn try_await_reply(
        &self,
        what: &str,
        reference_id: &str,
    ) -> Result<pb::CommandResponse, String> {
        self.try_await_frame(what, |captured| {
            captured.iter().find_map(|(_, m)| {
                pb::CommandResponse::decode(m.as_slice())
                    .ok()
                    .filter(|r| r.reference_id == reference_id)
            })
        })
    }

    /// Write `command` and wait for its successful reply. `name` is the
    /// command as a refusal spells it.
    pub fn try_command(
        &self,
        name: &str,
        reference_id: &str,
        command: pb::command_request::Command,
    ) -> Result<(), String> {
        self.try_send(reference_id, command)
            .map_err(|e| format!("{name} could not be written: {e}"))?;
        self.try_await_success(&format!("the {name} reply"), reference_id)
    }

    /// Answer a spawn request the way the controller does: `spawned` under the
    /// request's own id, or under a generated `<factory>-<suffix>` when it
    /// carried none, or refused with `refusal`. Answers the id it named.
    pub fn try_answer_spawn(
        &self,
        reference_id: &str,
        request: &pb::SpawnWorkflowRequest,
        refusal: Option<&str>,
    ) -> std::io::Result<String> {
        let (outcome, error, id) = match refusal {
            Some(reason) => ("refused", reason.to_string(), request.instance_id.clone()),
            None if request.instance_id.is_empty() => {
                ("spawned", String::new(), generated_id(&request.factory_id))
            }
            None => ("spawned", String::new(), request.instance_id.clone()),
        };
        self.try_send(
            reference_id,
            pb::command_request::Command::SpawnWorkflowResponse(pb::SpawnWorkflowResponse {
                outcome: outcome.to_string(),
                error,
                instance_id: id.clone(),
            }),
        )?;
        Ok(id)
    }

    /// Wait for a SUCCESSFUL command reply carrying `reference_id`. A refusal
    /// carries the runner's own errors, which is the whole of what an SDK
    /// author needs to see.
    pub fn try_await_success(&self, what: &str, reference_id: &str) -> Result<(), String> {
        let reply = self.try_await_reply(what, reference_id)?;
        if reply.status == "success" {
            return Ok(());
        }
        Err(format!(
            "{what} was refused: status `{}`, errors {:?}",
            reply.status, reply.errors
        ))
    }
}

/// The `RunnerShutdownNotification` for `operative` in a captured frame, as
/// its reference id and serialized state. A graceful shutdown ships one per
/// live operative and blocks on the ack, so both the ABI scenario and the
/// smoke run read the frame through here rather than each decoding it.
pub fn shutdown_notification(
    payload_id: &str,
    message: &[u8],
    operative: &str,
) -> Option<(String, Vec<u8>)> {
    match command_in(payload_id, message)? {
        (reference_id, pb::command_request::Command::RunnerShutdownNotification(n))
            if n.operative_id == operative =>
        {
            Some((reference_id, n.serialized_state))
        }
        _ => None,
    }
}

/// The `SpawnWorkflowRequest` in a captured frame, with its reference id.
pub fn spawn_request(
    payload_id: &str,
    message: &[u8],
) -> Option<(String, pb::SpawnWorkflowRequest)> {
    match command_in(payload_id, message)? {
        (reference_id, pb::command_request::Command::SpawnWorkflowRequest(request)) => {
            Some((reference_id, request))
        }
        _ => None,
    }
}

/// The command a runner→controller frame carries, with its reference id.
fn command_in(payload_id: &str, message: &[u8]) -> Option<(String, pb::command_request::Command)> {
    if payload_id != COMMAND_PAYLOAD_ID {
        return None;
    }
    let request = pb::CommandRequest::decode(message).ok()?;
    Some((request.reference_id, request.command?))
}

/// An id shaped like the ones the controller generates: the factory, a dash
/// and 16 lowercase base32 characters. Distinct per call, not random.
fn generated_id(factory_id: &str) -> String {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let seed = flz_core::util::nonce_suffix();
    let suffix: String = (0..16)
        .map(|i| char::from(ALPHABET[((seed >> (5 * i)) & 31) as usize]))
        .collect();
    format!("{factory_id}-{suffix}")
}

/// What the runner has sent so far, for a refusal to name.
fn describe(frames: &[CapturedFrame]) -> String {
    if frames.is_empty() {
        return "nothing at all".to_string();
    }
    let ids: Vec<&str> = frames.iter().map(|(id, _)| id.as_str()).collect();
    format!("{ids:?}")
}

fn capture(stream: FlezhaStream, frames: Frames, closed: Arc<AtomicUsize>) {
    let (captured, arrived) = &*frames;
    while let Ok((payload_id, message, _metadata)) = payloader::extract_payload(&stream) {
        captured.lock().unwrap().push((payload_id, message));
        arrived.notify_all();
    }
    // Under the frames lock, so a waiter cannot read the count between the
    // increment and the notify and sleep through its own answer.
    let _held = captured.lock().unwrap();
    closed.fetch_add(1, Ordering::SeqCst);
    arrived.notify_all();
}

impl Scenario {
    /// Drive one runner from connect to shutdown. `Ok` means the runner
    /// connected, started the class, produced `expect_outputs` outputs and
    /// acknowledged the disconnect; `Err` names the frame that was awaited.
    pub fn run(&self, report: Progress<'_>) -> Result<(), String> {
        let control = socket_dir::control_socket_path_in(&self.socket_dir)
            .map_err(|e| format!("control socket path unresolved: {e}"))?;
        let listener =
            UnixListener::bind(&control).map_err(|e| format!("bind {control} failed: {e}"))?;
        report(&format!("listening on {control}"));

        // Bound before `ConnectSocket` is sent: the runner dials them the
        // moment it handles that command.
        let primary = self.bind_data("primary")?;
        let secondary = self.bind_data("secondary")?;

        let controller = Controller::over(listener, self.timeout);
        report("awaiting the runner's connect frame");
        let factories = controller.try_await_frame("the connect frame", |captured| {
            captured
                .iter()
                .find(|(id, _)| id == "connect_control_channel")
                .map(|(_, m)| String::from_utf8_lossy(m).into_owned())
        })?;
        if !factories.contains(&self.factory_id) {
            return Err(format!(
                "the runner connected without declaring `{}`; it declared {factories}",
                self.factory_id
            ));
        }
        report(&format!("connected, declaring `{}`", self.factory_id));

        self.handshake(&controller, report)?;
        // In the order the runner opens them, and never for ever.
        let primary = accept_within(primary, "primary", self.timeout)?;
        let secondary = accept_within(secondary, "secondary", self.timeout)?;
        report("data streams accepted");

        self.start_operative(&controller, report)?;
        self.feed(&primary, report)?;
        let seen = self.count_outputs(secondary, report)?;
        report(&format!("{seen} outputs seen"));
        self.disconnect(&controller, report)
    }

    fn bind_data(&self, half: &str) -> Result<UnixListener, String> {
        let path = socket_dir::stream_socket_path_in(&self.socket_dir, &self.runner_id, half)
            .map_err(|e| format!("{half} stream path unresolved: {e}"))?;
        UnixListener::bind(&path).map_err(|e| format!("bind {path} failed: {e}"))
    }

    /// `AssignRunnerId` then `ConnectSocket`, the order the controller's own
    /// session module pins.
    fn handshake(&self, controller: &Controller, report: Progress<'_>) -> Result<(), String> {
        controller.try_command(
            "AssignRunnerId",
            "smoke-assign",
            pb::command_request::Command::AssignRunnerId(pb::AssignRunnerId {
                runner_id: self.runner_id.clone(),
            }),
        )?;
        report(&format!("runner id assigned: {}", self.runner_id));

        controller.try_command(
            "ConnectSocket",
            "smoke-connect",
            pb::command_request::Command::ConnectSocket(pb::ConnectSocket { sockets_count: 1 }),
        )
    }

    fn start_operative(&self, controller: &Controller, report: Progress<'_>) -> Result<(), String> {
        controller.try_command(
            "AddOperative",
            "smoke-add",
            pb::command_request::Command::AddOperative(pb::AddOperative {
                operative_id: self.operative_id.clone(),
                factory_id: self.factory_id.clone(),
                ..Default::default()
            }),
        )?;
        report(&format!("`{}` started", self.operative_id));
        Ok(())
    }

    /// One framed input per payload on the primary stream. The payload id is
    /// the operative id (what the runner routes by), and the channel rides
    /// the metadata, exactly as the controller's own ingest builds it.
    fn feed(&self, primary: &UnixStream, report: Progress<'_>) -> Result<(), String> {
        let metadata: payloader::PayloadMetadata = self
            .channel
            .iter()
            .map(|c| ("channel".to_string(), c.clone()))
            .collect();
        for payload in &self.inputs {
            let frame = payloader::build_from_binary(&self.operative_id, payload, metadata.clone())
                .map_err(|e| format!("an input frame could not be built: {e}"))?;
            (&*primary)
                .write_all(&frame)
                .map_err(|e| format!("an input frame could not be written: {e}"))?;
        }
        report(&format!("{} inputs fed", self.inputs.len()));
        Ok(())
    }

    /// Count output frames on the secondary stream. An output is a frame
    /// under the operative id; the heartbeat and the record frames beside it
    /// carry their own payload ids and are drained.
    fn count_outputs(&self, secondary: UnixStream, report: Progress<'_>) -> Result<usize, String> {
        if self.expect_outputs == 0 {
            return Ok(0);
        }
        report(&format!("awaiting {} outputs", self.expect_outputs));
        secondary
            .set_read_timeout(Some(self.timeout))
            .map_err(|e| format!("the secondary stream would not take a timeout: {e}"))?;
        let stream = FlezhaStream::Unix(secondary);
        let deadline = Instant::now() + self.timeout;
        let mut seen = 0;
        let mut other: Vec<String> = Vec::new();
        while seen < self.expect_outputs {
            if Instant::now() >= deadline {
                return Err(format!(
                    "timed out after {:?} with {seen} of {} outputs; the runner sent {other:?}",
                    self.timeout, self.expect_outputs
                ));
            }
            match payloader::extract_payload(&stream) {
                Ok((payload_id, _, _)) if payload_id == self.operative_id => seen += 1,
                Ok((payload_id, _, _)) => other.push(payload_id),
                Err(e) => {
                    return Err(format!(
                        "the secondary stream ended after {seen} of {} outputs: {e}",
                        self.expect_outputs
                    ))
                }
            }
        }
        Ok(seen)
    }

    /// A terminal disconnect: the runner runs its graceful shutdown, answers,
    /// and ends. Terminal rather than cooperative because a smoke tier wants
    /// the runner gone when it is over, not left listening for a controller
    /// that will not come back. Its exit status is the orchestrator's signal
    /// to restart and is deliberately non-zero, so the tier's verdict is this
    /// binary's exit code and never the runner's.
    fn disconnect(&self, controller: &Controller, report: Progress<'_>) -> Result<(), String> {
        controller.try_command(
            "RunnerDisconnect",
            "smoke-disconnect",
            pb::command_request::Command::RunnerDisconnect(pb::RunnerDisconnect {
                terminal: true,
                reason: "the smoke tier is done".to_string(),
            }),
        )?;
        self.acknowledge_shipped_state(controller, report);
        controller.await_closed(self.timeout)?;
        report("the runner shut down");
        Ok(())
    }

    /// A graceful shutdown ships the operative's state and blocks on the ack.
    /// Answering it ends the run at the runner's pace instead of at its
    /// timeout; a class with no state to ship sends nothing, and that is not
    /// a failure, so the wait is bounded by the runner's own and its
    /// expiry is reported rather than returned.
    fn acknowledge_shipped_state(&self, controller: &Controller, report: Progress<'_>) {
        let found = controller.try_await_frame_within(
            "the shutdown notification",
            flz_core::shutdown::ACK_TIMEOUT,
            |captured| {
                captured
                    .iter()
                    .find_map(|(id, m)| shutdown_notification(id, m, &self.operative_id))
            },
        );
        let Ok((reference_id, state)) = found else {
            report("no state was shipped on shutdown");
            return;
        };
        report(&format!("{} bytes of state shipped", state.len()));
        if let Err(e) = controller.try_send(
            &reference_id,
            pb::command_request::Command::RunnerShutdownAck(pb::RunnerShutdownAck {
                operative_id: self.operative_id.clone(),
            }),
        ) {
            report(&format!("the shutdown ack could not be written: {e}"));
        }
    }
}

/// One connection the runner dials, without blocking for ever on one that
/// never does.
fn accept_within(
    listener: UnixListener,
    half: &str,
    timeout: Duration,
) -> Result<UnixStream, String> {
    let accepted = Arc::new((Mutex::new(None), Condvar::new()));
    let slot = Arc::clone(&accepted);
    thread::spawn(move || {
        let taken = listener.accept().map(|(stream, _)| stream);
        let (held, arrived) = &*slot;
        *held.lock().unwrap() = Some(taken);
        arrived.notify_all();
    });
    let (held, arrived) = &*accepted;
    let (mut guard, waited) = arrived
        .wait_timeout_while(held.lock().unwrap(), timeout, |taken| taken.is_none())
        .unwrap();
    if waited.timed_out() {
        return Err(format!(
            "timed out after {timeout:?} waiting for the runner to dial the {half} stream"
        ));
    }
    guard
        .take()
        .expect("the waiter only returns once the slot is filled")
        .map_err(|e| format!("accepting the {half} stream failed: {e}"))
}
