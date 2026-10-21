//! A host's runner session, the one every SDK drives: the factory set it
//! declares, the start and its bounded wait for the controller's answer, and a
//! factory change after start, which reaches the controller as
//! `UpdateFactories`.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};
use std::time::Duration;

use crossbeam::channel::{Receiver, Sender};
use flz_transport::errors::{ErrorModel, FlezhaError};

use crate::runner::{ClientResponseMessage, RemovalReply, RunnerMessage};
use crate::{pb, Code};

/// The setting naming how long a start waits for the controller's answer.
const START_WAIT_VAR: &str = "FLEZHA_RUNNER_START_WAIT_MS";

const DEFAULT_START_WAIT_MS: u64 = 10_000;

/// How long a start waits for the controller's answer, and an undeclare after
/// start for the coordinator's: [`START_WAIT_VAR`] as the host handed it over,
/// else as the environment holds it, 10 s by default; `None`, from `0`, waits
/// for ever.
fn start_wait() -> Option<Duration> {
    let ms = flz_transport::env::connect_raw(START_WAIT_VAR)
        .and_then(|raw| flz_transport::env::parse_ms(&raw))
        .unwrap_or(DEFAULT_START_WAIT_MS);
    (ms > 0).then(|| Duration::from_millis(ms))
}

/// What a session refuses.
#[derive(Debug)]
pub enum SessionError {
    /// The factory id is already declared on this session.
    DuplicateFactoryId(String),
    /// An undeclare of a factory this session never declared.
    NotDefined(String),
    /// Live instances of the factory exist; it stays declared.
    RelayTypeInUse {
        factory_id: String,
        live_instances: usize,
    },
    AlreadyStarted,
    /// The runner thread ended before the controller answered.
    ConnectFailed(&'static str),
    /// The controller refused the connect, under its own code.
    Refused(FlezhaError),
    /// No answer within the start wait.
    Unconfirmed {
        wait_ms: u64,
    },
    /// The runner thread is gone or did not answer within the wait.
    Unavailable(&'static str),
}

impl ErrorModel for SessionError {
    fn to_error(&self) -> FlezhaError {
        match self {
            SessionError::DuplicateFactoryId(id) => {
                Code::DuplicateFactoryId.with([("factory_id", id.as_str().into())])
            }
            SessionError::NotDefined(id) => {
                Code::ClassNotRegistered.with([("factory", id.as_str().into())])
            }
            SessionError::RelayTypeInUse {
                factory_id,
                live_instances,
            } => Code::RelayTypeInUse.with([
                ("factory_id", factory_id.as_str().into()),
                ("live_instances", (*live_instances).into()),
            ]),
            SessionError::AlreadyStarted => Code::RunnerAlreadyRunning.with([]),
            SessionError::ConnectFailed(reason) => {
                Code::ControllerUnreachable.with([("reason", (*reason).into())])
            }
            SessionError::Refused(error) => error.clone(),
            SessionError::Unconfirmed { wait_ms } => {
                Code::ConnectUnconfirmed.with([("wait_ms", (*wait_ms).into())])
            }
            SessionError::Unavailable(reason) => {
                Code::RunnerUnavailable.with([("reason", (*reason).into())])
            }
        }
    }
}

flz_transport::model_error!(SessionError);

/// One runner's session. The runner thread serializes `declared` into every
/// connect and reconnect init frame, the resync of record. `started` holds the
/// runner thread's channel once a start was confirmed.
#[derive(Default)]
pub struct Session {
    declared: Arc<RwLock<Vec<String>>>,
    started: Mutex<Option<Sender<RunnerMessage>>>,
}

impl Session {
    pub fn is_started(&self) -> bool {
        self.started().is_some()
    }

    /// Declare `id` once `register` has registered its class, refusing an id
    /// already declared; after start the controller is sent the full set.
    pub fn declare<E: From<SessionError>>(
        &self,
        id: &str,
        register: impl FnOnce() -> Result<(), E>,
    ) -> Result<(), E> {
        {
            let mut declared = self
                .declared
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            if declared.iter().any(|d| d == id) {
                return Err(SessionError::DuplicateFactoryId(id.to_string()).into());
            }
            register()?;
            declared.push(id.to_string());
        }
        self.sync_factories();
        Ok(())
    }

    /// Undeclare `id` and unregister its class. After start the coordinator
    /// checks for live instances, serialized with `AddRelay` so the check
    /// cannot race a bind, and the controller is sent the remaining set.
    pub fn undeclare(&self, id: &str) -> Result<(), SessionError> {
        let known = (self.declared.read().unwrap_or_else(PoisonError::into_inner))
            .iter()
            .any(|d| d == id);
        if !known {
            return Err(SessionError::NotDefined(id.to_string()));
        }
        // Cloned out, so no session lock is held while the coordinator answers.
        let runner_tx = self.started().clone();
        match runner_tx {
            Some(runner_tx) => remove_factory(&runner_tx, id)?,
            None => crate::relay::registry::unregister_class(id),
        }
        self.declared
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|d| d != id);
        self.sync_factories();
        Ok(())
    }

    /// Connect with the connection settings a host hands over in place of the
    /// environment (`flz_transport::env::CONNECT_SETTINGS`; none clears an
    /// earlier start's), and wait for the controller's answer for
    /// `FLEZHA_RUNNER_START_WAIT_MS`. A start that fails leaves no runner
    /// dialling behind it, so the host may start again in this process.
    pub fn start(
        &self,
        namespace: &str,
        settings: Vec<(&'static str, String)>,
    ) -> Result<(), SessionError> {
        let mut started = self.started();
        if started.is_some() {
            return Err(SessionError::AlreadyStarted);
        }
        flz_transport::env::hand_over_connect_settings(settings);
        let (runner_tx, runner_rx) = crossbeam::channel::unbounded();
        let (reply_tx, replies) = crossbeam::channel::unbounded();
        let bridge = crate::shutdown::ShutdownBridge::new();
        crate::shutdown::register_bridge(Arc::clone(&bridge));
        // The coordinator channel, process-globally, so the cell-state sink
        // can drive a reconnect without a handle threaded through every layer.
        // Installed before the thread starts, so a refusal below withdraws it.
        crate::runner::install_reconnect_trigger(runner_tx.clone());
        crate::runner::start(
            namespace.to_string(),
            Arc::clone(&self.declared),
            (runner_tx.clone(), runner_rx),
            reply_tx,
            bridge,
        );
        if let Err(refused) = await_confirmation(&replies, start_wait()) {
            crate::control_unit::abandon_starts();
            let _ = runner_tx.send(RunnerMessage::Abandon);
            // A channel nobody reads: a request asked now answers unavailable at once.
            crate::runner::install_reconnect_trigger(crossbeam::channel::unbounded().0);
            return Err(refused);
        }
        *started = Some(runner_tx);
        Ok(())
    }

    /// End the session the host holds: an undeclare after it unregisters
    /// locally. The runner thread is left to whoever stops it.
    pub fn end(&self) {
        self.started().take();
    }

    fn started(&self) -> MutexGuard<'_, Option<Sender<RunnerMessage>>> {
        self.started.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Ship the full declared set after a post-start change. Fire and forget:
    /// the set is whole, so a redelivery changes nothing, and a lost frame is
    /// corrected by the next reconnect's init frame.
    fn sync_factories(&self) {
        let started = self.started();
        let Some(runner_tx) = started.as_ref() else {
            return;
        };
        let factories = crate::relay::registry::advertised(
            &self.declared.read().unwrap_or_else(PoisonError::into_inner),
        );
        let _ = runner_tx.send(RunnerMessage::control_request(
            format!("update-factories-{}", crate::util::nonce_suffix()),
            pb::command_request::Command::UpdateFactories(pb::UpdateFactories { factories }),
        ));
    }
}

/// Ask the coordinator to remove `id`, holding no session lock while it
/// answers, for at most the start wait: a coordinator that stays busy (a
/// controller outage it is reconnecting through) must not hold a host thread
/// for as long. A caller that gives up claims the reply back first, so the
/// removal it was refused is never applied after it.
fn remove_factory(runner_tx: &Sender<RunnerMessage>, id: &str) -> Result<(), SessionError> {
    const GONE: SessionError = SessionError::Unavailable("runner thread exited");
    let (answer_tx, answer) = crossbeam::channel::bounded(1);
    let reply = RemovalReply::new(Some(answer_tx));
    let removal = RunnerMessage::RemoveFactory {
        factory_id: id.to_string(),
        reply: reply.clone(),
    };
    runner_tx.send(removal).map_err(|_| GONE)?;
    let answered = match start_wait() {
        Some(wait) => answer.recv_timeout(wait).ok(),
        None => answer.recv().ok(),
    };
    let answered = match answered {
        Some(result) => result,
        None if reply.claim().is_some() => {
            return Err(SessionError::Unavailable(
                "no answer to the removal within FLEZHA_RUNNER_START_WAIT_MS",
            ))
        }
        // Claimed by the coordinator, which is answering now.
        None => answer.recv().map_err(|_| GONE)?,
    };
    answered.map_err(|live_instances| SessionError::RelayTypeInUse {
        factory_id: id.to_string(),
        live_instances,
    })
}

/// The runner thread's answer to a start, waiting `wait` (`None`: for ever).
/// A closed channel means the thread died before answering: a connect failure
/// the caller handles, never a panic.
fn await_confirmation(
    replies: &Receiver<ClientResponseMessage>,
    wait: Option<Duration>,
) -> Result<(), SessionError> {
    let reply = match wait {
        None => replies.recv().map_err(|_| None),
        Some(wait) => replies
            .recv_timeout(wait)
            .map_err(|e| e.is_timeout().then_some(wait)),
    };
    match reply {
        Ok(ClientResponseMessage::RunnerIdConfirmation) => Ok(()),
        Ok(ClientResponseMessage::ConnectRefused(error)) => Err(SessionError::Refused(error)),
        Err(Some(wait)) => Err(SessionError::Unconfirmed {
            wait_ms: wait.as_millis() as u64,
        }),
        Err(None) => Err(SessionError::ConnectFailed(
            "the runner thread exited before confirming the connection",
        )),
    }
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
