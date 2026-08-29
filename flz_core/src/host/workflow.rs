//! Workflow lifecycles of a host class: a run-sugar body runs as one
//! `WorkflowRun` activation with a live session the ABI's `recv` / `cancelled`
//! resolve against, and a structured stage's answer folds into the driver's
//! collect sink. The invariant: the driver's ladder and barriers stay
//! driver-side, so the host runs exactly one attempt of anything.

use std::any::Any;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use super::entity::{BytesBuffer, HostPayload};
use crate::flusher::FlushableBuffer;
use crate::poll_core::EnginePayload;
use crate::schema;
use crate::util::LockRecover;
use crate::workflow::{WorkflowCollectSink, WorkflowError};

/// Map one workflow step's work outcome into the driver's collect sink: changes
/// into that entity's flush buffer, edge outputs collected for the next stage,
/// plugin outputs staged/forwarded by the sink. Runs on the thread that
/// completed the step, with its driver parked at the barrier the fold's result
/// answers.
pub fn fold_workflow_outcome(
    flush_buffer: &Mutex<Box<dyn FlushableBuffer>>,
    outcome: schema::ActivationOutcome,
    sink: &mut dyn WorkflowCollectSink,
) -> Result<(), WorkflowError> {
    let effects = match outcome {
        schema::ActivationOutcome::Work(effects) => effects,
        schema::ActivationOutcome::Failed(err) => {
            return Err(WorkflowError::new(err.message, err.non_retryable));
        }
        _ => {
            return Err(WorkflowError::from(
                "workflow step answered with a non-work outcome".to_string(),
            ));
        }
    };
    for effect in effects {
        fold_effect(flush_buffer, effect, sink)?;
    }
    Ok(())
}

/// One item's effect into the buffer and the sink; an error verdict is the
/// step's failure.
fn fold_effect(
    flush_buffer: &Mutex<Box<dyn FlushableBuffer>>,
    effect: schema::ItemEffect,
    sink: &mut dyn WorkflowCollectSink,
) -> Result<(), WorkflowError> {
    match effect.verdict {
        schema::Verdict::Ok => {}
        schema::Verdict::RetryableError { message } => {
            return Err(WorkflowError::new(message, false));
        }
        schema::Verdict::NonRetryableError { message } => {
            return Err(WorkflowError::new(message, true));
        }
    }
    if !effect.changes.is_empty() {
        let mut buffer = flush_buffer.lock_recover();
        if let Some(bytes) = buffer.as_any_mut().downcast_mut::<BytesBuffer>() {
            bytes.changes.extend(effect.changes);
        }
    }
    for output in effect.outputs {
        match output.target {
            schema::OutputTarget::Edge { channel, .. } => {
                sink.collect(Arc::new(HostPayload {
                    bytes: output.payload,
                    channel,
                    keys: output.key.into_iter().collect(),
                    event_time: output.event_time,
                    metadata: output.metadata,
                }) as Arc<dyn Any + Send + Sync>);
            }
            schema::OutputTarget::Plugin { channel } => {
                sink.emit_plugin_bytes(&channel, output.payload);
            }
        }
    }
    Ok(())
}

/// Live run-sugar bodies keyed by instance id — what the ABI's workflow
/// session calls (`emit` / `recv` / `cancelled`) resolve against while the
/// host runs the imperative body.
static WORKFLOW_SESSIONS: std::sync::LazyLock<dashmap::DashMap<String, Arc<HostWorkflowSession>>> =
    std::sync::LazyLock::new(dashmap::DashMap::new);

pub struct HostWorkflowSession {
    cancel: Arc<std::sync::atomic::AtomicBool>,
    inbox: Mutex<crossbeam::channel::Receiver<crate::workflow::instance::InboxItem>>,
}

/// The ABI's `flz_core_workflow_recv`: block up to `timeout` for the next
/// inbox item of a live run-sugar body. `Ok(None)` = timeout; `Err` = no
/// such session / inbox closed.
pub fn workflow_session_recv(
    instance_id: &str,
    timeout: std::time::Duration,
) -> Result<Option<Vec<u8>>, String> {
    let session = WORKFLOW_SESSIONS
        .get(instance_id)
        .map(|s| Arc::clone(s.value()))
        .ok_or_else(|| format!("no live workflow run body for instance `{instance_id}`"))?;
    let inbox = { session.inbox.lock_recover() };
    match inbox.recv_timeout(timeout) {
        Ok(item) => Ok(Some(item.payload)),
        Err(crossbeam::channel::RecvTimeoutError::Timeout) => Ok(None),
        Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
            Err("workflow inbox closed".to_string())
        }
    }
}

/// The ABI's `flz_core_workflow_cancelled`.
pub fn workflow_session_cancelled(instance_id: &str) -> bool {
    WORKFLOW_SESSIONS
        .get(instance_id)
        .map(|s| s.cancel.load(Ordering::Acquire))
        .unwrap_or(true)
}

/// What a run-sugar body answered, for the driver's `classify`.
type SugarOutcome = (std::thread::Result<Result<(), WorkflowError>>, Vec<u8>);

/// Drive one host run-sugar body: register the session, cross the seed as a
/// [`schema::ActivationKind::WorkflowRun`] activation, and block for the
/// terminal completion — exactly as long as a typed `run` body would run.
/// Returns `(body outcome, result payload)` for the driver's `classify`.
pub fn run_workflow_sugar(
    instance_id: &Arc<str>,
    factory_id: &str,
    args: &str,
    cancel: &Arc<std::sync::atomic::AtomicBool>,
    inbox: crossbeam::channel::Receiver<crate::workflow::instance::InboxItem>,
) -> SugarOutcome {
    WORKFLOW_SESSIONS.insert(
        instance_id.to_string(),
        Arc::new(HostWorkflowSession {
            cancel: Arc::clone(cancel),
            inbox: Mutex::new(inbox),
        }),
    );
    let (reply, rx) = crossbeam::channel::bounded(1);
    crate::poll_core::global().submit_direct(EnginePayload::Command {
        target: schema::EntityRef {
            operative: instance_id.to_string(),
            procedure: 0,
            entity_key: None,
        },
        change_seq: 0,
        factory: factory_id.to_string(),
        kind: schema::ActivationKind::WorkflowRun {
            args: args.as_bytes().to_vec(),
        },
        reply,
    });
    let outcome = rx.recv();
    WORKFLOW_SESSIONS.remove(instance_id.as_ref());
    session_outcome(outcome)
}

/// The body's terminal answer — or the failure that stands in for one when
/// the host answered with something else or dropped the activation.
fn session_outcome(
    outcome: Result<schema::ActivationOutcome, crossbeam::channel::RecvError>,
) -> SugarOutcome {
    match outcome {
        Ok(schema::ActivationOutcome::WorkflowDone { result, .. }) => (Ok(Ok(())), result),
        Ok(schema::ActivationOutcome::Failed(err)) => (
            Ok(Err(WorkflowError::new(err.message, err.non_retryable))),
            Vec::new(),
        ),
        Ok(_) => (
            Ok(Err(WorkflowError::from(
                "workflow run answered with a non-terminal outcome",
            ))),
            Vec::new(),
        ),
        Err(_) => (
            Ok(Err(WorkflowError::from(
                "host context dropped the workflow run activation",
            ))),
            Vec::new(),
        ),
    }
}
