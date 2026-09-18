//! Workflow lifecycles of a host class: a stage's answer folds into the
//! driver's collect sink. The invariant: the driver's ladder and barriers stay
//! driver-side, so the host runs exactly one attempt of anything.

use std::any::Any;
use std::sync::{Arc, Mutex};

use super::entity::{BytesBuffer, HostPayload};
use crate::flusher::FlushableBuffer;
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
    if let Some(result) = effect.complete {
        sink.complete(result);
    }
    Ok(())
}
