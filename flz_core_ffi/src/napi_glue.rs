//! N-API bindings over this crate's C ABI for the Node SDK's native binding,
//! one export per C entry point.
//!
//! A `worker_thread` is a separate isolate with its own module instance, so
//! the one context lives in [`CONTEXT`], a process global every isolate
//! reaches. The event-loop rule is [`off_loop`]: `flz_core_poll` is never
//! exported (the fetchers in [`crate::napi_host`] drive it), and every other
//! blocking entry point answers a promise resolved from the host's own
//! growable pool — under the default overflow policy a completion parks
//! until the consumer's mailbox drains, and the consumer drains only when its
//! handler runs on an event loop. Payload `Vec`s move into `Buffer`s without
//! copying.

use std::sync::atomic::{AtomicPtr, Ordering};

use flz_core::schema;
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::veneer::{self, CtxPtr, Refusal};
use crate::FlzContext;

/// The one context this process holds. Null until `contextNew`.
static CONTEXT: AtomicPtr<FlzContext> = AtomicPtr::new(std::ptr::null_mut());

/// The one mapping of a refusal onto a JavaScript error: a value the binding
/// could not map is an argument error, everything else a generic failure.
impl From<Refusal> for Error {
    fn from(refusal: Refusal) -> Self {
        match refusal {
            Refusal::Core(message) | Refusal::Schema(message) => {
                Error::new(Status::GenericFailure, message)
            }
            Refusal::Shape(message) => Error::new(Status::InvalidArg, message),
        }
    }
}

/// The live context, or an error naming what the caller skipped.
pub(crate) fn context() -> Result<CtxPtr> {
    let ptr = CONTEXT.load(Ordering::Acquire);
    if ptr.is_null() {
        return Err(Error::new(
            Status::InvalidArg,
            "flz_core context not created: call contextNew first",
        ));
    }
    Ok(CtxPtr::of(ptr))
}

/// Run one blocking ABI call off the event loop, and answer a promise with it.
///
/// The single place the event-loop rule is implemented. Every entry point that
/// can park goes through here, so "no blocking core call runs on a JavaScript
/// thread" is a property of one function rather than of four correct copies —
/// and the pool the work lands on is the growable one, which is what keeps a
/// parked completion from exhausting it.
fn off_loop<'a, T, F>(env: &'a Env, ctx: CtxPtr, work: F) -> Result<Object<'a>>
where
    T: ToNapiValue + 'static,
    F: FnOnce(CtxPtr) -> Result<T> + Send + 'static,
{
    let (deferred, promise) = env.create_deferred::<T, _>()?;
    crate::napi_host::blocking_pool().submit(move || match work(ctx) {
        Ok(value) => deferred.resolve(move |_| Ok(value)),
        Err(failure) => deferred.reject(failure),
    });
    Ok(promise)
}

// ---------------------------------------------------------------------------
// The decoded activation, as JavaScript sees it.
//
// The veneer decodes postcard on the Rust side (`schema::decode_activation`)
// so the SDK never carries a second implementation of the wire contract. The
// shape is flat with a `kind` discriminator rather than a union, because napi
// object mapping has no sum type and a discriminated record is what JavaScript
// switches on anyway.
// ---------------------------------------------------------------------------

/// One metadata pair. An ordered list rather than a map: the schema allows a
/// repeated key and the order is part of what the controller sees.
#[napi(object)]
pub struct JsMeta {
    pub key: String,
    pub value: String,
}

#[napi(object)]
pub struct JsWorkItem {
    /// `"input"` or `"timer"`.
    pub kind: String,
    pub channel: Option<u32>,
    pub payload: Option<Buffer>,
    pub metadata: Option<Vec<JsMeta>>,
    pub consumed_offset: Option<BigInt>,
    pub replay: Option<bool>,
    /// The core-evaluated declared clock for this item; what `ctx.now()` serves.
    pub now: Option<BigInt>,
    /// `"event_time"` or `"processing_time"` on a timer item.
    pub timer_kind: Option<String>,
    pub scheduled_at: Option<BigInt>,
}

#[napi(object)]
pub struct JsRestoreBody {
    pub bytes: Buffer,
    /// Absent when the body was written at the running schema version.
    pub from_version: Option<u32>,
}

#[napi(object)]
pub struct JsKeyRequest {
    pub channel: u32,
    pub payloads: Vec<Buffer>,
    pub want_keys: bool,
    pub want_event_time: bool,
}

#[napi(object)]
pub struct JsActivation {
    pub token: BigInt,
    pub operative: String,
    pub procedure: u32,
    pub entity_key: Option<String>,
    pub change_seq: BigInt,
    pub factory: String,
    /// `work` | `restore_fold` | `snapshot_request` | `live_state_read` |
    /// `apply_state` | `migration_serialize` | `key_request` | `workflow_run` |
    /// `compensate_run`.
    pub kind: String,
    pub items: Option<Vec<JsWorkItem>>,
    pub snapshot: Option<JsRestoreBody>,
    pub changes: Option<Vec<JsRestoreBody>>,
    pub state_json: Option<Buffer>,
    pub key_request: Option<JsKeyRequest>,
    pub args: Option<Buffer>,
    pub outputs: Option<Vec<Buffer>>,
}

#[napi(object)]
pub struct JsOutput {
    /// `"edge"` or `"plugin"`.
    pub target: String,
    pub channel: Option<u32>,
    pub consumer: Option<u32>,
    pub plugin_channel: Option<String>,
    pub key: Option<String>,
    pub event_time: Option<BigInt>,
    pub metadata: Option<Vec<JsMeta>>,
    pub payload: Buffer,
}

#[napi(object)]
pub struct JsTimerOp {
    /// `"event_time"` or `"processing_time"`.
    pub kind: String,
    pub scheduled_at: BigInt,
    pub register: bool,
}

#[napi(object)]
pub struct JsDlqEntry {
    pub channel: String,
    pub error: String,
    pub attempts: u32,
    pub payload: Option<Buffer>,
}

#[napi(object)]
pub struct JsItemEffect {
    /// `"ok"` | `"retryable"` | `"non_retryable"`.
    pub verdict: String,
    pub message: Option<String>,
    pub changes: Option<Vec<Buffer>>,
    pub outputs: Option<Vec<JsOutput>>,
    pub consumed_offset: Option<BigInt>,
    pub event_time: BigInt,
    pub timers: Option<Vec<JsTimerOp>>,
    pub checkpoint: Option<bool>,
    pub dlq: Option<JsDlqEntry>,
}

#[napi(object)]
pub struct JsPrepareAnswer {
    pub keys: Option<Vec<String>>,
    pub event_time: Option<BigInt>,
    /// Present when this payload's prepare failed; the input's retry ladder
    /// takes it from here.
    pub error: Option<String>,
    pub non_retryable: Option<bool>,
}

#[napi(object)]
pub struct JsActivationResult {
    pub token: BigInt,
    /// `work` | `restored` | `snapshot` | `state_json` | `state_applied` |
    /// `migration_state` | `prepared` | `failed` | `workflow_done`.
    pub outcome: String,
    pub effects: Option<Vec<JsItemEffect>>,
    pub state: Option<Buffer>,
    pub json: Option<Buffer>,
    pub prepared: Option<Vec<JsPrepareAnswer>>,
    pub error: Option<String>,
    pub non_retryable: Option<bool>,
    pub workflow_completed: Option<bool>,
    pub workflow_result: Option<Buffer>,
}

fn big(v: u64) -> BigInt {
    BigInt::from(v)
}

fn from_big(what: &str, v: &BigInt) -> Result<u64> {
    let (_, value, lossless) = v.get_u64();
    if !lossless {
        return Err(Error::new(
            Status::InvalidArg,
            format!("{what} does not fit an unsigned 64-bit integer"),
        ));
    }
    Ok(value)
}

fn meta_out(pairs: Vec<(String, String)>) -> Vec<JsMeta> {
    pairs
        .into_iter()
        .map(|(key, value)| JsMeta { key, value })
        .collect()
}

fn meta_in(pairs: Option<Vec<JsMeta>>) -> Vec<(String, String)> {
    pairs
        .unwrap_or_default()
        .into_iter()
        .map(|m| (m.key, m.value))
        .collect()
}

fn timer_kind_out(kind: schema::TimerKind) -> String {
    kind.as_str().to_string()
}

fn timer_kind_in(kind: &str) -> Result<schema::TimerKind> {
    match kind {
        "event_time" => Ok(schema::TimerKind::EventTime),
        "processing_time" => Ok(schema::TimerKind::ProcessingTime),
        other => Err(Error::new(
            Status::InvalidArg,
            format!("unknown timer kind {other:?}"),
        )),
    }
}

/// Decode a polled activation into the shape JavaScript dispatches on. The
/// payload `Vec`s move into `Buffer`s, so the user payload reaches the handler
/// with no copy beyond the one postcard already made.
pub fn activation_to_js(act: schema::Activation) -> JsActivation {
    let mut out = JsActivation {
        token: big(act.token),
        operative: act.target.operative,
        procedure: act.target.procedure,
        entity_key: act.target.entity_key,
        change_seq: big(act.change_seq),
        factory: act.factory,
        kind: String::new(),
        items: None,
        snapshot: None,
        changes: None,
        state_json: None,
        key_request: None,
        args: None,
        outputs: None,
    };
    match act.kind {
        schema::ActivationKind::Work(items) => {
            out.kind = "work".into();
            out.items = Some(
                items
                    .into_iter()
                    .map(|item| match item {
                        schema::WorkItem::Input(i) => JsWorkItem {
                            kind: "input".into(),
                            channel: Some(i.channel),
                            payload: Some(i.payload.into()),
                            metadata: Some(meta_out(i.metadata)),
                            consumed_offset: i.consumed_offset.map(big),
                            replay: Some(i.replay),
                            now: Some(big(i.now)),
                            timer_kind: None,
                            scheduled_at: None,
                        },
                        schema::WorkItem::Timer(t) => JsWorkItem {
                            kind: "timer".into(),
                            channel: None,
                            payload: None,
                            metadata: None,
                            consumed_offset: None,
                            replay: None,
                            now: Some(big(t.scheduled_at)),
                            timer_kind: Some(timer_kind_out(t.kind)),
                            scheduled_at: Some(big(t.scheduled_at)),
                        },
                    })
                    .collect(),
            );
        }
        schema::ActivationKind::RestoreFold(fold) => {
            out.kind = "restore_fold".into();
            out.snapshot = fold.snapshot.map(|b| JsRestoreBody {
                bytes: b.bytes.into(),
                from_version: b.from_version,
            });
            out.changes = Some(
                fold.changes
                    .into_iter()
                    .map(|b| JsRestoreBody {
                        bytes: b.bytes.into(),
                        from_version: b.from_version,
                    })
                    .collect(),
            );
        }
        schema::ActivationKind::SnapshotRequest => out.kind = "snapshot_request".into(),
        schema::ActivationKind::LiveStateRead => out.kind = "live_state_read".into(),
        schema::ActivationKind::ApplyState { state_json } => {
            out.kind = "apply_state".into();
            out.state_json = Some(state_json.into());
        }
        schema::ActivationKind::MigrationSerialize => out.kind = "migration_serialize".into(),
        schema::ActivationKind::KeyRequest(req) => {
            out.kind = "key_request".into();
            out.key_request = Some(JsKeyRequest {
                channel: req.channel,
                payloads: req.payloads.into_iter().map(Buffer::from).collect(),
                want_keys: req.want_keys,
                want_event_time: req.want_event_time,
            });
        }
        schema::ActivationKind::WorkflowRun { args } => {
            out.kind = "workflow_run".into();
            out.args = Some(args.into());
        }
        schema::ActivationKind::CompensateRun { outputs } => {
            out.kind = "compensate_run".into();
            out.outputs = Some(outputs.into_iter().map(Buffer::from).collect());
        }
    }
    out
}

fn output_in(o: JsOutput) -> Result<schema::Output> {
    let target = match o.target.as_str() {
        "edge" => schema::OutputTarget::Edge {
            channel: o.channel.ok_or_else(|| {
                Error::new(Status::InvalidArg, "edge output is missing `channel`")
            })?,
            consumer: o.consumer.ok_or_else(|| {
                Error::new(Status::InvalidArg, "edge output is missing `consumer`")
            })?,
        },
        "plugin" => schema::OutputTarget::Plugin {
            channel: o.plugin_channel.ok_or_else(|| {
                Error::new(
                    Status::InvalidArg,
                    "plugin output is missing `pluginChannel`",
                )
            })?,
        },
        other => {
            return Err(Error::new(
                Status::InvalidArg,
                format!("unknown output target {other:?}"),
            ))
        }
    };
    Ok(schema::Output {
        target,
        key: o.key,
        event_time: o
            .event_time
            .as_ref()
            .map(|v| from_big("output eventTime", v))
            .transpose()?,
        metadata: meta_in(o.metadata),
        payload: o.payload.to_vec(),
    })
}

/// One prepare answer per payload, in order. A per-payload refusal is a
/// `WorkError` inside the answer — the input's retry ladder takes it from
/// there — while a malformed answer is a napi error thrown at the caller, so
/// the two failure kinds cannot be collected together.
fn prepared_in(
    answers: Vec<JsPrepareAnswer>,
) -> Result<Vec<std::result::Result<schema::PrepareAnswer, schema::WorkError>>> {
    let mut out = Vec::with_capacity(answers.len());
    for a in answers {
        out.push(match a.error {
            Some(message) => Err(schema::WorkError {
                message,
                non_retryable: a.non_retryable.unwrap_or(false),
            }),
            None => Ok(schema::PrepareAnswer {
                keys: a.keys.unwrap_or_default(),
                event_time: a
                    .event_time
                    .as_ref()
                    .map(|v| from_big("prepare eventTime", v))
                    .transpose()?,
            }),
        });
    }
    Ok(out)
}

fn effect_in(e: JsItemEffect) -> Result<schema::ItemEffect> {
    let verdict = match e.verdict.as_str() {
        "ok" => schema::Verdict::Ok,
        "retryable" => schema::Verdict::RetryableError {
            message: e.message.clone().unwrap_or_default(),
        },
        "non_retryable" => schema::Verdict::NonRetryableError {
            message: e.message.clone().unwrap_or_default(),
        },
        other => {
            return Err(Error::new(
                Status::InvalidArg,
                format!("unknown verdict {other:?}"),
            ))
        }
    };
    Ok(schema::ItemEffect {
        verdict,
        changes: e
            .changes
            .unwrap_or_default()
            .into_iter()
            .map(|b| b.to_vec())
            .collect(),
        outputs: e
            .outputs
            .unwrap_or_default()
            .into_iter()
            .map(output_in)
            .collect::<Result<_>>()?,
        consumed_offset: e
            .consumed_offset
            .as_ref()
            .map(|v| from_big("consumedOffset", v))
            .transpose()?,
        event_time: from_big("eventTime", &e.event_time)?,
        timers: e
            .timers
            .unwrap_or_default()
            .into_iter()
            .map(|t| {
                Ok(schema::TimerOp {
                    kind: timer_kind_in(&t.kind)?,
                    scheduled_at: from_big("timer scheduledAt", &t.scheduled_at)?,
                    register: t.register,
                })
            })
            .collect::<Result<_>>()?,
        checkpoint: e.checkpoint.unwrap_or(false),
        dlq: e.dlq.map(|d| schema::DlqEntry {
            channel: d.channel,
            error: d.error,
            attempts: d.attempts,
            payload: d.payload.map(|p| p.to_vec()),
        }),
    })
}

/// Build the postcard `ActivationResult` from the shape JavaScript answers
/// with. Encoding lives here for the same reason decoding does: one
/// implementation of the wire contract, in the crate that owns it.
pub fn result_from_js(r: JsActivationResult) -> Result<schema::ActivationResult> {
    let outcome = match r.outcome.as_str() {
        "work" => schema::ActivationOutcome::Work(
            r.effects
                .unwrap_or_default()
                .into_iter()
                .map(effect_in)
                .collect::<Result<_>>()?,
        ),
        "restored" => schema::ActivationOutcome::Restored,
        "snapshot" => schema::ActivationOutcome::Snapshot {
            state: r.state.map(|b| b.to_vec()).unwrap_or_default(),
        },
        "state_json" => schema::ActivationOutcome::StateJson {
            json: r.json.map(|b| b.to_vec()).unwrap_or_default(),
        },
        "state_applied" => schema::ActivationOutcome::StateApplied,
        "migration_state" => schema::ActivationOutcome::MigrationState {
            json: r.json.map(|b| b.to_vec()).unwrap_or_default(),
        },
        "prepared" => {
            schema::ActivationOutcome::Prepared(prepared_in(r.prepared.unwrap_or_default())?)
        }
        "failed" => schema::ActivationOutcome::Failed(schema::WorkError {
            message: r.error.unwrap_or_else(|| "activation failed".into()),
            non_retryable: r.non_retryable.unwrap_or(false),
        }),
        "workflow_done" => schema::ActivationOutcome::WorkflowDone {
            completed: r.workflow_completed.unwrap_or(false),
            result: r.workflow_result.map(|b| b.to_vec()).unwrap_or_default(),
        },
        other => {
            return Err(Error::new(
                Status::InvalidArg,
                format!("unknown activation outcome {other:?}"),
            ))
        }
    };
    Ok(schema::ActivationResult {
        token: from_big("token", &r.token)?,
        outcome,
    })
}

// ---------------------------------------------------------------------------
// The entry points. Every one below corresponds to exactly one C entry point.
// ---------------------------------------------------------------------------

/// The activation schema version this native binary speaks.
#[napi]
pub fn schema_version() -> u32 {
    crate::flz_core_schema_version()
}

/// Create the process context, negotiating the schema version. Throws naming
/// both versions on a mismatch, before any controller connection is attempted.
#[napi]
pub fn context_new(shim_schema_version: u32) -> Result<()> {
    if !CONTEXT.load(Ordering::Acquire).is_null() {
        return Err(Error::new(
            Status::InvalidArg,
            "flz_core context already created for this process",
        ));
    }
    let ctx = veneer::context_new(shim_schema_version)?;
    CONTEXT.store(ctx, Ordering::Release);
    Ok(())
}

/// Destroy the process context. The host's threads must already be stopped.
#[napi]
pub fn context_free() {
    let ctx = CONTEXT.swap(std::ptr::null_mut(), Ordering::AcqRel);
    if !ctx.is_null() {
        veneer::context_free(CtxPtr::of(ctx));
    }
}

/// Declare one host class from its JSON declaration. Throws the core's refusal.
#[napi]
pub fn declare_class(json: Buffer) -> Result<()> {
    Ok(veneer::declare_class(context()?, &json)?)
}

/// Connect to the controller and start the runner loop. Asynchronous: the ABI
/// call waits up to ten seconds for the connect confirmation, which is ten
/// seconds an event loop may not spend inside native code.
#[napi(ts_return_type = "Promise<void>")]
pub fn start<'a>(env: &'a Env, namespace: String) -> Result<Object<'a>> {
    let namespace = veneer::c_string("namespace", &namespace)?;
    off_loop(env, context()?, move |ctx| {
        Ok(veneer::start(ctx, &namespace)?)
    })
}

/// Answer one activation. Asynchronous, and this is the entry point the rule
/// exists for: under the default `DropNothing` overflow policy the routing
/// push parks until the consumer's mailbox drains, and the consumer drains
/// only when its handler runs on an event loop.
#[napi(ts_return_type = "Promise<void>")]
pub fn complete<'a>(env: &'a Env, result: JsActivationResult) -> Result<Object<'a>> {
    let token = from_big("token", &result.token)?;
    let result = result_from_js(result)?;
    off_loop(env, context()?, move |ctx| {
        let verdict = veneer::complete(ctx, &result);
        crate::napi_host::release_token(token);
        Ok(verdict?)
    })
}

/// Abandon a polled activation: re-hand its untouched items and release its
/// entity. Synchronous by design — the answer guard runs it from a `finally`
/// and from a dying worker's cleanup, where a promise would be one more thing
/// that can be dropped.
#[napi]
pub fn abandon(token: BigInt) -> Result<()> {
    let ctx = context()?;
    let token = from_big("token", &token)?;
    crate::napi_host::release_token(token);
    Ok(veneer::abandon(ctx, token)?)
}

/// Wake every blocked poller and stop the runner loop.
#[napi]
pub fn shutdown() -> Result<()> {
    veneer::shutdown(context()?);
    Ok(())
}

/// Emit one output payload from a live workflow run-sugar body. Non-blocking
/// in the core (an unbounded coordinator send), so synchronous here.
#[napi]
pub fn workflow_emit(instance_id: String, payload: Buffer) -> Result<()> {
    let ctx = context()?;
    let instance = veneer::c_string("instance id", &instance_id)?;
    Ok(veneer::workflow_emit(ctx, &instance, &payload)?)
}

/// The next inbox item of a live workflow body, or `null` on timeout.
/// Asynchronous: it parks for the whole timeout when the inbox is empty.
#[napi(ts_return_type = "Promise<Buffer | null>")]
pub fn workflow_recv<'a>(env: &'a Env, instance_id: String, timeout_ms: u32) -> Result<Object<'a>> {
    let instance = veneer::c_string("instance id", &instance_id)?;
    off_loop(env, context()?, move |ctx| {
        Ok(veneer::workflow_recv(ctx, &instance, u64::from(timeout_ms))?.map(Buffer::from))
    })
}

/// Whether the named workflow body's cancellation token has tripped.
#[napi]
pub fn workflow_cancelled(instance_id: String) -> Result<bool> {
    let ctx = context()?;
    let instance = veneer::c_string("instance id", &instance_id)?;
    Ok(veneer::workflow_cancelled(ctx, &instance))
}

/// Spawn-or-deliver a workflow instance; resolves with the controller's
/// outcome string. Asynchronous: the ABI call waits up to thirty seconds.
#[napi(ts_return_type = "Promise<string>")]
pub fn spawn_workflow<'a>(
    env: &'a Env,
    instance_id: String,
    factory_id: String,
    payload: Buffer,
    if_running: String,
) -> Result<Object<'a>> {
    let instance = veneer::c_string("instance id", &instance_id)?;
    let factory = veneer::c_string("factory id", &factory_id)?;
    let mode = veneer::c_string("ifRunning", &if_running)?;
    let payload = payload.to_vec();
    off_loop(env, context()?, move |ctx| {
        Ok(veneer::spawn_workflow(
            ctx, &instance, &factory, &payload, &mode,
        )?)
    })
}

/// The core's monotonic clock in nanoseconds.
#[napi]
pub fn monotonic_nanos() -> BigInt {
    big(crate::flz_core_monotonic_nanos())
}

/// When the named entry channel last received a frame, on the same clock;
/// `0n` when no frame ever has.
#[napi]
pub fn entry_last_delivery_nanos(channel: String) -> Result<BigInt> {
    let channel = veneer::c_string("channel", &channel)?;
    Ok(big(veneer::entry_last_delivery_nanos(&channel)))
}

/// The context's last recorded error, or `null`.
#[napi]
pub fn last_error() -> Option<String> {
    let ptr = CONTEXT.load(Ordering::Acquire);
    if ptr.is_null() {
        return None;
    }
    veneer::last_error(CtxPtr::of(ptr))
}

/// Register the host's log callback: the one host function native threads
/// ever call, for this host's own diagnostics.
#[napi]
pub fn set_log_callback(callback: Option<Function<crate::napi_host::JsLogLine, ()>>) -> Result<()> {
    crate::napi_host::set_log_callback(callback)
}

/// One exported function, and the C ABI entry point behind it.
#[napi(object)]
pub struct JsNativeExport {
    /// The JavaScript name.
    pub name: String,
    /// The C ABI entry point it reaches, or `null` for host bookkeeping that
    /// reaches none.
    pub abi: Option<String>,
    /// Whether that entry point can block. Every one of these is a promise.
    pub blocking: bool,
}

/// Every function this addon exports, with the C ABI entry point behind it.
///
/// Two properties are asserted against this table rather than reviewed, so
/// neither can lapse by omission when an export is added later:
///
/// * the veneer adds no ABI surface — every export names an existing entry
///   point or is host bookkeeping that reaches none;
/// * no blocking entry point is synchronously reachable — `poll` and
///   `waitStarvation` are absent from the exports entirely, and every export
///   marked blocking returns a promise.
#[napi]
pub fn native_surface() -> Vec<JsNativeExport> {
    // The veneer: one export, one C entry point, blocking as the ABI says.
    fn veneer(name: &str, abi: &str) -> JsNativeExport {
        host(name, Some(abi), crate::BLOCKING_ENTRY_POINTS.contains(&abi))
    }
    fn host(name: &str, abi: Option<&str>, blocking: bool) -> JsNativeExport {
        JsNativeExport {
            name: name.to_string(),
            abi: abi.map(str::to_string),
            blocking,
        }
    }
    vec![
        veneer("schemaVersion", "flz_core_schema_version"),
        veneer("contextNew", "flz_core_context_new"),
        veneer("contextFree", "flz_core_context_free"),
        veneer("declareClass", "flz_core_declare_class"),
        veneer("start", "flz_core_start"),
        veneer("complete", "flz_core_complete"),
        veneer("abandon", "flz_core_abandon"),
        veneer("shutdown", "flz_core_shutdown"),
        veneer("workflowEmit", "flz_core_workflow_emit"),
        veneer("workflowRecv", "flz_core_workflow_recv"),
        veneer("workflowCancelled", "flz_core_workflow_cancelled"),
        veneer("spawnWorkflow", "flz_core_spawn_workflow"),
        veneer("monotonicNanos", "flz_core_monotonic_nanos"),
        veneer(
            "entryLastDeliveryNanos",
            "flz_core_entry_last_delivery_nanos",
        ),
        veneer("lastError", "flz_core_last_error"),
        host("setLogCallback", None, false),
        // The host's threads. `startHost` is how `flz_core_poll` is reached at
        // all: it is driven from native fetchers and never exported, because
        // an event loop inside a blocking poll is an event loop running
        // nothing else.
        host("startHost", Some("flz_core_poll"), false),
        host("stopHost", Some("flz_core_poll"), true),
        host("registerDispatch", None, false),
        host("unregisterDispatch", None, false),
        host("dispatchLoopCount", None, false),
        host("hostStats", None, false),
        host("setHostDrainedCallback", None, false),
        host("watchStdinEof", Some("flz_core_shutdown"), false),
        host("nativeSurface", None, false),
    ]
}
