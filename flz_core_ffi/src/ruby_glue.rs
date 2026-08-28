//! Ruby bindings over this crate's own C ABI, for the `flezha` gem's native
//! extension. One-to-one with the C entry points — the magnus layer adds
//! nothing the ABI does not have, so a Ruby-only side door cannot grow here.
//!
//! ## One context per process
//! The ABI's rule is "one opaque context per host process". Ruby has one VM
//! per process and the extension is loaded once into it, so the context lives
//! in [`CONTEXT`] and no handle is carried through Ruby — a raw pointer
//! reaching Ruby as an Integer would be a number nothing validates.
//!
//! ## The GVL rule
//! Every blocking entry point runs inside [`without_gvl`], which is the single
//! place the rule is implemented: `poll`, `wait_starvation`, `complete`,
//! `workflow_recv`, `start` and `spawn_workflow` all park in native code with
//! the global VM lock released, so no blocking core call stops another Ruby
//! thread from running Ruby. `workflow_recv` is the sharp one — a workflow
//! stage waiting seconds on its inbox with the GVL held would freeze the whole
//! VM, fetchers included.
//!
//! No unblocking function is passed. The ABI has no per-thread wake
//! (`flz_core_shutdown` releases *every* poller), so the timeout the caller
//! passes to `poll` is what bounds how quickly `Thread#kill`, `Interrupt` and
//! runner close are observed. That is a stated bound, not an accident, and the
//! gem documents the value it uses.
//!
//! ## No log callback
//! `flz_core_set_log_callback` is not veneered here, exactly as it is not in
//! `jni_glue.rs`. The ABI entry point is a no-op stub, and the diagnostics a
//! host would route through it come from threads the core owns — which CRuby
//! cannot accept a call from: `rb_thread_call_with_gvl` is documented as
//! usable only from inside an `rb_thread_call_without_gvl` callback, and
//! "you cannot convert a non-Ruby thread into a Ruby thread using this API".
//! The Node veneer needs the bridge because its fetchers are native; this
//! host's threads are Ruby's own and log in Ruby.
//!
//! ## Buffer ownership
//! A core buffer is decoded and freed through [`crate::flz_core_buf_free`]
//! before the call that produced it returns, so no native buffer escapes into
//! the Ruby heap. Payloads reach Ruby as binary `String`s, which own their
//! bytes.

use std::ffi::{c_void, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::OnceLock;

use flz_core::schema;
use magnus::prelude::*;
use magnus::value::Opaque;
use magnus::{function, Error, ExceptionClass, RArray, RHash, RString, Ruby, TryConvert, Value};

use crate::{
    FlzContext, FLZ_OK, FLZ_POLL_ACTIVATION, FLZ_POLL_SHUTTING_DOWN, FLZ_POLL_TIMEOUT,
    FLZ_STARVATION_OBSERVED,
};

/// The one context this process holds. Null until `context_new`.
static CONTEXT: AtomicPtr<FlzContext> = AtomicPtr::new(std::ptr::null_mut());

/// `Flezha::Native::Error`, minted at init and reachable from the module, so
/// the handle can never dangle.
static ERROR_CLASS: OnceLock<Opaque<ExceptionClass>> = OnceLock::new();
/// `Flezha::Native::SchemaMismatch`.
static SCHEMA_ERROR_CLASS: OnceLock<Opaque<ExceptionClass>> = OnceLock::new();

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// The Ruby handle of the calling thread.
///
/// Every function below is reached only from Ruby, through `function!`, so the
/// handle is always there. magnus itself takes the same shortcut on the return
/// path of every bound function, which is the same thread and the same
/// precondition — stating it once here beats threading an impossible error
/// through twenty signatures.
fn ruby() -> Ruby {
    // SAFETY: reached only from a `function!`-bound entry point, which Ruby
    // calls with the GVL held on a thread it owns.
    unsafe { Ruby::get_unchecked() }
}

fn error_class(ruby: &Ruby) -> ExceptionClass {
    ERROR_CLASS
        .get()
        .map(|c| ruby.get_inner(*c))
        .unwrap_or_else(|| ruby.exception_standard_error())
}

fn schema_error_class(ruby: &Ruby) -> ExceptionClass {
    SCHEMA_ERROR_CLASS
        .get()
        .map(|c| ruby.get_inner(*c))
        .unwrap_or_else(|| error_class(ruby))
}

/// A native failure as the Ruby exception it raises.
fn native_error(ruby: &Ruby, message: impl Into<String>) -> Error {
    Error::new(error_class(ruby), message.into())
}

/// The context's last recorded error, as the message for a raised exception.
fn last_error_of(ctx: *const FlzContext) -> String {
    let ptr = unsafe { crate::flz_core_last_error(ctx) };
    if ptr.is_null() {
        "unknown native failure".to_string()
    } else {
        unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned()
    }
}

fn core_error(ruby: &Ruby, ctx: *const FlzContext) -> Error {
    native_error(ruby, last_error_of(ctx))
}

/// The live context, or an error naming what the caller skipped.
fn context(ruby: &Ruby) -> Result<*mut FlzContext, Error> {
    let ptr = CONTEXT.load(Ordering::Acquire);
    if ptr.is_null() {
        return Err(native_error(
            ruby,
            "flz_core context not created: call Flezha::Native.context_new first",
        ));
    }
    Ok(ptr)
}

fn c_string(ruby: &Ruby, what: &str, value: &str) -> Result<CString, Error> {
    CString::new(value).map_err(|_| native_error(ruby, format!("{what} contains a NUL byte")))
}

// ---------------------------------------------------------------------------
// The GVL seam
// ---------------------------------------------------------------------------

/// What crosses into the GVL-released call and back. The closure is moved in
/// and its answer moved out; nothing here touches a Ruby object, which is the
/// whole precondition for running without the lock.
struct GvlCall<F, T> {
    work: Option<F>,
    answer: Option<std::thread::Result<T>>,
}

unsafe extern "C" fn gvl_trampoline<F, T>(data: *mut c_void) -> *mut c_void
where
    F: FnOnce() -> T,
{
    let call = &mut *(data as *mut GvlCall<F, T>);
    // A panic must not unwind across the C frame that called us; it is
    // captured here and resumed once the GVL is back.
    let work = call.work.take();
    call.answer = Some(catch_unwind(AssertUnwindSafe(move || {
        (work.expect("GVL closure taken twice"))()
    })));
    std::ptr::null_mut()
}

/// Run one blocking ABI call with the global VM lock released.
///
/// The single place the GVL rule is implemented, so "no blocking core call
/// holds the GVL" is a property of one function rather than of six correct
/// copies. `work` runs on this same OS thread and MUST NOT touch a Ruby
/// object — every caller below converts to and from Ruby outside it.
///
/// No unblocking function is registered: the ABI has no per-thread wake, so
/// the call's own timeout is the interrupt granularity (see the module docs).
fn without_gvl<F, T>(work: F) -> T
where
    F: FnOnce() -> T,
{
    let mut call = GvlCall {
        work: Some(work),
        answer: None,
    };
    unsafe {
        rb_sys::rb_thread_call_without_gvl(
            Some(gvl_trampoline::<F, T>),
            &mut call as *mut GvlCall<F, T> as *mut c_void,
            None,
            std::ptr::null_mut(),
        );
    }
    match call.answer.expect("GVL closure did not run") {
        Ok(value) => value,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

/// Run one ABI call that hands out a buffer, with the GVL released, and take
/// ownership of what it handed out.
///
/// The ownership rule this module promises, in the one place it is
/// implemented: the buffer is copied out and freed through
/// [`crate::flz_core_buf_free`] **before the GVL is reacquired**, so no native
/// buffer can reach the Ruby heap however the caller answers.
///
/// `handed_out` is the status that means a buffer was produced —
/// [`FLZ_POLL_ACTIVATION`] for poll and the workflow receive, [`FLZ_OK`] for
/// spawn. Every other status leaves the out pointers untouched.
fn without_gvl_taking_buffer<F>(handed_out: i32, call: F) -> (i32, Option<Vec<u8>>)
where
    F: FnOnce(*mut *mut u8, *mut usize) -> i32 + Send,
{
    without_gvl(move || {
        let mut buf: *mut u8 = std::ptr::null_mut();
        let mut len: usize = 0;
        let status = call(&mut buf, &mut len);
        if status != handed_out {
            return (status, None);
        }
        let owned = unsafe { std::slice::from_raw_parts(buf, len) }.to_vec();
        unsafe { crate::flz_core_buf_free(buf, len) };
        (status, Some(owned))
    })
}

// ---------------------------------------------------------------------------
// Ruby <-> schema conversion
//
// The veneer decodes postcard on the Rust side (`schema::decode_activation`)
// so the gem never carries a second implementation of the wire contract. The
// shape is a flat Hash with a `:kind` discriminator rather than a class per
// variant, because Ruby has no sum type and a discriminated Hash is what the
// dispatcher case/whens on anyway. Absent fields are absent, not nil.
// ---------------------------------------------------------------------------

fn bytes(ruby: &Ruby, v: Vec<u8>) -> Value {
    ruby.str_from_slice(&v).as_value()
}

fn read_bytes(ruby: &Ruby, what: &str, value: Value) -> Result<Vec<u8>, Error> {
    let s = RString::from_value(value)
        .ok_or_else(|| native_error(ruby, format!("{what} must be a String")))?;
    Ok(unsafe { s.as_slice() }.to_vec())
}

/// A Hash entry, or `None` when the key is absent or explicitly nil.
fn opt<T: TryConvert>(ruby: &Ruby, hash: RHash, key: &str) -> Result<Option<T>, Error> {
    match hash.get(ruby.sym_new(key)) {
        None => Ok(None),
        Some(v) if v.is_nil() => Ok(None),
        Some(v) => Ok(Some(T::try_convert(v)?)),
    }
}

fn req<T: TryConvert>(ruby: &Ruby, hash: RHash, key: &str, what: &str) -> Result<T, Error> {
    opt(ruby, hash, key)?.ok_or_else(|| native_error(ruby, format!("{what} is missing `{key}`")))
}

fn opt_bytes(ruby: &Ruby, hash: RHash, key: &str) -> Result<Option<Vec<u8>>, Error> {
    match opt::<Value>(ruby, hash, key)? {
        None => Ok(None),
        Some(v) => Ok(Some(read_bytes(ruby, key, v)?)),
    }
}

fn opt_array(ruby: &Ruby, hash: RHash, key: &str) -> Result<Vec<Value>, Error> {
    match opt::<RArray>(ruby, hash, key)? {
        None => Ok(Vec::new()),
        // SAFETY: the slice is copied out before any Ruby call can run and
        // move or collect the backing array.
        Some(a) => Ok(unsafe { a.as_slice() }.to_vec()),
    }
}

/// Metadata is an Array of two-element `[key, value]` Arrays rather than a
/// Hash: the schema allows a repeated key, and the order is part of what the
/// controller sees.
fn meta_out(ruby: &Ruby, pairs: Vec<(String, String)>) -> Value {
    let out = ruby.ary_new_capa(pairs.len());
    for (key, value) in pairs {
        let pair = ruby.ary_new_capa(2);
        let _ = pair.push(key);
        let _ = pair.push(value);
        let _ = out.push(pair);
    }
    out.as_value()
}

fn meta_in(ruby: &Ruby, hash: RHash, key: &str) -> Result<Vec<(String, String)>, Error> {
    let mut out = Vec::new();
    for entry in opt_array(ruby, hash, key)? {
        let pair = RArray::from_value(entry)
            .ok_or_else(|| native_error(ruby, "metadata entry must be a [key, value] Array"))?;
        let pair: Vec<String> = pair.to_vec()?;
        if pair.len() != 2 {
            return Err(native_error(
                ruby,
                "metadata entry must be a [key, value] Array",
            ));
        }
        let mut it = pair.into_iter();
        out.push((it.next().unwrap(), it.next().unwrap()));
    }
    Ok(out)
}

fn timer_kind_in(ruby: &Ruby, kind: &str) -> Result<schema::TimerKind, Error> {
    match kind {
        "event_time" => Ok(schema::TimerKind::EventTime),
        "processing_time" => Ok(schema::TimerKind::ProcessingTime),
        other => Err(native_error(ruby, format!("unknown timer kind {other:?}"))),
    }
}

fn restore_body(ruby: &Ruby, body: schema::RestoreBody) -> Value {
    let h = ruby.hash_new();
    let _ = h.aset(ruby.sym_new("bytes"), bytes(ruby, body.bytes));
    if let Some(v) = body.from_version {
        let _ = h.aset(ruby.sym_new("from_version"), v);
    }
    h.as_value()
}

/// Decode a polled activation into the Hash the gem dispatches on.
pub fn activation_to_ruby(ruby: &Ruby, act: schema::Activation) -> Result<RHash, Error> {
    let out = ruby.hash_new();
    let sym = |name: &str| ruby.sym_new(name);
    out.aset(sym("token"), act.token)?;
    out.aset(sym("operative"), act.target.operative)?;
    out.aset(sym("procedure"), act.target.procedure)?;
    if let Some(key) = act.target.entity_key {
        out.aset(sym("entity_key"), key)?;
    }
    out.aset(sym("change_seq"), act.change_seq)?;
    out.aset(sym("factory"), act.factory)?;

    match act.kind {
        schema::ActivationKind::Work(items) => {
            out.aset(sym("kind"), sym("work"))?;
            let list = ruby.ary_new_capa(items.len());
            for item in items {
                let h = ruby.hash_new();
                match item {
                    schema::WorkItem::Input(i) => {
                        h.aset(sym("kind"), sym("input"))?;
                        h.aset(sym("channel"), i.channel)?;
                        h.aset(sym("payload"), bytes(ruby, i.payload))?;
                        h.aset(sym("metadata"), meta_out(ruby, i.metadata))?;
                        if let Some(offset) = i.consumed_offset {
                            h.aset(sym("consumed_offset"), offset)?;
                        }
                        h.aset(sym("replay"), i.replay)?;
                        h.aset(sym("now"), i.now)?;
                    }
                    schema::WorkItem::Timer(t) => {
                        h.aset(sym("kind"), sym("timer"))?;
                        h.aset(sym("timer_kind"), sym(t.kind.as_str()))?;
                        h.aset(sym("scheduled_at"), t.scheduled_at)?;
                        // A timer firing's `now` IS its scheduled_at, so a
                        // chained `timer_after` anchors replay-stably.
                        h.aset(sym("now"), t.scheduled_at)?;
                    }
                }
                list.push(h)?;
            }
            out.aset(sym("items"), list)?;
        }
        schema::ActivationKind::RestoreFold(fold) => {
            out.aset(sym("kind"), sym("restore_fold"))?;
            if let Some(snapshot) = fold.snapshot {
                out.aset(sym("snapshot"), restore_body(ruby, snapshot))?;
            }
            let changes = ruby.ary_new_capa(fold.changes.len());
            for body in fold.changes {
                changes.push(restore_body(ruby, body))?;
            }
            out.aset(sym("changes"), changes)?;
        }
        schema::ActivationKind::SnapshotRequest => {
            out.aset(sym("kind"), sym("snapshot_request"))?
        }
        schema::ActivationKind::LiveStateRead => out.aset(sym("kind"), sym("live_state_read"))?,
        schema::ActivationKind::ApplyState { state_json } => {
            out.aset(sym("kind"), sym("apply_state"))?;
            out.aset(sym("state_json"), bytes(ruby, state_json))?;
        }
        schema::ActivationKind::MigrationSerialize => {
            out.aset(sym("kind"), sym("migration_serialize"))?
        }
        schema::ActivationKind::KeyRequest(req) => {
            out.aset(sym("kind"), sym("key_request"))?;
            let h = ruby.hash_new();
            h.aset(sym("channel"), req.channel)?;
            let payloads = ruby.ary_new_capa(req.payloads.len());
            for p in req.payloads {
                payloads.push(bytes(ruby, p))?;
            }
            h.aset(sym("payloads"), payloads)?;
            h.aset(sym("want_keys"), req.want_keys)?;
            h.aset(sym("want_event_time"), req.want_event_time)?;
            out.aset(sym("key_request"), h)?;
        }
        schema::ActivationKind::WorkflowRun { args } => {
            out.aset(sym("kind"), sym("workflow_run"))?;
            out.aset(sym("args"), bytes(ruby, args))?;
        }
        schema::ActivationKind::CompensateRun { outputs } => {
            out.aset(sym("kind"), sym("compensate_run"))?;
            let list = ruby.ary_new_capa(outputs.len());
            for o in outputs {
                list.push(bytes(ruby, o))?;
            }
            out.aset(sym("outputs"), list)?;
        }
    }
    Ok(out)
}

fn output_in(ruby: &Ruby, value: Value) -> Result<schema::Output, Error> {
    let h =
        RHash::from_value(value).ok_or_else(|| native_error(ruby, "an output must be a Hash"))?;
    let target: String = req(ruby, h, "target", "output")?;
    let target = match target.as_str() {
        "edge" => schema::OutputTarget::Edge {
            channel: req(ruby, h, "channel", "edge output")?,
            consumer: req(ruby, h, "consumer", "edge output")?,
        },
        "plugin" => schema::OutputTarget::Plugin {
            channel: req(ruby, h, "plugin_channel", "plugin output")?,
        },
        other => {
            return Err(native_error(
                ruby,
                format!("unknown output target {other:?}"),
            ))
        }
    };
    Ok(schema::Output {
        target,
        key: opt(ruby, h, "key")?,
        event_time: opt(ruby, h, "event_time")?,
        metadata: meta_in(ruby, h, "metadata")?,
        payload: opt_bytes(ruby, h, "payload")?.unwrap_or_default(),
    })
}

fn effect_in(ruby: &Ruby, value: Value) -> Result<schema::ItemEffect, Error> {
    let h = RHash::from_value(value)
        .ok_or_else(|| native_error(ruby, "an item effect must be a Hash"))?;
    let verdict: String = req(ruby, h, "verdict", "item effect")?;
    let message: Option<String> = opt(ruby, h, "message")?;
    let verdict = match verdict.as_str() {
        "ok" => schema::Verdict::Ok,
        "retryable" => schema::Verdict::RetryableError {
            message: message.unwrap_or_default(),
        },
        "non_retryable" => schema::Verdict::NonRetryableError {
            message: message.unwrap_or_default(),
        },
        other => return Err(native_error(ruby, format!("unknown verdict {other:?}"))),
    };

    let mut changes = Vec::new();
    for c in opt_array(ruby, h, "changes")? {
        changes.push(read_bytes(ruby, "change", c)?);
    }
    let mut outputs = Vec::new();
    for o in opt_array(ruby, h, "outputs")? {
        outputs.push(output_in(ruby, o)?);
    }
    let mut timers = Vec::new();
    for t in opt_array(ruby, h, "timers")? {
        let t =
            RHash::from_value(t).ok_or_else(|| native_error(ruby, "a timer op must be a Hash"))?;
        let kind: String = req(ruby, t, "kind", "timer op")?;
        timers.push(schema::TimerOp {
            kind: timer_kind_in(ruby, &kind)?,
            scheduled_at: req(ruby, t, "scheduled_at", "timer op")?,
            register: req(ruby, t, "register", "timer op")?,
        });
    }
    let dlq = match opt::<RHash>(ruby, h, "dlq")? {
        None => None,
        Some(d) => Some(schema::DlqEntry {
            channel: req(ruby, d, "channel", "dlq entry")?,
            error: req(ruby, d, "error", "dlq entry")?,
            attempts: req(ruby, d, "attempts", "dlq entry")?,
            payload: opt_bytes(ruby, d, "payload")?,
        }),
    };

    Ok(schema::ItemEffect {
        verdict,
        changes,
        outputs,
        consumed_offset: opt(ruby, h, "consumed_offset")?,
        event_time: opt(ruby, h, "event_time")?.unwrap_or(0),
        timers,
        checkpoint: opt(ruby, h, "checkpoint")?.unwrap_or(false),
        dlq,
    })
}

/// One prepare answer per payload, in order. A per-payload refusal is a
/// `WorkError` inside the answer — the input's retry ladder takes it from
/// there — while a malformed answer raises at the caller, so the two failure
/// kinds cannot be collected together.
fn prepared_in(
    ruby: &Ruby,
    answers: Vec<Value>,
) -> Result<Vec<Result<schema::PrepareAnswer, schema::WorkError>>, Error> {
    let mut out = Vec::with_capacity(answers.len());
    for value in answers {
        let h = RHash::from_value(value)
            .ok_or_else(|| native_error(ruby, "a prepare answer must be a Hash"))?;
        out.push(match opt::<String>(ruby, h, "error")? {
            Some(message) => Err(schema::WorkError {
                message,
                non_retryable: opt(ruby, h, "non_retryable")?.unwrap_or(false),
            }),
            None => Ok(schema::PrepareAnswer {
                keys: match opt::<RArray>(ruby, h, "keys")? {
                    None => Vec::new(),
                    Some(a) => a.to_vec()?,
                },
                event_time: opt(ruby, h, "event_time")?,
            }),
        });
    }
    Ok(out)
}

/// Build the postcard `ActivationResult` from the Hash Ruby answers with.
/// Encoding lives here for the same reason decoding does: one implementation
/// of the wire contract, in the crate that owns it.
pub fn result_from_ruby(ruby: &Ruby, r: RHash) -> Result<schema::ActivationResult, Error> {
    let outcome: String = req(ruby, r, "outcome", "activation result")?;
    let outcome = match outcome.as_str() {
        "work" => {
            let mut effects = Vec::new();
            for e in opt_array(ruby, r, "effects")? {
                effects.push(effect_in(ruby, e)?);
            }
            schema::ActivationOutcome::Work(effects)
        }
        "restored" => schema::ActivationOutcome::Restored,
        "snapshot" => schema::ActivationOutcome::Snapshot {
            state: opt_bytes(ruby, r, "state")?.unwrap_or_default(),
        },
        "state_json" => schema::ActivationOutcome::StateJson {
            json: opt_bytes(ruby, r, "json")?.unwrap_or_default(),
        },
        "state_applied" => schema::ActivationOutcome::StateApplied,
        "migration_state" => schema::ActivationOutcome::MigrationState {
            json: opt_bytes(ruby, r, "json")?.unwrap_or_default(),
        },
        "prepared" => {
            schema::ActivationOutcome::Prepared(prepared_in(ruby, opt_array(ruby, r, "prepared")?)?)
        }
        "failed" => schema::ActivationOutcome::Failed(schema::WorkError {
            message: opt(ruby, r, "error")?.unwrap_or_else(|| "activation failed".into()),
            non_retryable: opt(ruby, r, "non_retryable")?.unwrap_or(false),
        }),
        "workflow_done" => schema::ActivationOutcome::WorkflowDone {
            completed: opt(ruby, r, "workflow_completed")?.unwrap_or(false),
            result: opt_bytes(ruby, r, "workflow_result")?.unwrap_or_default(),
        },
        other => {
            return Err(native_error(
                ruby,
                format!("unknown activation outcome {other:?}"),
            ))
        }
    };
    Ok(schema::ActivationResult {
        token: req(ruby, r, "token", "activation result")?,
        outcome,
    })
}

// ---------------------------------------------------------------------------
// The entry points. Every one below corresponds to exactly one C entry point.
// ---------------------------------------------------------------------------

/// The activation schema version this native library speaks.
fn schema_version() -> u32 {
    crate::flz_core_schema_version()
}

/// Create the process context, negotiating the schema version. Raises
/// `SchemaMismatch` naming both versions, before any controller connection is
/// attempted.
fn context_new(shim_schema_version: u32) -> Result<(), Error> {
    let ruby = ruby();
    if !CONTEXT.load(Ordering::Acquire).is_null() {
        return Err(native_error(
            &ruby,
            "flz_core context already created for this process",
        ));
    }
    let ctx = crate::flz_core_context_new(shim_schema_version);
    if ctx.is_null() {
        let ptr = crate::flz_core_init_error();
        let message = if ptr.is_null() {
            "flz_core context creation failed".to_string()
        } else {
            unsafe { CStr::from_ptr(ptr) }
                .to_string_lossy()
                .into_owned()
        };
        return Err(Error::new(schema_error_class(&ruby), message));
    }
    CONTEXT.store(ctx, Ordering::Release);
    Ok(())
}

/// Destroy the process context. The host's threads must already be stopped.
fn context_free() {
    let ctx = CONTEXT.swap(std::ptr::null_mut(), Ordering::AcqRel);
    if !ctx.is_null() {
        unsafe { crate::flz_core_context_free(ctx) };
    }
}

/// Declare one host class from its JSON declaration. Raises the core's refusal.
fn declare_class(json: RString) -> Result<(), Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    let json = unsafe { json.as_slice() }.to_vec();
    let status = unsafe { crate::flz_core_declare_class(ctx, json.as_ptr(), json.len()) };
    if status != FLZ_OK {
        return Err(core_error(&ruby, ctx));
    }
    Ok(())
}

/// Connect to the controller and start the runner loop. Blocking: the ABI call
/// waits up to ten seconds for the connect confirmation.
fn start(namespace: String) -> Result<(), Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    let namespace = c_string(&ruby, "namespace", &namespace)?;
    let ctx_addr = ctx as usize;
    let status = without_gvl(move || unsafe {
        crate::flz_core_start(ctx_addr as *mut FlzContext, namespace.as_ptr())
    });
    if status != FLZ_OK {
        return Err(core_error(&ruby, ctx));
    }
    Ok(())
}

/// Block up to `timeout_ms` for one activation. Returns the activation Hash,
/// `nil` on timeout, or `:shutting_down` once every poller has been released.
fn poll(timeout_ms: u64) -> Result<Value, Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    let ctx_addr = ctx as usize;
    let (status, encoded) =
        without_gvl_taking_buffer(FLZ_POLL_ACTIVATION, move |buf, len| unsafe {
            crate::flz_core_poll(ctx_addr as *mut FlzContext, timeout_ms, buf, len)
        });
    match status {
        FLZ_POLL_ACTIVATION => {
            let act = schema::decode_activation(&encoded.unwrap_or_default())
                .map_err(|e| native_error(&ruby, format!("activation failed to decode: {e}")))?;
            Ok(activation_to_ruby(&ruby, act)?.as_value())
        }
        FLZ_POLL_TIMEOUT => Ok(ruby.qnil().as_value()),
        FLZ_POLL_SHUTTING_DOWN => Ok(ruby.sym_new("shutting_down").as_value()),
        _ => Err(core_error(&ruby, ctx)),
    }
}

/// Park up to `timeout_ms` until the core observes starvation. Answers the
/// ABI's three-way status directly (`1` observed, `0` timeout, `2` shutting
/// down), so a supervisor loop allocates nothing per park.
fn wait_starvation(timeout_ms: u64) -> Result<i32, Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    let ctx_addr = ctx as usize;
    let status = without_gvl(move || unsafe {
        crate::flz_core_wait_starvation(ctx_addr as *mut FlzContext, timeout_ms)
    });
    match status {
        FLZ_STARVATION_OBSERVED | FLZ_POLL_TIMEOUT | FLZ_POLL_SHUTTING_DOWN => Ok(status),
        _ => Err(core_error(&ruby, ctx)),
    }
}

/// Answer one activation. Blocking, and this is the entry point the GVL rule
/// exists for on the completion side: under the default `DropNothing` overflow
/// policy the routing push parks until the consumer's mailbox drains, and the
/// consumer drains only when its handler runs on another Ruby thread.
fn complete(result: RHash) -> Result<(), Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    let encoded = schema::encode_result(&result_from_ruby(&ruby, result)?);
    let ctx_addr = ctx as usize;
    let status = without_gvl(move || unsafe {
        crate::flz_core_complete(ctx_addr as *mut FlzContext, encoded.as_ptr(), encoded.len())
    });
    if status != FLZ_OK {
        return Err(core_error(&ruby, ctx));
    }
    Ok(())
}

/// Abandon a polled activation: re-hand its untouched items and release its
/// entity. Non-blocking by design — the dispatcher runs it from an `ensure`,
/// where anything that can park is one more thing that can go wrong.
fn abandon(token: u64) -> Result<(), Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    let status = unsafe { crate::flz_core_abandon(ctx, token) };
    if status != FLZ_OK {
        return Err(core_error(&ruby, ctx));
    }
    Ok(())
}

/// Wake every blocked poller and stop the runner loop.
fn shutdown() -> Result<(), Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    unsafe { crate::flz_core_shutdown(ctx) };
    Ok(())
}

/// Emit one output payload from a live workflow run-sugar body. Non-blocking
/// in the core (an unbounded coordinator send), so it holds the GVL.
fn workflow_emit(instance_id: String, payload: RString) -> Result<(), Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    let instance = c_string(&ruby, "instance id", &instance_id)?;
    let payload = unsafe { payload.as_slice() }.to_vec();
    let status = unsafe {
        crate::flz_core_workflow_emit(ctx, instance.as_ptr(), payload.as_ptr(), payload.len())
    };
    if status != FLZ_OK {
        return Err(core_error(&ruby, ctx));
    }
    Ok(())
}

/// The next inbox item of a live workflow body, or `nil` on timeout.
fn workflow_recv(instance_id: String, timeout_ms: u64) -> Result<Value, Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    let instance = c_string(&ruby, "instance id", &instance_id)?;
    let ctx_addr = ctx as usize;
    let (status, payload) =
        without_gvl_taking_buffer(FLZ_POLL_ACTIVATION, move |buf, len| unsafe {
            crate::flz_core_workflow_recv(
                ctx_addr as *mut FlzContext,
                instance.as_ptr(),
                timeout_ms,
                buf,
                len,
            )
        });
    match status {
        FLZ_POLL_ACTIVATION => Ok(bytes(&ruby, payload.unwrap_or_default())),
        FLZ_POLL_TIMEOUT => Ok(ruby.qnil().as_value()),
        _ => Err(core_error(&ruby, ctx)),
    }
}

/// Whether the named workflow body's cancellation token has tripped.
fn workflow_cancelled(instance_id: String) -> Result<bool, Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    let instance = c_string(&ruby, "instance id", &instance_id)?;
    Ok(unsafe { crate::flz_core_workflow_cancelled(ctx, instance.as_ptr()) } != 0)
}

/// Spawn-or-deliver a workflow instance; answers the controller's outcome
/// string. Blocking: the ABI call waits up to thirty seconds.
fn spawn_workflow(
    instance_id: String,
    factory_id: String,
    payload: RString,
    if_running: String,
) -> Result<String, Error> {
    let ruby = ruby();
    let ctx = context(&ruby)?;
    let instance = c_string(&ruby, "instance id", &instance_id)?;
    let factory = c_string(&ruby, "factory id", &factory_id)?;
    let mode = c_string(&ruby, "if_running", &if_running)?;
    let payload = unsafe { payload.as_slice() }.to_vec();
    let ctx_addr = ctx as usize;
    let (status, outcome) = without_gvl_taking_buffer(FLZ_OK, move |out, out_len| unsafe {
        crate::flz_core_spawn_workflow(
            ctx_addr as *mut FlzContext,
            instance.as_ptr(),
            factory.as_ptr(),
            payload.as_ptr(),
            payload.len(),
            mode.as_ptr(),
            out,
            out_len,
        )
    });
    if status != FLZ_OK {
        return Err(core_error(&ruby, ctx));
    }
    Ok(String::from_utf8_lossy(&outcome.unwrap_or_default()).into_owned())
}

/// The core's monotonic clock in nanoseconds.
fn monotonic_nanos() -> u64 {
    crate::flz_core_monotonic_nanos()
}

/// When the named entry channel last received a frame, on the same clock;
/// `0` when no frame ever has.
fn entry_last_delivery_nanos(channel: String) -> Result<u64, Error> {
    let ruby = ruby();
    let channel = c_string(&ruby, "channel", &channel)?;
    Ok(unsafe { crate::flz_core_entry_last_delivery_nanos(channel.as_ptr()) })
}

/// The context's last recorded error, or `nil`.
fn last_error() -> Option<String> {
    let ptr = CONTEXT.load(Ordering::Acquire);
    if ptr.is_null() {
        return None;
    }
    let msg = unsafe { crate::flz_core_last_error(ptr) };
    if msg.is_null() {
        return None;
    }
    Some(
        unsafe { CStr::from_ptr(msg) }
            .to_string_lossy()
            .into_owned(),
    )
}

/// Every function this extension exports, with the C ABI entry point behind
/// it: `[name, abi_entry_point_or_nil, blocking]`.
///
/// Two properties are asserted against this table rather than reviewed, so
/// neither can lapse by omission when an export is added later:
///
/// * the veneer adds no ABI surface — every export names an existing entry
///   point, and this veneer has no host bookkeeping of its own because the
///   Ruby host's threads are Ruby's;
/// * every export marked blocking runs inside [`without_gvl`].
fn native_surface() -> Result<RArray, Error> {
    let ruby = ruby();
    let out = ruby.ary_new_capa(NATIVE_SURFACE.len());
    for (name, abi, blocking) in NATIVE_SURFACE {
        let row = ruby.ary_new_capa(3);
        row.push(*name)?;
        row.push(*abi)?;
        row.push(*blocking)?;
        out.push(row)?;
    }
    Ok(out)
}

/// The table `native_surface` renders and the Rust-side test asserts against.
pub const NATIVE_SURFACE: &[(&str, &str, bool)] = &[
    ("schema_version", "flz_core_schema_version", false),
    ("context_new", "flz_core_context_new", false),
    ("context_free", "flz_core_context_free", false),
    ("declare_class", "flz_core_declare_class", false),
    ("start", "flz_core_start", true),
    ("poll", "flz_core_poll", true),
    ("wait_starvation", "flz_core_wait_starvation", true),
    ("complete", "flz_core_complete", true),
    ("abandon", "flz_core_abandon", false),
    ("shutdown", "flz_core_shutdown", false),
    ("workflow_emit", "flz_core_workflow_emit", false),
    ("workflow_recv", "flz_core_workflow_recv", true),
    ("workflow_cancelled", "flz_core_workflow_cancelled", false),
    ("spawn_workflow", "flz_core_spawn_workflow", true),
    ("monotonic_nanos", "flz_core_monotonic_nanos", false),
    (
        "entry_last_delivery_nanos",
        "flz_core_entry_last_delivery_nanos",
        false,
    ),
    ("last_error", "flz_core_last_error", false),
    ("native_surface", "flz_core_schema_version", false),
];

/// Define `Flezha::Native` and its exception classes. Named `Init_flz_core_ffi`
/// so `require "flz_core_ffi"` finds it.
#[magnus::init]
pub fn init(ruby: &Ruby) -> Result<(), Error> {
    let flezha = ruby.define_module("Flezha")?;
    let native = flezha.define_module("Native")?;

    let error = native.define_error("Error", ruby.exception_standard_error())?;
    let _ = ERROR_CLASS.set(Opaque::from(error));
    let schema_mismatch = native.define_error("SchemaMismatch", error)?;
    let _ = SCHEMA_ERROR_CLASS.set(Opaque::from(schema_mismatch));

    native.define_module_function("schema_version", function!(schema_version, 0))?;
    native.define_module_function("context_new", function!(context_new, 1))?;
    native.define_module_function("context_free", function!(context_free, 0))?;
    native.define_module_function("declare_class", function!(declare_class, 1))?;
    native.define_module_function("start", function!(start, 1))?;
    native.define_module_function("poll", function!(poll, 1))?;
    native.define_module_function("wait_starvation", function!(wait_starvation, 1))?;
    native.define_module_function("complete", function!(complete, 1))?;
    native.define_module_function("abandon", function!(abandon, 1))?;
    native.define_module_function("shutdown", function!(shutdown, 0))?;
    native.define_module_function("workflow_emit", function!(workflow_emit, 2))?;
    native.define_module_function("workflow_recv", function!(workflow_recv, 2))?;
    native.define_module_function("workflow_cancelled", function!(workflow_cancelled, 1))?;
    native.define_module_function("spawn_workflow", function!(spawn_workflow, 4))?;
    native.define_module_function("monotonic_nanos", function!(monotonic_nanos, 0))?;
    native.define_module_function(
        "entry_last_delivery_nanos",
        function!(entry_last_delivery_nanos, 1),
    )?;
    native.define_module_function("last_error", function!(last_error, 0))?;
    native.define_module_function("native_surface", function!(native_surface, 0))?;
    Ok(())
}
