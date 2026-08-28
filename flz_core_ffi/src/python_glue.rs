//! Python bindings over this crate's own C ABI, for the `flezha` wheel's
//! native module. One-to-one with the C entry points — the PyO3 layer adds
//! nothing the ABI does not have, so a Python-only side door cannot grow here.
//!
//! ## The context is an object, not a static
//! The ABI's rule is "one opaque context per host process", and the Ruby and
//! JNI veneers keep the pointer in a process-wide static because their runtime
//! is one interpreter per process. CPython is not: sub-interpreters are a
//! supported execution model, and a `static` is shared by all of them while a
//! `#[pyclass]` is reached only through the interpreter that holds a reference
//! to it. So the handle lives in [`Core`], every entry point is a method on
//! it, and no mutable static in this module holds a context — which is what
//! keeps a sub-interpreter executor a later swap rather than a rewrite.
//!
//! The object is opaque: Python cannot construct one from an integer, cannot
//! read the pointer out of it, and cannot make a second one for the same
//! context. That is what a raw pointer crossing as an `int` would not give.
//!
//! ## The GIL rule
//! Every blocking entry point runs inside [`Python::detach`]: `poll`,
//! `wait_starvation`, `complete`, `workflow_recv`, `start` and
//! `spawn_workflow` all park in native code with the global interpreter lock
//! released, so no blocking core call stops another Python thread from running
//! Python. `workflow_recv` is the sharp one — a workflow stage waiting seconds
//! on its inbox with the GIL held would freeze the whole interpreter, fetchers
//! included. `complete` is included because it can park behind an entity gate
//! or a `DropNothing` producer, and the thread that drains that consumer is
//! another Python thread.
//!
//! No unblocking function is passed. The ABI has no per-thread wake
//! (`flz_core_shutdown` releases *every* poller), so the timeout the caller
//! passes to `poll` is what bounds how quickly `KeyboardInterrupt`, a
//! termination signal and runner close are observed. That is a stated bound,
//! not an accident, and the package documents the value it uses.
//!
//! ## No log callback
//! `flz_core_set_log_callback` is not veneered here, exactly as it is not in
//! `jni_glue.rs` or `ruby_glue.rs`. The ABI entry point is a no-op stub, and
//! the diagnostics a host would route through it come from threads the core
//! owns. The Node veneer needs the bridge because its own host layer
//! (`napi_host.rs`) produces those lines; this host's threads are Python's own
//! and log through Python's `logging`.
//!
//! ## Buffer ownership
//! A core buffer is decoded and freed through [`crate::flz_core_buf_free`]
//! before the call that produced it returns, so no native buffer escapes into
//! the Python heap. The decoded user payload then reaches Python as a
//! [`PayloadView`] — a buffer-protocol object over bytes this module owns — so
//! the common decode path allocates no intermediate `bytes`; a caller who
//! wants raw bytes calls `bytes(view)` and gets the copy.

use std::ffi::{c_int, c_void, CStr, CString};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, Ordering};

use flz_core::schema;
use pyo3::conversion::FromPyObjectOwned;
use pyo3::exceptions::{PyBufferError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyModule, PyString, PyTuple};
use pyo3::{create_exception, ffi, IntoPyObjectExt};

use crate::{
    FlzContext, FLZ_OK, FLZ_POLL_ACTIVATION, FLZ_POLL_SHUTTING_DOWN, FLZ_POLL_TIMEOUT,
    FLZ_STARVATION_OBSERVED,
};

create_exception!(
    flz_core_ffi,
    NativeError,
    PyRuntimeError,
    "A failure reported by the flz_core native library."
);
create_exception!(
    flz_core_ffi,
    SchemaMismatch,
    NativeError,
    "The native library and this build speak different activation schema versions."
);

fn native_error(message: impl Into<String>) -> PyErr {
    NativeError::new_err(message.into())
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

fn c_string(what: &str, value: &str) -> PyResult<CString> {
    CString::new(value).map_err(|_| native_error(format!("{what} contains a NUL byte")))
}

// ---------------------------------------------------------------------------
// The payload view
// ---------------------------------------------------------------------------

/// A read-only buffer over bytes this module owns.
///
/// The common decode path never materialises a `bytes`: `msgpack.unpackb`,
/// `json.loads` and every other codec that takes a buffer read straight out of
/// this object. `bytes(view)` is the explicit copy for a caller who wants one.
///
/// The bytes are Rust's, not the core's — the core's buffer is freed before
/// the poll that produced it returns — so nothing here can outlive the ABI's
/// ownership rules however long Python keeps the view.
#[pyclass(module = "flezha.flz_core_ffi", name = "PayloadView", frozen)]
pub struct PayloadView {
    bytes: Vec<u8>,
    /// Set by [`PayloadView::release`] once the invocation that produced this
    /// view is over. A later export is refused rather than served, so a view
    /// stashed in user state names the rule instead of reading stale bytes.
    released: AtomicBool,
}

impl PayloadView {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            released: AtomicBool::new(false),
        }
    }
}

#[pymethods]
impl PayloadView {
    /// A view over the caller's bytes.
    ///
    /// The veneer itself hands views out from `poll`, never from here; the
    /// constructor exists because a buffer type Python cannot build is a
    /// contract only Rust can test, and the view's contract — that it reads as
    /// a buffer, copies on request, and refuses after release — is one the
    /// package's own suite must be able to check.
    #[new]
    fn new_from_bytes(body: &[u8]) -> Self {
        Self::new(body.to_vec())
    }

    /// How many bytes the view exposes.
    fn __len__(&self) -> usize {
        self.bytes.len()
    }

    /// A copy, for a caller who wants to own the bytes.
    fn tobytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.bytes)
    }

    /// End the view's validity. The dispatcher calls this from the guard that
    /// answers the activation, so the lifetime the packaging contract states —
    /// "the borrowed view's validity ends with the invocation" — is enforced
    /// rather than documented.
    fn release(&self) {
        self.released.store(true, Ordering::Release);
    }

    fn __repr__(&self) -> String {
        format!("<flezha PayloadView of {} bytes>", self.bytes.len())
    }

    /// # Safety
    /// Called by CPython with a `Py_buffer` it owns.
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        if slf.get().released.load(Ordering::Acquire) {
            return Err(PyBufferError::new_err(
                "this payload view belongs to an activation that has already been answered; \
                 copy the bytes with bytes(view) if they must outlive the invocation",
            ));
        }
        unsafe { fill_readonly_view(view, flags, &slf.get().bytes, slf.clone().into_any()) }
    }

    /// # Safety
    /// Called by CPython with the `Py_buffer` `__getbuffer__` filled.
    unsafe fn __releasebuffer__(&self, view: *mut ffi::Py_buffer) {
        // The format string is the one allocation `__getbuffer__` makes.
        drop(unsafe { CString::from_raw((*view).format) });
    }
}

/// Fill a `Py_buffer` from a read-only slice, keeping `owner` alive for as
/// long as the export lasts.
///
/// # Safety
/// `view` must be a `Py_buffer` CPython handed to `__getbuffer__`, and `data`
/// must live as long as `owner` does.
unsafe fn fill_readonly_view(
    view: *mut ffi::Py_buffer,
    flags: c_int,
    data: &[u8],
    owner: Bound<'_, PyAny>,
) -> PyResult<()> {
    if view.is_null() {
        return Err(PyBufferError::new_err("Py_buffer is null"));
    }
    if (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE {
        return Err(PyBufferError::new_err("a payload view is read-only"));
    }
    unsafe {
        (*view).buf = data.as_ptr() as *mut c_void;
        (*view).len = data.len() as ffi::Py_ssize_t;
        (*view).readonly = 1;
        (*view).itemsize = 1;
        (*view).format = if (flags & ffi::PyBUF_FORMAT) == ffi::PyBUF_FORMAT {
            // Freed in `__releasebuffer__`; CPython owns neither end of it.
            CString::new("B").unwrap().into_raw()
        } else {
            std::ptr::null_mut()
        };
        (*view).ndim = 1;
        (*view).shape = if (flags & ffi::PyBUF_ND) == ffi::PyBUF_ND {
            &mut (*view).len
        } else {
            std::ptr::null_mut()
        };
        (*view).strides = if (flags & ffi::PyBUF_STRIDES) == ffi::PyBUF_STRIDES {
            &mut (*view).itemsize
        } else {
            std::ptr::null_mut()
        };
        (*view).suboffsets = std::ptr::null_mut();
        (*view).internal = std::ptr::null_mut();
        // The export holds a reference, so the bytes cannot be freed under a
        // live `memoryview`.
        (*view).obj = owner.into_ptr();
    }
    Ok(())
}

fn payload_view(py: Python<'_>, bytes: Vec<u8>) -> PyResult<Py<PyAny>> {
    Ok(Py::new(py, PayloadView::new(bytes))?.into_any())
}

// ---------------------------------------------------------------------------
// Python <-> schema conversion
//
// The veneer decodes postcard on the Rust side (`schema::decode_activation`)
// so the package never carries a second implementation of the wire contract.
// The shape is a flat dict with a `kind` discriminator rather than a class per
// variant, because that is what the dispatcher branches on anyway and it costs
// no type construction per activation. Absent fields are absent, not None.
// ---------------------------------------------------------------------------

fn read_bytes(what: &str, value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(view) = value.cast::<PayloadView>() {
        return Ok(view.get().bytes.clone());
    }
    if let Ok(b) = value.cast::<PyBytes>() {
        return Ok(b.as_bytes().to_vec());
    }
    Vec::<u8>::from_bound(value).map_err(|_| native_error(format!("{what} must be bytes")))
}

/// A value this module reads out of an answer dict.
///
/// Named once because `Bound::extract` is generic over the borrow AND carries
/// its own error type, so the higher-ranked bound that makes `?` work would
/// otherwise be written at each of the reader functions below.
trait Extractable<'py>: Sized {
    fn from_bound(value: &Bound<'py, PyAny>) -> PyResult<Self>;
}

impl<'py, T> Extractable<'py> for T
where
    T: FromPyObjectOwned<'py>,
    for<'a> <T as FromPyObject<'a, 'py>>::Error: Into<PyErr>,
{
    fn from_bound(value: &Bound<'py, PyAny>) -> PyResult<Self> {
        value.extract::<T>().map_err(Into::into)
    }
}

/// A dict entry, or `None` when the key is absent or explicitly None.
fn opt<'py, T: Extractable<'py>>(dict: &Bound<'py, PyDict>, key: &str) -> PyResult<Option<T>> {
    match dict.get_item(key)? {
        None => Ok(None),
        Some(v) if v.is_none() => Ok(None),
        Some(v) => Ok(Some(T::from_bound(&v)?)),
    }
}

fn req<'py, T: Extractable<'py>>(dict: &Bound<'py, PyDict>, key: &str, what: &str) -> PyResult<T> {
    opt(dict, key)?.ok_or_else(|| native_error(format!("{what} is missing `{key}`")))
}

fn opt_bytes(dict: &Bound<'_, PyDict>, key: &str) -> PyResult<Option<Vec<u8>>> {
    match dict.get_item(key)? {
        None => Ok(None),
        Some(v) if v.is_none() => Ok(None),
        Some(v) => Ok(Some(read_bytes(key, &v)?)),
    }
}

fn opt_seq<'py>(dict: &Bound<'py, PyDict>, key: &str) -> PyResult<Vec<Bound<'py, PyAny>>> {
    match dict.get_item(key)? {
        None => Ok(Vec::new()),
        Some(v) if v.is_none() => Ok(Vec::new()),
        Some(v) => v.try_iter()?.collect(),
    }
}

fn as_dict<'py>(value: &Bound<'py, PyAny>, what: &str) -> PyResult<Bound<'py, PyDict>> {
    value
        .cast::<PyDict>()
        .map_err(|_| native_error(format!("{what} must be a dict")))
        .cloned()
}

/// Metadata is a list of `(key, value)` pairs rather than a dict: the schema
/// allows a repeated key, and the order is part of what the controller sees.
fn meta_out<'py>(py: Python<'py>, pairs: Vec<(String, String)>) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    for (key, value) in pairs {
        out.append(PyTuple::new(py, [key, value])?)?;
    }
    Ok(out)
}

fn meta_in(dict: &Bound<'_, PyDict>, key: &str) -> PyResult<Vec<(String, String)>> {
    let mut out = Vec::new();
    for entry in opt_seq(dict, key)? {
        let pair = Vec::<String>::from_bound(&entry)
            .map_err(|_| native_error("a metadata entry must be a (key, value) pair"))?;
        if pair.len() != 2 {
            return Err(native_error("a metadata entry must be a (key, value) pair"));
        }
        let mut it = pair.into_iter();
        out.push((it.next().unwrap(), it.next().unwrap()));
    }
    Ok(out)
}

fn timer_kind_in(kind: &str) -> PyResult<schema::TimerKind> {
    match kind {
        "event_time" => Ok(schema::TimerKind::EventTime),
        "processing_time" => Ok(schema::TimerKind::ProcessingTime),
        other => Err(native_error(format!("unknown timer kind {other:?}"))),
    }
}

fn restore_body<'py>(py: Python<'py>, body: schema::RestoreBody) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("bytes", payload_view(py, body.bytes)?)?;
    if let Some(v) = body.from_version {
        out.set_item("from_version", v)?;
    }
    Ok(out)
}

/// Decode a polled activation into the dict the package dispatches on.
pub fn activation_to_python<'py>(
    py: Python<'py>,
    act: schema::Activation,
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("token", act.token)?;
    out.set_item("operative", act.target.operative)?;
    out.set_item("procedure", act.target.procedure)?;
    if let Some(key) = act.target.entity_key {
        out.set_item("entity_key", key)?;
    }
    out.set_item("change_seq", act.change_seq)?;
    out.set_item("factory", act.factory)?;

    match act.kind {
        schema::ActivationKind::Work(items) => {
            out.set_item("kind", "work")?;
            let list = PyList::empty(py);
            for item in items {
                let h = PyDict::new(py);
                match item {
                    schema::WorkItem::Input(i) => {
                        h.set_item("kind", "input")?;
                        h.set_item("channel", i.channel)?;
                        h.set_item("payload", payload_view(py, i.payload)?)?;
                        h.set_item("metadata", meta_out(py, i.metadata)?)?;
                        if let Some(offset) = i.consumed_offset {
                            h.set_item("consumed_offset", offset)?;
                        }
                        h.set_item("replay", i.replay)?;
                        h.set_item("now", i.now)?;
                    }
                    schema::WorkItem::Timer(t) => {
                        h.set_item("kind", "timer")?;
                        h.set_item("timer_kind", t.kind.as_str())?;
                        h.set_item("scheduled_at", t.scheduled_at)?;
                        // A timer firing's `now` IS its scheduled_at, so a
                        // chained `timer_after` anchors replay-stably.
                        h.set_item("now", t.scheduled_at)?;
                    }
                }
                list.append(h)?;
            }
            out.set_item("items", list)?;
        }
        schema::ActivationKind::RestoreFold(fold) => {
            out.set_item("kind", "restore_fold")?;
            if let Some(snapshot) = fold.snapshot {
                out.set_item("snapshot", restore_body(py, snapshot)?)?;
            }
            let changes = PyList::empty(py);
            for body in fold.changes {
                changes.append(restore_body(py, body)?)?;
            }
            out.set_item("changes", changes)?;
        }
        schema::ActivationKind::SnapshotRequest => out.set_item("kind", "snapshot_request")?,
        schema::ActivationKind::LiveStateRead => out.set_item("kind", "live_state_read")?,
        schema::ActivationKind::ApplyState { state_json } => {
            out.set_item("kind", "apply_state")?;
            // JSON rather than the codec's own format: `json.loads` takes
            // bytes and not an arbitrary buffer, so this one crosses as bytes.
            out.set_item("state_json", PyBytes::new(py, &state_json))?;
        }
        schema::ActivationKind::MigrationSerialize => {
            out.set_item("kind", "migration_serialize")?
        }
        schema::ActivationKind::KeyRequest(request) => {
            out.set_item("kind", "key_request")?;
            let h = PyDict::new(py);
            h.set_item("channel", request.channel)?;
            let payloads = PyList::empty(py);
            for p in request.payloads {
                payloads.append(payload_view(py, p)?)?;
            }
            h.set_item("payloads", payloads)?;
            h.set_item("want_keys", request.want_keys)?;
            h.set_item("want_event_time", request.want_event_time)?;
            out.set_item("key_request", h)?;
        }
        schema::ActivationKind::WorkflowRun { args } => {
            out.set_item("kind", "workflow_run")?;
            out.set_item("args", payload_view(py, args)?)?;
        }
        schema::ActivationKind::CompensateRun { outputs } => {
            out.set_item("kind", "compensate_run")?;
            let list = PyList::empty(py);
            for o in outputs {
                list.append(payload_view(py, o)?)?;
            }
            out.set_item("outputs", list)?;
        }
    }
    Ok(out)
}

fn output_in(value: &Bound<'_, PyAny>) -> PyResult<schema::Output> {
    let h = as_dict(value, "an output")?;
    let target: String = req(&h, "target", "output")?;
    let target = match target.as_str() {
        "edge" => schema::OutputTarget::Edge {
            channel: req(&h, "channel", "edge output")?,
            consumer: req(&h, "consumer", "edge output")?,
        },
        "plugin" => schema::OutputTarget::Plugin {
            channel: req(&h, "plugin_channel", "plugin output")?,
        },
        other => return Err(native_error(format!("unknown output target {other:?}"))),
    };
    Ok(schema::Output {
        target,
        key: opt(&h, "key")?,
        event_time: opt(&h, "event_time")?,
        metadata: meta_in(&h, "metadata")?,
        payload: opt_bytes(&h, "payload")?.unwrap_or_default(),
    })
}

fn effect_in(value: &Bound<'_, PyAny>) -> PyResult<schema::ItemEffect> {
    let h = as_dict(value, "an item effect")?;
    let verdict: String = req(&h, "verdict", "item effect")?;
    let message: Option<String> = opt(&h, "message")?;
    let verdict = match verdict.as_str() {
        "ok" => schema::Verdict::Ok,
        "retryable" => schema::Verdict::RetryableError {
            message: message.unwrap_or_default(),
        },
        "non_retryable" => schema::Verdict::NonRetryableError {
            message: message.unwrap_or_default(),
        },
        other => return Err(native_error(format!("unknown verdict {other:?}"))),
    };

    let mut changes = Vec::new();
    for c in opt_seq(&h, "changes")? {
        changes.push(read_bytes("change", &c)?);
    }
    let mut outputs = Vec::new();
    for o in opt_seq(&h, "outputs")? {
        outputs.push(output_in(&o)?);
    }
    let mut timers = Vec::new();
    for t in opt_seq(&h, "timers")? {
        let t = as_dict(&t, "a timer op")?;
        let kind: String = req(&t, "kind", "timer op")?;
        timers.push(schema::TimerOp {
            kind: timer_kind_in(&kind)?,
            scheduled_at: req(&t, "scheduled_at", "timer op")?,
            register: req(&t, "register", "timer op")?,
        });
    }
    let dlq = match h.get_item("dlq")? {
        None => None,
        Some(d) if d.is_none() => None,
        Some(d) => {
            let d = as_dict(&d, "a dlq entry")?;
            Some(schema::DlqEntry {
                channel: req(&d, "channel", "dlq entry")?,
                error: req(&d, "error", "dlq entry")?,
                attempts: req(&d, "attempts", "dlq entry")?,
                payload: opt_bytes(&d, "payload")?,
            })
        }
    };

    Ok(schema::ItemEffect {
        verdict,
        changes,
        outputs,
        consumed_offset: opt(&h, "consumed_offset")?,
        event_time: opt(&h, "event_time")?.unwrap_or(0),
        timers,
        checkpoint: opt(&h, "checkpoint")?.unwrap_or(false),
        dlq,
    })
}

/// One prepare answer per payload, in order. A per-payload refusal is a
/// `WorkError` inside the answer — the input's retry ladder takes it from
/// there — while a malformed answer raises at the caller, so the two failure
/// kinds cannot be collected together.
fn prepared_in(
    answers: Vec<Bound<'_, PyAny>>,
) -> PyResult<Vec<Result<schema::PrepareAnswer, schema::WorkError>>> {
    let mut out = Vec::with_capacity(answers.len());
    for value in answers {
        let h = as_dict(&value, "a prepare answer")?;
        out.push(match opt::<String>(&h, "error")? {
            Some(message) => Err(schema::WorkError {
                message,
                non_retryable: opt(&h, "non_retryable")?.unwrap_or(false),
            }),
            None => Ok(schema::PrepareAnswer {
                keys: opt(&h, "keys")?.unwrap_or_default(),
                event_time: opt(&h, "event_time")?,
            }),
        });
    }
    Ok(out)
}

/// Build the postcard `ActivationResult` from the dict Python answers with.
/// Encoding lives here for the same reason decoding does: one implementation
/// of the wire contract, in the crate that owns it.
pub fn result_from_python(r: &Bound<'_, PyDict>) -> PyResult<schema::ActivationResult> {
    let outcome: String = req(r, "outcome", "activation result")?;
    let outcome = match outcome.as_str() {
        "work" => {
            let mut effects = Vec::new();
            for e in opt_seq(r, "effects")? {
                effects.push(effect_in(&e)?);
            }
            schema::ActivationOutcome::Work(effects)
        }
        "restored" => schema::ActivationOutcome::Restored,
        "snapshot" => schema::ActivationOutcome::Snapshot {
            state: opt_bytes(r, "state")?.unwrap_or_default(),
        },
        "state_json" => schema::ActivationOutcome::StateJson {
            json: opt_bytes(r, "json")?.unwrap_or_default(),
        },
        "state_applied" => schema::ActivationOutcome::StateApplied,
        "migration_state" => schema::ActivationOutcome::MigrationState {
            json: opt_bytes(r, "json")?.unwrap_or_default(),
        },
        "prepared" => schema::ActivationOutcome::Prepared(prepared_in(opt_seq(r, "prepared")?)?),
        "failed" => schema::ActivationOutcome::Failed(schema::WorkError {
            message: opt(r, "error")?.unwrap_or_else(|| "activation failed".into()),
            non_retryable: opt(r, "non_retryable")?.unwrap_or(false),
        }),
        "workflow_done" => schema::ActivationOutcome::WorkflowDone {
            completed: opt(r, "workflow_completed")?.unwrap_or(false),
            result: opt_bytes(r, "workflow_result")?.unwrap_or_default(),
        },
        other => {
            return Err(native_error(format!(
                "unknown activation outcome {other:?}"
            )))
        }
    };
    Ok(schema::ActivationResult {
        token: req(r, "token", "activation result")?,
        outcome,
    })
}

// ---------------------------------------------------------------------------
// The context object. Every method below corresponds to exactly one C entry
// point.
// ---------------------------------------------------------------------------

/// The one context this host holds, as an opaque Python object.
///
/// `frozen` because nothing about it is reassignable from Python and the ABI
/// is thread-safe by contract — which is also what makes the manual `Sync`
/// sound: every method below is a call into `flz_core_ffi`, whose entry points
/// take `*mut FlzContext` from any thread by design.
#[pyclass(module = "flezha.flz_core_ffi", name = "Core", frozen)]
pub struct Core {
    ctx: NonNull<FlzContext>,
    freed: AtomicBool,
}

// SAFETY: `FlzContext` is the ABI's own thread-safe handle — its interior is
// `DashMap`, `Mutex`, `AtomicBool` and `RwLock`, and every entry point is
// documented as callable from any thread, `flz_core_complete` explicitly so.
//
// This is what the `#[pyclass]` must NOT be marked `unsendable` for: that
// attribute pins an object to its creating thread and panics on a call from any
// other, which is precisely what a worker pool is. The pool's own tests use a
// Python fake and would not have found it; the e2e did, on the first poll.
unsafe impl Send for Core {}
unsafe impl Sync for Core {}

impl Core {
    fn ptr(&self) -> PyResult<*mut FlzContext> {
        if self.freed.load(Ordering::Acquire) {
            return Err(native_error(
                "the flz_core context has been freed; create a new one",
            ));
        }
        Ok(self.ctx.as_ptr())
    }

    fn core_error(&self) -> PyErr {
        native_error(last_error_of(self.ctx.as_ptr()))
    }
}

#[pymethods]
impl Core {
    /// Create the process context, negotiating the schema version. Raises
    /// `SchemaMismatch` naming both versions, before any controller connection
    /// is attempted.
    #[new]
    fn new(shim_schema_version: u32) -> PyResult<Self> {
        let ctx = crate::flz_core_context_new(shim_schema_version);
        match NonNull::new(ctx) {
            Some(ctx) => Ok(Self {
                ctx,
                freed: AtomicBool::new(false),
            }),
            None => {
                let ptr = crate::flz_core_init_error();
                let message = if ptr.is_null() {
                    "flz_core context creation failed".to_string()
                } else {
                    unsafe { CStr::from_ptr(ptr) }
                        .to_string_lossy()
                        .into_owned()
                };
                Err(SchemaMismatch::new_err(message))
            }
        }
    }

    /// Destroy the context. The host's threads must already be stopped.
    fn free(&self) {
        if !self.freed.swap(true, Ordering::AcqRel) {
            unsafe { crate::flz_core_context_free(self.ctx.as_ptr()) };
        }
    }

    /// Declare one host class from its JSON declaration. Raises the core's
    /// refusal.
    fn declare_class(&self, json: &[u8]) -> PyResult<()> {
        let ctx = self.ptr()?;
        let status = unsafe { crate::flz_core_declare_class(ctx, json.as_ptr(), json.len()) };
        if status != FLZ_OK {
            return Err(self.core_error());
        }
        Ok(())
    }

    /// Connect to the controller and start the runner loop. Blocking: the ABI
    /// call waits up to ten seconds for the connect confirmation.
    fn start(&self, py: Python<'_>, namespace: &str) -> PyResult<()> {
        let ctx = self.ptr()? as usize;
        let namespace = c_string("namespace", namespace)?;
        let status = py.detach(move || unsafe {
            crate::flz_core_start(ctx as *mut FlzContext, namespace.as_ptr())
        });
        if status != FLZ_OK {
            return Err(self.core_error());
        }
        Ok(())
    }

    /// Block up to `timeout_ms` for one activation. Returns the activation
    /// dict, `None` on timeout, or the string `"shutting_down"` once every
    /// poller has been released.
    fn poll<'py>(&self, py: Python<'py>, timeout_ms: u64) -> PyResult<Bound<'py, PyAny>> {
        let ctx = self.ptr()? as usize;
        let (status, encoded) = py.detach(move || {
            take_buffer(FLZ_POLL_ACTIVATION, |buf, len| unsafe {
                crate::flz_core_poll(ctx as *mut FlzContext, timeout_ms, buf, len)
            })
        });
        match status {
            FLZ_POLL_ACTIVATION => {
                let act = schema::decode_activation(&encoded.unwrap_or_default())
                    .map_err(|e| native_error(format!("activation failed to decode: {e}")))?;
                Ok(activation_to_python(py, act)?.into_any())
            }
            FLZ_POLL_TIMEOUT => Ok(py.None().into_bound(py)),
            FLZ_POLL_SHUTTING_DOWN => Ok(PyString::new(py, "shutting_down").into_any()),
            _ => Err(self.core_error()),
        }
    }

    /// Park up to `timeout_ms` until the core observes starvation. Answers the
    /// ABI's three-way status directly (`1` observed, `0` timeout, `2` shutting
    /// down), so a supervisor loop allocates nothing per park.
    fn wait_starvation(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<i32> {
        let ctx = self.ptr()? as usize;
        let status = py.detach(move || unsafe {
            crate::flz_core_wait_starvation(ctx as *mut FlzContext, timeout_ms)
        });
        match status {
            FLZ_STARVATION_OBSERVED | FLZ_POLL_TIMEOUT | FLZ_POLL_SHUTTING_DOWN => Ok(status),
            _ => Err(self.core_error()),
        }
    }

    /// Answer one activation. Blocking, and this is the entry point the GIL
    /// rule exists for on the completion side: under the default `DropNothing`
    /// overflow policy the routing push parks until the consumer's mailbox
    /// drains, and the consumer drains only when its handler runs on another
    /// Python thread.
    fn complete(&self, py: Python<'_>, result: &Bound<'_, PyDict>) -> PyResult<()> {
        let ctx = self.ptr()? as usize;
        let encoded = schema::encode_result(&result_from_python(result)?);
        let status = py.detach(move || unsafe {
            crate::flz_core_complete(ctx as *mut FlzContext, encoded.as_ptr(), encoded.len())
        });
        if status != FLZ_OK {
            return Err(self.core_error());
        }
        Ok(())
    }

    /// Abandon a polled activation: re-hand its untouched items and release its
    /// entity. Non-blocking by design — the dispatcher runs it from a
    /// `finally`, where anything that can park is one more thing that can go
    /// wrong.
    fn abandon(&self, token: u64) -> PyResult<()> {
        let ctx = self.ptr()?;
        let status = unsafe { crate::flz_core_abandon(ctx, token) };
        if status != FLZ_OK {
            return Err(self.core_error());
        }
        Ok(())
    }

    /// Wake every blocked poller and stop the runner loop.
    fn shutdown(&self) -> PyResult<()> {
        let ctx = self.ptr()?;
        unsafe { crate::flz_core_shutdown(ctx) };
        Ok(())
    }

    /// Emit one output payload from a live workflow run-sugar body.
    /// Non-blocking in the core (an unbounded coordinator send), so it holds
    /// the GIL.
    fn workflow_emit(&self, instance_id: &str, payload: &[u8]) -> PyResult<()> {
        let ctx = self.ptr()?;
        let instance = c_string("instance id", instance_id)?;
        let status = unsafe {
            crate::flz_core_workflow_emit(ctx, instance.as_ptr(), payload.as_ptr(), payload.len())
        };
        if status != FLZ_OK {
            return Err(self.core_error());
        }
        Ok(())
    }

    /// The next inbox item of a live workflow body, or `None` on timeout.
    fn workflow_recv<'py>(
        &self,
        py: Python<'py>,
        instance_id: &str,
        timeout_ms: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ctx = self.ptr()? as usize;
        let instance = c_string("instance id", instance_id)?;
        let (status, payload) = py.detach(move || {
            take_buffer(FLZ_POLL_ACTIVATION, |buf, len| unsafe {
                crate::flz_core_workflow_recv(
                    ctx as *mut FlzContext,
                    instance.as_ptr(),
                    timeout_ms,
                    buf,
                    len,
                )
            })
        });
        match status {
            FLZ_POLL_ACTIVATION => {
                Ok(payload_view(py, payload.unwrap_or_default())?.into_bound(py))
            }
            FLZ_POLL_TIMEOUT => Ok(py.None().into_bound(py)),
            _ => Err(self.core_error()),
        }
    }

    /// Whether the named workflow body's cancellation token has tripped.
    fn workflow_cancelled(&self, instance_id: &str) -> PyResult<bool> {
        let ctx = self.ptr()?;
        let instance = c_string("instance id", instance_id)?;
        Ok(unsafe { crate::flz_core_workflow_cancelled(ctx, instance.as_ptr()) } != 0)
    }

    /// Spawn-or-deliver a workflow instance; answers the controller's outcome
    /// string. Blocking: the ABI call waits up to thirty seconds.
    fn spawn_workflow(
        &self,
        py: Python<'_>,
        instance_id: &str,
        factory_id: &str,
        payload: &[u8],
        if_running: &str,
    ) -> PyResult<String> {
        let ctx = self.ptr()? as usize;
        let instance = c_string("instance id", instance_id)?;
        let factory = c_string("factory id", factory_id)?;
        let mode = c_string("if_running", if_running)?;
        let payload = payload.to_vec();
        let (status, outcome) = py.detach(move || {
            take_buffer(FLZ_OK, |out, out_len| unsafe {
                crate::flz_core_spawn_workflow(
                    ctx as *mut FlzContext,
                    instance.as_ptr(),
                    factory.as_ptr(),
                    payload.as_ptr(),
                    payload.len(),
                    mode.as_ptr(),
                    out,
                    out_len,
                )
            })
        });
        if status != FLZ_OK {
            return Err(self.core_error());
        }
        Ok(String::from_utf8_lossy(&outcome.unwrap_or_default()).into_owned())
    }

    /// The context's last recorded error, or `None`.
    fn last_error(&self) -> Option<String> {
        if self.freed.load(Ordering::Acquire) {
            return None;
        }
        let msg = unsafe { crate::flz_core_last_error(self.ctx.as_ptr()) };
        if msg.is_null() {
            return None;
        }
        Some(
            unsafe { CStr::from_ptr(msg) }
                .to_string_lossy()
                .into_owned(),
        )
    }
}

/// Run one ABI call that hands out a buffer, and take ownership of what it
/// handed out.
///
/// The ownership rule this module promises, in the one place it is
/// implemented: the buffer is copied out and freed through
/// [`crate::flz_core_buf_free`] inside the same GIL-released window, so no
/// native buffer can reach the Python heap however the caller answers.
///
/// `handed_out` is the status that means a buffer was produced —
/// [`FLZ_POLL_ACTIVATION`] for poll and the workflow receive, [`FLZ_OK`] for
/// spawn. Every other status leaves the out pointers untouched.
fn take_buffer<F>(handed_out: i32, call: F) -> (i32, Option<Vec<u8>>)
where
    F: FnOnce(*mut *mut u8, *mut usize) -> i32,
{
    let mut buf: *mut u8 = std::ptr::null_mut();
    let mut len: usize = 0;
    let status = call(&mut buf, &mut len);
    if status != handed_out {
        return (status, None);
    }
    let owned = unsafe { std::slice::from_raw_parts(buf, len) }.to_vec();
    unsafe { crate::flz_core_buf_free(buf, len) };
    (status, Some(owned))
}

// ---------------------------------------------------------------------------
// Module-level entry points that take no context
// ---------------------------------------------------------------------------

/// The activation schema version this native library speaks.
#[pyfunction]
fn schema_version() -> u32 {
    crate::flz_core_schema_version()
}

/// The core's monotonic clock in nanoseconds.
#[pyfunction]
fn monotonic_nanos() -> u64 {
    crate::flz_core_monotonic_nanos()
}

/// When the named entry channel last received a frame, on the same clock;
/// `0` when no frame ever has.
#[pyfunction]
fn entry_last_delivery_nanos(channel: &str) -> PyResult<u64> {
    let channel = c_string("channel", channel)?;
    Ok(unsafe { crate::flz_core_entry_last_delivery_nanos(channel.as_ptr()) })
}

/// Whether this extension was built for a free-threaded interpreter.
///
/// Reported in the runner's startup line, so a benchmark can never be
/// ambiguous about which wheel tier produced it. Read from the build rather
/// than from `sys`, because what matters is the binary that is running.
#[pyfunction]
fn free_threaded() -> bool {
    cfg!(Py_GIL_DISABLED)
}

/// Every function this extension exports, with the C ABI entry point behind
/// it: `(name, abi_entry_point, blocking)`.
///
/// Two properties are asserted against this table rather than reviewed, so
/// neither can lapse by omission when an export is added later:
///
/// * the veneer adds no ABI surface — every export names an existing entry
///   point, and this veneer has no host bookkeeping of its own because the
///   Python host's threads are Python's;
/// * every export marked blocking runs inside `Python::detach`.
#[pyfunction]
fn native_surface<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    for (name, abi, blocking) in NATIVE_SURFACE {
        out.append((*name, *abi, *blocking).into_bound_py_any(py)?)?;
    }
    Ok(out)
}

/// The table `native_surface` renders and the Rust-side test asserts against.
pub const NATIVE_SURFACE: &[(&str, &str, bool)] = &[
    ("schema_version", "flz_core_schema_version", false),
    ("Core", "flz_core_context_new", false),
    ("Core.free", "flz_core_context_free", false),
    ("Core.declare_class", "flz_core_declare_class", false),
    ("Core.start", "flz_core_start", true),
    ("Core.poll", "flz_core_poll", true),
    ("Core.wait_starvation", "flz_core_wait_starvation", true),
    ("Core.complete", "flz_core_complete", true),
    ("Core.abandon", "flz_core_abandon", false),
    ("Core.shutdown", "flz_core_shutdown", false),
    ("Core.workflow_emit", "flz_core_workflow_emit", false),
    ("Core.workflow_recv", "flz_core_workflow_recv", true),
    (
        "Core.workflow_cancelled",
        "flz_core_workflow_cancelled",
        false,
    ),
    ("Core.spawn_workflow", "flz_core_spawn_workflow", true),
    ("monotonic_nanos", "flz_core_monotonic_nanos", false),
    (
        "entry_last_delivery_nanos",
        "flz_core_entry_last_delivery_nanos",
        false,
    ),
    ("Core.last_error", "flz_core_last_error", false),
    ("free_threaded", "flz_core_schema_version", false),
    ("native_surface", "flz_core_schema_version", false),
];

/// Define the `flezha.flz_core_ffi` extension module.
#[pymodule]
#[pyo3(name = "flz_core_ffi")]
pub fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Core>()?;
    m.add_class::<PayloadView>()?;
    m.add("NativeError", m.py().get_type::<NativeError>())?;
    m.add("SchemaMismatch", m.py().get_type::<SchemaMismatch>())?;
    m.add_function(wrap_pyfunction!(schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(monotonic_nanos, m)?)?;
    m.add_function(wrap_pyfunction!(entry_last_delivery_nanos, m)?)?;
    m.add_function(wrap_pyfunction!(free_threaded, m)?)?;
    m.add_function(wrap_pyfunction!(native_surface, m)?)?;
    Ok(())
}
