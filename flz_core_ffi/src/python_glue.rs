//! Python bindings over this crate's C ABI for the `flezha` wheel's native
//! module, one method of [`Core`] per C entry point.
//!
//! The context is an object, not a static: CPython runs sub-interpreters,
//! and a `#[pyclass]` is reached only through the interpreter holding it.
//! Every blocking entry point runs inside [`Python::detach`], so a call that
//! parks in the core never holds the interpreter lock; the poll timeout the
//! caller passes bounds how quickly an interrupt or a runner close is seen,
//! because the ABI has no per-thread wake. The user payload reaches Python
//! as a [`PayloadView`] — a buffer-protocol object over bytes this module
//! owns — so the common decode path allocates no intermediate `bytes`.

use std::ffi::{c_int, c_void, CString};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, Ordering};

use pyo3::exceptions::{PyBufferError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyModule, PyString, PyTuple};
use pyo3::{create_exception, ffi, IntoPyObjectExt};

use crate::veneer::{self, shape, CtxPtr, Polled, Refusal};
use crate::FlzContext;

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

/// The one mapping of a refusal onto the module's exception classes.
impl From<Refusal> for PyErr {
    fn from(refusal: Refusal) -> PyErr {
        match refusal {
            Refusal::Core(message) | Refusal::Shape(message) => native_error(message),
            Refusal::Schema(message) => SchemaMismatch::new_err(message),
        }
    }
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

fn payload_view(py: Python<'_>, bytes: Vec<u8>) -> PyResult<Bound<'_, PyAny>> {
    Ok(Bound::new(py, PayloadView::new(bytes))?.into_any())
}

// ---------------------------------------------------------------------------
// Python <-> schema conversion
//
// The veneer decodes postcard on the Rust side (`schema::decode_activation`)
// so the package never carries a second implementation of the wire contract.
// The shape is a flat dict with a `kind` discriminator rather than a class per
// variant, because that is what the dispatcher branches on anyway and it costs
// no type construction per activation. Absent fields are absent, not None.
// Which key carries which field is `veneer::shape`'s; this module says only
// how Python builds and reads a value.
// ---------------------------------------------------------------------------

/// The Python half of [`shape::Sink`] and [`shape::Source`]: a handle over
/// the interpreter token, not state.
#[derive(Clone, Copy)]
pub struct Values<'py>(pub Python<'py>);

impl<'py> shape::Sink for Values<'py> {
    type Value = Bound<'py, PyAny>;
    type Map = Bound<'py, PyDict>;
    type List = Bound<'py, PyList>;
    type Error = PyErr;

    fn map(&self) -> PyResult<Self::Map> {
        Ok(PyDict::new(self.0))
    }

    fn set(&self, map: &mut Self::Map, key: shape::Key, value: Self::Value) -> PyResult<()> {
        map.set_item(key.spell(<Self as shape::Sink>::CAMEL), value)
    }

    fn finish_map(&self, map: Self::Map) -> PyResult<Self::Value> {
        Ok(map.into_any())
    }

    fn list(&self, _capacity: usize) -> PyResult<Self::List> {
        Ok(PyList::empty(self.0))
    }

    fn push(&self, list: &mut Self::List, value: Self::Value) -> PyResult<()> {
        list.append(value)
    }

    fn finish_list(&self, list: Self::List) -> PyResult<Self::Value> {
        Ok(list.into_any())
    }

    fn bytes(&self, v: Vec<u8>) -> PyResult<Self::Value> {
        payload_view(self.0, v)
    }

    /// `json.loads` takes bytes and not an arbitrary buffer, so state JSON is
    /// the one payload that crosses as `bytes`.
    fn json(&self, v: Vec<u8>) -> PyResult<Self::Value> {
        Ok(PyBytes::new(self.0, &v).into_any())
    }

    fn text(&self, v: String) -> PyResult<Self::Value> {
        Ok(PyString::new(self.0, &v).into_any())
    }

    fn word(&self, v: &'static str) -> PyResult<Self::Value> {
        Ok(PyString::new(self.0, v).into_any())
    }

    fn pair(&self, key: String, value: String) -> PyResult<Self::Value> {
        Ok(PyTuple::new(self.0, [key, value])?.into_any())
    }

    fn u64(&self, v: u64) -> PyResult<Self::Value> {
        v.into_bound_py_any(self.0)
    }

    fn u32(&self, v: u32) -> PyResult<Self::Value> {
        v.into_bound_py_any(self.0)
    }

    fn bool(&self, v: bool) -> PyResult<Self::Value> {
        v.into_bound_py_any(self.0)
    }
}

impl<'py> shape::Source for Values<'py> {
    type Value = Bound<'py, PyAny>;
    type Map = Bound<'py, PyDict>;
    type Error = PyErr;

    fn get(&self, map: &Self::Map, key: shape::Key) -> PyResult<Option<Self::Value>> {
        Ok(
            match map.get_item(key.spell(<Self as shape::Source>::CAMEL))? {
                Some(v) if !v.is_none() => Some(v),
                _ => None,
            },
        )
    }

    fn map(&self, v: &Self::Value, what: &str) -> PyResult<Self::Map> {
        v.cast::<PyDict>()
            .map_err(|_| native_error(format!("{what} must be a dict")))
            .cloned()
    }

    fn text(&self, v: &Self::Value) -> PyResult<String> {
        v.extract::<String>()
    }

    fn bytes(&self, v: &Self::Value, what: &str) -> PyResult<Vec<u8>> {
        read_bytes(what, v)
    }

    fn u64(&self, v: &Self::Value, _what: &str) -> PyResult<u64> {
        v.extract::<u64>()
    }

    fn u32(&self, v: &Self::Value) -> PyResult<u32> {
        v.extract::<u32>()
    }

    fn bool(&self, v: &Self::Value) -> PyResult<bool> {
        v.extract::<bool>()
    }

    fn items(&self, v: &Self::Value) -> PyResult<Vec<Self::Value>> {
        v.try_iter()?.collect()
    }

    fn pair(&self, v: &Self::Value) -> PyResult<(String, String)> {
        let malformed = || native_error("a metadata entry must be a (key, value) pair");
        let pair = v.extract::<Vec<String>>().map_err(|_| malformed())?;
        let mut it = pair.into_iter();
        match (it.next(), it.next(), it.next()) {
            (Some(key), Some(value), None) => Ok((key, value)),
            _ => Err(malformed()),
        }
    }
}

/// The bytes behind a payload: the view the veneer handed out, a `bytes`, or
/// anything else that reads as a byte sequence.
fn read_bytes(what: &str, value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(view) = value.cast::<PayloadView>() {
        return Ok(view.get().bytes.clone());
    }
    if let Ok(b) = value.cast::<PyBytes>() {
        return Ok(b.as_bytes().to_vec());
    }
    value
        .extract::<Vec<u8>>()
        .map_err(|_| native_error(format!("{what} must be bytes")))
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
    fn ptr(&self) -> PyResult<CtxPtr> {
        if self.freed.load(Ordering::Acquire) {
            return Err(native_error(
                "the flz_core context has been freed; create a new one",
            ));
        }
        Ok(CtxPtr::of(self.ctx.as_ptr()))
    }
}

#[pymethods]
impl Core {
    /// Create the process context, negotiating the schema version. Raises
    /// `SchemaMismatch` naming both versions, before any controller connection
    /// is attempted.
    #[new]
    fn new(shim_schema_version: u32) -> PyResult<Self> {
        let ctx = NonNull::new(veneer::context_new(shim_schema_version)?)
            .ok_or_else(|| native_error("flz_core context creation failed"))?;
        Ok(Self {
            ctx,
            freed: AtomicBool::new(false),
        })
    }

    /// Destroy the context. The host's threads must already be stopped.
    fn free(&self) {
        if !self.freed.swap(true, Ordering::AcqRel) {
            veneer::context_free(CtxPtr::of(self.ctx.as_ptr()));
        }
    }

    /// Declare one host class from its JSON declaration. Raises the core's
    /// refusal.
    fn declare_class(&self, json: &[u8]) -> PyResult<()> {
        Ok(veneer::declare_class(self.ptr()?, json)?)
    }

    /// Judge one host class declaration without registering it. Answers the
    /// derived declaration as JSON; raises the core's verdict on a refusal.
    ///
    /// The raised exception carries that verdict as the core's own JSON —
    /// `{"issues":[{"name","text"}]}` — on its `verdict` attribute, so the SDK
    /// reads which refusals fired off data rather than out of the rendered
    /// message, which is unchanged.
    fn check_class<'py>(&self, py: Python<'py>, json: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
        let ctx = self.ptr()?;
        match veneer::check_class(ctx, json) {
            Ok(derived) => Ok(PyBytes::new(py, &derived)),
            Err((refusal, verdict)) => {
                let refused = PyErr::from(refusal);
                refused
                    .value(py)
                    .setattr("verdict", PyBytes::new(py, &verdict))?;
                Err(refused)
            }
        }
    }

    /// Connect to the controller and start the runner loop. Blocking: the ABI
    /// call waits up to ten seconds for the connect confirmation.
    fn start(&self, py: Python<'_>, namespace: &str) -> PyResult<()> {
        let ctx = self.ptr()?;
        let namespace = veneer::c_string("namespace", namespace)?;
        Ok(py.detach(move || veneer::start(ctx, &namespace))?)
    }

    /// Start the declared class as a local run: no controller, no socket.
    /// The alternative to `start`, never an addition. The host keeps driving
    /// its own poll loop, which is what runs the procedures.
    fn local_start(&self, factory_id: &str, operative_id: &str) -> PyResult<()> {
        let ctx = self.ptr()?;
        let factory = veneer::c_string("factory id", factory_id)?;
        let operative = veneer::c_string("operative id", operative_id)?;
        Ok(veneer::local_start(ctx, &factory, &operative)?)
    }

    /// Queue the declared sink `channel` for `local_next_output`. A channel
    /// the declaration does not carry is refused here, by name.
    fn local_on_sink(&self, channel: &str) -> PyResult<()> {
        let ctx = self.ptr()?;
        let channel = veneer::c_string("sink channel", channel)?;
        Ok(veneer::local_on_sink(ctx, &channel)?)
    }

    /// Feed one value's wire bytes to a declared entry channel. Blocking: the
    /// entry path parks while the consumer's mailbox is full.
    fn local_feed(&self, py: Python<'_>, channel: &str, payload: &[u8]) -> PyResult<()> {
        let ctx = self.ptr()?;
        let channel = veneer::c_string("entry channel", channel)?;
        let payload = payload.to_vec();
        Ok(py.detach(move || veneer::local_feed(ctx, &channel, &payload))?)
    }

    /// The next value a sink released as the output dict this SDK already
    /// reads, or `None` on timeout. Blocking for the whole timeout.
    fn local_next_output<'py>(
        &self,
        py: Python<'py>,
        timeout_ms: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ctx = self.ptr()?;
        match py.detach(move || veneer::local_next_output(ctx, timeout_ms))? {
            Some(encoded) => {
                let output = flz_core::schema::decode_output(&encoded)
                    .map_err(|e| native_error(format!("output failed to decode: {e}")))?;
                shape::sink_output(&Values(py), output)
            }
            None => Ok(py.None().into_bound(py)),
        }
    }

    /// Park until the graph has nothing left to do. A timeout is a refusal,
    /// never a silent success.
    fn local_run_until_idle(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<()> {
        let ctx = self.ptr()?;
        Ok(py.detach(move || veneer::local_run_until_idle(ctx, timeout_ms))?)
    }

    /// End the local run and release its Entities. A context holding none is
    /// a no-op, so a host's close path needs no test of its own.
    fn local_stop(&self) -> PyResult<()> {
        veneer::local_stop(self.ptr()?);
        Ok(())
    }

    /// Block up to `timeout_ms` for one activation. Returns the activation
    /// dict, `None` on timeout, or the string `"shutting_down"` once every
    /// poller has been released.
    fn poll<'py>(&self, py: Python<'py>, timeout_ms: u64) -> PyResult<Bound<'py, PyAny>> {
        let ctx = self.ptr()?;
        match py.detach(move || veneer::poll(ctx, timeout_ms))? {
            Polled::Activation(act) => shape::activation(&Values(py), act),
            Polled::Timeout => Ok(py.None().into_bound(py)),
            Polled::ShuttingDown => Ok(PyString::new(py, "shutting_down").into_any()),
        }
    }

    /// Park up to `timeout_ms` until the core observes starvation. Answers the
    /// ABI's three-way status directly (`1` observed, `0` timeout, `2` shutting
    /// down), so a supervisor loop allocates nothing per park.
    fn wait_starvation(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<i32> {
        let ctx = self.ptr()?;
        Ok(py.detach(move || veneer::wait_starvation(ctx, timeout_ms))?)
    }

    /// Answer one activation. Blocking: under the default `DropNothing`
    /// overflow policy the routing push parks until the consumer's mailbox
    /// drains, and the consumer drains only when its handler runs on another
    /// Python thread.
    fn complete<'py>(&self, py: Python<'py>, result: &Bound<'py, PyDict>) -> PyResult<()> {
        let ctx = self.ptr()?;
        let result = shape::result(&Values(py), result)?;
        Ok(py.detach(move || veneer::complete(ctx, &result))?)
    }

    /// Abandon a polled activation: re-hand its untouched items and release its
    /// entity. Non-blocking by design — the dispatcher runs it from a
    /// `finally`, where anything that can park is one more thing that can go
    /// wrong.
    fn abandon(&self, token: u64) -> PyResult<()> {
        Ok(veneer::abandon(self.ptr()?, token)?)
    }

    /// Wake every blocked poller and stop the runner loop.
    fn shutdown(&self) -> PyResult<()> {
        veneer::shutdown(self.ptr()?);
        Ok(())
    }

    /// Watch standard input and end the process on end of file, on the core's
    /// own thread. The host reads descriptor 0 nowhere.
    fn watch_stdin_eof(&self, grace_ms: u32) -> PyResult<()> {
        self.ptr()?;
        veneer::watch_stdin_eof(grace_ms);
        Ok(())
    }

    /// Emit one output payload from a live workflow run-sugar body.
    /// Non-blocking in the core (an unbounded coordinator send), so it holds
    /// the GIL.
    fn workflow_emit(&self, instance_id: &str, payload: &[u8]) -> PyResult<()> {
        let ctx = self.ptr()?;
        let instance = veneer::c_string("instance id", instance_id)?;
        Ok(veneer::workflow_emit(ctx, &instance, payload)?)
    }

    /// The next inbox item of a live workflow body, or `None` on timeout.
    fn workflow_recv<'py>(
        &self,
        py: Python<'py>,
        instance_id: &str,
        timeout_ms: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ctx = self.ptr()?;
        let instance = veneer::c_string("instance id", instance_id)?;
        match py.detach(move || veneer::workflow_recv(ctx, &instance, timeout_ms))? {
            Some(payload) => payload_view(py, payload),
            None => Ok(py.None().into_bound(py)),
        }
    }

    /// Whether the named workflow body's cancellation token has tripped.
    fn workflow_cancelled(&self, instance_id: &str) -> PyResult<bool> {
        let ctx = self.ptr()?;
        let instance = veneer::c_string("instance id", instance_id)?;
        Ok(veneer::workflow_cancelled(ctx, &instance))
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
        let ctx = self.ptr()?;
        let instance = veneer::c_string("instance id", instance_id)?;
        let factory = veneer::c_string("factory id", factory_id)?;
        let mode = veneer::c_string("if_running", if_running)?;
        let payload = payload.to_vec();
        Ok(py.detach(move || veneer::spawn_workflow(ctx, &instance, &factory, &payload, &mode))?)
    }

    /// The context's last recorded error, or `None`.
    fn last_error(&self) -> Option<String> {
        if self.freed.load(Ordering::Acquire) {
            return None;
        }
        veneer::last_error(CtxPtr::of(self.ctx.as_ptr()))
    }
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
    let channel = veneer::c_string("channel", channel)?;
    Ok(veneer::entry_last_delivery_nanos(&channel))
}

/// Register an episode latch over the named entry channel in the core; the
/// tick that advances it is a Python thread of the host's own.
#[pyfunction]
fn idle_watch_new(channel: &str, threshold_nanos: u64) -> PyResult<u64> {
    let channel = veneer::c_string("channel", channel)?;
    Ok(veneer::idle_watch_new(&channel, threshold_nanos))
}

/// Advance a watch's latch to now: `(status, gap_nanos)`.
#[pyfunction]
fn idle_watch_poll(id: u64) -> (i32, u64) {
    veneer::idle_watch_poll(id)
}

/// Drop a watch.
#[pyfunction]
fn idle_watch_free(id: u64) {
    veneer::idle_watch_free(id)
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
    for (name, abi) in NATIVE_SURFACE {
        let blocking = crate::veneer::BLOCKING_ENTRY_POINTS.contains(abi);
        out.append((*name, *abi, blocking).into_bound_py_any(py)?)?;
    }
    Ok(out)
}

/// The table `native_surface` renders and the Rust-side test asserts against:
/// `(name, abi_entry_point)`, blocking derived from
/// [`crate::veneer::BLOCKING_ENTRY_POINTS`].
pub const NATIVE_SURFACE: &[(&str, &str)] = &[
    ("schema_version", "flz_core_schema_version"),
    ("Core", "flz_core_context_new"),
    ("Core.free", "flz_core_context_free"),
    ("Core.declare_class", "flz_core_declare_class"),
    ("Core.check_class", "flz_core_check_class"),
    ("Core.start", "flz_core_start"),
    ("Core.local_start", "flz_core_local_start"),
    ("Core.local_on_sink", "flz_core_local_on_sink"),
    ("Core.local_feed", "flz_core_local_feed"),
    ("Core.local_next_output", "flz_core_local_next_output"),
    ("Core.local_run_until_idle", "flz_core_local_run_until_idle"),
    ("Core.local_stop", "flz_core_local_stop"),
    ("Core.poll", "flz_core_poll"),
    ("Core.wait_starvation", "flz_core_wait_starvation"),
    ("Core.complete", "flz_core_complete"),
    ("Core.abandon", "flz_core_abandon"),
    ("Core.shutdown", "flz_core_shutdown"),
    ("Core.watch_stdin_eof", "flz_core_watch_stdin_eof"),
    ("Core.workflow_emit", "flz_core_workflow_emit"),
    ("Core.workflow_recv", "flz_core_workflow_recv"),
    ("Core.workflow_cancelled", "flz_core_workflow_cancelled"),
    ("Core.spawn_workflow", "flz_core_spawn_workflow"),
    ("monotonic_nanos", "flz_core_monotonic_nanos"),
    (
        "entry_last_delivery_nanos",
        "flz_core_entry_last_delivery_nanos",
    ),
    ("idle_watch_new", "flz_core_idle_watch_new"),
    ("idle_watch_poll", "flz_core_idle_watch_poll"),
    ("idle_watch_free", "flz_core_idle_watch_free"),
    ("Core.last_error", "flz_core_last_error"),
    ("free_threaded", "flz_core_schema_version"),
    ("native_surface", "flz_core_schema_version"),
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
    m.add_function(wrap_pyfunction!(idle_watch_new, m)?)?;
    m.add_function(wrap_pyfunction!(idle_watch_poll, m)?)?;
    m.add_function(wrap_pyfunction!(idle_watch_free, m)?)?;
    m.add_function(wrap_pyfunction!(free_threaded, m)?)?;
    m.add_function(wrap_pyfunction!(native_surface, m)?)?;
    Ok(())
}
