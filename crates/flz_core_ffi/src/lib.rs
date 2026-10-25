//! The C ABI over `flz_core`'s poll/complete engine.
//!
//! One opaque context per host process. Any host thread may call `poll`; any
//! host thread — including a different one — may call `complete`. Ownership
//! follows the workspace FFI convention: whoever allocates frees; buffers the
//! core hands out are returned through [`flz_core_buf_free`].
//!
//! Schema negotiation happens at context creation: a shim built against a
//! different `SCHEMA_VERSION` is refused with an error naming both versions,
//! before any controller connection is attempted.
//!
//! # Failures: one error document
//!
//! Every failing entry answers its failing status ([`FLZ_ERR`], or NULL from
//! [`flz_core_context_new`]) and leaves the JSON error document
//! `{"error":{"code","class","message","details","hint"}}` in
//! [`flz_core_last_error`], or [`flz_core_init_error`] for a context-free one,
//! each kept per thread: read it on the thread that made the failing call;
//! never in a buffer. A caller branches on `code` and `class`, never `message`.
//!
//! # Notices: the host's own log
//!
//! The core's notices reach the host through [`flz_core_set_log_callback`] or
//! [`flz_core_take_notices`]; with neither they go to standard error. This
//! crate installs no `tracing` subscriber: a veneer is loaded into a process
//! the application owns.

/// Bind `ctx` to the context it points at, or return [`FLZ_ERR`] for NULL.
macro_rules! live {
    ($ctx:ident) => {
        let Some($ctx) = $ctx.as_ref() else {
            return crate::FLZ_ERR;
        };
    };
    (mut $ctx:ident) => {
        let Some($ctx) = $ctx.as_mut() else {
            return crate::FLZ_ERR;
        };
    };
}

mod abi;
mod error;
#[cfg(feature = "fake-controller")]
pub mod fake_controller;
#[cfg(feature = "jni")]
pub mod jni_glue;
// `napi-derive` emits the module-registration constructor only outside
// `cfg(test)`, so under the test harness every exported entry point looks
// unreferenced. The allow is scoped to that build, and to these two modules,
// so genuinely dead code elsewhere still reports.
#[cfg_attr(test, allow(dead_code))]
#[cfg(feature = "napi")]
pub mod napi_glue;
#[cfg_attr(test, allow(dead_code))]
#[cfg(feature = "napi")]
mod napi_host;
#[cfg(feature = "python")]
pub mod python_glue;
#[cfg(feature = "ruby")]
pub mod ruby_glue;
pub mod veneer;

use std::collections::HashMap;
use std::ffi::CString;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

/// The entry points the source-file splits hold, exported from the crate root
/// like every other C symbol: a module is not an ABI boundary.
pub use abi::*;
use dashmap::DashMap;
pub use error::Error;
use flz_core::poll_core::EngineActivation;
use flz_core::spawn::SpawnHandle;
use flz_core::ErrorModel;

/// Poll produced an activation; the out buffer holds its encoded bytes.
pub const FLZ_POLL_ACTIVATION: i32 = 1;
/// Poll timed out with no work.
pub const FLZ_POLL_TIMEOUT: i32 = 0;
/// The context is shutting down; every blocked poller is released.
pub const FLZ_POLL_SHUTTING_DOWN: i32 = 2;
/// Failure; [`flz_core_last_error`] holds the JSON error document.
pub const FLZ_ERR: i32 = -1;
/// Success for non-poll calls.
pub const FLZ_OK: i32 = 0;
/// A spawn result read found no answer yet; the out buffer is untouched.
pub const FLZ_SPAWN_PENDING: i32 = 1;
/// A spawn read's wait that ends at the core's own bound for the request.
pub const FLZ_WAIT_CORE_BOUND: u64 = u64::MAX;
/// [`flz_core_effect_begin`]: run the function, then answer
/// [`flz_core_effect_end`] or [`flz_core_effect_release`].
pub const FLZ_EFFECT_RUN: i32 = 0;
/// [`flz_core_effect_begin`]: the out buffer holds the kept result; the
/// function does not run. [`flz_core_effect_end`]: the out buffer holds the
/// result kept first, which the call returns instead of its own.
pub const FLZ_EFFECT_KEPT: i32 = 1;
/// [`flz_core_effect_begin`]: nothing re-runs this invocation, so nothing is
/// kept; run the function, return its result and answer
/// [`flz_core_effect_release`].
pub const FLZ_EFFECT_UNRECORDED: i32 = 2;

/// What a call's place in its invocation is counted by: spawns by offset,
/// effects by work item and name.
#[derive(Hash, PartialEq, Eq)]
enum Counted {
    Spawn(i64),
    Effect(u32, String),
}

/// An effect whose function is running, by work item.
struct Running {
    name: String,
    identity: String,
    offset: Option<u64>,
}

/// A polled activation the host has not answered, with the spawn handles its
/// tasklets made: they live exactly as long as it does.
struct Outstanding {
    activation: EngineActivation,
    spawns: Vec<u64>,
    /// Calls made so far per [`Counted`] key: a spawn's position and an
    /// effect's ordinal in the invocation.
    positions: HashMap<Counted, u32>,
    /// The effects whose functions are running, by work item. A list rather
    /// than a map: built for every activation, it must cost nothing until an
    /// effect runs.
    effects: Vec<(u32, Running)>,
    /// The next part number per work item: counted per invocation attempt,
    /// and a retry is handed out in an activation of its own.
    part_sequences: HashMap<u32, u32>,
}

/// Opaque context handle.
pub struct FlzContext {
    /// Unique for the process's lifetime, so an error recorded on a freed
    /// context is never read on one the allocator puts at its address.
    id: u64,
    outstanding: DashMap<u64, Outstanding>,
    session: flz_core::session::Session,
    /// The local run this context drives and the channel its sinks release
    /// onto, when it has one. A started context dials a controller and never
    /// holds one; the two are alternatives.
    local: Mutex<Option<LocalSession>>,
    /// The host's end of that channel, held apart from the run so a thread
    /// parked in [`flz_core_local_next_output`] holds nothing `feed` needs.
    local_outputs: Mutex<Option<std::sync::mpsc::Receiver<Vec<u8>>>>,
    /// The spawn handles of every outstanding activation, by number.
    spawns: DashMap<u64, Arc<Mutex<SpawnHandle>>>,
    last_spawn: AtomicU64,
    /// The poll-worker pool [`flz_core_pool_open`] opened, at most one.
    pool: Mutex<Option<Arc<flz_core::poll_pool::Governor>>>,
}

/// Where a registered sink's releases go: the host takes them back through
/// [`flz_core_local_next_output`].
type LocalReleases = std::sync::mpsc::Sender<Vec<u8>>;

/// What a local run is, from the ABI's side: the run itself, and the sender
/// its registered sinks release onto.
struct LocalSession {
    run: Arc<flz_core::local::LocalRun>,
    releases: std::sync::mpsc::Sender<Vec<u8>>,
}

thread_local! {
    static INIT_ERROR: std::cell::RefCell<Option<CString>> = const { std::cell::RefCell::new(None) };
    /// The last failure this thread recorded, and the id of the context it was recorded on.
    static LAST_ERROR: std::cell::RefCell<Option<(u64, CString)>> = const { std::cell::RefCell::new(None) };
    static RENDERED: std::cell::RefCell<Option<CString>> = const { std::cell::RefCell::new(None) };
}

/// The JSON error document a failing entry leaves behind. JSON escapes every
/// control character, so the document never holds the NUL a C string ends at.
fn error_document(error: &Error) -> CString {
    CString::new(error.to_error().json()).unwrap_or_default()
}

fn set_init_error(error: Error) {
    INIT_ERROR.with(|slot| *slot.borrow_mut() = Some(error_document(&error)));
}

/// Hand `bytes` to the caller through the out pointers; the caller frees
/// them with [`flz_core_buf_free`].
///
/// # Safety
/// `buf_out`/`len_out` must be valid writable pointers.
unsafe fn hand_out(bytes: Vec<u8>, buf_out: *mut *mut u8, len_out: *mut usize) {
    let mut bytes = bytes.into_boxed_slice();
    *buf_out = bytes.as_mut_ptr();
    *len_out = bytes.len();
    std::mem::forget(bytes);
}

impl Outstanding {
    /// The effect running in `item`, which no longer is.
    fn take_effect(&mut self, item: u32) -> Option<Running> {
        let at = self
            .effects
            .iter()
            .position(|(running, _)| *running == item)?;
        Some(self.effects.swap_remove(at).1)
    }

    /// The next call's place among those counted under `counted`.
    fn next_position(&mut self, counted: Counted) -> u32 {
        let position = self.positions.entry(counted).or_insert(0);
        let next = *position;
        *position += 1;
        next
    }
}

impl FlzContext {
    /// Record `error` as the context's last error; the status every failing
    /// entry point returns. The one place an [`Error`] becomes a C string.
    fn fail(&self, error: Error) -> i32 {
        LAST_ERROR.with(|slot| *slot.borrow_mut() = Some((self.id, error_document(&error))));
        FLZ_ERR
    }

    /// [`FLZ_OK`], or the refusal recorded as by [`Self::fail`].
    fn status<E: Into<Error>>(&self, result: Result<(), E>) -> i32 {
        result.map_or_else(|error| self.fail(error.into()), |()| FLZ_OK)
    }

    /// Take a polled activation by token, releasing the spawn handles it
    /// made. `Err` carries the status the entry point returns, with the reason
    /// already recorded for `last_error` — an unknown token means the veneer
    /// answered twice, or answered one it never held, and either is a defect
    /// to surface rather than a no-op.
    fn take_outstanding(&self, token: u64) -> Result<EngineActivation, i32> {
        let (_, outstanding) = self
            .outstanding
            .remove(&token)
            .ok_or_else(|| self.fail(Error::UnknownToken(token)))?;
        // Dropping a handle reports a failure nobody read, or hands a pending
        // one to the reaper; a later read of its number answers released.
        for handle in outstanding.spawns {
            self.spawns.remove(&handle);
        }
        Ok(outstanding.activation)
    }

    /// This context's local run and its release channel, cloned out of the
    /// lock so a call that blocks holds nothing. `Err` is the refusal a
    /// local-mode call gets on a context that has none.
    fn local_session(&self) -> Result<(Arc<flz_core::local::LocalRun>, LocalReleases), Error> {
        self.local
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|session| (Arc::clone(&session.run), session.releases.clone()))
            .ok_or(Error::NoLocalRun)
    }
}

#[cfg(all(test, unix))]
mod lib_tests;
