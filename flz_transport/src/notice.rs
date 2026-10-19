//! The lines the core writes for a reader: a warning, an error, a connect or
//! retry line, the local-mode banner.
//!
//! Every such line goes through [`notice`], and each host registers one hook
//! that writes it through its own log. With no hook the line goes to standard
//! error, so a host that registers nothing still shows it. A host that must not
//! be called back on a core thread (an interpreter, a BEAM scheduler) hooks
//! [`push`] and drains the bounded queue on a thread of its own with [`take`].

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;

use crossbeam::channel::{bounded, Receiver, Sender, TrySendError};

/// How serious a notice is. `Always` is the local-mode banner: every hook
/// writes it whatever its level filter says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum Level {
    Info = 1,
    Warn = 2,
    Error = 3,
    Always = 4,
}

impl Level {
    pub fn name(self) -> &'static str {
        match self {
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
            Level::Always => "always",
        }
    }
}

/// One queued notice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    pub level: Level,
    pub target: &'static str,
    pub text: String,
}

/// What a host registers: it takes the level, the target and the text.
pub type HookFn = dyn Fn(Level, &'static str, &str) + Send + Sync;

type Hook = Arc<HookFn>;

static HOOK: RwLock<Option<Hook>> = RwLock::new(None);

/// Notices a full queue (or a host's own full callback queue) dropped, ever.
pub static NOTICES_DROPPED_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Dropped since the last [`take`], which reports them in one line.
static DROPPED_SINCE_TAKE: AtomicU64 = AtomicU64::new(0);

/// How many notices the queue holds before it drops the newest.
pub const QUEUE_CAPACITY: usize = 1024;

/// Write one notice through the registered hook, or to standard error.
pub fn notice(level: Level, target: &'static str, text: &str) {
    let hook = HOOK.read().unwrap_or_else(|p| p.into_inner()).clone();
    match hook {
        Some(hook) => hook(level, target, text),
        None => write_stderr(level, text),
    }
}

/// `notice(Level::$level, module_path!(), &format!(...))`.
#[macro_export]
macro_rules! notice {
    ($level:ident, $($arg:tt)+) => {
        $crate::notice::notice(
            $crate::notice::Level::$level,
            module_path!(),
            &format!($($arg)+),
        )
    };
}

/// Route every later notice to `hook`; a second call replaces the first.
pub fn set_hook(hook: Box<HookFn>) {
    *HOOK.write().unwrap_or_else(|p| p.into_inner()) = Some(Arc::from(hook));
}

/// Back to standard error.
pub fn clear_hook() {
    *HOOK.write().unwrap_or_else(|p| p.into_inner()) = None;
}

/// The writer used without a hook: `[flezha] ` then `warning: ` or `error: `
/// for those levels, so a reader of the stream can tell the level.
fn write_stderr(level: Level, text: &str) {
    let marker = match level {
        Level::Warn => "warning: ",
        Level::Error => "error: ",
        Level::Info | Level::Always => "",
    };
    let line = format!("[flezha] {marker}{text}");
    // Through `eprintln!` rather than [`say`], so a test harness captures the
    // line; the panic it raises on a closed standard error is caught here.
    let _ = std::panic::catch_unwind(|| eprintln!("{line}"));
}

/// A line to standard error that cannot take the caller down with it.
///
/// A conformance harness stops a runner by closing its pipes, so the very
/// thread that then has to end the process is writing to a pipe with no reader.
/// `eprintln!` panics on that EPIPE, and a panicked watcher leaves the runner
/// alive for ever. The lines said right before the process ends come here
/// directly, past every hook: no host is left to drain them.
pub fn say(line: &str) {
    use std::io::Write;
    let _ = writeln!(std::io::stderr(), "{line}");
}

fn queue() -> &'static (Sender<Notice>, Receiver<Notice>) {
    static QUEUE: OnceLock<(Sender<Notice>, Receiver<Notice>)> = OnceLock::new();
    QUEUE.get_or_init(|| bounded(QUEUE_CAPACITY))
}

/// The hook of a host that drains: queue the notice, dropping it when the
/// queue is full.
pub fn push(level: Level, target: &'static str, text: &str) {
    let notice = Notice {
        level,
        target,
        text: text.to_string(),
    };
    if let Err(TrySendError::Full(_)) = queue().0.try_send(notice) {
        count_dropped();
    }
}

/// Count one notice a full queue dropped.
pub fn count_dropped() {
    NOTICES_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
    DROPPED_SINCE_TAKE.fetch_add(1, Ordering::Relaxed);
}

/// Register `receiver` to be sent every later notice: the notices are queued
/// and a thread of the core's own sends each to the oldest receiver `send`
/// accepts it for, forgetting the ones it refuses; with none left a notice goes
/// to standard error. For a host that must not be called on the thread that
/// wrote the notice: a BEAM scheduler cannot send. The first call starts the
/// thread.
pub fn forward_to<T: Copy + PartialEq + Send + 'static>(
    receivers: &'static Mutex<Option<Vec<T>>>,
    receiver: T,
    send: fn(T, &Notice) -> bool,
) -> std::io::Result<()> {
    let mut held = receivers.lock().unwrap_or_else(|p| p.into_inner());
    if held.is_none() {
        std::thread::Builder::new()
            .name("flz-notices".into())
            .spawn(move || loop {
                for notice in take(Duration::from_secs(3600)) {
                    if !send_to_oldest(receivers, send, &notice) {
                        write_stderr(notice.level, &notice.text);
                    }
                }
            })?;
        set_hook(Box::new(push));
    }
    let held = held.get_or_insert_with(Vec::new);
    if !held.contains(&receiver) {
        held.push(receiver);
    }
    Ok(())
}

fn send_to_oldest<T: Copy>(
    receivers: &Mutex<Option<Vec<T>>>,
    send: fn(T, &Notice) -> bool,
    notice: &Notice,
) -> bool {
    let mut held = receivers.lock().unwrap_or_else(|p| p.into_inner());
    let held = held.get_or_insert_with(Vec::new);
    while let Some(&receiver) = held.first() {
        if send(receiver, notice) {
            return true;
        }
        held.remove(0);
    }
    false
}

/// Every queued notice, waiting up to `wait` for the first; a drop since the
/// last take ends the batch with one line naming how many.
pub fn take(wait: Duration) -> Vec<Notice> {
    let rx = &queue().1;
    let mut batch: Vec<Notice> = Vec::new();
    if wait > Duration::ZERO {
        batch.extend(rx.recv_timeout(wait).ok());
    }
    batch.extend(rx.try_iter());
    let dropped = DROPPED_SINCE_TAKE.swap(0, Ordering::Relaxed);
    if dropped > 0 {
        batch.push(Notice {
            level: Level::Warn,
            target: module_path!(),
            text: format!("{dropped} notices dropped: the notice queue was full"),
        });
    }
    batch
}

#[cfg(test)]
#[path = "notice_tests.rs"]
pub(crate) mod tests;
