//! What the scenario drives the ABI with: a fake controller on a real control
//! socket, and the poll/complete pair every phase calls.
//!
//! The controller captures every runner→controller frame and wakes
//! [`Controller::await_frame`] on each one, so a phase waits for the frame it
//! needs rather than for a duration.

use std::os::unix::net::UnixListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use flz_core::schema;
use flz_core_ffi::veneer::{last_error_of, CtxPtr};
use flz_core_ffi::*;
use flz_transport::transport::FlezhaStream;
use prost::Message;

/// A captured runner→controller frame: its payload id and its message bytes.
pub type CapturedFrame = (String, Vec<u8>);

type Frames = Arc<(Mutex<Vec<CapturedFrame>>, Condvar)>;

/// The controller end of the control channel: accepts, captures every frame
/// the runner writes, and holds the write half so a phase can push commands.
pub struct Controller {
    accepted: Arc<AtomicUsize>,
    frames: Frames,
    writer: Arc<Mutex<Option<FlezhaStream>>>,
}

impl Controller {
    pub fn start(dir: &std::path::Path) -> Self {
        let listener = UnixListener::bind(dir.join("runner_control_channel.sock"))
            .expect("bind control socket");
        let controller = Self {
            accepted: Arc::new(AtomicUsize::new(0)),
            frames: Arc::new((Mutex::new(Vec::new()), Condvar::new())),
            writer: Arc::new(Mutex::new(None)),
        };
        let counter = Arc::clone(&controller.accepted);
        let frames = Arc::clone(&controller.frames);
        let writer_slot = Arc::clone(&controller.writer);
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(s) = stream else { break };
                counter.fetch_add(1, Ordering::SeqCst);
                let write_half = s.try_clone().expect("clone control stream");
                *writer_slot.lock().unwrap() = Some(FlezhaStream::Unix(write_half));
                let frames = Arc::clone(&frames);
                thread::spawn(move || capture(FlezhaStream::Unix(s), frames));
            }
        });
        controller
    }

    /// How many control connections have been accepted.
    pub fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    /// Push a controller→runner command down the accepted connection.
    pub fn send(&self, reference_id: &str, command: flz_core::pb::command_request::Command) {
        use std::io::Write;
        let request = flz_core::pb::CommandRequest {
            reference_id: reference_id.to_string(),
            command: Some(command),
        };
        let frame = flz_transport::service::payloader::build_from_binary(
            "cmd",
            &request.encode_to_vec(),
            Default::default(),
        );
        let mut guard = self.writer.lock().unwrap();
        let stream = guard.as_mut().expect("control connection accepted");
        (&*stream).write_all(&frame).expect("write command frame");
    }

    /// Wait until `probe` finds what it is looking for among the captured
    /// frames, or fail naming `what`.
    pub fn await_frame<T>(&self, what: &str, probe: impl Fn(&[CapturedFrame]) -> Option<T>) -> T {
        let (captured, arrived) = &*self.frames;
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut frames = captured.lock().unwrap();
        loop {
            if let Some(found) = probe(&frames) {
                return found;
            }
            let left = deadline.saturating_duration_since(Instant::now());
            assert!(!left.is_zero(), "timed out waiting for {what}");
            frames = arrived.wait_timeout(frames, left).unwrap().0;
        }
    }

    /// Wait for the command reply carrying `reference_id`.
    pub fn await_reply(&self, what: &str, reference_id: &str) -> flz_core::pb::CommandResponse {
        self.await_frame(what, |captured| {
            captured.iter().find_map(|(id, m)| {
                (id == "cmd")
                    .then(|| flz_core::pb::CommandResponse::decode(m.as_slice()).ok())
                    .flatten()
                    .filter(|r| r.reference_id == reference_id)
            })
        })
    }

    /// Wait for a successful command reply carrying `reference_id`.
    pub fn await_success(&self, what: &str, reference_id: &str) {
        assert_eq!(self.await_reply(what, reference_id).status, "success");
    }
}

fn capture(stream: FlezhaStream, frames: Frames) {
    let (captured, arrived) = &*frames;
    while let Ok((payload_id, message, _metadata)) =
        flz_transport::service::payloader::extract_payload(&stream)
    {
        captured.lock().unwrap().push((payload_id, message));
        arrived.notify_all();
    }
}

/// Poll through the ABI, panicking on error; returns (status, decoded).
pub fn abi_poll(ctx: *mut FlzContext, timeout_ms: u64) -> (i32, Option<schema::Activation>) {
    let mut buf: *mut u8 = std::ptr::null_mut();
    let mut len: usize = 0;
    let status = unsafe { flz_core_poll(ctx, timeout_ms, &mut buf, &mut len) };
    if status != FLZ_POLL_ACTIVATION {
        return (status, None);
    }
    let bytes = unsafe { std::slice::from_raw_parts(buf, len) }.to_vec();
    unsafe { flz_core_buf_free(buf, len) };
    let act = schema::decode_activation(&bytes).expect("activation decodes");
    crate::shape::assert_activation_maps(&act);
    (status, Some(act))
}

/// Map a result through the shape and assert it changed nothing.
pub fn through_veneers(result: &schema::ActivationResult) -> schema::ActivationResult {
    crate::shape::through_veneer(result)
}

pub fn abi_complete(ctx: *mut FlzContext, result: &schema::ActivationResult) {
    abi_complete_mapped(ctx, &through_veneers(result));
}

/// Hand an already-mapped result to the ABI. Callable from any thread.
pub fn abi_complete_mapped(ctx: *mut FlzContext, result: &schema::ActivationResult) {
    let bytes = schema::encode_result(result);
    let status = unsafe { flz_core_complete(ctx, bytes.as_ptr(), bytes.len()) };
    if status != FLZ_OK {
        panic!("complete failed: {}", last_error(ctx));
    }
}

/// The activation's input payloads, in batch order.
pub fn work_payloads(act: &schema::Activation) -> Vec<Vec<u8>> {
    let schema::ActivationKind::Work(items) = &act.kind else {
        panic!("expected a work batch");
    };
    items
        .iter()
        .map(|item| match item {
            schema::WorkItem::Input(input) => input.payload.clone(),
            other => panic!("expected inputs, got {other:?}"),
        })
        .collect()
}

/// The last error the context recorded, read the way a binding reads it.
pub fn last_error(ctx: *mut FlzContext) -> String {
    last_error_of(CtxPtr::of(ctx))
}

pub fn ok_effect(consumed_offset: Option<u64>, timers: Vec<schema::TimerOp>) -> schema::ItemEffect {
    schema::ItemEffect {
        verdict: schema::Verdict::Ok,
        changes: vec![],
        outputs: vec![],
        consumed_offset,
        event_time: 0,
        timers,
        checkpoint: false,
        dlq: None,
    }
}
