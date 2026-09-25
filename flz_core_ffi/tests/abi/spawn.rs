//! A tasklet's spawn through the C ABI: the request never waits, the result
//! read carries the id the request resolved to, a refusal stays in its handle,
//! a replay sends nothing, and a failure nobody read is logged once when its
//! activation completes.
//!
//! The scenario runs in a child process of this test binary. The runner and the
//! engine are process-global, and what it asserts about the log can only be
//! read from the child's standard error.

use std::ffi::CString;
use std::io::{BufRead, BufReader, Read};
use std::process::{Command, Stdio};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use flz_core::pb::SpawnBoundedRequest;
use flz_core::schema;
use flz_core_ffi::fake_controller::spawn_request as spawn_request_frame;
use flz_core_ffi::veneer::{self, with_buffer, CtxPtr, Refusal};
use flz_core_ffi::*;
use flz_transport::transport::FlezhaStream;
use serde_json::{json, Value};

use crate::harness::{abi_complete, last_error, ok_effect, Controller, Demanded};

const CHILD: &str = "FLZ_ABI_SPAWN_CHILD";

/// The two failures the scenario leaves unread: one answered before its
/// activation completes, one answered after.
const ANSWERED_UNREAD: &str =
    "id `w-unread`) failed: the controller refused the spawn: denied-unread";
const REAPED: &str = "id `w-reaped`) failed: the controller refused the spawn: denied-late";

#[test]
fn a_spawn_resolves_through_its_handle() {
    if std::env::var_os(CHILD).is_some() {
        return scenario();
    }

    let mut child = Command::new(std::env::current_exe().expect("the test binary"))
        .args([
            "--exact",
            "spawn::a_spawn_resolves_through_its_handle",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("run the scenario in a child");

    let stderr = BufReader::new(child.stderr.take().expect("a piped standard error"));
    let (lines, logged) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in stderr.lines().map_while(Result::ok) {
            let _ = lines.send(line);
        }
    });

    // The child keeps its runner up until its standard input closes, so the
    // reaper's line has somewhere to be written.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut log: Vec<String> = Vec::new();
    while !(log.iter().any(|l| l.contains(ANSWERED_UNREAD))
        && log.iter().any(|l| l.contains(REAPED)))
    {
        let left = deadline.saturating_duration_since(Instant::now());
        match logged.recv_timeout(left) {
            Ok(line) => log.push(line),
            Err(_) => break,
        }
    }
    drop(child.stdin.take());
    let status = child.wait().expect("the child ends");
    reader.join().expect("the log reader ends");
    log.extend(logged.try_iter());
    let log = log.join("\n");

    assert!(status.success(), "the spawn scenario failed:\n{log}");
    let warnings: Vec<&str> = log
        .lines()
        .filter(|line| line.starts_with("[flezha] spawn_bounded("))
        .collect();
    assert_eq!(
        warnings.len(),
        2,
        "every unread failure is logged exactly once, and nothing read is logged:\n{log}"
    );
    for (expected, what) in [
        (ANSWERED_UNREAD, "a failure answered before completion"),
        (REAPED, "a failure answered after completion"),
    ] {
        assert!(
            warnings
                .iter()
                .any(|w| w.contains("factory `worker`") && w.contains(expected)),
            "{what} names its id, its factory and its reason:\n{log}"
        );
    }
}

fn scenario() {
    let ctx = flz_core_context_new(flz_core::SCHEMA_VERSION);
    assert!(!ctx.is_null(), "matching schema creates a context");
    let controller = Controller::start(&crate::lifecycle::socket_dir());
    crate::lifecycle::declares_and_starts(ctx, &controller);
    let (sock, _peer) = std::os::unix::net::UnixStream::pair().unwrap();
    let stream = Arc::new(FlezhaStream::Unix(sock));
    let act = crate::lifecycle::routes_two_entry_payloads(ctx, &stream);

    a_request_with_unreadable_arguments_is_refused(ctx, act.token);
    a_request_returns_before_its_answer_and_resolves_to_the_generated_id(
        ctx,
        &controller,
        act.token,
    );
    a_refusal_stays_in_its_handle(ctx, &controller, act.token);
    a_replay_sends_nothing(ctx, &controller, act.token);
    an_unread_failure_is_logged_when_its_activation_completes(ctx, &controller, &act);

    // Held until the parent has read the reaper's line.
    let _ = std::io::stdin().read_to_end(&mut Vec::new());
}

fn request(
    ctx: *mut FlzContext,
    token: u64,
    id: &str,
    if_running: &str,
    replay: bool,
) -> Result<u64, Refusal> {
    let text = |value: &str| CString::new(value).expect("no NUL byte");
    veneer::spawn_request(
        CtxPtr::of(ctx),
        token,
        &text("worker"),
        &text(id),
        b"payload",
        &text(if_running),
        replay,
    )
}

/// One result read through the C entry: its status, and the JSON it handed out.
fn read(ctx: *mut FlzContext, handle: u64, timeout_ms: u64) -> (i32, Option<Value>) {
    with_buffer(
        &[FLZ_OK, FLZ_SPAWN_FAILED],
        |out, out_len| unsafe { flz_core_spawn_result(ctx, handle, timeout_ms, out, out_len) },
        |bytes| serde_json::from_slice(bytes).expect("the answer is JSON"),
    )
}

/// The `n`th spawn request the runner sent, counting from zero.
fn nth_request(controller: &Controller, n: usize) -> (String, SpawnBoundedRequest) {
    controller.await_frame(&format!("spawn request #{n}"), |captured| {
        captured
            .iter()
            .filter_map(|(id, m)| spawn_request_frame(id, m))
            .nth(n)
    })
}

fn a_request_with_unreadable_arguments_is_refused(ctx: *mut FlzContext, token: u64) {
    let refused = request(ctx, token + 1_000, "", "", false);
    assert_eq!(
        refused,
        Err(Refusal::Core(format!(
            "unknown activation token {}",
            token + 1_000
        )))
    );
    let refused = request(ctx, token, "w1", "later", false);
    assert_eq!(
        refused,
        Err(Refusal::Core(
            "if_running must be `deliver` or `error`, not `later`".to_string()
        ))
    );
}

/// Request #0.
fn a_request_returns_before_its_answer_and_resolves_to_the_generated_id(
    ctx: *mut FlzContext,
    controller: &Controller,
    token: u64,
) {
    let handle = request(ctx, token, "", "", false).expect("a request is accepted");
    assert_eq!(
        read(ctx, handle, 0),
        (FLZ_SPAWN_PENDING, None),
        "the controller has not answered"
    );

    let (reference_id, sent) = nth_request(controller, 0);
    assert_eq!(
        (sent.instance_id.as_str(), sent.factory_id.as_str()),
        ("", "worker"),
        "an empty id asks the controller to generate one"
    );
    assert_eq!(sent.if_running, "deliver");
    assert_eq!(sent.payload, b"payload");

    // A read that never waits answers pending even while another thread's
    // waiting read holds the handle.
    let context = ctx as usize;
    let waiting = std::thread::spawn(move || read(context as *mut FlzContext, handle, 5_000));
    std::thread::sleep(Duration::from_millis(200));
    let asked = Instant::now();
    assert_eq!(read(ctx, handle, 0), (FLZ_SPAWN_PENDING, None));
    assert!(
        asked.elapsed() < Duration::from_secs(1),
        "a zero-timeout read waited {:?} for another reader",
        asked.elapsed()
    );

    let generated = controller
        .try_answer_spawn(&reference_id, &sent, None)
        .expect("answer the spawn");
    assert!(generated.starts_with("worker-"));

    assert_eq!(
        waiting.join().expect("the waiting read"),
        (FLZ_OK, Some(json!({"id": generated, "outcome": "spawned"})))
    );
    assert_eq!(
        read(ctx, handle, 0),
        (FLZ_OK, Some(json!({"id": generated, "outcome": "spawned"}))),
        "a resolved handle answers every read the same while its activation is outstanding"
    );
}

/// Request #1.
fn a_refusal_stays_in_its_handle(ctx: *mut FlzContext, controller: &Controller, token: u64) {
    let handle = request(ctx, token, "w-refused", "error", false)
        .expect("a request the controller will refuse is still accepted");
    let (reference_id, sent) = nth_request(controller, 1);
    assert_eq!(
        (sent.instance_id.as_str(), sent.if_running.as_str()),
        ("w-refused", "error")
    );
    controller
        .try_answer_spawn(&reference_id, &sent, Some("quota"))
        .expect("refuse the spawn");

    assert_eq!(
        read(ctx, handle, 5_000),
        (
            FLZ_SPAWN_FAILED,
            Some(json!({
                "id": "w-refused",
                "error": "refused",
                "message": "the controller refused the spawn: quota",
            }))
        )
    );
}

/// Request #2 is the first live request after the two replays.
fn a_replay_sends_nothing(ctx: *mut FlzContext, controller: &Controller, token: u64) {
    let named = request(ctx, token, "w-replayed", "deliver", true).expect("a replay");
    let unnamed = request(ctx, token, "", "", true).expect("a replay");
    let after = request(ctx, token, "w-after-replay", "", false).expect("a request");

    let (reference_id, sent) = nth_request(controller, 2);
    assert_eq!(
        sent.instance_id, "w-after-replay",
        "the control stream is ordered, so a replay that sent would come first"
    );
    controller
        .try_answer_spawn(&reference_id, &sent, None)
        .expect("answer the spawn");

    let ctx_ptr = CtxPtr::of(ctx);
    let answer = |handle, timeout_ms| -> Value {
        let json = veneer::spawn_result(ctx_ptr, handle, timeout_ms)
            .expect("a live handle")
            .expect("an answer");
        serde_json::from_str(&json).expect("the answer is JSON")
    };
    assert_eq!(
        answer(named, 0),
        json!({"id": "w-replayed", "outcome": "skipped_on_replay"})
    );
    assert_eq!(
        answer(unnamed, 0),
        json!({"id": "", "outcome": "skipped_on_replay"})
    );
    assert_eq!(
        answer(after, 5_000),
        json!({"id": "w-after-replay", "outcome": "spawned"})
    );
}

/// Requests #3 to #5. The runner reads the control stream in order, so once
/// the last of them resolves, the first one's answer has arrived as well.
fn an_unread_failure_is_logged_when_its_activation_completes(
    ctx: *mut FlzContext,
    controller: &Controller,
    act: &schema::Activation,
) {
    let unread = request(ctx, act.token, "w-unread", "", false).expect("a request");
    let reaped = request(ctx, act.token, "w-reaped", "", false).expect("a request");
    let read_one = request(ctx, act.token, "w-read", "", false).expect("a request");
    let (unread_ref, unread_sent) = nth_request(controller, 3);
    let (reaped_ref, reaped_sent) = nth_request(controller, 4);
    let (read_ref, read_sent) = nth_request(controller, 5);

    controller
        .try_answer_spawn(&unread_ref, &unread_sent, Some("denied-unread"))
        .expect("refuse the spawn");
    controller
        .try_answer_spawn(&read_ref, &read_sent, Some("denied-read"))
        .expect("refuse the spawn");
    assert_eq!(read(ctx, read_one, 5_000).0, FLZ_SPAWN_FAILED);

    abi_complete(
        ctx,
        &schema::ActivationResult {
            token: act.token,
            outcome: schema::ActivationOutcome::Work(vec![
                ok_effect(None, vec![]),
                ok_effect(None, vec![]),
            ]),
        },
    );
    for handle in [unread, reaped, read_one] {
        assert_eq!(
            read(ctx, handle, 0),
            (FLZ_ERR, None),
            "completing the activation released every handle it made"
        );
        assert_eq!(
            last_error(ctx),
            format!("spawn handle {handle} was released")
        );
    }
    assert!(
        request(ctx, act.token, "", "", false).is_err(),
        "a completed activation makes no further request"
    );

    controller
        .try_answer_spawn(&reaped_ref, &reaped_sent, Some("denied-late"))
        .expect("refuse the spawn");
}
