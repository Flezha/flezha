//! `ctx.effect` through the C ABI: a begun call runs and is kept once the
//! controller answers, a result the controller kept first wins, results the
//! bind ships are served without running, an invocation with no input-log
//! offset records nothing, and the refusals name their rule.
//!
//! The scenario runs in a child process of this test binary, because the
//! runner and the engine are process-global.

use std::ffi::CString;
use std::process::Command;
use std::sync::Arc;

use flz_core::pb::command_request::Command as Cmd;
use flz_core::pb::{EffectResult, EffectResultAck, EffectResults};
use flz_core::schema;
use flz_core_ffi::veneer::{self, CtxPtr, EffectStart, Refusal};
use flz_core_ffi::*;
use flz_transport::errors::FlezhaError;
use flz_transport::transport::FlezhaStream;

use crate::harness::{abi_complete, abi_poll, ok_effect, Controller, Demanded};

const CHILD: &str = "FLZ_ABI_EFFECT_CHILD";

#[test]
fn an_effect_is_kept_once_and_served_to_a_re_run() {
    if std::env::var_os(CHILD).is_some() {
        return scenario();
    }
    let output = Command::new(std::env::current_exe().expect("the test binary"))
        .args([
            "--exact",
            "effect::an_effect_is_kept_once_and_served_to_a_re_run",
        ])
        .env(CHILD, "1")
        .output()
        .expect("run the scenario in a child");
    assert!(
        output.status.success(),
        "the effect scenario failed:\n{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn scenario() {
    let ctx = flz_core_context_new(flz_core::SCHEMA_VERSION);
    let controller = Controller::start(&crate::lifecycle::socket_dir());
    crate::lifecycle::declares_and_starts(ctx, &controller);
    let (sock, _peer) = std::os::unix::net::UnixStream::pair().unwrap();
    let stream = Arc::new(FlezhaStream::Unix(sock));

    let act = deliver(ctx, &stream, &[Some(4), Some(5)]);
    the_refusals_name_their_rule(ctx, act.token);
    let sent = a_run_is_kept_once_the_controller_answers(ctx, &controller, act.token);
    a_result_kept_first_wins(ctx, &controller, act.token);
    an_ordinal_counts_earlier_calls_of_a_name(ctx, act.token);
    complete(ctx, &act);

    a_shipped_result_is_served_without_running(ctx, &controller, &stream, &sent);
    a_refused_keep_is_not_recorded(ctx, &controller, &stream);
    no_offset_records_nothing(ctx, &controller, &stream);
}

fn begin(ctx: *mut FlzContext, token: u64, item: u32, name: &str) -> Result<EffectStart, Refusal> {
    let name = CString::new(name).expect("no NUL byte");
    veneer::effect_begin(CtxPtr::of(ctx), token, item, &name)
}

fn code_of(refusal: Refusal) -> String {
    match refusal {
        Refusal::Core(document) => {
            FlezhaError::from_document(&document)
                .expect("a refusal is a JSON error document")
                .code
        }
        other => panic!("expected a core refusal, got {other:?}"),
    }
}

/// Route one input per offset into the probe's entry channel and poll the
/// batch they make.
fn deliver(
    ctx: *mut FlzContext,
    stream: &Arc<FlezhaStream>,
    offsets: &[Option<u64>],
) -> schema::Activation {
    let instance =
        flz_core::runner::live_instance("abi_probe").expect("AddRelay built the instance");
    let entry = instance.topology.channel(0).expect("entry channel");
    let wrap = entry.wrap_input.as_ref().expect("entry wrapper");
    for offset in offsets {
        let value = wrap(b"input".to_vec()).expect("bytes passthrough cannot fail");
        flz_core::routing::route_to_consumers(
            &instance, entry, &value, stream, *offset, None, false, None,
        );
    }
    let (status, act) = abi_poll(ctx, 2_000);
    assert_eq!(status, FLZ_POLL_ACTIVATION, "queued inputs hand out");
    act.unwrap()
}

fn complete(ctx: *mut FlzContext, act: &schema::Activation) {
    let schema::ActivationKind::Work(items) = &act.kind else {
        panic!("a work activation");
    };
    abi_complete(
        ctx,
        &schema::ActivationResult {
            token: act.token,
            outcome: schema::ActivationOutcome::Work(
                items.iter().map(|_| ok_effect(None, vec![])).collect(),
            ),
        },
    );
}

/// The `n`th effect result the runner sent, counting from zero.
fn nth_result(controller: &Controller, n: usize) -> (String, EffectResult) {
    controller.await_frame(&format!("effect result #{n}"), |captured| {
        captured
            .iter()
            .filter_map(|(id, m)| flz_core_ffi::fake_controller::effect_result(id, m))
            .nth(n)
    })
}

fn the_refusals_name_their_rule(ctx: *mut FlzContext, token: u64) {
    assert_eq!(
        code_of(begin(ctx, token, 0, "").unwrap_err()),
        "effect.bad_name"
    );
    assert_eq!(
        code_of(begin(ctx, token, 0, &"m".repeat(256)).unwrap_err()),
        "effect.bad_name"
    );
    assert_eq!(
        code_of(begin(ctx, token + 1_000, 0, "model").unwrap_err()),
        "effect.outside_invocation"
    );
    assert_eq!(
        code_of(begin(ctx, token, 9, "model").unwrap_err()),
        "effect.outside_invocation",
        "an item the activation does not hold"
    );
}

/// Result #0: item 0 (offset 4) runs, a nested call is refused, and the end
/// waits for the controller's ack.
fn a_run_is_kept_once_the_controller_answers(
    ctx: *mut FlzContext,
    controller: &Controller,
    token: u64,
) -> EffectResult {
    assert_eq!(begin(ctx, token, 0, "model").unwrap(), EffectStart::Run);
    assert_eq!(
        code_of(begin(ctx, token, 0, "tool").unwrap_err()),
        "effect.nested",
        "a call inside a running function"
    );

    let context = ctx as usize;
    let ending = std::thread::spawn(move || {
        veneer::effect_end(
            CtxPtr::of(context as *mut FlzContext),
            token,
            0,
            b"answer-4",
        )
    });
    let (reference_id, sent) = nth_result(controller, 0);
    assert_eq!(
        (
            sent.relay_id.as_str(),
            sent.offset,
            sent.name.as_str(),
            sent.payload.as_slice()
        ),
        ("abi_probe", 4, "model", &b"answer-4"[..])
    );
    assert_eq!(sent.identity.len(), 32);
    assert!(!sent.cell_identity.is_empty());
    controller.send(
        &reference_id,
        Cmd::EffectResultAck(EffectResultAck::default()),
    );
    assert_eq!(
        ending.join().expect("the end"),
        Ok(None),
        "the call returns its own result"
    );
    sent
}

/// Result #1: item 1 (offset 5); the controller kept another result first.
fn a_result_kept_first_wins(ctx: *mut FlzContext, controller: &Controller, token: u64) {
    assert_eq!(begin(ctx, token, 1, "model").unwrap(), EffectStart::Run);
    let context = ctx as usize;
    let ending = std::thread::spawn(move || {
        veneer::effect_end(CtxPtr::of(context as *mut FlzContext), token, 1, b"second")
    });
    let (reference_id, sent) = nth_result(controller, 1);
    assert_eq!(sent.offset, 5);
    controller.send(
        &reference_id,
        Cmd::EffectResultAck(EffectResultAck {
            error: None,
            first: true,
            payload: b"first".to_vec(),
        }),
    );
    assert_eq!(ending.join().expect("the end"), Ok(Some(b"first".to_vec())));
}

/// A second call of one name at one offset is another identity, and a
/// release after a failed function keeps nothing.
fn an_ordinal_counts_earlier_calls_of_a_name(ctx: *mut FlzContext, token: u64) {
    assert_eq!(begin(ctx, token, 0, "model").unwrap(), EffectStart::Run);
    veneer::effect_release(CtxPtr::of(ctx), token, 0);
    assert_eq!(
        begin(ctx, token, 0, "model").unwrap(),
        EffectStart::Run,
        "a released call leaves the item free"
    );
    veneer::effect_release(CtxPtr::of(ctx), token, 0);
}

/// A bind's shipped result for offset 6 is served to the call that names it,
/// and the function never runs.
fn a_shipped_result_is_served_without_running(
    ctx: *mut FlzContext,
    controller: &Controller,
    stream: &Arc<FlezhaStream>,
    first: &EffectResult,
) {
    let origin = flz_core::origin::InvocationOrigin {
        cell_identity: &first.cell_identity,
        offset: Some(6),
    };
    let identity = origin.effect_identity(flz_core::origin::Phase::Run, "model", 0);
    controller.send(
        "effect-results-1",
        Cmd::EffectResults(EffectResults {
            relay_id: "abi_probe".to_string(),
            results: vec![EffectResult {
                identity,
                offset: 6,
                payload: b"kept-6".to_vec(),
                cell_identity: first.cell_identity.clone(),
                ..Default::default()
            }],
        }),
    );
    controller.await_success("the effect results reply", "effect-results-1");

    let act = deliver(ctx, stream, &[Some(6)]);
    assert_eq!(
        begin(ctx, act.token, 0, "model").unwrap(),
        EffectStart::Kept(b"kept-6".to_vec())
    );
    assert_eq!(
        begin(ctx, act.token, 0, "model").unwrap(),
        EffectStart::Run,
        "the second call of the name is another identity"
    );
    veneer::effect_release(CtxPtr::of(ctx), act.token, 0);
    complete(ctx, &act);
}

/// Result #2: the controller refuses, so the invocation must run again.
fn a_refused_keep_is_not_recorded(
    ctx: *mut FlzContext,
    controller: &Controller,
    stream: &Arc<FlezhaStream>,
) {
    let act = deliver(ctx, stream, &[Some(7)]);
    assert_eq!(begin(ctx, act.token, 0, "model").unwrap(), EffectStart::Run);
    let token = act.token;
    let context = ctx as usize;
    let ending = std::thread::spawn(move || {
        veneer::effect_end(CtxPtr::of(context as *mut FlzContext), token, 0, b"r")
    });
    let (reference_id, _) = nth_result(controller, 2);
    let refusal = FlezhaError::new("authz.no_role_binding", Default::default());
    controller.send(
        &reference_id,
        Cmd::EffectResultAck(EffectResultAck {
            error: Some((&refusal).into()),
            ..Default::default()
        }),
    );
    assert_eq!(
        code_of(ending.join().expect("the end").unwrap_err()),
        "effect.not_recorded"
    );
    complete(ctx, &act);
}

/// An input that took no offset (a local mode input, or one sent with
/// `runner.send`'s local option, which `BoundRelays::send` delivers without
/// one) runs its function and sends nothing.
fn no_offset_records_nothing(
    ctx: *mut FlzContext,
    controller: &Controller,
    stream: &Arc<FlezhaStream>,
) {
    let act = deliver(ctx, stream, &[None]);
    assert_eq!(
        begin(ctx, act.token, 0, "model").unwrap(),
        EffectStart::Unrecorded
    );
    veneer::effect_release(CtxPtr::of(ctx), act.token, 0);
    complete(ctx, &act);
    let sent = controller.await_frame("the frames so far", |captured| {
        Some(
            captured
                .iter()
                .filter_map(|(id, m)| flz_core_ffi::fake_controller::effect_result(id, m))
                .count(),
        )
    });
    assert_eq!(sent, 3, "only the three recorded calls sent a result");
}
