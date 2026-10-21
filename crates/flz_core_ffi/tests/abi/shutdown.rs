//! The last phase: `flz_core_shutdown` ships every live Cell's state before
//! it wakes the pollers, and a stopping runner opens no new control
//! connection. On its own because it is the one phase that ends the runner.

use std::ffi::CString;
use std::thread;
use std::time::{Duration, Instant};

use flz_core::schema;
use flz_core::test_support::wait_until;
use flz_core_ffi::fake_controller::shutdown_notification;
use flz_core_ffi::*;

use crate::harness::{
    abi_complete_mapped, abi_poll, last_document, last_error, Controller, Demanded,
};

/// `flz_core_shutdown` ships every live Cell's state as a
/// `RunnerShutdownNotification` before the core reports shutting down: the
/// poller blocked at that moment is handed the state reads first, the
/// controller sees the notification, and only then is the poller woken.
pub(crate) fn shutdown_ships_state_then_wakes_the_poller(
    ctx: *mut FlzContext,
    controller: &Controller,
) {
    let ctx_addr = ctx as usize;
    let host = thread::spawn(move || serve_until_shutdown(ctx_addr as *mut FlzContext));
    // The poller stays blocked for as long as it is watched; shutdown is what
    // frees it.
    assert!(
        !wait_until(Duration::from_millis(100), Duration::ZERO, || {
            host.is_finished()
        }),
        "the poller returned before shutdown"
    );
    let connections = controller.accepted();
    unsafe { flz_core_shutdown(ctx) };

    let (reference_id, state) =
        controller.await_frame("RunnerShutdownNotification for eff-1", |captured| {
            captured
                .iter()
                .find_map(|(id, m)| shutdown_notification(id, m, "eff-1"))
        });
    let state = String::from_utf8_lossy(&state);
    assert!(
        state.contains("\"n\":9"),
        "the notification carries the state the host serialized: {state}"
    );
    controller.send(
        &reference_id,
        flz_core::pb::command_request::Command::RunnerShutdownAck(
            flz_core::pb::RunnerShutdownAck {
                relay_id: "eff-1".to_string(),
            },
        ),
    );

    let (served, waited) = host.join().expect("host loop returns");
    assert!(served >= 1, "the host was handed no state read");
    assert!(
        waited < Duration::from_secs(5),
        "shutdown must wake promptly, waited {waited:?}"
    );
    // The context's session ended with the shutdown: a host's cleanup
    // undeclare unregisters locally, live instance or not.
    let factory = CString::new("abi_eff").unwrap();
    assert_eq!(
        unsafe { flz_core_undeclare_class(ctx, factory.as_ptr()) },
        FLZ_OK,
        "{}",
        last_error(ctx)
    );
    // Only after the poller has been woken, and briefly: a stopping runner
    // that re-registered would be handed work it will never do.
    assert!(
        !wait_until(Duration::from_millis(100), Duration::ZERO, || {
            controller.accepted() != connections
        }),
        "a stopping runner must not open a new control connection"
    );

    // Shut down for the life of the process: a later start, a local run's
    // included, is refused by name rather than handed a dead core.
    let again = flz_core_context_new(flz_core::SCHEMA_VERSION);
    let name = CString::new("after_shutdown").unwrap();
    assert_eq!(unsafe { flz_core_start(again, name.as_ptr()) }, FLZ_ERR);
    assert_eq!(last_document(again).code, "runtime.core_shut_down");
    assert_eq!(
        unsafe { flz_core_local_start(again, name.as_ptr(), name.as_ptr()) },
        FLZ_ERR
    );
    assert_eq!(last_document(again).code, "runtime.core_shut_down");
    unsafe { flz_core_context_free(again) };
}

/// The host during a shutdown: answer every state read with a
/// distinctive state, and report how many it answered and how long its poll
/// took to be woken.
fn serve_until_shutdown(ctx: *mut FlzContext) -> (usize, Duration) {
    let started = Instant::now();
    let mut served = 0;
    loop {
        match abi_poll(ctx, 30_000) {
            (FLZ_POLL_SHUTTING_DOWN, _) => return (served, started.elapsed()),
            (FLZ_POLL_ACTIVATION, Some(act)) => {
                assert!(
                    matches!(act.kind, schema::ActivationKind::LiveStateRead),
                    "expected a state read during shutdown, got {:?}",
                    act.kind
                );
                served += 1;
                abi_complete_mapped(
                    ctx,
                    &schema::ActivationResult {
                        token: act.token,
                        outcome: schema::ActivationOutcome::StateJson {
                            json: br#"{"n":9}"#.to_vec(),
                        },
                    },
                );
            }
            (status, _) => panic!("unexpected poll status {status} during shutdown"),
        }
    }
}
