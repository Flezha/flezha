//! The last phase: `flz_core_shutdown` ships every live Entity's state before
//! it wakes the pollers, and a stopping runner opens no new control
//! connection. On its own because it is the one phase that ends the runner.

use std::thread;
use std::time::{Duration, Instant};

use flz_core::schema;
use flz_core_ffi::fake_controller::shutdown_notification;
use flz_core_ffi::*;

use crate::harness::{abi_complete_mapped, abi_poll, Controller, Demanded};

/// `flz_core_shutdown` ships every live Entity's state as a
/// `RunnerShutdownNotification` before the core reports shutting down: the
/// poller blocked at that moment is handed the migration serializes first, the
/// controller sees the notification, and only then is the poller woken.
pub(crate) fn shutdown_ships_state_then_wakes_the_poller(
    ctx: *mut FlzContext,
    controller: &Controller,
) {
    let ctx_addr = ctx as usize;
    let host = thread::spawn(move || serve_until_shutdown(ctx_addr as *mut FlzContext));
    // The poller stays blocked for as long as it is watched; shutdown is what
    // frees it.
    let watched = Instant::now();
    while watched.elapsed() < Duration::from_millis(100) {
        assert!(!host.is_finished(), "the poller returned before shutdown");
        thread::yield_now();
    }
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
                operative_id: "eff-1".to_string(),
            },
        ),
    );

    let (served, waited) = host.join().expect("host loop returns");
    assert!(served >= 1, "the host was handed no migration serialize");
    assert!(
        waited < Duration::from_secs(5),
        "shutdown must wake promptly, waited {waited:?}"
    );
    // Only after the poller has been woken, and briefly: a stopping runner
    // that re-registered would be handed work it will never do.
    let watched = Instant::now();
    while watched.elapsed() < Duration::from_millis(100) {
        assert_eq!(
            controller.accepted(),
            connections,
            "a stopping runner must not open a new control connection"
        );
        thread::yield_now();
    }
}

/// The host during a shutdown: answer every migration serialize with a
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
                    matches!(act.kind, schema::ActivationKind::MigrationSerialize),
                    "expected a migration serialize during shutdown, got {:?}",
                    act.kind
                );
                served += 1;
                abi_complete_mapped(
                    ctx,
                    &schema::ActivationResult {
                        token: act.token,
                        outcome: schema::ActivationOutcome::MigrationState {
                            json: br#"{"n":9}"#.to_vec(),
                        },
                    },
                );
            }
            (status, _) => panic!("unexpected poll status {status} during shutdown"),
        }
    }
}
