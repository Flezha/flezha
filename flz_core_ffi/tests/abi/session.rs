//! A factory change after start, through the C ABI against the fake
//! controller: a declare and an idle undeclare each ship the full factory set
//! as `UpdateFactories`, and an undeclare with a live instance is refused by
//! the runner's coordinator. In a child process, because the runner is
//! process-global.

use std::ffi::CString;

use flz_core::pb::command_request::Command;
use flz_core_ffi::*;
use prost::Message;

use crate::harness::{last_document, last_error, Controller, Demanded};

fn declare(ctx: *mut FlzContext, id: &str) -> i32 {
    let decl = format!(
        r#"{{"factory_id": "{id}", "sdk_lang": 1, "sdk_version": "0.1.0",
             "channels": [{{"name": "in", "kind": "entry",
                            "chains": [[{{"link": "into", "tasklet": 0}}]]}}],
             "tasklets": [{{"name": "pass"}}]}}"#
    );
    unsafe { flz_core_declare_class(ctx, decl.as_ptr(), decl.len()) }
}

fn undeclare(ctx: *mut FlzContext, id: &str) -> i32 {
    let id = CString::new(id).unwrap();
    unsafe { flz_core_undeclare_class(ctx, id.as_ptr()) }
}

/// The factory ids of the `nth` `UpdateFactories` frame the runner wrote.
fn shipped(controller: &Controller, nth: usize) -> Vec<String> {
    controller.await_frame("UpdateFactories", |captured| {
        captured
            .iter()
            .filter(|(id, _)| id == "control_command")
            .filter_map(
                |(_, frame)| match flz_core::pb::CommandRequest::decode(&frame[..]) {
                    Ok(flz_core::pb::CommandRequest {
                        command: Some(Command::UpdateFactories(update)),
                        ..
                    }) => Some(update.factories.into_iter().map(|f| f.id).collect()),
                    _ => None,
                },
            )
            .nth(nth)
    })
}

#[test]
fn a_factory_change_after_start_reaches_the_controller() {
    if std::env::var_os("FLZ_ABI_SETTINGS_CHILD").is_none() {
        return crate::settings::in_child(
            "session::a_factory_change_after_start_reaches_the_controller",
        );
    }
    let controller = Controller::start(&crate::lifecycle::socket_dir());
    crate::harness::record_notices();
    let ctx = flz_core_context_new(flz_core::SCHEMA_VERSION);
    assert_eq!(declare(ctx, "session_a"), FLZ_OK, "{}", last_error(ctx));
    let ns = CString::new("session_ns").unwrap();
    assert_eq!(
        unsafe { flz_core_start(ctx, ns.as_ptr()) },
        FLZ_OK,
        "{}",
        last_error(ctx)
    );

    assert_eq!(declare(ctx, "session_b"), FLZ_OK, "{}", last_error(ctx));
    assert_eq!(shipped(&controller, 0), ["session_a", "session_b"]);

    controller.send(
        "ref-add-a",
        Command::AddRelay(flz_core::pb::AddRelay {
            relay_id: "session_a_1".to_string(),
            factory_id: "session_a".to_string(),
            ..Default::default()
        }),
    );
    controller.await_success("AddRelay response", "ref-add-a");

    assert_eq!(undeclare(ctx, "session_b"), FLZ_OK, "{}", last_error(ctx));
    assert_eq!(shipped(&controller, 1), ["session_a"]);

    assert_eq!(undeclare(ctx, "session_a"), FLZ_ERR);
    let refused = last_document(ctx);
    assert_eq!(refused.code, "runtime.relay_type_in_use");
    assert_eq!(refused.details["factory_id"], "session_a");
    assert_eq!(refused.details["live_instances"], 1);
    assert_eq!(crate::harness::notices(), Vec::<String>::new());
}
