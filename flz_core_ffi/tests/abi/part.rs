//! A tasklet's part through the C ABI: numbered per work item, stamped with
//! the item's attempt and Cell, written at once, and refused once its
//! activation has completed.
//!
//! The scenario runs in a child process of this test binary, because the
//! engine is process-global and another scenario's poll would take its batch.

use std::os::unix::net::UnixStream;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use flz_core::schema;
use flz_core_ffi::fake_controller::part_line;
use flz_core_ffi::veneer::{self, CtxPtr, Refusal};
use flz_core_ffi::*;
use flz_transport::errors::FlezhaError;
use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;

use crate::harness::{abi_complete, abi_poll, ok_effect};

const CHILD: &str = "FLZ_ABI_PART_CHILD";

#[test]
fn a_part_leaves_at_once_and_dies_with_its_activation() {
    if std::env::var_os(CHILD).is_some() {
        return scenario();
    }
    let output = Command::new(std::env::current_exe().expect("the test binary"))
        .args([
            "--exact",
            "part::a_part_leaves_at_once_and_dies_with_its_activation",
        ])
        .env(CHILD, "1")
        .output()
        .expect("run the scenario in a child");
    assert!(
        output.status.success(),
        "the part scenario failed:\n{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn scenario() {
    let ctx = flz_core_context_new(flz_core::SCHEMA_VERSION);
    let decl = br#"{
        "factory_id": "abi_part", "sdk_lang": 1, "sdk_version": "0.1.0",
        "channels": [
            {"name": "in", "kind": "entry", "chains": [[{"link": "into", "tasklet": 0}]]},
            {"name": "answer", "kind": "plugin", "producer": 0,
             "chains": [[{"link": "sink", "name": "answer"}]]}],
        "tasklets": [{"name": "p0"}]
    }"#;
    veneer::declare_class(CtxPtr::of(ctx), decl).expect("the class declares");
    let instance = flz_core::operator::instance::OperatorInstance::of_class("abi_part", "abi_part")
        .expect("the class builds an instance");
    let (sock, mut peer) = UnixStream::pair().unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let stream = Arc::new(FlezhaStream::Unix(sock));
    let entry = instance.topology.channel(0).expect("entry channel");
    for payload in [b"alpha".to_vec(), b"beta".to_vec()] {
        let value = entry.wrap_input.as_ref().expect("entry wrapper")(payload).unwrap();
        flz_core::routing::route_to_consumers(
            &instance, entry, &value, &stream, None, None, false, None,
        );
    }
    let (status, act) = abi_poll(ctx, 2_000);
    assert_eq!(status, FLZ_POLL_ACTIVATION);
    let act = act.expect("a batch of two");

    let part = |item: u32, value: &[u8]| veneer::part(CtxPtr::of(ctx), act.token, item, value);
    part(1, b"b0").expect("the second item's first part");
    part(1, b"b1").expect("its second");
    part(0, b"a0").expect("the first item's first part");
    for (number, payload) in [("0", "b0"), ("1", "b1"), ("0", "a0")] {
        let (payload_id, message, metadata) = payloader::extract_payload(&mut peer).unwrap();
        assert_eq!(payload_id, flz_core::part::PART_PAYLOAD_ID);
        assert_eq!(
            part_line(&message, &metadata),
            format!("part {number} attempt 1 channel answer cell abi_part/p0/ payload {payload}")
        );
    }

    assert_eq!(
        code_of(part(2, b"x")),
        "part.outside_invocation",
        "no third item"
    );
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
    assert_eq!(code_of(part(0, b"late")), "part.outside_invocation");
}

fn code_of(refused: Result<(), Refusal>) -> String {
    match refused {
        Err(Refusal::Core(document)) => {
            FlezhaError::from_document(&document)
                .expect("a refusal is an error document")
                .code
        }
        other => panic!("expected a core refusal, got {other:?}"),
    }
}
