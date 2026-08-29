//! The Python half of the shape, proved with no Python process involved: an
//! embedded CPython interpreter, and every primitive `veneer::shape` asks a
//! language for built and read back. The mapping itself is `shape`'s.

use flz_core_ffi::python_glue::{PayloadView, Values, NATIVE_SURFACE};
use flz_core_ffi::veneer::shape;
use pyo3::prelude::*;

/// CPython attaches from any thread once the interpreter is initialised,
/// so this is an ordinary `#[test]` — unlike Ruby's, which must run inside
/// the scenario that booted the VM.
#[test]
fn every_primitive_round_trips() {
    Python::initialize();
    Python::attach(|py| {
        let values = Values(py);
        crate::shape::primitives_round_trip(&values);

        // Python spells a vocabulary word as text, and state JSON as
        // `bytes` rather than as the borrowed view a payload crosses as.
        let word = shape::Sink::word(&values, shape::word::WORK).expect("a word");
        assert_eq!(word.extract::<String>().expect("text"), shape::word::WORK);
        let json = shape::Sink::json(&values, b"{}".to_vec()).expect("state json");
        assert!(json.is_instance_of::<pyo3::types::PyBytes>());
        let payload = shape::Sink::bytes(&values, b"{}".to_vec()).expect("a payload");
        assert!(payload.is_instance_of::<PayloadView>());
        let copied = payload.call_method0("tobytes").expect("a view copies");
        assert_eq!(copied.extract::<Vec<u8>>().expect("bytes"), b"{}");
    });
}

/// The extension exports exactly what the surface table names, every entry
/// names a C ABI entry point, and every blocking one is a call the veneer
/// runs with the GIL released. Asserted on this side because it is the
/// side that can go wrong without anyone installing the wheel.
#[test]
fn the_native_surface_is_the_declared_one() {
    let blocking: Vec<&str> = NATIVE_SURFACE
        .iter()
        .filter(|(_, abi)| flz_core_ffi::BLOCKING_ENTRY_POINTS.contains(abi))
        .map(|(name, _)| *name)
        .collect();
    assert_eq!(
        blocking,
        [
            "Core.start",
            "Core.poll",
            "Core.wait_starvation",
            "Core.complete",
            "Core.workflow_recv",
            "Core.spawn_workflow"
        ]
    );
    for (name, abi) in NATIVE_SURFACE {
        assert!(
            abi.starts_with("flz_core_"),
            "{name} names {abi}, which is not a C ABI entry point"
        );
    }

    // Every blocking export is a `py.detach` call site, so the GIL rule is
    // asserted rather than reviewed: a new blocking entry point that
    // forgot to release the lock fails here.
    let source = include_str!("../../src/python_glue.rs");
    let released = source.matches("py.detach(").count();
    assert_eq!(
        released,
        blocking.len(),
        "every blocking export releases the GIL exactly once"
    );
}
