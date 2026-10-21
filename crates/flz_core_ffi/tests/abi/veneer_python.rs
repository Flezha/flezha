//! The Python half of the shape, proved with no Python process involved: an
//! embedded CPython interpreter, and every primitive `veneer::shape` asks a
//! language for built and read back. The mapping itself is `shape`'s.

use flz_core_ffi::python_glue::{PayloadView, Values};
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

/// Every blocking export is a `py.detach` call site, so the GIL rule is
/// asserted rather than reviewed: a new blocking entry point that forgot to
/// release the lock fails here.
#[test]
fn every_blocking_export_releases_the_gil() {
    let blocking = flz_core_ffi::veneer::glue_entries()
        .filter(|entry| entry.blocking)
        .count();
    let source = include_str!("../../src/python_glue.rs");
    assert_eq!(
        source.matches("py.detach(").count(),
        blocking,
        "every blocking export releases the GIL exactly once"
    );
}
