//! The Python binding's own contract: the context is an opaque object that
//! owns its lifetime and no process-wide global holds it, so a
//! sub-interpreter executor reaches only its own.

use flz_core_ffi::python_glue::Core;
use pyo3::prelude::*;

/// No mutable process-wide global holds the binding handle.
///
/// This is the discipline that keeps a sub-interpreter executor a swap
/// rather than a rewrite, and it is exactly the kind of thing a later
/// change reintroduces by accident — a `static CONTEXT` is the shortest
/// path to "one context per process" and is what the Ruby and JNI veneers
/// use, because their runtime has one interpreter per process and CPython
/// does not.
#[test]
fn no_process_wide_global_holds_the_context() {
    let source = include_str!("../../src/python_glue.rs");
    for (index, line) in source.lines().enumerate() {
        let code = line.split("//").next().unwrap_or("").trim();
        if code.starts_with("static ") || code.starts_with("pub static ") {
            panic!(
                "src/python_glue.rs:{} declares a static ({code}); the binding handle is a \
                 #[pyclass] so that a sub-interpreter reaches only its own",
                index + 1
            );
        }
    }
    assert!(
        source.contains("#[pyclass(module = \"flezha.flz_core_ffi\", name = \"Core\""),
        "the context must stay an opaque pyclass"
    );
}

/// A context is created, used and freed entirely through the object, and
/// freeing twice is a no-op rather than a double free.
#[test]
fn the_context_object_owns_its_lifetime() {
    Python::attach(|py| {
        // Constructed the way Python constructs it, so the test drives the
        // surface the wheel exposes rather than the Rust constructor.
        let core = py
            .get_type::<Core>()
            .call1((flz_core::SCHEMA_VERSION,))
            .expect("context");
        assert!(core
            .call_method0("last_error")
            .expect("last_error")
            .is_none());
        core.call_method0("free").expect("free");
        core.call_method0("free").expect("free is idempotent");
        // Every entry point refuses once the context is gone, rather than
        // dereferencing a freed pointer.
        assert!(core
            .call_method1("declare_class", (b"{}".as_slice(),))
            .is_err());
    });
}

/// A build against another activation schema is refused at context
/// creation, naming both versions, before any connection is attempted.
#[test]
fn a_schema_mismatch_refuses_before_connecting() {
    Python::attach(|py| {
        let err = py
            .get_type::<Core>()
            .call1((flz_core::SCHEMA_VERSION + 1,))
            .expect_err("must refuse");
        let message = err.value(py).to_string();
        assert!(
            message.contains(&flz_core::SCHEMA_VERSION.to_string())
                && message.contains(&(flz_core::SCHEMA_VERSION + 1).to_string()),
            "the refusal names both versions: {message}"
        );
        assert!(err.is_instance_of::<flz_core_ffi::python_glue::SchemaMismatch>(py));
    });
}
