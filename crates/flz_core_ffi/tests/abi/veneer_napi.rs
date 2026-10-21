//! The Node addon's surface. Its half of the shape — how JavaScript builds
//! and reads a value — cannot be driven from here: a Rust test binary has no
//! `Env`, so `Values` is proved by the compile and by the Node suite, and the
//! mapping it feeds is `shape`'s.

use flz_core_ffi::napi_glue::native_surface;

/// Every blocking export is typed as a promise, so the event-loop rule is
/// asserted rather than reviewed: a new blocking entry point exported as a
/// synchronous call fails here.
#[test]
fn every_blocking_export_is_a_promise() {
    let blocking = native_surface().iter().filter(|e| e.blocking).count();
    let promised = [
        include_str!("../../src/napi_glue.rs"),
        include_str!("../../src/napi_host/mod.rs"),
    ]
    .iter()
    .map(|source| source.matches("ts_return_type = \"Promise<").count())
    .sum::<usize>();
    assert_eq!(
        promised, blocking,
        "every blocking export returns a promise"
    );
}
