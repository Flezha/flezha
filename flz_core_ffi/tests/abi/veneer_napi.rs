//! The Node addon's surface. Its half of the shape — how JavaScript builds
//! and reads a value — cannot be driven from here: a Rust test binary has no
//! `Env`, so `Values` is proved by the compile and by the Node suite, and the
//! mapping it feeds is `shape`'s.

use flz_core_ffi::napi_glue::native_surface;
/// The addon exports exactly what the surface table names, every blocking
/// entry point is a promise, and neither `poll` nor `waitStarvation` is
/// among them. Asserted here as well as from Node, because this is the
/// side that can go wrong without anyone installing the package.
#[test]
fn the_native_surface_is_the_declared_one() {
    let surface = native_surface();
    let blocking: Vec<&str> = surface
        .iter()
        .filter(|e| e.blocking)
        .map(|e| e.name.as_str())
        .collect();
    assert_eq!(
        blocking,
        [
            "start",
            "localFeed",
            "localNextOutput",
            "localRunUntilIdle",
            "complete",
            "workflowRecv",
            "spawnWorkflow",
            "adminCall",
            "stopHost"
        ]
    );
    for name in ["poll", "waitStarvation"] {
        assert!(
            !surface.iter().any(|e| e.name == name),
            "{name} must not be exported to JavaScript"
        );
    }
    for entry in &surface {
        if let Some(abi) = &entry.abi {
            assert!(
                abi.starts_with("flz_core_"),
                "{} names {abi}, which is not a C ABI entry point",
                entry.name
            );
        }
    }
}
