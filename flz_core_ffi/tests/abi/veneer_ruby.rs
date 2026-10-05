//! The Ruby half of the shape, proved with no Ruby process involved: an
//! embedded CRuby VM, and every primitive `veneer::shape` asks a language for
//! built and read back. The mapping itself is `shape`'s.

use flz_core_ffi::ruby_glue::{Values, NATIVE_SURFACE};
use flz_core_ffi::veneer::shape;
use magnus::{Ruby, Symbol};

/// Called from inside `c_abi_full_scenario` rather than as a `#[test]` of
/// its own: `magnus::embed::init` runs once per process and records the
/// caller's stack frame as the GC scan root, so every Ruby value must sit
/// deeper in that one test's stack.
pub fn primitives_round_trip() {
    // SAFETY: the scenario booted the VM on this thread before reaching
    // any code that calls this.
    let ruby = unsafe { Ruby::get_unchecked() };
    let values = Values(&ruby);
    crate::shape::primitives_round_trip(&values);

    // Ruby is the one language whose vocabulary word is not text.
    let word = shape::Sink::word(&values, shape::word::WORK).expect("a word");
    let word = Symbol::from_value(word).expect("a word is a Symbol");
    assert_eq!(word.name().unwrap(), shape::word::WORK);
}

/// The extension exports exactly what the surface table names, every entry
/// names a C ABI entry point, and every blocking one is a call the veneer
/// runs with the GVL released. Asserted on this side because it is the
/// side that can go wrong without anyone installing the gem.
#[test]
fn the_native_surface_is_the_declared_one() {
    let blocking: Vec<&str> = NATIVE_SURFACE
        .iter()
        .filter(|(_, abi)| flz_core_ffi::veneer::BLOCKING_ENTRY_POINTS.contains(abi))
        .map(|(name, _)| *name)
        .collect();
    assert_eq!(
        blocking,
        [
            "start",
            "local_feed",
            "local_next_output",
            "local_run_until_idle",
            "poll",
            "wait_starvation",
            "complete",
            "spawn_result",
            "effect_end",
            "admin_call",
            "send_message"
        ]
    );
    for (name, abi) in NATIVE_SURFACE {
        assert!(
            abi.starts_with("flz_core_"),
            "{name} names {abi}, which is not a C ABI entry point"
        );
    }
    // The read-only verdict rides beside the registration: the gem's
    // `Graph.validate` submits through it, so a table naming one and not the
    // other leaves the SDK's own front door unreachable.
    let names: Vec<&str> = NATIVE_SURFACE.iter().map(|(name, _)| *name).collect();
    for required in ["declare_class", "check_class"] {
        assert!(
            names.contains(&required),
            "the surface table names no {required}"
        );
    }
}
