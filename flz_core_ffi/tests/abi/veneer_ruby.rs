//! The Ruby half of the shape, proved with no Ruby process involved: an
//! embedded CRuby VM, and every primitive `veneer::shape` asks a language for
//! built and read back. The mapping itself is `shape`'s.

use flz_core_ffi::ruby_glue::Values;
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

/// Every blocking export is a `without_gvl` call site, so the GVL rule is
/// asserted rather than reviewed: a new blocking entry point that forgot to
/// release the lock fails here.
#[test]
fn every_blocking_export_releases_the_gvl() {
    let blocking = flz_core_ffi::veneer::glue_entries()
        .filter(|entry| entry.blocking)
        .count();
    let source = include_str!("../../src/ruby_glue.rs");
    assert_eq!(
        source.matches("without_gvl(move").count(),
        blocking,
        "every blocking export releases the GVL exactly once"
    );
}
