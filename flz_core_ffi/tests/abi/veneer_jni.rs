//! The Java SDK's native surface, asserted rather than reviewed.
//!
//! Java reaches the core through JNI rather than through the C ABI directly,
//! so its surface is two hand-written lists, `FlzCore.java`'s `static native`
//! declarations and `jni_glue.rs`'s `Java_com_flezha…` exports. The declared
//! table is `test/fixtures/jni_surface.txt`, `<method>|<entry point>` in
//! declaration order: this test holds both lists to it, and the Java SDK's own
//! suite holds the class to it where no Rust build can run.

use crate::surface::fixture_lines;

/// The declared surface, `(java_name, abi_entry_point)` in declaration order.
fn native_surface() -> Vec<(String, String)> {
    fixture_lines("jni_surface.txt")
        .iter()
        .map(|row| {
            let (name, abi) = row.split_once('|').unwrap_or_else(|| {
                panic!("jni_surface.txt row `{row}` is not `method|entry point`")
            });
            (name.to_string(), abi.to_string())
        })
        .collect()
}

/// `FlzCore.java`, read from the repository the crate sits in.
fn flz_core_java() -> String {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../sdk/java/flezha-sdk-native/src/main/java/com/flezha/sdk/native_/FlzCore.java"
    );
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("reading {path}: {e}"))
}

/// Every `static native` name the Java class declares, in declaration order.
fn declared_natives(source: &str) -> Vec<String> {
    source
        .lines()
        .filter_map(|line| line.trim().strip_prefix("public static native "))
        .filter_map(|rest| {
            // `<return type> <name>(` — the name is what precedes the paren.
            let name = rest.split('(').next()?.split_whitespace().last()?;
            Some(name.to_string())
        })
        .collect()
}

/// Every JNI symbol `jni_glue.rs` exports, by its Java-side name. The mangling
/// is `Java_com_flezha_sdk_native_1_FlzCore_<name>`; `native_1` is JNI's escape
/// for the trailing underscore in the `native_` package.
fn exported_symbols() -> Vec<String> {
    let source = include_str!("../../src/jni_glue.rs");
    let mut names: Vec<String> = source
        .lines()
        // The declaration line only; a doc comment naming a symbol is prose.
        .filter(|line| line.starts_with("pub extern \"system\" fn "))
        .filter_map(|line| {
            let rest = line.strip_prefix("pub extern \"system\" fn ")?;
            let symbol = rest.split(['(', '<']).next()?;
            symbol
                .strip_prefix("Java_com_flezha_sdk_native_1_FlzCore_")
                .map(str::to_string)
        })
        .collect();
    names.sort();
    names
}

/// The three lists agree in both directions: the Java declarations, the JNI
/// exports and the declared table. A symbol added on one side and not the
/// others fails here rather than at the first call.
#[test]
fn the_java_native_surface_is_the_declared_one() {
    let surface = native_surface();
    let declared: Vec<&str> = surface.iter().map(|(name, _)| name.as_str()).collect();
    let java = declared_natives(&flz_core_java());

    assert_eq!(
        java, declared,
        "FlzCore.java's `static native` declarations are not the declared surface"
    );

    let mut sorted = declared.clone();
    sorted.sort();
    assert_eq!(
        exported_symbols(),
        sorted,
        "jni_glue.rs exports a different set of JNI symbols than the table declares"
    );
}

/// Every entry names a C ABI entry point, and no entry names one twice: the
/// binding adds no ABI surface of its own.
#[test]
fn every_java_export_reaches_one_c_entry_point() {
    let surface = native_surface();
    let mut seen: Vec<&str> = Vec::new();
    for (name, abi) in &surface {
        assert!(
            abi.starts_with("flz_core_"),
            "{name} names {abi}, which is not a C ABI entry point"
        );
        assert!(
            !seen.contains(&abi.as_str()),
            "{abi} is reached by two Java exports; one export, one entry point"
        );
        seen.push(abi);
    }

    // The read-only verdict rides beside the registration: the SDK's
    // `Graph.validate` submits through it, so a table naming one and not the
    // other leaves the SDK's own front door unreachable.
    for required in ["declareClass", "checkClass"] {
        assert!(
            surface.iter().any(|(name, _)| name == required),
            "the surface table names no {required}"
        );
    }
}

/// Every blocking entry point Java reaches is one the SDK calls off the
/// dispatch thread. Named here so a new blocking entry point cannot join the
/// surface unnoticed.
#[test]
fn the_blocking_entry_points_are_the_declared_ones() {
    let blocking: Vec<String> = native_surface()
        .into_iter()
        .filter(|(_, abi)| flz_core_ffi::veneer::BLOCKING_ENTRY_POINTS.contains(&abi.as_str()))
        .map(|(name, _)| name)
        .collect();

    assert_eq!(
        blocking,
        [
            "start",
            "localFeed",
            "localNextOutput",
            "localRunUntilIdle",
            "poll",
            "complete",
            "waitStarvation",
            "spawnResult",
            "effectEnd",
            "adminCall",
            "sendMessage"
        ]
    );
}
