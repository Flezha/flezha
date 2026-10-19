//! The native surface as data: `veneer::SURFACE` against what the crate
//! exports, `test/fixtures/abi_surface.txt`, and, for the rows Java reaches,
//! `test/fixtures/jni_surface.txt` and `jni_glue.rs`'s exports.
//!
//! A host binding that declares the surface in its own language (PHP's
//! `cdef`, Java's `FlzCore`, each glue's loader) asserts against those files
//! rather than against this crate's source, so its suite runs from a tree that
//! carries the fixtures and no Rust at all. This test is the other end of
//! that: the files are only worth reading if they cannot drift from the crate.

use std::fs;
use std::path::{Path, PathBuf};

use flz_core_ffi::veneer::{Entry, Reach, SURFACE};

/// Every `flz_core_*` entry point the crate exports, from its own source.
///
/// Three files, because the C ABI is split three ways: the general entry
/// points, the local-mode ones and the notice ones. Reading fewer reports a
/// name the crate does export as one it does not.
pub fn exported_entry_points() -> Vec<String> {
    let mut names: Vec<String> = ["src/lib.rs", "src/knob.rs", "src/local.rs", "src/notice.rs"]
        .iter()
        .flat_map(|relative| {
            let source = fs::read_to_string(crate_path(relative))
                .expect("a source of this crate is readable");
            source
                .lines()
                .filter_map(entry_point_named)
                .collect::<Vec<_>>()
        })
        .collect();
    names.sort();
    names
}

/// The entry point one line declares, if it declares one.
fn entry_point_named(line: &str) -> Option<String> {
    let rest = line
        .trim_start()
        .strip_prefix("pub unsafe extern \"C\" fn ")
        .or_else(|| line.trim_start().strip_prefix("pub extern \"C\" fn "))?;
    Some(rest.split(['(', '<']).next()?.trim().to_string())
}

fn crate_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

/// One fixture's entries, comments and blanks dropped, as every SDK reads it.
fn fixture_lines(name: &str) -> Vec<String> {
    let path = crate_path(&format!("../test/fixtures/{name}"));
    let text = fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "{} is a surface a host binding declares against: {e}",
            path.display()
        )
    });
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_string)
        .collect()
}

/// Every JNI symbol `jni_glue.rs` exports, by its Java-side name. The mangling
/// is `Java_com_flezha_sdk_native_1_FlzCore_<name>`; `native_1` is JNI's escape
/// for the trailing underscore in the `native_` package.
fn jni_exports() -> Vec<String> {
    include_str!("../../src/jni_glue.rs")
        .lines()
        .filter_map(|line| line.strip_prefix("pub extern \"system\" fn "))
        .filter_map(|rest| {
            rest.split(['(', '<'])
                .next()?
                .strip_prefix("Java_com_flezha_sdk_native_1_FlzCore_")
                .map(str::to_string)
        })
        .collect()
}

/// `left` and `right` name the same entries, each once; a failure names every
/// entry one side has and the other lacks.
fn assert_same((left, mut l): (&str, Vec<String>), (right, mut r): (&str, Vec<String>)) {
    l.sort();
    r.sort();
    let only = |a: &[String], b: &[String]| -> Vec<String> {
        a.iter().filter(|name| !b.contains(name)).cloned().collect()
    };
    assert!(
        l == r,
        "only in {left}: {:?}; only in {right}: {:?}",
        only(&l, &r),
        only(&r, &l)
    );
}

fn table() -> (&'static str, Vec<String>) {
    let rows = SURFACE.iter().map(|entry| entry.abi.to_string()).collect();
    ("veneer::SURFACE", rows)
}

#[test]
fn the_table_is_every_entry_point_the_crate_exports() {
    assert_same(table(), ("the crate's exports", exported_entry_points()));
}

#[test]
fn the_fixture_is_the_table() {
    let fixture = fixture_lines("abi_surface.txt");
    assert_same(table(), ("test/fixtures/abi_surface.txt", fixture));
}

#[test]
fn the_jni_rows_are_the_java_surface() {
    let jni = || {
        SURFACE
            .iter()
            .filter(|entry| matches!(entry.reach, Reach::Jni | Reach::Every))
    };
    let rows = jni()
        .map(|entry| format!("{}|{}", entry.camel_name(), entry.abi))
        .collect();
    let fixture = fixture_lines("jni_surface.txt");
    assert_same(
        ("the JNI rows of veneer::SURFACE", rows),
        ("test/fixtures/jni_surface.txt", fixture),
    );

    let methods = jni().map(Entry::camel_name).collect();
    assert_same(
        ("the JNI rows of veneer::SURFACE", methods),
        ("jni_glue.rs's exports", jni_exports()),
    );
}
