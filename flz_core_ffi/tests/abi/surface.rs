//! The C ABI surface as data: `test/fixtures/abi_surface.txt` against what the
//! crate exports.
//!
//! A host binding that declares the surface in its own language (PHP's
//! `cdef`, Node's addon declaration) asserts against that file rather than
//! against this crate's source, so its suite runs from a tree that carries the
//! fixtures and no Rust at all. This test is the other end of that: the file
//! is only worth reading if it cannot drift from the crate.

use std::fs;
use std::path::{Path, PathBuf};

/// Every `flz_core_*` entry point the crate exports, from its own source.
///
/// Two files, because the C ABI is two: the general entry points and the
/// local-mode ones. Reading only the first reports a name the crate does
/// export as one it does not.
pub fn exported_entry_points() -> Vec<String> {
    let mut names: Vec<String> = ["src/lib.rs", "src/local.rs"]
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

/// The fixture's entries, comments and blanks dropped, as every SDK reads it.
fn fixture_entry_points() -> Vec<String> {
    let path = crate_path("../test/fixtures/abi_surface.txt");
    let text = fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "{} is the surface every host binding declares against: {e}",
            path.display()
        )
    });
    let mut names: Vec<String> = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_string)
        .collect();
    names.sort();
    names
}

#[test]
fn the_fixture_lists_every_entry_point_the_crate_exports() {
    let listed = fixture_entry_points();
    let missing: Vec<String> = exported_entry_points()
        .into_iter()
        .filter(|name| !listed.contains(name))
        .collect();

    assert!(
        missing.is_empty(),
        "the crate exports {missing:?}, which test/fixtures/abi_surface.txt does not list; \
         a host binding reading the file would never learn the entry point exists"
    );
}

#[test]
fn the_fixture_lists_nothing_the_crate_does_not_export() {
    let exported = exported_entry_points();
    let extra: Vec<String> = fixture_entry_points()
        .into_iter()
        .filter(|name| !exported.contains(name))
        .collect();

    assert!(
        extra.is_empty(),
        "test/fixtures/abi_surface.txt lists {extra:?}, which the crate does not export; \
         a host binding that declared one would fail at its first call rather than at load"
    );
}
