//! `test/fixtures/wire_constants.txt`, held to the Rust sources: every row is a
//! constant declared here (or a reserved `flz.` key the Rust code reads or
//! writes), and every such constant is a row. The other runtimes assert their
//! own literals against the same rows, so a value changed here fails each of
//! them through the file.

use std::collections::BTreeSet;
use std::path::Path;

const FIXTURE: &str = include_str!("../../../test/fixtures/wire_constants.txt");

/// The Rust production sources the rows come from: the core, the transport
/// and the reference SDK, which stamps the output keys every SDK stamps.
const ROOTS: [&str; 3] = ["src", "../flz_transport/src", "../../sdk/rust/src"];

fn production_sources(dir: &Path, out: &mut Vec<String>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy();
        if name == "tests" || name.ends_with("_tests") || name.ends_with("tests.rs") {
            continue;
        }
        if path.is_dir() {
            production_sources(&path, out);
        } else if name.ends_with(".rs") {
            let text = std::fs::read_to_string(&path).unwrap();
            out.push(text.split("#[cfg(test)]").next().unwrap().to_string());
        }
    }
}

/// The row a top-level `const NAME: <ty> = <value>;` declaration stands for.
fn constant_row(line: &str) -> Option<String> {
    let decl = line
        .strip_prefix("pub ")
        .unwrap_or(line)
        .strip_prefix("const ")?;
    let (name, rest) = decl.split_once(": ")?;
    let (ty, value) = rest.split_once(" = ")?;
    let value = value.strip_suffix(';')?;
    let row = |table: &str, name: &str, value: &str| Some(format!("{table}|{name}|{value}"));
    match (name, ty) {
        ("SCHEMA_VERSION", "u32") => row("schema_version", "activation", value),
        ("MAX_NAME_BYTES", "usize") => row("effect", "name_max_bytes", value),
        (_, "u8") if name.starts_with("SDK_LANG_") => {
            row("sdk_lang", &name["SDK_LANG_".len()..].to_lowercase(), value)
        }
        (_, "u8") if name.starts_with("FORMAT_ID_") => row(
            "format_id",
            &name["FORMAT_ID_".len()..].to_lowercase(),
            value,
        ),
        (_, "&str") if name.ends_with("_PAYLOAD_ID") => {
            let id = value.trim_matches('"');
            row("payload_id", id, id)
        }
        _ => None,
    }
}

/// Every `"flz.<name>"` literal in `text`, as its row.
fn key_rows(text: &str) -> impl Iterator<Item = String> + '_ {
    text.split("\"flz.").skip(1).filter_map(|tail| {
        let (name, _) = tail.split_once('"')?;
        let well_formed =
            !name.is_empty() && name.bytes().all(|b| b.is_ascii_lowercase() || b == b'_');
        well_formed.then(|| format!("flz_key|{name}|flz.{name}"))
    })
}

#[test]
fn every_fixture_row_is_a_rust_constant_and_every_constant_a_row() {
    let mut sources = Vec::new();
    for root in ROOTS {
        production_sources(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join(root),
            &mut sources,
        );
    }
    let declared: BTreeSet<String> = sources
        .iter()
        .flat_map(|text| text.lines().filter_map(constant_row).chain(key_rows(text)))
        .collect();
    let pinned: BTreeSet<String> = FIXTURE
        .lines()
        .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
        .map(str::to_string)
        .collect();

    let missing: Vec<String> = declared.difference(&pinned).cloned().collect();
    let stale: Vec<String> = pinned.difference(&declared).cloned().collect();
    assert!(
        missing.is_empty() && stale.is_empty(),
        "test/fixtures/wire_constants.txt disagrees with the Rust sources.\n\
         Declared in Rust, not in the file:\n{}\nIn the file, declared nowhere in Rust:\n{}",
        missing.join("\n"),
        stale.join("\n"),
    );
}
