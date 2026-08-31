//! What one run reports: a human table, or one JSON document.

use std::io::{self, Write};

use super::plan::{Action, Root};

pub(super) fn print_human(roots: &[Root], duration_ms: u64, dry_run: bool) {
    let stdout = io::stdout();
    let mut h = stdout.lock();

    let cleared_verb = if dry_run { "would clear" } else { "cleared" };
    let kept_verb = if dry_run { "would keep" } else { "kept" };

    for r in roots {
        match &r.action {
            Action::Clear => {
                let _ = writeln!(
                    h,
                    "{:11} {:48} ({} files, {})",
                    cleared_verb,
                    r.label(),
                    r.files,
                    format_bytes(r.bytes)
                );
            }
            Action::Keep(reason) => {
                let _ = writeln!(h, "{:11} {:48} ({})", kept_verb, r.label(), reason);
            }
        }
    }
    let _ = writeln!(h, "reset complete in {} ms", duration_ms);
}

// The object is built by hand: `serde_json`'s `Map` is a `BTreeMap`, so `json!`
// would emit `bytes, files, root, rung` where the contract — and
// `cli/tests/reset.rs` — reads `root, files, bytes, rung`.
pub(super) fn print_json(roots: &[Root], duration_ms: u64, dry_run: bool) {
    let mut cleared: Vec<String> = Vec::new();
    let mut kept: Vec<String> = Vec::new();

    for r in roots {
        // The rung rides beside the pinned fields, never in place of one: only
        // the socket root has a source in question, so only it carries it.
        let rung = match r.rung {
            Some(rung) => format!(",\"rung\":{}", json_str(rung)),
            None => String::new(),
        };
        let root = json_str(&r.name());

        match &r.action {
            Action::Clear => cleared.push(format!(
                "{{\"root\":{},\"files\":{},\"bytes\":{}{}}}",
                root, r.files, r.bytes, rung
            )),
            Action::Keep(reason) => kept.push(format!(
                "{{\"root\":{},\"reason\":{}{}}}",
                root,
                json_str(reason),
                rung
            )),
        }
    }

    let body = format!(
        "{{\"cleared\":[{}],\"kept\":[{}],\"duration_ms\":{},\"dry_run\":{}}}",
        cleared.join(","),
        kept.join(","),
        duration_ms,
        dry_run
    );

    let stdout = io::stdout();
    let mut h = stdout.lock();
    let _ = writeln!(h, "{}", body);
}

fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn format_bytes(n: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;

    if n >= GIB {
        format!("{:.1} GiB", n as f64 / GIB as f64)
    } else if n >= MIB {
        format!("{:.1} MiB", n as f64 / MIB as f64)
    } else if n >= KIB {
        format!("{:.1} KiB", n as f64 / KIB as f64)
    } else {
        format!("{} B", n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_bytes_units() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.0 KiB");
        assert_eq!(format_bytes(1024 * 1024), "1.0 MiB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GiB");
    }

    #[test]
    fn json_str_escapes_quotes_and_backslashes() {
        assert_eq!(json_str("a\"b"), "\"a\\\"b\"");
        assert_eq!(json_str("a\\b"), "\"a\\\\b\"");
        assert_eq!(json_str("plain"), "\"plain\"");
    }
}
