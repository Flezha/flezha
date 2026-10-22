//! Every help screen `flz` prints, rendered from the verb rows and the
//! catalogue the binary embeds.
//!
//! A verb's screen is its description (the first sentence of the first
//! operation it calls, or its own `about`), its usage line and its options,
//! each option described by its own `doc` where the CLI answers for it, or
//! else by the operation's argument of the same name. A command's screen lists
//! its verbs the same way. Only each screen's notes (`notes.rs`) are written by
//! hand: examples and what only the CLI knows. `crates/cli/tests/golden/` holds every
//! screen captured from the binary, which proves a change to a row, a
//! description or this renderer is deliberate.

use std::sync::OnceLock;

use serde_json::Value;

use crate::args::{self, OptSpec, UNIVERSAL};
use crate::dispatch::{Body, Command, Verb};

mod notes;
pub(crate) use notes::*;

/// One block of hand-written help text, without its trailing newline.
pub(crate) type Help = &'static str;

pub(crate) fn render(help: &str) {
    println!("{}", help);
}

/// The catalogue of every operation a controller publishes, as
/// `mix flezha.catalogue` wrote it: what `flz help --json` prints, what every
/// door call `flz` sends is held to, and what a verb's screen describes it by.
pub(crate) const CATALOGUE: &str = include_str!("../catalogue.json");

/// `flz help`: the screen, or under `--json` the catalogue.
pub(crate) fn handle_help(read: &args::Reading) {
    if read.output_format() == "json" {
        print!("{}", CATALOGUE);
    } else {
        render(&command_screen(&crate::FLZ));
    }
}

/// The line every command's list gives the `help` it answers.
const HELP_ROW: &str = "Show this help message.";
const WIDTH: usize = 80;
const INDENT: usize = 4;
/// The widest first column the second one is aligned past; a wider entry puts
/// its text on the line below.
const FIRST_MAX: usize = 32;

/// The operation `name` in the embedded catalogue.
fn operation(name: &str) -> Option<&'static Value> {
    static PARSED: OnceLock<Value> = OnceLock::new();
    let catalogue = PARSED
        .get_or_init(|| serde_json::from_str(CATALOGUE).expect("the embedded catalogue is JSON"));
    catalogue["operations"]
        .as_array()?
        .iter()
        .find(|op| op["name"] == name)
}

/// What a verb does, in one line: a table's title, the description of the
/// first operation it calls, or its own.
fn description(verb: &Verb) -> &'static str {
    match &verb.body {
        Body::Table(table) => table.title,
        Body::Run(_) => verb
            .calls
            .first()
            .and_then(|name| operation(name)?["doc"].as_str())
            .or(verb.about)
            .unwrap_or_default(),
    }
}

/// What one of `verb`'s options carries: its own text, which only an option
/// the CLI itself answers for has (`--namespace` is `default` when unwritten,
/// whatever an operation does with an empty one), or else the argument of the
/// same name of an operation the verb calls.
fn option_text(verb: &Verb, spec: &OptSpec) -> &'static str {
    let name = spec.long.trim_start_matches('-').replace('-', "_");
    spec.doc
        .or_else(|| {
            verb.calls
                .iter()
                .filter_map(|call| operation(call)?["args"].as_array())
                .flatten()
                .find(|arg| arg["name"] == name.as_str())
                .and_then(|arg| arg["doc"].as_str())
        })
        .unwrap_or_default()
}

/// What `--help` on one verb prints.
pub(crate) fn verb_screen(cmd: &Command, verb: &Verb) -> String {
    let rows: Vec<(String, &str)> = verb
        .options
        .iter()
        .chain(UNIVERSAL)
        .map(|spec| (spec.spelling(), option_text(verb, spec)))
        .collect();

    let screen = format!(
        "{} {} - {}\n\n{}\n\nOPTIONS:\n{}",
        cmd.path,
        verb.name,
        description(verb),
        args::usage_line(cmd.path, verb),
        columns(&rows)
    );
    with_notes(screen, verb.notes)
}

/// What `help` on a command prints, and what a bare invocation falls back to.
pub(crate) fn command_screen(cmd: &Command) -> String {
    let mut rows: Vec<(String, &str)> = cmd
        .verbs
        .iter()
        .map(|verb| (verb.name.to_string(), description(verb)))
        .collect();
    if !cmd.verbs.iter().any(|verb| verb.name == "help") {
        rows.push(("help".to_string(), HELP_ROW));
    }

    let screen = format!(
        "{} - {}\n\nUSAGE:\n    {} <COMMAND>\n\nCOMMANDS:\n{}",
        cmd.path,
        cmd.title,
        cmd.path,
        columns(&rows)
    );
    with_notes(screen, cmd.notes)
}

/// The table `docs/public/topics/cli/command_reference.md` carries for one
/// command: each verb's usage line and description. The page is checked
/// against it, so it is built only for that test.
#[cfg(test)]
pub(crate) fn reference(cmd: &Command) -> String {
    let mut out = String::from("| Command | What it does |\n| --- | --- |\n");
    for verb in cmd.verbs {
        if let Body::Run(_) = verb.body {
            let usage = args::synopsis(cmd.path, verb);
            out.push_str(&format!(
                "| `{}` | {} |\n",
                usage.replace('|', "\\|"),
                description(verb).replace('|', "\\|")
            ));
        }
    }
    out
}

fn with_notes(screen: String, notes: Option<Help>) -> String {
    match notes {
        Some(notes) => format!("{}\n\n{}", screen, notes),
        None => screen,
    }
}

/// A block of rows, each one's text starting two columns past the widest
/// first column that fits `FIRST_MAX`, and wrapped at `WIDTH`.
fn columns(rows: &[(String, &str)]) -> String {
    let widest = rows
        .iter()
        .map(|(first, _)| first.len())
        .filter(|width| *width <= FIRST_MAX)
        .max()
        .unwrap_or_default();
    let at = INDENT + widest + 2;

    let lines: Vec<String> = rows
        .iter()
        .map(|(first, text)| {
            let lead = format!("{:INDENT$}{}", "", first);
            if first.len() > widest {
                format!("{}\n{:at$}{}", lead, "", wrap(text, at))
            } else {
                format!("{:at$}{}", lead, wrap(text, at))
            }
        })
        .collect();
    lines.join("\n")
}

/// `text` in lines of at most `WIDTH` columns, starting at column `at` and
/// indented to it after the first.
fn wrap(text: &str, at: usize) -> String {
    let mut lines: Vec<String> = vec![String::new()];
    for word in text.split_whitespace() {
        let line = lines.last_mut().expect("one line at least");
        if !line.is_empty() && at + line.len() + 1 + word.len() > WIDTH {
            lines.push(word.to_string());
        } else {
            if !line.is_empty() {
                line.push(' ');
            }
            line.push_str(word);
        }
    }
    lines.join(&format!("\n{:at$}", ""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::{commands, leaf_rows};

    /// fails when: a row names an operation the embedded catalogue does not
    /// declare, so its screen describes nothing and its door call is refused.
    #[test]
    fn every_operation_a_row_calls_is_in_the_catalogue() {
        let unknown: Vec<String> = leaf_rows()
            .into_iter()
            .flat_map(|(cmd, verb)| verb.calls.iter().map(move |call| (cmd, verb, call)))
            .filter(|(_, _, call)| operation(call).is_none())
            .map(|(cmd, verb, call)| format!("{} {} calls {}", cmd.path, verb.name, call))
            .collect();

        assert!(unknown.is_empty(), "{}", unknown.join("\n"));
    }

    /// fails when: a verb row declares no positional, no option and no
    /// argument it sends, yet calls an operation that requires an argument.
    #[test]
    fn a_verb_that_declares_nothing_calls_nothing_that_needs_an_argument() {
        let undeclared: Vec<String> = leaf_rows()
            .into_iter()
            .filter(|(_, verb)| {
                verb.positionals.is_empty() && verb.options.is_empty() && verb.sends.is_empty()
            })
            .flat_map(|(cmd, verb)| verb.calls.iter().map(move |call| (cmd, verb, call)))
            .filter(|(_, _, call)| {
                operation(call)
                    .and_then(|op| op["args"].as_array())
                    .is_some_and(|args| args.iter().any(|arg| arg["required"] == true))
            })
            .map(|(cmd, verb, call)| format!("{} {} calls {}", cmd.path, verb.name, call))
            .collect();

        assert!(undeclared.is_empty(), "{}", undeclared.join("\n"));
    }

    /// fails when: a verb has no description, because it calls no operation
    /// and says nothing of its own, or one of its options has no text.
    #[test]
    fn every_verb_and_every_option_it_reads_is_described() {
        let mut silent: Vec<String> = Vec::new();

        for (cmd, verb) in leaf_rows() {
            if description(verb).is_empty() {
                silent.push(format!("{} {}", cmd.path, verb.name));
            }
            for spec in verb.options {
                if option_text(verb, spec).is_empty() {
                    silent.push(format!("{} {} {}", cmd.path, verb.name, spec.long));
                }
            }
        }
        silent.extend(
            UNIVERSAL
                .iter()
                .filter(|spec| spec.doc.is_none())
                .map(|spec| spec.long.to_string()),
        );

        assert!(
            silent.is_empty(),
            "described by nothing:\n{}",
            silent.join("\n")
        );
    }

    const PAGE: &str = "../../docs/public/topics/cli/command_reference.md";
    const END: &str = "<!-- flz-reference:end -->";

    fn begin(path: &str) -> String {
        format!("<!-- flz-reference:begin {} -->", path)
    }

    /// fails when: a command's block on the reference page is not its
    /// rendering, or the page lacks one for a command with a leaf verb. The
    /// failure prints the block to paste.
    #[test]
    fn the_command_reference_holds_every_command_as_rendered() {
        let page =
            std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(PAGE))
                .expect("the reference page");
        let mut wrong: Vec<String> = Vec::new();
        let mut rendered_blocks = 0;

        for cmd in commands() {
            let rendered = reference(cmd);
            if !rendered.contains("| `") {
                continue;
            }
            rendered_blocks += 1;
            let block = page
                .split_once(&format!("{}\n", begin(cmd.path)))
                .and_then(|(_, rest)| rest.split_once(END))
                .map(|(block, _)| block);
            if block != Some(rendered.as_str()) {
                wrong.push(format!("{}\n{}{}", begin(cmd.path), rendered, END));
            }
        }

        let on_page = page.matches("<!-- flz-reference:begin ").count();
        if on_page != rendered_blocks {
            wrong.push(format!(
                "the page holds {on_page} blocks, not {rendered_blocks}"
            ));
        }
        assert!(
            wrong.is_empty(),
            "{} blocks of {} differ from their rendering; paste:\n\n{}",
            wrong.len(),
            PAGE,
            wrong.join("\n\n")
        );
    }
}
