//! One dispatcher for every command table, `flz`'s own included, and the row
//! every verb is declared in.
//!
//! Sixteen verbs each hand-wrote the same three decisions — what a bare
//! invocation says, which spellings print the help screen, and how an
//! unrecognised subcommand is refused. They are fields of a table now, so the
//! decisions are made once and the wording stays each command's own.
//!
//! A verb's grammar is a field of the same row: what it takes positionally,
//! which options it reads, whether it refuses a word it does not recognise.
//! `args::read` is the only thing that reads the caller's words, and a
//! handler is given its answer and nothing else — there is no argument list to
//! fall back on, so a verb cannot read a word its row does not declare.

use flz_transport::errors::FlezhaError;
use serde_json::Value;

use crate::args::{self, Open, OptSpec, Positional, Reading};
use crate::help::{self, Help};
use crate::rpc;

/// What a verb does with the words the walk read for it.
pub(crate) enum Body {
    /// A leaf verb: it runs with its reading, and with nothing else.
    Run(fn(&Reading)),
    /// A branch: the words after this one belong to another table.
    Table(&'static Command),
}

/// One verb: what the caller may type, and what runs when they do. Every
/// reader, refusal, arity check and usage line for the verb is derived from
/// this row and written beside it nowhere.
pub(crate) struct Verb {
    pub(crate) name: &'static str,
    pub(crate) body: Body,
    pub(crate) positionals: &'static [Positional],
    pub(crate) options: &'static [OptSpec],
    /// Whether a word this row does not declare stops the run.
    pub(crate) strict: bool,
    pub(crate) open: Open,
    /// Pairs of options this verb refuses to be given together.
    pub(crate) conflicts: &'static [(&'static str, &'static str)],
    /// The screen this verb's own `--help` prints; `None` prints one rendered
    /// from this row and the verb's row in its command's screen.
    pub(crate) help: Option<Help>,
}

impl Verb {
    pub(crate) const fn new(name: &'static str, run: fn(&Reading)) -> Self {
        Verb {
            name,
            body: Body::Run(run),
            positionals: &[],
            options: &[],
            strict: false,
            open: Open::Closed,
            conflicts: &[],
            help: None,
        }
    }

    /// A verb that is itself a table: `flz relay` before `stop`.
    pub(crate) const fn table(name: &'static str, table: &'static Command) -> Self {
        Verb {
            name,
            body: Body::Table(table),
            positionals: &[],
            options: &[],
            strict: false,
            open: Open::Closed,
            conflicts: &[],
            help: None,
        }
    }

    pub(crate) const fn taking(mut self, positionals: &'static [Positional]) -> Self {
        self.positionals = positionals;
        self
    }

    pub(crate) const fn reading(mut self, options: &'static [OptSpec]) -> Self {
        self.options = options;
        self
    }

    /// Refuse a word this row does not declare: an option it does not read, or
    /// a plain word no positional takes. The verbs that do are those a misread
    /// word costs more than a refusal: one that blocks the caller for as long
    /// as its options say, deletes state, starts a relay without the setting
    /// meant, serves a program nobody watches (`flz mcp`), writes a folder
    /// where a tar stream was asked for (`flz template create`), or reads an
    /// option's value as the relay whose parked entries to touch (`flz dlq`).
    pub(crate) const fn strict(mut self) -> Self {
        self.strict = true;
        self
    }

    /// This verb carries a settings document whose keys it does not own.
    pub(crate) const fn open(mut self) -> Self {
        self.open = Open::Settings;
        self
    }

    /// Options this verb refuses to be given together, because each one
    /// answers the other's question differently.
    pub(crate) const fn refusing(
        mut self,
        conflicts: &'static [(&'static str, &'static str)],
    ) -> Self {
        self.conflicts = conflicts;
        self
    }

    pub(crate) const fn screen(mut self, help: Help) -> Self {
        self.help = Some(help);
        self
    }
}

pub(crate) struct Command {
    /// What a usage line calls this command — `flz relay`.
    pub(crate) path: &'static str,
    /// The screen `help` prints, and the one a bare invocation falls back to.
    pub(crate) help: Help,
    pub(crate) verbs: &'static [Verb],
    /// What a bare `flz <command>` answers.
    pub(crate) missing: Missing,
    /// Whether the refusal prints the screen as well. Two commands do.
    pub(crate) unknown_prints_help: bool,
}

/// What a bare `flz <command>` answers: a refusal naming the command's own
/// help, or the screen itself. The refusal is rendered from the command's path
/// rather than written out per table, which is what kept three wordings of one
/// sentence alive across seven tables.
pub(crate) enum Missing {
    Refuse,
    Screen,
}

/// The spellings that print a command's screen, on every command.
const HELP_VERBS: &[&str] = &["help", "--help", "-h"];

/// What a command says about a word that names none of its verbs. Built from
/// the table, so a verb added to it is offered by name without a sentence
/// being edited anywhere.
fn unrecognised(cmd: &'static Command, word: &str) -> FlezhaError {
    let mut names: Vec<&str> = cmd.verbs.iter().map(|verb| verb.name).collect();
    if !names.contains(&"help") {
        names.push("help");
    }

    FlezhaError::with(
        "usage.unknown_command",
        [
            ("command", cmd.path.into()),
            ("word", word.into()),
            ("allowed", Value::from(names)),
        ],
    )
}

/// One verb: read the caller's words against its row, answer a request for
/// the screen, and otherwise run it.
fn leaf(cmd: &'static Command, verb: &'static Verb, handler: fn(&Reading), argv: &[String]) {
    let reading = args::or_refuse(args::read(cmd.path, verb, argv));

    if reading.help_asked() {
        return help::render(&verb_help(cmd, verb));
    }

    handler(&reading)
}

/// What a verb's `--help` prints: its own screen, or else its usage line, the
/// row its command's screen gives it and every option it reads, each taken
/// from the row or the screen so that neither can drift from what it prints.
fn verb_help(cmd: &Command, verb: &Verb) -> String {
    if let Some(own) = verb.help {
        return own.to_string();
    }

    let mut out = args::usage_line(cmd.path, verb);
    if let Some(row) = screen_row(cmd.help, cmd.path, verb.name) {
        out.push_str("\n\n");
        out.push_str(&row);
    }
    out.push_str("\n\nOPTIONS:");
    for spec in verb.options.iter().chain(args::UNIVERSAL) {
        out.push_str("\n    ");
        out.push_str(&spec.spelling());
    }
    out
}

/// The row a command's screen gives one verb in its list of verbs, with the
/// lines that continue it. A list's rows share its first line's indent, and a
/// row may spell the command's path before the verb, as `flz dlq`'s do.
fn screen_row(screen: &str, path: &str, name: &str) -> Option<String> {
    let block: Vec<&str> = screen
        .lines()
        .skip_while(|l| !matches!(l.trim_end(), "COMMANDS:" | "SUBCOMMANDS:" | "Usage:"))
        .skip(1)
        .take_while(|l| !l.trim().is_empty())
        .collect();
    let indent = |l: &str| l.len() - l.trim_start().len();
    let row_indent = indent(block.first()?);
    let names = |l: &str| {
        let l = l.trim_start();
        let words = l.strip_prefix(path).filter(|rest| rest.starts_with(' '));
        words.unwrap_or(l).split_whitespace().next() == Some(name)
    };

    let start = block
        .iter()
        .position(|l| indent(l) == row_indent && names(l))?;
    let continued = block[start + 1..]
        .iter()
        .take_while(|l| indent(l) > row_indent);
    let lines = std::iter::once(&block[start]).chain(continued);
    let lines: Vec<String> = lines.map(|l| format!("    {}", &l[row_indent..])).collect();
    Some(lines.join("\n"))
}

pub(crate) fn run(cmd: &'static Command, args: &[String]) {
    let Some(word) = args.first() else {
        if let Missing::Screen = cmd.missing {
            help::render(cmd.help);
        }
        rpc::fail(FlezhaError::with(
            "usage.command_required",
            [("command", cmd.path.into())],
        ));
    };

    // The table wins over the help spellings, so a command that implements
    // `help` itself keeps it.
    if let Some(verb) = cmd.verbs.iter().find(|verb| verb.name == word) {
        return match &verb.body {
            Body::Table(table) => run(table, &args[1..]),
            Body::Run(handler) => leaf(cmd, verb, *handler, &args[1..]),
        };
    }

    if HELP_VERBS.contains(&word.as_str()) {
        // `flz relay help stop` asks what `--help` on `stop` answers.
        let named = args
            .get(1)
            .and_then(|name| cmd.verbs.iter().find(|v| v.name == name.as_str()));
        return match named {
            Some(verb) if matches!(verb.body, Body::Run(_)) => help::render(&verb_help(cmd, verb)),
            _ => help::render(cmd.help),
        };
    }

    if cmd.unknown_prints_help {
        help::render(cmd.help);
    }
    rpc::fail(unrecognised(cmd, word));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::{owned, Kind};

    /// Every leaf verb in the tree, with the path its usage line names.
    fn leaves() -> Vec<(&'static str, &'static Verb)> {
        fn walk(cmd: &'static Command, out: &mut Vec<(&'static str, &'static Verb)>) {
            for verb in cmd.verbs {
                match &verb.body {
                    Body::Table(table) => walk(table, out),
                    Body::Run(_) => out.push((cmd.path, verb)),
                }
            }
        }

        let mut out = Vec::new();
        walk(&crate::FLZ, &mut out);
        out
    }

    /// Everything this verb needs before a case about one option can be about
    /// that option: a word for every positional, and every required option but
    /// the one under test.
    fn base_argv(verb: &Verb, under_test: &str) -> Vec<String> {
        let mut argv: Vec<String> = verb
            .positionals
            .iter()
            .map(|slot| match &slot.kind {
                Kind::Number => "1".to_string(),
                Kind::Choice { words, .. } => words[0].to_string(),
                _ => "x".to_string(),
            })
            .collect();

        for spec in verb.options.iter().filter(|spec| spec.required) {
            if spec.long != under_test {
                argv.push(spec.long.to_string());
                argv.push(value_for(spec).to_string());
            }
        }

        argv
    }

    /// A value this option will accept, carrying the three characters an
    /// argument list has historically lost: a quote, a backslash and a
    /// non-ASCII letter.
    fn value_for(spec: &crate::args::OptSpec) -> &'static str {
        match &spec.kind {
            Kind::Number => "7",
            // The word reading, so a value the walk parses as a number is not
            // what proves the option is read by name.
            Kind::NumberOr { words } => words[words.len() - 1],
            Kind::Choice { words, .. } => words[words.len() - 1],
            _ => "a\"b\\c\u{00e4}",
        }
    }

    /// fails when: an option is spelled with an underscore or anything but
    /// dashed lowercase words, or carries a short spelling that is not one dash
    /// and one letter. `-si` reads as `-s -i`, and one `--init_values` among
    /// dashed options is the spelling a caller has to remember as the
    /// exception.
    #[test]
    fn every_option_is_kebab_case_with_at_most_a_one_letter_short_spelling() {
        fn kebab(long: &str) -> bool {
            long.strip_prefix("--").is_some_and(|words| {
                words.split('-').all(|word| {
                    !word.is_empty()
                        && word
                            .chars()
                            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
                })
            })
        }

        fn one_letter(short: &str) -> bool {
            let mut chars = short.chars();
            chars.next() == Some('-')
                && chars.next().is_some_and(|c| c.is_ascii_alphabetic())
                && chars.next().is_none()
        }

        let universal = crate::args::UNIVERSAL.iter().map(|spec| ("flz", "*", spec));
        let declared = leaves()
            .into_iter()
            .flat_map(|(path, verb)| verb.options.iter().map(move |spec| (path, verb.name, spec)));

        let wrong: Vec<String> = universal
            .chain(declared)
            .filter(|(_, _, spec)| {
                !kebab(spec.long) || (spec.short != spec.long && !one_letter(spec.short))
            })
            .map(|(path, verb, spec)| format!("{} {}: {} {}", path, verb, spec.long, spec.short))
            .collect();

        assert!(
            wrong.is_empty(),
            "{} options are spelled outside the rule:\n{}",
            wrong.len(),
            wrong.join("\n")
        );
    }

    /// fails when: a verb reads an option by position rather than by name, or
    /// reads only some of the four spellings it accepts elsewhere. Every row,
    /// every declared option, written BEFORE the positionals as well as after.
    #[test]
    fn every_declared_option_reads_in_four_spellings_wherever_it_is_written() {
        let mut wrong: Vec<String> = Vec::new();

        for (path, verb) in leaves() {
            for spec in verb.options {
                let want = value_for(spec);
                let switch = matches!(spec.kind, Kind::Switch);

                let mut spellings: Vec<Vec<String>> = vec![vec![spec.long.to_string()]];
                if !switch {
                    spellings = vec![
                        vec![spec.long.to_string(), want.to_string()],
                        vec![format!("{}={}", spec.long, want)],
                        vec![spec.short.to_string(), want.to_string()],
                        vec![format!("{}={}", spec.short, want)],
                    ];
                }

                for written in spellings {
                    for before in [true, false] {
                        let mut argv = base_argv(verb, spec.long);
                        if before {
                            argv.splice(0..0, written.iter().cloned());
                        } else {
                            argv.extend(written.iter().cloned());
                        }

                        let where_ = if before { "before" } else { "after" };
                        match crate::args::read(
                            path,
                            verb,
                            &owned(&argv.iter().map(String::as_str).collect::<Vec<_>>()),
                        ) {
                            Err(refusal) => wrong.push(format!(
                                "{} {}: {:?} {} the positionals -> {}",
                                path, verb.name, written, where_, refusal
                            )),
                            Ok(read) => {
                                let got = read.opt(spec.long);
                                let expected = if switch { Some("true") } else { Some(want) };
                                if got != expected {
                                    wrong.push(format!(
                                        "{} {}: {:?} {} the positionals -> {:?}, want {:?}",
                                        path, verb.name, written, where_, got, expected
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        assert!(
            wrong.is_empty(),
            "{} readings answered the wrong thing:\n{}",
            wrong.len(),
            wrong.join("\n")
        );
    }

    /// The words a case adds to this verb's own, read. Named when the walk
    /// accepted them, and when `saying` is given and the refusal does not say
    /// it, so a caller collecting wrong answers collects nothing.
    fn refusal_for(
        path: &'static str,
        verb: &'static Verb,
        spec: &crate::args::OptSpec,
        case: &[String],
        saying: Option<String>,
    ) -> Option<String> {
        let mut argv = base_argv(verb, spec.long);
        argv.extend(case.iter().cloned());
        let read = crate::args::read(
            path,
            verb,
            &owned(&argv.iter().map(String::as_str).collect::<Vec<_>>()),
        );

        match (read, saying) {
            (Ok(_), _) => Some(format!("{} {}: {:?} was accepted", path, verb.name, case)),
            (Err(refusal), Some(want)) if !refusal.message.contains(&want) => Some(format!(
                "{} {}: {:?} -> {:?}, want a refusal saying {:?}",
                path, verb.name, case, refusal, want
            )),
            (Err(_), _) => None,
        }
    }

    /// fails when: a verb accepts an option written with nothing after it, or
    /// a number it cannot read, or a word outside a closed set, or a switch
    /// written with a value, which read as present whatever was typed after the
    /// `=` and so answered the opposite of the request (KB-258).
    #[test]
    fn every_declared_option_refuses_what_it_cannot_carry() {
        let mut wrong: Vec<String> = Vec::new();

        for (path, verb) in leaves() {
            for spec in verb.options {
                if matches!(spec.kind, Kind::Switch) {
                    // The refusal a caller reads is pinned here rather than
                    // per verb: one sentence, every switch in the tree.
                    wrong.extend(refusal_for(
                        path,
                        verb,
                        spec,
                        &[format!("{}=no", spec.long)],
                        Some(format!("{} takes no value", spec.long)),
                    ));
                    continue;
                }

                let mut cases: Vec<Vec<String>> = vec![vec![spec.long.to_string()]];
                if matches!(spec.kind, Kind::Number) {
                    cases.push(vec![spec.long.to_string(), "abc".to_string()]);
                }
                if matches!(spec.kind, Kind::Choice { .. }) {
                    cases.push(vec![spec.long.to_string(), "nope".to_string()]);
                }

                for case in cases {
                    wrong.extend(refusal_for(path, verb, spec, &case, None));
                }
            }
        }

        assert!(
            wrong.is_empty(),
            "{} unreadable values were answered wrongly:\n{}",
            wrong.len(),
            wrong.join("\n")
        );
    }

    /// fails when: a verb refuses a word it does not declare although its row
    /// does not say it does — or accepts one although the row says it does.
    #[test]
    fn only_the_rows_that_declare_strictness_refuse_an_unknown_word() {
        let mut wrong: Vec<String> = Vec::new();

        for (path, verb) in leaves() {
            let mut argv = base_argv(verb, "");
            argv.push("--nope".to_string());

            let refused = crate::args::read(
                path,
                verb,
                &owned(&argv.iter().map(String::as_str).collect::<Vec<_>>()),
            )
            .is_err();

            if refused != verb.strict {
                wrong.push(format!(
                    "{} {}: strict={} but --nope was {}",
                    path,
                    verb.name,
                    verb.strict,
                    if refused { "refused" } else { "accepted" }
                ));
            }
        }

        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }

    /// The option spellings a block of help text teaches. The settings
    /// families are not among them: `--X<key>`, `--hook` and its
    /// `--NAME.key` sub-options carry keys a provider owns, and a row declares
    /// that its verb takes settings rather than which keys exist.
    fn advertised_options(text: &str) -> Vec<&str> {
        text.split(|c: char| c.is_whitespace() || c == ',' || c == '|' || c == ']')
            .map(|word| word.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '-'))
            .map(|word| word.split('=').next().unwrap_or(word))
            .filter(|word| word.starts_with("--") && word.len() > 2)
            .filter(|word| {
                // `--<key>` is the shape a settings document is taught in, not
                // a spelling anyone types, and `settings::SETTING_KEYS` names
                // the plain words that document owns rather than a row.
                !word.starts_with("--X")
                    && !word.contains('.')
                    && !word.contains('<')
                    && !crate::settings::SETTING_KEYS.contains(word)
            })
            .collect()
    }

    /// Every option any verb under `cmd` declares, the universal two included:
    /// what a screen for that command may teach.
    fn declared_under(cmd: &'static Command) -> Vec<&'static str> {
        let mut out: Vec<&'static str> = crate::args::UNIVERSAL
            .iter()
            .flat_map(|spec| [spec.long, spec.short])
            .collect();

        for verb in cmd.verbs {
            match &verb.body {
                Body::Table(table) => out.extend(declared_under(table)),
                Body::Run(_) => {
                    out.extend(verb.options.iter().flat_map(|spec| [spec.long, spec.short]))
                }
            }
        }

        out
    }

    /// fails when: an error's hint names a `flz` command no row defines, so
    /// the next step it gives cannot be typed.
    #[test]
    fn every_hint_that_names_flz_names_a_verb_it_has() {
        let verbs: Vec<String> = leaves()
            .iter()
            .map(|(path, verb)| format!("{} {}", path, verb.name))
            .collect();
        let named = |at: &str| {
            verbs
                .iter()
                .any(|v| at == v || at.starts_with(&format!("{v} ")))
        };

        let stale: Vec<String> = flz_transport::errors::codes()
            .filter_map(|code| Some((code, flz_transport::errors::entry(code)?.hint.as_deref()?)))
            .filter(|(_code, hint)| {
                hint.match_indices("flz ")
                    .any(|(at, _)| !named(&hint[at..]))
            })
            .map(|(code, hint)| format!("{code}: {hint}"))
            .collect();

        assert!(stale.is_empty(), "hints naming no flz verb: {stale:#?}");
    }

    /// Every command in the tree, `flz` itself included.
    fn commands() -> Vec<&'static Command> {
        fn walk(cmd: &'static Command, out: &mut Vec<&'static Command>) {
            out.push(cmd);
            for verb in cmd.verbs {
                if let Body::Table(table) = &verb.body {
                    walk(table, out);
                }
            }
        }

        let mut out = Vec::new();
        walk(&crate::FLZ, &mut out);
        out
    }

    /// fails when: a screen or a usage line prints an option spelling no row
    /// declares, so a caller types a word the verb will not read. The
    /// screens stay hand-aligned bytes; this checks them against the
    /// declaration rather than generating them from it.
    #[test]
    fn every_option_a_screen_prints_is_declared_by_a_row() {
        let mut undeclared: Vec<String> = Vec::new();

        for cmd in commands() {
            let declared = declared_under(cmd);
            for token in advertised_options(cmd.help) {
                if !declared.contains(&token) {
                    undeclared.push(format!("{} screen prints undeclared {}", cmd.path, token));
                }
            }
        }

        for (path, verb) in leaves() {
            let declared: Vec<&str> = verb
                .options
                .iter()
                .chain(crate::args::UNIVERSAL.iter())
                .flat_map(|spec| [spec.long, spec.short])
                .collect();

            let mut texts = vec![crate::args::usage_line(path, verb)];
            if let Some(own) = verb.help {
                texts.push(own.to_string());
            }

            for text in texts {
                for token in advertised_options(&text) {
                    if !declared.contains(&token) {
                        undeclared.push(format!(
                            "{} {} prints undeclared {}",
                            path, verb.name, token
                        ));
                    }
                }
            }
        }

        undeclared.sort();
        undeclared.dedup();
        assert!(
            undeclared.is_empty(),
            "{} printed option spellings are declared by no row:\n{}",
            undeclared.len(),
            undeclared.join("\n")
        );
    }

    /// fails when: a verb declares an option no screen teaches, so the only
    /// way to find it is to read the source. The screen a verb is taught in is
    /// its own when it has one, else its command's.
    #[test]
    fn every_option_a_row_declares_appears_in_its_screen() {
        let mut untaught: Vec<String> = Vec::new();

        for cmd in commands() {
            for verb in cmd.verbs {
                let Body::Run(_) = &verb.body else { continue };
                let taught = format!("{}\n{}", verb_help(cmd, verb), cmd.help);

                for spec in verb.options {
                    if !taught.contains(spec.long) {
                        untaught.push(format!(
                            "{} {} declares {}, no screen teaches it",
                            cmd.path, verb.name, spec.long
                        ));
                    }
                }
            }
        }

        assert!(
            untaught.is_empty(),
            "{} declared options appear in no screen:\n{}",
            untaught.len(),
            untaught.join("\n")
        );
    }

    /// fails when: a verb with no screen of its own is helped with its
    /// command's whole screen, or its command's list of verbs gives it no row
    /// for its help to carry.
    #[test]
    fn a_verbs_help_is_its_usage_line_and_its_row() {
        let mut wrong: Vec<String> = Vec::new();

        for cmd in commands() {
            for verb in cmd.verbs.iter().filter(|verb| verb.help.is_none()) {
                let Body::Run(_) = &verb.body else { continue };
                let text = verb_help(cmd, verb);
                let row = screen_row(cmd.help, cmd.path, verb.name);

                if !text.starts_with(&crate::args::usage_line(cmd.path, verb))
                    || text.contains("COMMANDS:")
                    || !row.is_some_and(|row| text.contains(&row))
                {
                    wrong.push(format!("{} {}:\n{}", cmd.path, verb.name, text));
                }
            }
        }

        assert!(wrong.is_empty(), "{}", wrong.join("\n\n"));
    }

    /// fails when: a verb's usage line stops naming what the verb reads — the
    /// line is rendered from the row, so this pins that it says every required
    /// positional and every option.
    #[test]
    fn a_usage_line_names_every_positional_and_option_its_row_declares() {
        let mut missing: Vec<String> = Vec::new();

        for (path, verb) in leaves() {
            let line = crate::args::usage_line(path, verb);

            for slot in verb.positionals {
                if !line.contains(slot.name) {
                    missing.push(format!("{} {}: {}", path, verb.name, slot.name));
                }
            }
            for spec in verb.options {
                if !line.contains(spec.long) {
                    missing.push(format!("{} {}: {}", path, verb.name, spec.long));
                }
            }
        }

        assert!(missing.is_empty(), "{}", missing.join("\n"));
    }
}
