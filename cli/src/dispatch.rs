//! One dispatcher for every command table, `flz`'s own included.
//!
//! Sixteen verbs each hand-wrote the same three decisions — what a bare
//! invocation says, which spellings print the help screen, and how an
//! unrecognised subcommand is refused. They are fields of a table now, so the
//! decisions are made once and the wording stays each command's own.

use std::process::exit;

use crate::help::{self, Help};

/// A subcommand handler. It is given the whole argument slice, its own name
/// included, because that is what the majority of the tree already expects.
pub(crate) type Handler = fn(&[String]);

pub(crate) struct Command {
    /// The screen `help` prints, and the one a bare invocation falls back to.
    pub(crate) help: Help,
    pub(crate) verbs: &'static [(&'static str, Handler)],
    /// The spellings that print the screen. `flz plugin` carries none: `help`
    /// is one of its own verbs, and it refuses `--help` today.
    pub(crate) help_verbs: &'static [&'static str],
    /// stderr for a bare `flz <command>`; `None` prints the screen instead.
    pub(crate) missing: Option<&'static str>,
    /// stderr for an unrecognised subcommand. The one `{}` takes the word.
    pub(crate) unknown: &'static str,
    /// Whether the refusal prints the screen as well. Two commands do.
    pub(crate) unknown_prints_help: bool,
}

pub(crate) const HELP_VERBS: &[&str] = &["help", "--help", "-h"];

/// The same three spellings minus the bare word, for a command where `help` is
/// a subcommand of its own.
pub(crate) const HELP_FLAGS: &[&str] = &["--help", "-h"];

/// A subcommand's own screen — `flz controller stop --help`. `true` means the
/// caller is done and must not run its RPC.
pub(crate) fn subcommand_help(args: &[String], screen: Help, spellings: &[&str]) -> bool {
    let asked = args.iter().any(|a| spellings.contains(&a.as_str()));
    if asked {
        help::render(screen);
    }
    asked
}

pub(crate) fn run(cmd: &Command, args: &[String]) {
    let Some(verb) = args.first() else {
        match cmd.missing {
            Some(line) => eprintln!("{}", line),
            None => help::render(cmd.help),
        }
        exit(1);
    };

    // The table wins over the help spellings, so a command that implements
    // `help` itself keeps it.
    if let Some((_, handler)) = cmd.verbs.iter().find(|(name, _)| name == verb) {
        return handler(args);
    }

    if cmd.help_verbs.contains(&verb.as_str()) {
        return help::render(cmd.help);
    }

    eprintln!("{}", cmd.unknown.replace("{}", verb));
    if cmd.unknown_prints_help {
        help::render(cmd.help);
    }
    exit(1);
}
