//! What a `flz` verb takes, declared — and the one walk that reads it.
//!
//! A row declares its positionals, its options (the long spelling, the short
//! alias, the kind of value, whether it is required, the knob that answers it)
//! and whether it refuses a word it does not recognise. `read` walks the
//! caller's words once against that row: every option is taken wherever it
//! stands, and the positionals are the words that remain. So the reader, the
//! refusal, the arity check and the usage line are all derived from the row,
//! and none of them is written beside a verb.
//!
//! The words a row declares it does NOT read — a provider's settings document
//! — are stepped over here and read in `settings.rs`.

use std::env;
use std::path::PathBuf;
use std::str::FromStr;

use flz_transport::errors::FlezhaError;
use serde_json::Value;

use crate::dispatch::Verb;
use crate::rpc;
use crate::settings::bare_setting;

/// How one word reads against one option: it carries its own value after an
/// `=`, it takes the word after it, or it is not this option at all.
pub(crate) enum Spelling<'a> {
    Attached(&'a str),
    Separated,
    Other,
}

/// One word against one option's four spellings — `--long=value`, `--long
/// value`, `-s=value`, `-s value` — named here and nowhere else. `extract_param`
/// walks this answer for the value; a reader that also refuses what it does not
/// recognise walks it to know which words the option owns.
pub(crate) fn resolve_spelling<'a>(arg: &'a str, long_key: &str, short_key: &str) -> Spelling<'a> {
    for key in [long_key, short_key] {
        if let Some(value) = arg.strip_prefix(&format!("{}=", key)) {
            return Spelling::Attached(value);
        }
    }

    if arg == long_key || arg == short_key {
        Spelling::Separated
    } else {
        Spelling::Other
    }
}

/// One number, one refusal, whichever way the caller spelled it. A value
/// standing in a positional slot is the same request as one behind a flag, and
/// a verb that reads only the flag answers one typo two ways; `key` is what the
/// verb's usage line calls the argument, so the refusal names what was typed.
pub(crate) fn read_number<T: FromStr>(key: &str, written: &str) -> Result<T, FlezhaError> {
    written.parse().map_err(|_| {
        FlezhaError::with(
            "usage.invalid_number",
            [("option", key.into()), ("value", written.into())],
        )
    })
}

/// A refusal the caller sees, and the exit that follows it, for the handlers
/// that answer `()` and cannot propagate one.
pub(crate) fn or_refuse<T>(read: Result<T, FlezhaError>) -> T {
    read.unwrap_or_else(|refusal| rpc::fail(refusal))
}

/// Whether the caller asked for JSON anywhere in `argv`, read before the walk
/// so that a refusal of the command line itself is written in the form asked
/// for.
pub(crate) fn asks_json(argv: &[String]) -> bool {
    argv.iter().enumerate().any(|(i, word)| {
        word == JSON_SWITCH.long
            || [OUTPUT.long, OUTPUT.short].iter().any(|key| {
                word == &format!("{key}=json")
                    || (word == key && argv.get(i + 1).map(String::as_str) == Some("json"))
            })
    })
}

// --- the declaration, and the one walk that reads it -----------------------

/// What an option's value is, and so how it is read and how it is refused.
pub(crate) enum Kind {
    /// Any word.
    Text,
    /// A word that must parse as a number.
    Number,
    /// No value: written, or not written.
    Switch,
    /// One of a closed set of words. `noun` is what the refusal calls the
    /// thing that was not recognised.
    Choice {
        noun: &'static str,
        words: &'static [&'static str],
    },
    /// A number, or one of a closed set of words that mean something a number
    /// cannot — `--ttl never` for an entry with no expiry. The parse is still
    /// the walk's, so a value that is neither is refused in the shared
    /// sentence; what the words mean is the handler's.
    NumberOr { words: &'static [&'static str] },
}

/// One option a verb declares: its two spellings, what its value is called,
/// whether the verb refuses to run without it, and the environment variable
/// that answers when the caller writes no flag.
pub(crate) struct OptSpec {
    pub(crate) long: &'static str,
    pub(crate) short: &'static str,
    pub(crate) kind: Kind,
    pub(crate) required: bool,
    /// What the usage line calls the value.
    value: &'static str,
    env: Option<&'static str>,
}

impl OptSpec {
    pub(crate) const fn text(long: &'static str, short: &'static str, value: &'static str) -> Self {
        OptSpec {
            long,
            short,
            kind: Kind::Text,
            required: false,
            value,
            env: None,
        }
    }

    pub(crate) const fn number(
        long: &'static str,
        short: &'static str,
        value: &'static str,
    ) -> Self {
        OptSpec {
            kind: Kind::Number,
            ..OptSpec::text(long, short, value)
        }
    }

    pub(crate) const fn switch(long: &'static str, short: &'static str) -> Self {
        OptSpec {
            kind: Kind::Switch,
            ..OptSpec::text(long, short, "")
        }
    }

    /// A number, or one of `words`. The usage line names both readings, so a
    /// help screen that teaches the option teaches the word too.
    pub(crate) const fn number_or(
        long: &'static str,
        short: &'static str,
        value: &'static str,
        words: &'static [&'static str],
    ) -> Self {
        OptSpec {
            kind: Kind::NumberOr { words },
            ..OptSpec::text(long, short, value)
        }
    }

    pub(crate) const fn choice(
        long: &'static str,
        short: &'static str,
        noun: &'static str,
        words: &'static [&'static str],
    ) -> Self {
        OptSpec {
            kind: Kind::Choice { noun, words },
            ..OptSpec::text(long, short, "")
        }
    }

    pub(crate) const fn required(mut self) -> Self {
        self.required = true;
        self
    }

    /// The knob that answers this option when the caller writes no flag —
    /// the one place in this crate that name is read.
    pub(crate) const fn answered_by(mut self, name: &'static str) -> Self {
        self.env = Some(name);
        self
    }

    /// How this option is written in a usage line, and so what a help screen
    /// that teaches it has to print.
    pub(crate) fn spelling(&self) -> String {
        let mut out = self.long.to_string();
        if self.short != self.long {
            out.push_str(" | ");
            out.push_str(self.short);
        }
        match &self.kind {
            Kind::Switch => {}
            Kind::Choice { words, .. } => {
                out.push(' ');
                out.push_str(&words.join("|"));
            }
            _ => {
                out.push(' ');
                out.push_str(self.value);
            }
        }
        out
    }
}

/// One positional argument a verb declares.
pub(crate) struct Positional {
    pub(crate) name: &'static str,
    pub(crate) kind: Kind,
    pub(crate) required: bool,
}

impl Positional {
    pub(crate) const fn required(name: &'static str) -> Self {
        Positional {
            name,
            kind: Kind::Text,
            required: true,
        }
    }

    pub(crate) const fn optional(name: &'static str) -> Self {
        Positional {
            required: false,
            ..Positional::required(name)
        }
    }

    /// A positional whose value must read as a number, refused here rather
    /// than at the verb that will use it.
    pub(crate) const fn number(name: &'static str) -> Self {
        Positional {
            kind: Kind::Number,
            ..Positional::required(name)
        }
    }
}

/// Whether a verb carries words it does not declare.
#[derive(PartialEq, Eq, Clone, Copy)]
pub(crate) enum Open {
    /// Every word is a declared option or a declared positional.
    Closed,
    /// A settings document rides with the declared words: `X<key>=<value>`
    /// provider settings, the `--hook NAME` families, and the plain
    /// `--key value` options the controller reads beside them. Those keys
    /// belong to the provider or the controller and not to this surface, so the
    /// walk steps over them and `Reading::rest` hands them on unread.
    Settings,
}

pub(crate) const TOKEN: OptSpec =
    OptSpec::text("--token", "--token", "<token>").answered_by("FLEZHA_TOKEN");
pub(crate) const HELP: OptSpec = OptSpec::switch("--help", "-h");

/// The options every verb carries, declared once so that no row repeats them
/// and no verb scans the argument list for them. The output format selects a
/// data answer's form where a verb has one, and a failure's form on every verb.
pub(crate) const UNIVERSAL: &[OptSpec] = &[TOKEN, HELP, OUTPUT, JSON_SWITCH];

/// The options more than one verb shares, spelled once each.
pub(crate) const NAMESPACE: OptSpec = OptSpec::text("--namespace", "-n", "<namespace>");
/// The document an operator starts from, on a start and on every firing of a
/// schedule.
pub(crate) const INIT_VALUES: OptSpec = OptSpec::text("--init-values", "--init-values", "<json>");
pub(crate) const OUTPUT: OptSpec =
    OptSpec::choice("--output", "-o", "output format", &["text", "json"]);
/// The switch spelling of `--output json`. `Reading::output_format` is where
/// the two meet, so they cannot mean different things on different verbs.
pub(crate) const JSON_SWITCH: OptSpec = OptSpec::switch("--json", "--json");
/// The window a bounded stream runs for, and how long it runs when the
/// caller names none. Two verbs tail; one option, one default.
pub(crate) const DURATION_MS: OptSpec =
    OptSpec::number("--duration-ms", "--duration-ms", "<milliseconds>");
pub(crate) const DEFAULT_WINDOW_MS: u64 = 30_000;
pub(crate) const APP_DATA_ROOT: OptSpec =
    OptSpec::text("--app-data-root", "--app-data-root", "<path>")
        .answered_by("FLEZHA_APP_DATA_ROOT");
/// The `app_data/` root when the caller named none. One spelling: two verbs
/// read this root, and a default written twice is a default that drifts.
pub(crate) const APP_DATA_ROOT_DEFAULT: &str = "./app_data";

/// The `app_data/` root this run was given, from the option, the variable it
/// is answered by, or the default.
pub(crate) fn app_data_root(read: &Reading) -> PathBuf {
    PathBuf::from(
        read.opt(APP_DATA_ROOT.long)
            .unwrap_or(APP_DATA_ROOT_DEFAULT),
    )
}

/// Every option a row declares, in declaration order, with what the caller
/// wrote for it.
type Slots = Vec<(&'static str, Option<String>)>;

/// The caller's words, read once against one verb's declaration: every
/// positional it declared, every option it declared, and the settings document
/// it declared it carries.
pub(crate) struct Reading {
    usage: String,
    positionals: Slots,
    options: Slots,
    rest: Vec<String>,
}

impl Reading {
    /// A positional the row declared required: the walk refused the run before
    /// this could be absent.
    pub(crate) fn pos(&self, name: &str) -> &str {
        self.maybe_pos(name).unwrap_or_default()
    }

    /// The positional in the slot at `index`, for the verbs that share a
    /// handler and name the same slot differently — `flz state list` calls it
    /// an operator class and `flz state inbox` an instance.
    pub(crate) fn positional_at(&self, index: usize) -> &str {
        self.positionals[index].1.as_deref().unwrap_or_default()
    }

    pub(crate) fn maybe_pos(&self, name: &str) -> Option<&str> {
        self.positionals
            .iter()
            .find(|(declared, _)| *declared == name)
            .unwrap_or_else(|| panic!("{} declares no positional '{}'", self.usage, name))
            .1
            .as_deref()
    }

    pub(crate) fn opt(&self, long: &str) -> Option<&str> {
        self.options
            .iter()
            .find(|(declared, _)| *declared == long)
            .unwrap_or_else(|| panic!("{} declares no option '{}'", self.usage, long))
            .1
            .as_deref()
    }

    /// A declared number. The walk refused an unreadable one, so this parse
    /// cannot fail.
    pub(crate) fn num<T: FromStr>(&self, long: &str) -> Option<T> {
        self.opt(long).and_then(|written| written.parse().ok())
    }

    pub(crate) fn switch(&self, long: &str) -> bool {
        self.opt(long).is_some()
    }

    /// The namespace every scoped verb carries, empty when none was written.
    pub(crate) fn namespace(&self) -> &str {
        self.opt(NAMESPACE.long).unwrap_or_default()
    }

    /// The rendered output format, whichever of the two spellings carried it.
    /// `--json` wins over an `--output` the walk has already had to accept.
    pub(crate) fn output_format(&self) -> &'static str {
        if self.switch(JSON_SWITCH.long) {
            return "json";
        }
        match self.opt(OUTPUT.long) {
            Some("json") => "json",
            _ => "text",
        }
    }

    /// A JSON-valued option, defaulting to the empty object.
    pub(crate) fn json(&self, long: &str) -> &str {
        match self.opt(long) {
            Some(value) if !value.is_empty() => value,
            _ => "{}",
        }
    }

    /// The words the row declared it does not read: a provider's settings and
    /// its own options document, in the order they were written.
    pub(crate) fn rest(&self) -> &[String] {
        &self.rest
    }

    /// The caller's identity, wherever it was written and whether it came from
    /// the flag or from the environment.
    pub(crate) fn token(&self) -> Option<&str> {
        self.opt(TOKEN.long).filter(|token| !token.is_empty())
    }

    pub(crate) fn help_asked(&self) -> bool {
        self.switch(HELP.long)
    }
}

/// The one refusal for a verb invoked with too few words, naming the row's
/// usage line so that it says what the verb actually reads.
fn missing_positional(usage: &str) -> FlezhaError {
    FlezhaError::with("usage.missing_argument", [("usage", usage.into())])
}

/// How many words an undeclared settings entry claims at `i`, or `None` when
/// the word there is a plain one. `extract_x_flags`, `extract_opts` and
/// `extract_extension_flags` read the entries back out of `Reading::rest`, so
/// the walk only has to know how far each one reaches.
fn open_entry(args: &[String], i: usize) -> Option<usize> {
    let arg = args[i].as_str();

    if bare_setting(arg).is_some() {
        return Some(1);
    }
    if !arg.starts_with('-') {
        return None;
    }
    if arg.contains('=') || i + 1 >= args.len() {
        return Some(1);
    }
    Some(2)
}

/// The usage line one verb's row renders — the only place this surface says
/// what a verb takes, so it cannot disagree with what the verb reads.
pub(crate) fn usage_line(path: &str, verb: &Verb) -> String {
    let mut out = format!("Usage: {} {}", path, verb.name);

    for slot in verb.positionals {
        out.push_str(&optional_unless(slot.required, slot.name.to_string()));
    }

    if verb.open == Open::Settings {
        out.push_str(" [X<key>=<value> ...] [--<key> <value> ...]");
    }

    for opt in verb.options {
        out.push_str(&optional_unless(opt.required, opt.spelling()));
    }

    out
}

/// One word of a usage line: bracketed when the verb runs without it.
fn optional_unless(required: bool, written: String) -> String {
    if required {
        format!(" {}", written)
    } else {
        format!(" [{}]", written)
    }
}

/// The one walk. `path` is what a usage line calls the command
/// (`flz operator`); `argv` is what the caller wrote after the verb's own
/// name. Options are read wherever they stand, the positionals are the words
/// that remain, and the refusals are the row's rather than each verb's own.
pub(crate) fn read(
    path: &str,
    verb: &'static Verb,
    argv: &[String],
) -> Result<Reading, FlezhaError> {
    let declared: Vec<&OptSpec> = verb.options.iter().chain(UNIVERSAL.iter()).collect();
    let usage = usage_line(path, verb);
    let mut options: Slots = declared.iter().map(|spec| (spec.long, None)).collect();

    let (plain, mut rest, asked_help) = scan(verb, &declared, argv, &mut options, &usage)?;

    // The screen answers before the grammar does: `flz observe enable --help`
    // asks what the verb takes, and refusing it for taking nothing would be
    // the one answer that cannot help.
    if asked_help || written(&options, HELP.long).is_some() {
        return Ok(Reading {
            usage,
            positionals: verb
                .positionals
                .iter()
                .map(|slot| (slot.name, None))
                .collect(),
            options: vec![(HELP.long, Some("true".to_string()))],
            rest,
        });
    }

    // The positionals are judged before the options are: a verb whose subject
    // will not read is refused for its subject, whatever else it was given.
    let (positionals, spare) = assign_positionals(verb, plain, &usage)?;
    rest.extend(spare);

    resolve_options(&declared, &mut options, &usage)?;
    refuse_conflicts(verb, &options)?;

    Ok(Reading {
        usage,
        positionals,
        options,
        rest,
    })
}

/// One pass over the caller's words: every declared option taken wherever it
/// stands, and what is left over — the plain words the positionals will be, the
/// settings the row does not read, and whether a screen was asked for.
fn scan(
    verb: &'static Verb,
    declared: &[&OptSpec],
    argv: &[String],
    options: &mut [(&'static str, Option<String>)],
    usage: &str,
) -> Result<(Vec<String>, Vec<String>, bool), FlezhaError> {
    let mut plain: Vec<String> = Vec::new();
    let mut rest: Vec<String> = Vec::new();
    let mut asked_help = false;
    let mut i = 0;

    while i < argv.len() {
        if let Some((long, value, width)) = claim(declared, argv, i)? {
            // First-wins: a repeated flag keeps the value written first.
            let slot = options
                .iter_mut()
                .find(|(declared, _)| *declared == long)
                .expect("a claimed option is a declared one");
            if slot.1.is_none() {
                slot.1 = Some(value);
            }
            i += width;
            continue;
        }

        // A verb with a screen of its own answers the bare word too: it is the
        // spelling `flz controller status help` has always used.
        if verb.help.is_some() && argv[i] == "help" {
            asked_help = true;
            i += 1;
            continue;
        }

        if verb.strict && argv[i].starts_with('-') {
            return Err(FlezhaError::with(
                "usage.unknown_option",
                [("option", argv[i].as_str().into()), ("usage", usage.into())],
            ));
        }
        if verb.strict && plain.len() == verb.positionals.len() {
            return Err(FlezhaError::with(
                "usage.unexpected_argument",
                [
                    ("argument", argv[i].as_str().into()),
                    ("usage", usage.into()),
                ],
            ));
        }

        i += spare(verb, argv, i, &mut plain, &mut rest);
    }

    Ok((plain, rest, asked_help))
}

/// A word no declared option claimed: a settings entry the row does not read,
/// a flag it does not know, or a positional. Answers how many words it took.
fn spare(
    verb: &'static Verb,
    argv: &[String],
    i: usize,
    plain: &mut Vec<String>,
    rest: &mut Vec<String>,
) -> usize {
    match open_entry(argv, i).filter(|_| verb.open == Open::Settings) {
        Some(width) => {
            rest.extend_from_slice(&argv[i..i + width]);
            width
        }
        None if argv[i].starts_with('-') => {
            rest.push(argv[i].clone());
            1
        }
        None => {
            plain.push(argv[i].clone());
            1
        }
    }
}

/// The plain words in the slots the row declared, each judged against the kind
/// its slot names. Answers the slots and the words no slot claimed.
fn assign_positionals(
    verb: &'static Verb,
    plain: Vec<String>,
    usage: &str,
) -> Result<(Slots, Vec<String>), FlezhaError> {
    let mut words = plain.into_iter();
    let mut positionals: Slots = Vec::new();

    for slot in verb.positionals {
        let value = words.next();
        match (&value, &slot.kind) {
            (None, _) if slot.required => return Err(missing_positional(usage)),
            (Some(value), Kind::Number) => {
                read_number::<u64>(slot.name, value)?;
            }
            (Some(value), Kind::NumberOr { words }) if !words.contains(&value.as_str()) => {
                read_number::<u64>(slot.name, value)?;
            }
            (Some(value), Kind::Choice { noun, words }) if !words.contains(&value.as_str()) => {
                return Err(unknown_word(noun, value, words))
            }
            _ => {}
        }
        positionals.push((slot.name, value));
    }

    Ok((positionals, words.collect()))
}

/// What each declared option finally carries: the flag's value, else the knob
/// it names, and the refusal for one the verb cannot run without.
fn resolve_options(
    declared: &[&OptSpec],
    options: &mut [(&'static str, Option<String>)],
    usage: &str,
) -> Result<(), FlezhaError> {
    for (spec, slot) in declared.iter().zip(options.iter_mut()) {
        // A knob answers the flag it shadows both when no flag was written and
        // when one was written empty: `--token ""` is not a credential.
        if let Some(name) = spec.env {
            if slot.1.as_deref().unwrap_or_default().is_empty() {
                slot.1 = env::var(name).ok().filter(|value| !value.is_empty());
            }
        }

        match slot.1.as_deref() {
            None | Some("") if spec.required => {
                return Err(FlezhaError::with(
                    "usage.option_required",
                    [("option", spec.spelling().into()), ("usage", usage.into())],
                ))
            }
            Some(value) if !value.is_empty() => check_value(spec, value)?,
            _ => {}
        }
    }

    Ok(())
}

/// The pairs a row declared it refuses together, because each answers the
/// other's question differently.
fn refuse_conflicts(
    verb: &'static Verb,
    options: &[(&'static str, Option<String>)],
) -> Result<(), FlezhaError> {
    for (one, other) in verb.conflicts {
        if written(options, one).is_some() && written(options, other).is_some() {
            return Err(FlezhaError::with(
                "usage.options_conflict",
                [("option", (*one).into()), ("other", (*other).into())],
            ));
        }
    }

    Ok(())
}

fn written<'a>(options: &'a [(&'static str, Option<String>)], long: &str) -> Option<&'a str> {
    options
        .iter()
        .find(|(declared, _)| *declared == long)
        .and_then(|(_, value)| value.as_deref())
}

/// The declared option the word at `i` writes, the value it carries and how
/// many words it took — or `None` when no declaration owns that word.
fn claim(
    declared: &[&OptSpec],
    argv: &[String],
    i: usize,
) -> Result<Option<(&'static str, String, usize)>, FlezhaError> {
    for spec in declared {
        let (written, width) = match resolve_spelling(&argv[i], spec.long, spec.short) {
            // A switch is written or it is not, so a value attached to one is
            // a word the verb cannot act on: reading it as present answers
            // `--all=no` by clearing every root.
            Spelling::Attached(_) if matches!(spec.kind, Kind::Switch) => {
                return Err(takes_no_value(spec))
            }
            Spelling::Attached(value) => (value.to_string(), 1),
            Spelling::Separated if matches!(spec.kind, Kind::Switch) => ("true".to_string(), 1),
            Spelling::Separated => match argv.get(i + 1) {
                Some(value) => (value.clone(), 2),
                // A trailing `--ttl` asks for a lifetime, not for the default.
                None => return Err(requires_a_value(spec)),
            },
            Spelling::Other => continue,
        };

        return Ok(Some((spec.long, written, width)));
    }

    Ok(None)
}

/// Whether a written value is one this option can carry. Judged after the
/// positionals are, so a verb whose subject will not read is refused for its
/// subject rather than for a flag beside it.
fn check_value(spec: &OptSpec, written: &str) -> Result<(), FlezhaError> {
    match &spec.kind {
        Kind::Number => read_number::<u64>(spec.long, written).map(|_: u64| ()),
        Kind::NumberOr { words } if !words.contains(&written) => {
            read_number::<u64>(spec.long, written).map(|_: u64| ())
        }
        Kind::Choice { noun, words } if !words.contains(&written) => {
            Err(unknown_word(noun, written, words))
        }
        _ => Ok(()),
    }
}

/// The one refusal for a word the declaration does not recognise, wherever it
/// was written: behind a flag or in a positional slot.
pub(crate) fn unknown_word(noun: &str, written: &str, words: &[&str]) -> FlezhaError {
    FlezhaError::with(
        "usage.unknown_word",
        [
            ("noun", noun.into()),
            ("value", written.into()),
            ("allowed", Value::from(words.to_vec())),
        ],
    )
}

fn requires_a_value(spec: &OptSpec) -> FlezhaError {
    FlezhaError::with(
        "usage.option_requires_value",
        [("option", spec.long.into())],
    )
}

fn takes_no_value(spec: &OptSpec) -> FlezhaError {
    FlezhaError::with(
        "usage.option_takes_no_value",
        [("option", spec.long.into())],
    )
}

/// An argument list as `env::args()` would produce it. Every test module in
/// this crate builds one.
#[cfg(test)]
pub(crate) fn owned(args: &[&str]) -> Vec<String> {
    args.iter().map(|s| s.to_string()).collect()
}
