# cli-command-surface Specification

## Purpose

The `flz` command line is an operator-facing surface: how an option may be spelled, what a
subcommand list must name, and that every screen is pinned by a capture. These rules were
enforced by habit and by three goldens, and stated in no specification — which is why an
option spelling and a subcommand list could each contradict nothing.
## Requirements
### Requirement: An option's four spellings SHALL be one option

Every `flz` option SHALL be read in all four of `--long=value`, `--long value`,
`-s=value` and `-s value`, and a short alias SHALL resolve to its long key in each of
them. A reader that recognises an option in one spelling and not another SHALL NOT
exist: an option the operator typed is either read or refused with the flag named,
never dropped in silence.

A bare word containing `=` and no leading `-` is a positional argument and SHALL NOT
be read as an option, except the plugin-setting form governed by the requirement
below.

#### Scenario: A short flag written with `=` reaches the request

- **WHEN** `flz plugin enable p prov -o=json` is run
- **THEN** the enable request SHALL carry the option `output` with the value `json`,
  identically to `--output=json`, `--output json` and `-o json`

#### Scenario: A positional `key=value` word is not an option

- **WHEN** an argument list contains a bare word such as `topic=t`
- **THEN** no option named `topic` SHALL be read from it

### Requirement: A subcommand list SHALL name every subcommand its command answers

A `flz` help screen carrying a `COMMANDS:` or `SUBCOMMANDS:` block SHALL list in that
block every subcommand the command answers, `help` included. A command that answers a
verb its own screen does not name leaves the operator to discover it elsewhere, which
is what a help screen exists to prevent.

A screen that documents whole invocations, or options rather than subcommands, carries
no such block and is not governed by this requirement.

#### Scenario: Every command table's screen offers `help`

- **WHEN** the help screens are enumerated
- **THEN** every screen carrying a `COMMANDS:` or `SUBCOMMANDS:` block SHALL list
  `help` among its rows

### Requirement: Every help screen SHALL be pinned byte for byte by a capture

Each screen `flz` prints SHALL be captured from the binary and compared byte for byte
by the test suite, so that any change to operator-visible text is deliberate and
visible in a diff. The screens are hand-aligned, so a renderer that computes a column
would silently change what an operator sees; the capture is what makes the text
editable at all.

The set of captures and the set of screens SHALL be equal, so that a screen cannot
stop being checked without the suite saying so.

#### Scenario: An edited screen fails until its capture is retaken

- **WHEN** a help screen's text changes and its capture does not
- **THEN** the suite SHALL fail, naming the screen and the first line that differs

#### Scenario: A screen without a capture fails

- **WHEN** a screen has no capture file, or a capture file names no screen
- **THEN** the suite SHALL fail

### Requirement: A plugin setting SHALL be read with or without its leading dashes

A repeatable plugin setting SHALL be read from `X<key>=<value>` as well as from
`--X<key>=<value>` and `--X<key> <value>`, on every surface that takes settings —
`flz plugin enable`, `flz namespace create` and `flz namespace set-quota`. The bare
spelling is what the CLI's own help screens and usage lines teach, and a setting an
operator typed that reaches no request is a plugin configured with nothing and told
nothing.

A word is a setting when, and only when, it is spelled `X<key>=<value>` with a
non-empty `<key>`. The `=` is what makes the bare spelling readable: with no leading
dashes there is nothing else to separate `Xhosts` from a positional word, so a bare
`X<key>` SHALL NOT consume the argument after it as its value, and a bare word whose
key does not begin with `X` SHALL remain a positional argument.

#### Scenario: The documented invocation carries its settings

- **WHEN** `flz plugin enable operative_kafka_producer my_provider Xhosts=kafka:9092 Xtopic=my_topic` is run
- **THEN** the enable request SHALL carry the settings `hosts` and `topic` with the
  values the operator typed, identically to the `--Xhosts=` / `--Xtopic=` spelling

#### Scenario: A bare setting does not eat the next word

- **WHEN** an argument list contains a bare `Xhosts` with no `=`
- **THEN** no setting SHALL be read from it, and the argument after it SHALL NOT be
  consumed as its value

### Requirement: Every option spelling the CLI prints SHALL be one it reads

A spelling the CLI prints SHALL be one the parser reads. Every option spelling that
appears in a `flz` help screen, or in a usage line the CLI prints, is read by the
parser that answers that command. The printed text is the interface's promise; a
spelling advertised and not read is an instruction that silently does nothing, which
is worse than one that is refused.

The plugin-setting family SHALL be checked mechanically: the test suite SHALL scan the
operator-visible text for every settings-shaped token it prints and assert the parser
reads each one back with the key and value the text shows, so that an example added to
a screen is covered by the check without anyone remembering to extend it.

#### Scenario: An advertised setting the parser cannot read fails the suite

- **WHEN** a help screen or usage line prints a settings-shaped token the parser does
  not read
- **THEN** the suite SHALL fail, naming the text it came from and the token

### Requirement: A verb whose effect the operator's words choose SHALL refuse a word it cannot act on

`flz boot await` and `flz reset` SHALL refuse an argument they do not recognise, naming the
argument on stderr, exiting non-zero and reaching no controller call. These two verbs are
the ones where guessing is worse than stopping: one blocks the operator for as long as its
option says, the other deletes state. A typed word neither can act on means the run about to
happen is not the run that was asked for.

Refusing an argument the verb **does not recognise** is a guarantee about these two verbs
and not about the whole command line: every other `flz` verb passes an unrecognised word on
and answers with its defaults. Refusing an option written with **no value** is a separate
guarantee and is given by every verb, under the requirement above; these two verbs are not
excused from it and SHALL NOT re-state it in a walk of their own.

The refusal SHALL survive any widening of how these verbs read their options — an option
read in more spellings must not be an argument list checked in fewer, because the silence
that replaces a refusal is the failure this surface's requirements exist to prevent.

#### Scenario: An argument the verb cannot act on stops the run

- **WHEN** `flz boot await --nope` is run
- **THEN** the run SHALL exit non-zero, name `--nope` on stderr, and reach no
  `CliManager` call

#### Scenario: An option written with no value stops the run

- **WHEN** `flz boot await --timeout` is run with nothing after it
- **THEN** the run SHALL exit non-zero saying the option requires a value, and SHALL NOT
  fall back to the default wait

### Requirement: An option written with no value SHALL be refused, never defaulted

Every `flz` option that takes a value SHALL be refused when the operator writes it with
nothing after it, naming the flag on stderr, exiting non-zero and reaching no controller
call. An option that is absent and an option written with no value are different requests:
the first asks for the default, the second asks for a value the operator did not supply.
A reader that cannot tell them apart answers the second with the first, in silence, and the
operator is told nothing — which is the failure "either read or refused with the flag named"
already forbids for a spelling and must equally forbid for a value.

The distinction SHALL be carried by the option scanner rather than re-derived by each
reader. A scanner that answers only *value* or *nothing* forces every caller to guess which
of the two it was handed, and a refusal written at each call site is the same sentence
copied once per option.

A value the operator wrote and left empty is a value that was supplied, not a value that is
missing: an option given an explicitly empty argument SHALL NOT be refused by this rule, and
the reader's own rule for an empty value applies.

#### Scenario: A trailing option is refused instead of taking the default

- **WHEN** `flz token issue p ns viewer --ttl` is run with nothing after `--ttl`
- **THEN** the run SHALL exit non-zero naming `--ttl` on stderr, and SHALL reach no
  `CliManager` call issuing a token with the default lifetime

#### Scenario: An option that is absent still takes its default

- **WHEN** `flz token issue p ns viewer` is run with no `--ttl` at all
- **THEN** the token SHALL be issued with the default lifetime, exactly as before

#### Scenario: A switch spelling may not short-circuit the refusal

- **WHEN** a command is given both the `--json` switch and a trailing `--output`
- **THEN** the run SHALL be refused naming `--output`, rather than answered with the
  format the switch asked for

#### Scenario: An explicitly empty value is not a missing value

- **WHEN** an option is written with an empty argument, such as `-iv ""`
- **THEN** the run SHALL NOT be refused, and the reader's own rule for an empty value
  applies

### Requirement: A numeric option SHALL be refused when its value is not a number

Every `flz` option whose value is a number SHALL refuse a value it cannot read, naming the
option and the value on stderr, exiting non-zero and reaching no controller call. Dropping
the value instead leaves the reader answering with its default, which is the request the
operator did not make: a `--limit` that will not parse becomes every record, a `--duration-ms`
that will not parse becomes the thirty-second default, and neither is distinguishable from an
option nobody typed.

Passing the unread value on to the controller is not an answer either. A value the command
line can see is wrong is refused where the option's name is still known, rather than sent on
as a string for something further away to reject in words that name neither the option nor
the operator's spelling of it.

The parse SHALL be carried by the option scanner rather than re-derived by each reader, for
the reason the neighbouring requirement gives: the refusal names the option, and the option's
name is what the scanner has and the caller does not.

A range rule is the option's own and is not this rule. An option that additionally requires
its number to be positive, or to fall in a band, SHALL refuse out-of-range values in its own
words; only "this is not a number" is shared.

#### Scenario: A non-numeric value stops the run instead of taking the default

- **WHEN** `flz audit read f --limit abc` is run
- **THEN** the run SHALL exit non-zero naming `--limit` and the value `abc` on stderr, and
  SHALL reach no `CliManager` call reading the log

#### Scenario: A non-numeric value is not passed on to the controller

- **WHEN** `flz operative scale g 2 --timeout abc` is run
- **THEN** the run SHALL be refused, and no request carrying `abc` as a timeout SHALL be
  built

#### Scenario: A numeric value is read exactly as before

- **WHEN** `flz audit read f --limit 5` is run
- **THEN** the read request SHALL carry the limit `5`

#### Scenario: An absent numeric option still takes its default

- **WHEN** `flz audit read f` is run with no `--limit` at all
- **THEN** the read request SHALL carry no limit, exactly as before

### Requirement: One invocation SHALL build one request

The same `flz` invocation SHALL build the same controller request every time it is run.
Where a verb renders its options as a JSON object, the keys SHALL be emitted in a fixed
order rather than one the process happens to choose.

An output that differs between two runs of one binary cannot be diffed against a run of
another, so it makes every comparison of two builds cost a proof that the difference was
not the change under test.

#### Scenario: Repeated identical invocations produce identical requests

- **WHEN** `flz operative start op fac --snapshot-interval 5 --partitions 4` is run
  repeatedly against the same binary
- **THEN** every run SHALL produce the same `CliManager` expression, byte for byte

