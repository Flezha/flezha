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
be read as an option.

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
