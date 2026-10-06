# project-templates Specification

## Purpose
A new project starts from a template the installed release carries: each SDK's tested
`examples/starter`, written into a folder or a tar stream by `flz template create`, and served
to an AI assistant by `flezha_new_project`.

## Requirements
### Requirement: Every SDK SHALL carry a tested `starter` example

Each of the seven SDK repositories SHALL hold `examples/starter`: a runnable project in the SDK's
current recommended form with one relay `starter` whose described entry channel `hit` takes
`{"page": string}`, is keyed by `page`, feeds the tasklet `count` and ends in the output `counts`; the
tasklet SHALL keep `total` in state, change it through a declared change, and emit
`{"page", "total"}`. It SHALL carry a test using the SDK's testing harness, with the replay check
where the SDK has one, run by the SDK's own suite, and a README with build, test and run commands.
It SHALL reference the SDK by package name and version only.

#### Scenario: The starter counts per page

- **WHEN** the starter's test sends `home`, `home`, `about`, `home`
- **THEN** the answers' totals are 1, 2, 1, 3

#### Scenario: Same in every SDK

- **WHEN** each SDK's suite runs
- **THEN** its starter test passes

### Requirement: The release SHALL carry the templates held to their sources

`mix flezha.ai_kit` SHALL copy every tracked file of each SDK's `examples/starter` but `.gitignore`
into `priv/ai_kit/templates/<language>/starter/` and write a manifest listing each example's
files, their executable bits, its `.gitignore` text, a one-line summary and the commands of its
README's "Run it" section. A file git does not track SHALL NOT be carried. `--check` SHALL fail naming
each copy that differs from its source, and each copy no source has.

#### Scenario: A changed starter without regeneration fails

- **WHEN** a file of `sdk/java/examples/starter` changes and the kit is not regenerated
- **THEN** `mix flezha.ai_kit --check` fails naming that file

#### Scenario: A stray file is not carried

- **WHEN** a file git does not track lies in `sdk/python/examples/starter`
- **THEN** `mix flezha.ai_kit --check` passes and no template holds it

### Requirement: `flz template list` SHALL list what the release carries

`flz template list` SHALL print each language with its examples and summaries from the installed
release's manifest, without reaching a controller, and with `--json` as data.

#### Scenario: No controller needed

- **WHEN** `flz template list` runs on a machine where no controller runs
- **THEN** it prints the seven languages with `starter` and exits 0

### Requirement: `flz template create` SHALL write a working project

`flz template create <language> <dir> [--example <name>] [--tar]` SHALL write the example's files
and its `.gitignore` into `<dir>` with the user's umask and the manifest's executable bits, then
print the README's run commands; it SHALL create `<dir>` and refuse one that exists with files
(`template.dir_not_empty`), refuse an unknown language or example (`template.unknown`, hint
`flz template list`), and never reach a controller. With `--tar` it SHALL write the same files as a
POSIX tar stream to stdout, touch no folder, and refuse when stdout is a terminal
(`usage.tar_to_terminal`, whose hint names the `| tar x` redirect).

#### Scenario: A created project builds and passes its test outside the repository

- **WHEN** `flz template create java my-app` runs in a scratch folder and the SDK is installed as
  the README says
- **THEN** `my-app` builds and its test passes, and running it against a local controller answers
  a `hit` with `{"page": ..., "total": 1}`

#### Scenario: Files from a container reach the host

- **WHEN** `docker exec <container> flz template create python my-app --tar | tar x` runs on the
  host
- **THEN** `my-app/` exists on the host owned by the host user, with the same files

#### Scenario: A non-empty folder is refused

- **WHEN** `flz template create java my-app` runs where `my-app/` holds a file
- **THEN** it exits by the error's class with `template.dir_not_empty` and writes nothing

#### Scenario: An unknown language

- **WHEN** `flz template create cobol my-app` runs
- **THEN** it answers `template.unknown` whose hint is `flz template list`

