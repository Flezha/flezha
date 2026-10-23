# project-templates Specification

## Purpose
A new project starts from a template: each SDK's tested `examples/starter`, published with
every release as one download per language and served to an AI assistant by
`flezha_new_project`.
## Requirements
### Requirement: Every SDK SHALL carry a tested `starter` example

Each SDK repository SHALL hold `examples/starter`: a runnable project in the SDK's
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
files, their executable bits, its `.gitignore` text and the commands of its README's "Run it"
section. A file git does not track SHALL NOT be carried. `--check` SHALL fail naming each copy
that differs from its source, and each copy no source has.

#### Scenario: A changed starter without regeneration fails

- **WHEN** a file of `sdk/java/examples/starter` changes and the kit is not regenerated
- **THEN** `mix flezha.ai_kit --check` fails naming that file

#### Scenario: A stray file is not carried

- **WHEN** a file git does not track lies in `sdk/python/examples/starter`
- **THEN** `mix flezha.ai_kit --check` passes and no template holds it

### Requirement: A release SHALL publish each starter as a download

`scripts/build-dist.sh --stage-release <dir>` SHALL write, for every template
`priv/ai_kit/templates/manifest.json` lists with the example `starter`, one archive
`flezha-<language>-starter.tar.gz` into `<dir>`, and SHALL list each in the release's SHA-256
manifest. An archive SHALL hold exactly the files the manifest lists for that template, plus
a `.gitignore` holding the manifest's text when it has one, all under the one folder
`flezha-<language>-starter/`, each file with mode `0755` when the manifest marks it executable
and `0644` otherwise. Its members SHALL be taken from the manifest, never from a listing of
the folder, and a file the manifest lists that the folder lacks SHALL refuse the staging,
naming it. The archives depend on no built asset, so they SHALL be written even when the
staging refuses for a missing one.

#### Scenario: One command gives a working project

- **WHEN** `flezha-java-starter.tar.gz` from a staged release is unpacked with `tar xz`
- **THEN** `flezha-java-starter/` holds every file and the `.gitignore` that `flezha_new_project`
  answers for `java`, with the same contents and the same executable bits

#### Scenario: Every language is staged

- **WHEN** `--stage-release` runs
- **THEN** the staging folder holds one starter archive per language the manifest lists, and
  the checksum manifest, when written, covers each of them

#### Scenario: A file the folder lacks refuses the staging

- **WHEN** the manifest lists a file that `priv/ai_kit/templates/python/starter/` does not hold
- **THEN** `--stage-release` fails naming that file, and writes no checksum manifest

#### Scenario: A stray file is not packed

- **WHEN** `priv/ai_kit/templates/go/starter/` holds a file the manifest does not list
- **THEN** `flezha-go-starter.tar.gz` does not hold it

