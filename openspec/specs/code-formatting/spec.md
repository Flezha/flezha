# code-formatting Specification

## Purpose

The code-formatting capability defines a project-wide source-shape contract across all three authored languages: `rustfmt.toml` is the single source of truth for Rust style, `.formatter.exs` for Elixir, and Spotless (palantir-java-format) configured under `sdk/java/pom.xml` for Java. All three checks run in CI and block merge on drift; all three have matching `apply` commands developers can run locally. The capability is layered on top of `code-quality-baseline` (build/clippy/test hygiene): formatting governs source-file shape, not compile output.
## Requirements
### Requirement: Project SHALL pin Rust formatting via a project-root `rustfmt.toml`

The project SHALL ship a `rustfmt.toml` at the repository root that pins the canonical Rust style. The file SHALL specify `edition = "2021"` and SHALL list every regenerated source path (e.g. `sdk/rust/src/generated`) in the `ignore` array so the formatter does not fight regenerators. Stable-only options SHALL appear above nightly-only options, with a comment block separating them so a stable `cargo fmt --all` invocation continues to work.

#### Scenario: Clean cargo fmt check from a fresh clone

- **WHEN** a developer runs `cargo fmt --all -- --check` after a fresh clone
- **THEN** the command exits 0
- **AND** no files under any project crate emit a diff

#### Scenario: A regenerated path is added without updating ignore

- **WHEN** a contributor introduces a new code-generation step that writes to a directory not yet listed in `rustfmt.toml`'s `ignore` array
- **THEN** the contributor SHALL update `rustfmt.toml` in the same PR to list the new directory
- **AND** the reviewer SHALL flag the omission if it is missing

#### Scenario: A new contributor uses stable rustfmt

- **WHEN** a contributor runs `cargo fmt --all` with stable rustfmt installed (no nightly)
- **THEN** the formatter applies all stable-supported options without error
- **AND** nightly-only options listed in `rustfmt.toml` are silently ignored by stable rustfmt

### Requirement: Project SHALL enforce `cargo fmt` in CI

A GitHub Actions workflow SHALL run `cargo fmt --all -- --check` on every pull request and push to `main`. The check SHALL fail the workflow when any project source file diverges from `rustfmt.toml`'s contract. The Rust toolchain version used by the check SHALL be pinned (via `rust-toolchain.toml` or an explicit workflow input) so a stable-rustfmt change does not retroactively break the gate, and the same pinned version SHALL be the one the local format script invokes.

The workflow's path filter SHALL be expressed as paths to ignore rather than paths to include, so a source tree added later is covered by default. A filter that must name every covered tree is a gate that silently stops covering the one it forgets.

#### Scenario: A PR introduces an unformatted Rust change

- **WHEN** a contributor opens a PR that modifies a Rust file in a way that violates `rustfmt.toml`
- **THEN** the format CI workflow fails
- **AND** the failing step's output names the Rust formatter as the check that failed
- **AND** merge is blocked until the contributor fixes the formatting

#### Scenario: A PR touches only prose

- **WHEN** a contributor opens a PR that modifies only files under `docs/` or `openspec/`
- **THEN** the format workflow's path filter skips the run
- **AND** no formatter check executes

#### Scenario: A PR touches a source tree the filter never named

- **WHEN** a contributor opens a PR that modifies a source tree added after the workflow was last edited
- **THEN** the format workflow runs
- **AND** the gate reports on it rather than skipping it

### Requirement: Project SHALL enforce `mix format` in CI

The same GitHub Actions workflow SHALL run `mix format --check-formatted` on every pull request and push to `main`. The check SHALL fail when any Elixir source file under the paths declared in `.formatter.exs`'s `inputs` glob is unformatted.

#### Scenario: An Elixir file is committed without `mix format`

- **WHEN** a contributor commits an `*.ex` or `*.exs` file with non-canonical indentation or alias ordering
- **THEN** `mix format --check-formatted` reports a diff
- **AND** the format CI workflow fails
- **AND** merge is blocked until the contributor runs `mix format` and recommits

#### Scenario: A new authored Elixir directory is added

- **WHEN** a contributor introduces a new top-level directory containing authored Elixir source (e.g. `priv/gen/`, `rel/overlays/`)
- **THEN** the contributor SHALL extend `.formatter.exs`'s `inputs` glob in the same PR to cover the new path
- **AND** the format check exercises the new path on subsequent CI runs

### Requirement: Project SHALL enforce Java formatting via Spotless

The Maven parent at `sdk/java/pom.xml` SHALL configure `spotless-maven-plugin` with the **palantir-java-format** style and bind `spotless:check` to the `verify` phase. The same GitHub Actions workflow SHALL run `mvn -f sdk/java/pom.xml -DskipTests spotless:check` on every pull request and push to `main`. The Spotless plugin version and the palantir-java-format version SHALL both be pinned explicitly in the pom so style updates are deliberate.

#### Scenario: A Java file is committed without Spotless apply

- **WHEN** a contributor commits a `*.java` file under `sdk/java/flezha-sdk-*/src/{main,test}/java` with non-canonical formatting
- **THEN** `mvn spotless:check` fails
- **AND** the format CI workflow fails
- **AND** merge is blocked until the contributor runs `mvn spotless:apply` and recommits

#### Scenario: Generated protobuf Java is excluded

- **WHEN** the Spotless plugin runs against the `flezha-sdk-protobuf` module
- **THEN** files under `target/generated-sources/` are NOT inspected
- **AND** the protobuf code-generation step continues to emit code in its own style without conflict

#### Scenario: Local `mvn verify` catches drift

- **WHEN** a developer runs `mvn -f sdk/java/pom.xml verify` locally
- **THEN** Spotless's `check` execution runs as part of the `verify` phase
- **AND** unformatted code fails the local build with the same error the CI gate would emit

### Requirement: Project SHALL ship local format scripts

The repository SHALL ship two scripts at `scripts/format-check.sh` and `scripts/format-apply.sh`. Both SHALL attempt every check they cover and report the outcome of each; neither SHALL let one check's failure — or one tool's absence — prevent the checks behind it from running. The check script SHALL run every language's formatter in check mode and then the licensing checks; the apply script SHALL run every language's formatter in write mode to fix the whole tree in one command. Both SHALL be tracked under version control and documented in the top-level `README.md`.

The scripts SHALL exit non-zero when anything is unformatted and non-zero when a tool they need is unavailable, using a different status for each, and the CI workflow SHALL invoke the same script rather than maintaining its own list of formatters.

#### Scenario: Developer runs format-check locally

- **WHEN** a developer runs `scripts/format-check.sh` from the repo root
- **THEN** the script runs every formatter it can, then the licensing checks
- **AND** a failure in one of them does not prevent the rest from running
- **AND** the summary names every check that failed and every tool that was missing
- **AND** on full success the script exits 0

#### Scenario: Developer fixes formatting in one shot

- **WHEN** a developer runs `scripts/format-apply.sh` from the repo root
- **THEN** the script runs every language's formatter in write mode
- **AND** every authored source file whose formatter is installed conforms to it on completion
- **AND** subsequent `scripts/format-check.sh` invocations exit 0 on a box with every formatter installed

### Requirement: Project SHALL track bulk-format SHAs in `.git-blame-ignore-revs`

The repository SHALL maintain a `.git-blame-ignore-revs` file at the project root listing every commit whose sole purpose was a mechanical formatter sweep (initial pass, formatter version bumps, etc). The file SHALL be in the format `git blame --ignore-revs-file` recognizes (one full SHA per line, optional comments).

#### Scenario: `git blame` after a formatter bump

- **WHEN** a developer runs `git blame --ignore-revs-file .git-blame-ignore-revs <file>` on any source file
- **THEN** lines whose only change was a formatter-applied reflow are attributed to the previous human author rather than to the bulk-format commit

#### Scenario: GitHub blame UI

- **WHEN** a developer views a file's blame on github.com
- **THEN** the GitHub blame UI honors `.git-blame-ignore-revs` automatically
- **AND** lines reflowed by the bulk-format pass do not appear as authored by the format-pass commit

### Requirement: Format pass SHALL be purely mechanical

A formatter sweep landed under this capability SHALL NOT introduce semantic changes. No public API SHALL be renamed, no test SHALL be modified beyond whitespace, no behavior SHALL be altered. Reviewers SHALL be able to verify a format-pass PR by confirming `cargo fmt --all`, `mix format`, and `mvn spotless:apply` reproduce the diff exactly.

#### Scenario: Reviewer reproduces the format pass

- **WHEN** a reviewer checks out the head of a format-pass PR onto the parent commit and runs `cargo fmt --all && mix format && mvn -f sdk/java/pom.xml -DskipTests spotless:apply`
- **THEN** `git diff` shows zero additional changes beyond what the PR already contains
- **AND** the reviewer signs off knowing no semantic code change is hidden in the bulk diff

#### Scenario: A semantic change sneaks into a format PR

- **WHEN** a PR purports to be a format pass but a reviewer's reproduction step shows extra non-formatter changes
- **THEN** the PR SHALL be rejected
- **AND** the contributor SHALL split the semantic change into a separate PR

### Requirement: Every language SHALL have a pinned formatter reachable through one command

The Node, Ruby, PHP and Python SDKs SHALL each declare one formatter as a pinned development dependency, and `scripts/format-apply.sh` / `scripts/format-check.sh` SHALL invoke every language's formatter, so that one command formats or checks the whole repository. The Elixir formatter version SHALL be the same locally and in CI.

#### Scenario: One command formats every language

- **WHEN** `scripts/format-apply.sh` runs twice on a clean tree
- **THEN** the second run SHALL change no file, and `scripts/format-check.sh` SHALL exit zero

#### Scenario: Local and CI formatter output agree

- **WHEN** `mix format --check-formatted` runs under the Elixir version CI pins and under the version the repository's toolchain pin names
- **THEN** both SHALL report the same result on the same tree

### Requirement: A formatter gate SHALL report every language it covers

A gate that checks formatting across several languages SHALL attempt every one
of them and report per language. An unavailable tool SHALL remove only its own
language from the report; it SHALL NOT prevent the remaining languages from
being checked, and SHALL NOT leave the operator unable to tell which were
examined.

The output SHALL distinguish a tool that could not run from a tree that is not
formatted. Both may fail the run, but a missing tool SHALL NOT suppress a
dirty-tree finding for a different language.

A language the gate claims to cover SHALL actually be attempted in continuous
integration. Where a formatter cannot run there, the workflow SHALL say so
where the step would be, rather than presenting a gate that covers it.

#### Scenario: A missing tool does not hide the other languages

- **GIVEN** a machine where one language's formatter is not installed
- **WHEN** the format gate runs
- **THEN** it SHALL report that tool as unavailable, and SHALL still report a verdict for every other language it covers

#### Scenario: An unformatted file is reported even when another tool is missing

- **GIVEN** a machine missing one formatter, and a tree with an unformatted file in a different language
- **WHEN** the gate runs
- **THEN** it SHALL report that file

#### Scenario: A gated language is gated in CI

- **WHEN** the formatting workflow is read
- **THEN** every language the gate covers SHALL either have a step that runs its formatter, or a comment where that step would be saying why it cannot

#### Scenario: The exit status says which finding it is

- **GIVEN** a machine missing one formatter
- **WHEN** the gate runs against a tree every available formatter accepts
- **THEN** it SHALL exit with a status distinct from the one it uses for an unformatted tree, so a caller can tell "not checked" from "not formatted" without parsing the output

