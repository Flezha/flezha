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

#### Scenario: Generated Java is excluded

- **WHEN** the Spotless plugin runs against a module whose build writes generated Java under `target/generated-sources/`
- **THEN** files under `target/generated-sources/` are NOT inspected, because the plugin's Java source set is `src/main/java` and `src/test/java` only
- **AND** the code-generation step continues to emit code in its own style without conflict

#### Scenario: Local `mvn verify` catches drift

- **WHEN** a developer runs `mvn -f sdk/java/pom.xml verify` locally
- **THEN** Spotless's `check` execution runs as part of the `verify` phase
- **AND** unformatted code fails the local build with the same error the CI gate would emit

### Requirement: Project SHALL ship local format scripts

The repository SHALL ship one script at `scripts/format-apply.sh` that runs every language's formatter in write mode to fix the whole tree in one command, and SHALL expose the same list in check mode together with the licensing checks as rows of the gate runner, reached by `python3 scripts/naming_gate.py --run @format`. The script and the runner SHALL attempt every check they cover and report the outcome of each; neither SHALL let one check's failure, or one tool's absence, prevent the checks behind it from running. Both SHALL be tracked under version control and documented in the top-level `README.md`.

There SHALL NOT be a second script that re-spells the same checks. Two lists drift: a local gate that claims to cover a language CI never runs is worse than no gate, because its green reads as a verdict.

The script and the runner SHALL exit non-zero when anything is unformatted, non-zero when a tool they need is unavailable, and non-zero when a tool they ran did not complete, using a different status for each, and the CI workflow SHALL invoke the same runner rather than maintaining its own list of formatters or of checks.

#### Scenario: Developer runs the format gate locally

- **WHEN** a developer runs `python3 scripts/naming_gate.py --run @format` from the repo root
- **THEN** the runner runs every formatter it can, then the licensing checks
- **AND** a failure in one of them does not prevent the rest from running
- **AND** the summary names every check that failed, every tool that was missing, and every tool that did not complete
- **AND** on full success the runner exits 0

#### Scenario: Developer fixes formatting in one shot

- **WHEN** a developer runs `scripts/format-apply.sh` from the repo root
- **THEN** the script runs every language's formatter in write mode
- **AND** every authored source file whose formatter is installed conforms to it on completion
- **AND** a subsequent `python3 scripts/naming_gate.py --run @format` exits 0 on a box with every formatter installed

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

The Node, Ruby, PHP and Python SDKs SHALL each declare one formatter as a pinned development dependency, and `scripts/format-apply.sh` SHALL invoke every language's formatter, so that one command formats the whole repository and one runner call checks it. The Elixir formatter version SHALL be the same locally and in CI.

#### Scenario: One command formats every language

- **WHEN** `scripts/format-apply.sh` runs twice on a clean tree
- **THEN** the second run SHALL change no file, and `python3 scripts/naming_gate.py --run @format` SHALL exit zero

#### Scenario: Local and CI formatter output agree

- **WHEN** `mix format --check-formatted` runs under the Elixir version CI pins and under the version the repository's toolchain pin names
- **THEN** both SHALL report the same result on the same tree

### Requirement: A formatter gate SHALL report every language it covers

A gate that checks formatting across several languages SHALL attempt every one
of them and report per language. An unavailable tool SHALL remove only its own
language from the report; it SHALL NOT prevent the remaining languages from
being checked, and SHALL NOT leave the user unable to tell which were
examined.

The output SHALL distinguish three outcomes, each under its own heading: a tool
that could not run, a tree that is not formatted, and a tool that ran and did
not complete. Any of them may fail the run, but no one of them SHALL suppress
another for a different language, and a tool that failed for its own reasons
SHALL NOT be reported as unformatted source. Naming the wrong cause sends the
next reader to fix something that is already correct.

How the gate tells a finding from a failure SHALL be declared per check rather
than inferred from the exit status alone, because a formatter that finds drift
and one that crashes may return the same status. The declaration SHALL be
either the exit code the tool documents for a finding, or the verdict the tool
prints when it has one. A check that declares neither has no finding state, and
every non-zero status from it is a failure to complete.

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

#### Scenario: A formatter that fails for its own reasons is not called unformatted

- **GIVEN** a formatter that is installed but cannot complete — a missing runtime extension, an unparseable input, a toolchain it cannot reach
- **WHEN** the gate runs
- **THEN** it SHALL report that language under the heading for a tool that did not complete, and SHALL NOT list it as unformatted
- **AND** it SHALL exit with a status distinct from both the unformatted status and the missing-tool status

#### Scenario: A tool whose two outcomes share one exit status is told apart by its verdict

- **GIVEN** a formatter that returns the same non-zero status for drift and for its own failure
- **WHEN** the gate runs it
- **THEN** the outcome SHALL be decided by the verdict sentence the tool prints for drift
- **AND** output that carries no such verdict SHALL be reported as a failure to complete rather than as drift

#### Scenario: A gate that wraps another gate carries the distinction up

- **GIVEN** a gate whose check is itself a gate of the same kind
- **WHEN** the inner gate reports that a tool did not complete
- **THEN** the outer gate SHALL report the same outcome for that check, rather than flattening it into an unformatted finding or a missing tool

