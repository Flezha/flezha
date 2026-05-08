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

A GitHub Actions workflow SHALL run `cargo fmt --all -- --check` on every pull request and push to `main`. The check SHALL fail the workflow when any project source file diverges from `rustfmt.toml`'s contract. The Rust toolchain version used by the check SHALL be pinned (via `rust-toolchain.toml` or an explicit workflow input) so a stable-rustfmt change does not retroactively break the gate.

#### Scenario: A PR introduces an unformatted Rust change

- **WHEN** a contributor opens a PR that modifies a Rust file in a way that violates `rustfmt.toml`
- **THEN** the format CI workflow fails
- **AND** the failure annotation cites `cargo fmt --all -- --check` as the failing step
- **AND** merge is blocked until the contributor fixes the formatting

#### Scenario: A PR touches only docs

- **WHEN** a contributor opens a PR that modifies only `*.md` files outside `lib/`, `sdk/`, `flz_*/`, `cli/`, or `test/apps/`
- **THEN** the format workflow's path filters skip the run
- **AND** no Rust formatter check executes

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

The repository SHALL ship two scripts at `scripts/format-check.sh` and `scripts/format-apply.sh`. The check script SHALL run `cargo fmt --all -- --check`, `mix format --check-formatted`, and `mvn -f sdk/java/pom.xml -DskipTests spotless:check` in sequence with `set -euo pipefail` so the first failing language stops the script. The apply script SHALL run `cargo fmt --all`, `mix format`, and `mvn -f sdk/java/pom.xml -DskipTests spotless:apply` in sequence to fix all three trees in one command. Both SHALL be tracked under version control and documented in the top-level `README.md`.

#### Scenario: Developer runs format-check locally

- **WHEN** a developer runs `scripts/format-check.sh` from the repo root
- **THEN** the script runs the Rust, Elixir, and Java format checks in that order
- **AND** the script exits non-zero on the first failure (Rust if Rust fails; Elixir if Rust passes but Elixir fails; etc.)
- **AND** on full success the script exits 0 with no stdout output

#### Scenario: Developer fixes formatting in one shot

- **WHEN** a developer runs `scripts/format-apply.sh` from the repo root
- **THEN** the script runs `cargo fmt --all`, `mix format`, and `mvn spotless:apply` in sequence
- **AND** every authored source file in the tree conforms to its respective formatter on completion
- **AND** subsequent `scripts/format-check.sh` invocations exit 0

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
