# operative-kind-naming Specification

<!-- kind-naming-record:begin -->

## Purpose

The naming contract for the two operative kinds. They are `stream` and `workflow`; the words `bounded` and `unbounded` are retired as kind names because they describe a property of the *data* while the distinction here is the *execution model* — and `bounded` was outright false for a durable `PerInput` operative, whose input is unbounded and whose iteration is the only bounded thing. This capability owns the identifier convention across SDK types, kind atoms, wire messages, CLI arguments, env vars and file paths; the rule separating the kind sense from the surviving capacity and plain-English senses; the machine-readable allowlist of preserved occurrences; and the CI gate that fails a reintroduction.

## Requirements

### Requirement: The kind sense of `bounded` / `unbounded` is separated from the capacity and plain-English senses

The words "bounded" and "unbounded" carry three distinct senses in this repository, and the rename SHALL apply to exactly one of them.

- **Kind sense (renamed):** any use naming an operative kind — kind atoms, SDK types, wire message names, CLI arguments, env vars, module and file paths, and prose describing the kind. This sense SHALL be renamed to `stream` / `workflow`.
- **Capacity sense (preserved):** `Flezha.Observability.OtpBackend.BoundedMailboxSource`, the source atom `:flezha_bounded_mailbox`, the event `:bounded_mailbox_unavailable`, and the "Backpressure and bounded mailboxes" material. Here "bounded" means a mailbox with a capacity limit and has no relation to the operative kind. This sense SHALL NOT be renamed.
- **Plain-English sense (preserved):** ordinary usage such as "bounded by the deadline", "a bounded (1024) FIFO", "bounded cardinality", "bounded loss window", and "unbounded growth". This sense SHALL NOT be renamed.

A machine-readable allowlist of every preserved occurrence SHALL be produced before any rename edit is made, and SHALL be the input to both the rename and its enforcement gate.

#### Scenario: The capacity-sense mailbox subsystem is untouched

- **WHEN** the observability sources are inspected after the rename
- **THEN** `Flezha.Observability.OtpBackend.BoundedMailboxSource`, the source atom `:flezha_bounded_mailbox`, and the event `:bounded_mailbox_unavailable` SHALL retain their names unchanged

#### Scenario: Plain-English usage survives

- **WHEN** documentation or code comments describe a limit using ordinary English — "bounded by", "a bounded FIFO", "unbounded growth"
- **THEN** the wording SHALL be unchanged by the rename

#### Scenario: The allowlist precedes the rename

- **WHEN** the rename is performed
- **THEN** an allowlist enumerating every preserved occurrence and its sense SHALL already exist, and SHALL be checked in alongside the change

### Requirement: No kind-sense occurrence of the old names survives in a shipped surface

After this change, no identifier, type name, trait or interface name, kind atom, wire message name, protobuf field name, CLI verb or argument value, config key, env var, module path, file path, directory name, code comment, or document SHALL use `bounded` or `unbounded` to name an operative kind. The prohibition SHALL NOT extend to `openspec/changes/archive/**` or to git history, which are historical records.

#### Scenario: Shipped surfaces are clean

- **WHEN** the controller, both SDKs, `flz_core`, protobuf, CLI, config, env vars, tests, examples, `docs/`, and `openspec/specs/` are inspected after this change
- **THEN** no kind-sense occurrence of `bounded` or `unbounded` SHALL be found

#### Scenario: The archive is preserved

- **WHEN** `openspec/changes/archive/**` is inspected after this change
- **THEN** its contents SHALL be unchanged, including their use of the old kind names

### Requirement: A gate mechanically prevents reintroduction

The repository SHALL carry an automated check that fails when a kind-sense occurrence of `bounded` or `unbounded` is introduced outside the excluded paths. The check SHALL read the preserved-occurrence allowlist rather than hard-coding exceptions, SHALL exclude `openspec/changes/archive/`, `.git/`, `target/`, `_build/`, and `dist/`, and SHALL run in CI alongside the existing formatting gate.

The check SHALL match any identifier *containing* the token, not only the bare word, so that a reintroduced `BoundedOperative` or `spawn_bounded` is caught. The binding word family (`bound`, `unbound`, `bind`, `rebind`, `inbound`, `outbound`, and their compounds) contains no `bounded` substring and SHALL therefore never be matched.

A block of prose whose subject *is* the naming — a changelog entry or API record naming the retired identifiers, or the glossary paragraph that retains `bounded`/`unbounded` as adjectives for the run — SHALL be exemptable by delimiting the block rather than by adding a regex that pins its exact wording. The delimiter SHALL be greppable and SHALL scope the exemption to the block, never to the whole file.

#### Scenario: A compound identifier is caught

- **WHEN** a change introduces an identifier such as `BoundedOperative` or `spawn_bounded`
- **THEN** the check SHALL fail, even though the bare word `bounded` appears nowhere on the line

#### Scenario: The binding word family does not trip the check

- **WHEN** the check runs against code using `bound_runner`, `unbound`, `rebind`, or `require_bound_principal`
- **THEN** it SHALL pass, and those identifiers SHALL NOT require allowlist entries

#### Scenario: A delimited naming-record block is exempt, but only within its delimiters

- **WHEN** a changelog entry, API record, or glossary paragraph is wrapped in the delimiters
- **THEN** the check SHALL ignore the wrapped lines, and SHALL still fail on a kind-sense occurrence in the same file outside them

#### Scenario: A reintroduced kind name fails the check

- **WHEN** a change adds an identifier or document naming an operative kind `bounded` or `unbounded`
- **THEN** the check SHALL fail and SHALL name the offending file and line

#### Scenario: An allowlisted occurrence passes

- **WHEN** the check runs against the preserved capacity-sense and plain-English occurrences
- **THEN** it SHALL pass without requiring those occurrences to be modified or individually suppressed

#### Scenario: Build artifacts and history do not trip the check

- **WHEN** the check runs in a tree containing `target/`, `_build/`, `dist/`, and `openspec/changes/archive/`
- **THEN** those paths SHALL be excluded and SHALL NOT cause a failure

### Requirement: Documentation retains `bounded` / `unbounded` as adjectives for the run

The words SHALL be retained in documentation where they are accurate — describing a *run*, not naming a *kind*. The glossary SHALL state the distinction, SHALL record that a `PerInput` workflow operative's input is unbounded while only its iteration is bounded, and SHALL record the one-sentence rationale for the rename so the naming decision is settled rather than reopened.

#### Scenario: The glossary states the run-level distinction

- **WHEN** `docs/glossary.md` is read after this change
- **THEN** it SHALL state that a workflow operative runs a bounded iteration to a terminal state and a stream operative folds an unbounded input forever

#### Scenario: The `PerInput` caveat is recorded

- **WHEN** the glossary entry for the workflow kind is read
- **THEN** it SHALL state explicitly that a durable `PerInput` workflow operative consumes an unbounded input stream and that only its iteration is bounded

#### Scenario: The rationale is recorded

- **WHEN** the glossary or the kind documentation is read
- **THEN** it SHALL record why the kinds are named `stream` and `workflow` rather than `unbounded` and `bounded`
<!-- kind-naming-record:end -->
