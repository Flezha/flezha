# feature-maturity Specification

## Purpose
What the project publishes about the readiness of its own parts: the maturity record and its two
labels, what each label promises, where a label must appear, the once-per-process announcement an
experimental feature makes when it is switched on, and the gate that keeps the record and the tree
agreeing.
## Requirements
### Requirement: The project SHALL publish one record naming every part that is not settled, and nothing else

The repository SHALL carry one published maturity record, `docs/public/publishing/maturity.md`,
holding two tables: the labelled entries, experimental or preview, and the support tier of
each SDK. A part the record does not name is stable. The record is the only declaration of
these facts: every other surface that states one of them SHALL agree with the record, and
no second list SHALL exist in code or prose.

An entry SHALL carry an id, the part it names, its status (`experimental` or `preview`),
the date it was marked, what the status promises, the surfaces that carry its marker, and
the site that announces it at runtime, or `none` for a preview entry. A tier row SHALL
carry the SDK, its tier, and the tier's promise.

Until Flezha reaches 1.0, the record SHALL say that the compatibility promise for a stable
part is that a change to its interface is named in the changelog, not that no change
happens, in agreement with the README.

The record is published with the repository, so it SHALL cite only what the published tree
carries, and SHALL NOT cite the defect ledger or any other working document kept out of
the repository.

At the change that introduces the record, it SHALL carry exactly one experimental entry,
the QUIC transport, and SHALL place the Rust, Java and Elixir SDKs in tier 1 and the Node,
Python, Ruby and PHP SDKs in tier 2.

#### Scenario: A feature the record does not name is stable

- **GIVEN** a feature that appears in no row of the record
- **WHEN** a reader asks whether it is covered by the compatibility promise
- **THEN** the answer SHALL be yes, and no other surface SHALL say otherwise

#### Scenario: The record and the README agree before 1.0

- **GIVEN** a release before 1.0
- **WHEN** the record's compatibility promise and the README's maturity notice are read
- **THEN** both SHALL say an interface may still change between releases, and the record
  SHALL say each change is named in the changelog

#### Scenario: The record at introduction

- **WHEN** the record is read at the change that introduces it
- **THEN** its experimental table SHALL hold one entry naming the QUIC transport, and its
  tier table SHALL hold one row per SDK, with Rust, Java and Elixir in tier 1 and Node, Python,
  Ruby and PHP in tier 2

#### Scenario: The record cites only the published tree

- **WHEN** the record is read
- **THEN** it SHALL cite no defect-ledger id and no path the published tree does not carry

### Requirement: Experimental SHALL mean the surface may change drastically and the feature may be dropped

An experimental entry SHALL promise exactly this: its surface may change drastically, and
the feature may be removed. An experimental feature SHALL be excluded from the
compatibility promise, and changing or removing it SHALL NOT be a compatibility break.

The label SHALL be used for a feature and never for an SDK binding. An experimental label
SHALL NOT gate, refuse, disable or alter the feature it names: the feature behaves
identically whether or not it is listed, and no acknowledgement setting SHALL be required
to use it.

#### Scenario: An experimental feature runs exactly as it would unlisted

- **GIVEN** a controller configured with QUIC among its transports
- **WHEN** it starts
- **THEN** the QUIC listener SHALL start, accept runners and carry traffic exactly as it
  would if the record did not list it, with no additional setting required

#### Scenario: Removing an experimental feature is not a compatibility break

- **GIVEN** an experimental entry
- **WHEN** a later change removes the feature it names
- **THEN** that change SHALL NOT be classified as a compatibility break, and SHALL remove the
  entry and its markers in the same change

### Requirement: A support tier SHALL state who runs a binding, and SHALL NOT imply that a capability is missing

A tier SHALL describe how much real-world exercise a binding has had, and nothing about its
feature set. Tier 1 SHALL mean the maintainer runs the binding. Tier 2 SHALL mean the
binding builds and passes the full conformance run in CI exactly as tier 1 does, and is not
yet exercised by the maintainer in a live deployment, so defects specific to that binding
are likely to be found by its users first.

Every tier SHALL be permanently supported: a binding SHALL NOT be removed for being tier 2.
A tier row SHALL NOT use the word experimental, and SHALL NOT state or imply that a
capability is missing or planned for removal.

A tier SHALL NOT excuse a deferred capability. A capability a binding does not carry SHALL
be declared in that runtime's conformance manifest, as the conformance requirements
already demand, and SHALL also be named in that binding's tier row, where a reader choosing
an SDK meets it.

#### Scenario: A tier 2 row states the promise and no gap

- **WHEN** the tier row for a tier 2 SDK is read
- **THEN** it SHALL state that the binding passes the full conformance run in CI and is
  permanently supported, and SHALL NOT contain the word experimental

#### Scenario: A deferral surfaces in the tier row

- **GIVEN** a runtime whose conformance manifest lists a capability under `deferred`
- **WHEN** that binding's tier row is read
- **THEN** it SHALL name the deferred capability

#### Scenario: Every binding's tier appears where its users install it

- **GIVEN** an SDK with a tracked package README
- **WHEN** that README is read
- **THEN** it SHALL state the binding's tier and link to its row in the record

### Requirement: An experimental feature SHALL announce itself when it is switched on, and not again per connection

When an experimental feature is switched on, the process that switched it on SHALL log one
warning naming the feature and pointing at its entry in the record. The warning SHALL be
emitted where the feature is started, and SHALL NOT repeat per connection, per
reconnection, per stream or per frame. A selection consulted on every connection attempt
SHALL announce once for the life of the process.

For the QUIC transport there are three such points, and each SHALL announce: the
controller's QUIC listener at startup; the transport selection in `flz_transport`, which
the Rust, Java, Node, Python, Ruby, PHP and Go runners all reach; and the Elixir SDK's own
transport selection, which does not go through `flz_transport`.

A support tier SHALL NOT be announced at runtime.

#### Scenario: A QUIC controller warns once

- **GIVEN** a controller whose transports include QUIC
- **WHEN** it starts and several runners connect over QUIC
- **THEN** exactly one warning naming the QUIC transport and the record SHALL have been
  logged by that controller

#### Scenario: A QUIC runner warns once

- **GIVEN** a runner of any SDK configured for the QUIC transport
- **WHEN** it connects, disconnects and reconnects
- **THEN** exactly one warning naming the QUIC transport and the record SHALL have been
  logged by that runner process

#### Scenario: A UDS process says nothing

- **GIVEN** a controller or a runner using only the UDS transport
- **WHEN** it starts
- **THEN** no maturity warning SHALL be logged

#### Scenario: A tier 2 runner says nothing about its tier

- **GIVEN** a runner built on a tier 2 SDK, using the UDS transport
- **WHEN** it starts
- **THEN** no maturity or tier warning SHALL be logged

### Requirement: Every tracked surface that documents an experimental feature SHALL carry its marker, and none SHALL call an unlisted feature experimental

The marker SHALL be one fixed form, naming the entry's status and id and linking to its
row in the record, `[experimental: <id>]` or `[preview: <id>]`, so a reader can follow it
and a gate can match it to an entry. A marker's label SHALL equal its entry's status.

Each surface the record lists for an entry SHALL carry that entry's marker. For the QUIC
transport those surfaces SHALL include at least the configuration reference rows for the
settings that select or configure it, the README's statement of the available transports,
and the changelog.

Outside the record and this requirement, no tracked prose surface SHALL describe a feature
as experimental unless the description is a marker for an entry the record lists. Prose
SHALL NOT be refused for using the word preview, which the project also uses for a
streamed part.

The record SHALL say which surfaces the gate checks and which it does not, so a green gate
is never read as covering documentation it cannot see.

#### Scenario: The configuration reference marks the QUIC settings

- **WHEN** a page under `docs/public/configuration/` is read
- **THEN** each row for a setting that selects or configures the QUIC transport SHALL carry
  the QUIC entry's marker

#### Scenario: An unlisted feature called experimental is refused

- **GIVEN** a tracked prose surface describing a feature as experimental
- **AND** the record lists no entry for that feature
- **WHEN** the gate runs
- **THEN** it SHALL fail naming the file and the line

#### Scenario: A marker whose label differs from its entry fails

- **GIVEN** a preview entry whose listed surface carries `[experimental: <id>]`, or an
  experimental entry whose listed surface carries `[preview: <id>]`
- **WHEN** the gate runs
- **THEN** it SHALL exit non-zero naming the entry and the surface

### Requirement: The record SHALL state how an entry is added, graduated and removed

The record SHALL carry the rule for changing it, so the question is answered once and in
the place a contributor meets it.

A feature introduced as experimental SHALL add its entry, markers and announce site in the
change that introduces it. A feature graduating to stable SHALL have its entry, markers and
announce site removed in the change that hardens it, and that change SHALL say so in the
changelog. Marking a feature experimental after it has been published as stable SHALL be
treated as a compatibility break and SHALL require its own proposal. A binding SHALL move
from tier 2 to tier 1 only by a change that records the live deployment that exercised it.

A part marked preview SHALL add its entry and markers in the change that marks it. A
preview part graduating to stable SHALL have its entry and markers removed in a change
that says so in the changelog. Marking a part preview after it has been published as
stable SHALL require its own proposal.

#### Scenario: Graduation removes every trace of the label

- **GIVEN** an experimental or preview entry whose part is being declared stable
- **WHEN** the graduating change lands
- **THEN** the entry, every marker it named and any runtime announcement SHALL be gone, and
  the changelog SHALL record the graduation

#### Scenario: Demoting a published stable feature needs its own proposal

- **GIVEN** a feature published as stable
- **WHEN** a change proposes to mark it experimental
- **THEN** the change SHALL be its own proposal, classified as a compatibility break

### Requirement: The maturity record SHALL be gated, and the gate SHALL land green and be shown able to fail

A gate SHALL check, on every pull request, that the record parses, that every entry and
every tier row carries its required fields, that ids are unique, that every entry's status
is `experimental` or `preview`, that every surface an entry lists exists and carries the
entry's marker with the matching label, that every announce site an experimental entry
names exists, that a preview entry's announce site is `none`, that no tier row breaks the
tier requirement, and that no tracked prose surface calls an unlisted feature
experimental.

The gate SHALL read only surfaces git tracks. It SHALL report how many experimental
entries, preview entries and tier rows it read, and each entry's id with the date it was
marked, beside its verdict, so a green that means "the parser matched nothing" is
distinguishable from a green that means the record is sound, and so an entry that has
been labelled for a long time is visible on every run.

The gate SHALL be green on the commit that introduces it, and SHALL be demonstrated to
fail before it is trusted: on an entry missing a required field, on a listed surface that
carries no marker, on a named announce site that does not exist, and on tracked prose that
calls an unlisted feature experimental. It SHALL pass once each is repaired.

#### Scenario: The gate is green at introduction

- **WHEN** the gate runs on the commit that introduces the record
- **THEN** it SHALL exit zero and report one experimental entry and a tier row per SDK

#### Scenario: The gate reports each label

- **WHEN** the gate runs after the preview label is introduced
- **THEN** it SHALL exit zero and report one experimental entry, no preview entries and
  eight tier rows

#### Scenario: A preview entry with an announce site fails

- **GIVEN** a preview entry whose announce site is anything other than `none`
- **WHEN** the gate runs
- **THEN** it SHALL exit non-zero naming the entry

#### Scenario: A listed surface without its marker fails

- **GIVEN** an entry that lists a tracked surface
- **AND** that surface carries no marker for the entry
- **WHEN** the gate runs
- **THEN** it SHALL exit non-zero naming the entry and the surface

#### Scenario: A named announce site that does not exist fails

- **GIVEN** an entry whose announce site names a file or function the tree does not carry
- **WHEN** the gate runs
- **THEN** it SHALL exit non-zero naming the entry and the site

#### Scenario: An untracked page is not judged

- **GIVEN** a documentation page that git does not track
- **WHEN** the gate runs
- **THEN** it SHALL neither require a marker on that page nor refuse prose on it

### Requirement: Preview SHALL mean the part stays and its surface may still change

A preview entry SHALL promise exactly this: the part it names will stay and SHALL NOT be
removed for being preview, and its surface may change between releases without a
compatibility break. Each such change SHALL still be named in the changelog. An entry
labelled preview MAY name a feature or an SDK.

Preview SHALL NOT gate, refuse, disable or alter the part it names, and SHALL NOT be
announced at runtime: a preview entry's announce site SHALL be `none`.

At the change that introduces the label, no entry SHALL be preview.

#### Scenario: A preview entry states its promise

- **WHEN** a preview entry is read
- **THEN** its promise SHALL say the part stays and its surface may change, and its
  announce site SHALL be `none`

#### Scenario: Nothing is preview at introduction

- **WHEN** the record is read at the change that introduces the preview label
- **THEN** it SHALL hold one entry, the QUIC transport with status `experimental`, and no
  SDK's tier row or marker SHALL have changed

