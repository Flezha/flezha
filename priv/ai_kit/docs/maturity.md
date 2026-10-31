<!--
SPDX-FileCopyrightText: 2026 Anuar Alfetahe

SPDX-License-Identifier: MPL-2.0
-->

# Feature maturity

This record says which parts of Flezha are not yet settled. It is the only place that says
so: every other page that states one of these facts agrees with this record, and no second
list exists in code or prose.

**Anything this record does not name is stable.** A stable feature is covered by the
compatibility promise: changing it in a way that breaks someone who depends on it is a
compatibility break, announced as one. The relay model, effectively-once delivery, the
correction window, the dead-letter queue, cell timers, state persistence and schema
evolution are all unnamed here, and therefore stable.

Flezha is pre-1.0 alpha software. Until 1.0, the compatibility promise means that every
change to a stable interface is named in the changelog, not that no change happens: as the
README says, interfaces may still change between releases.

## Three labels

The record uses three labels. They answer different questions and are kept apart on
purpose.

**Experimental** says whether a feature is settled. An experimental feature's surface may
change drastically, and the feature may be removed. It is outside the compatibility promise,
so changing or removing it is not a compatibility break. The label is used for features and
never for an SDK. It gates nothing: an experimental feature runs exactly as it would if this
record did not list it, and no acknowledgement setting is needed to use it. The process that
switches it on logs one warning, naming the feature and pointing at its row here, when the
feature starts; the warning is not repeated per connection, reconnection, stream or frame.

**Preview** says a part will stay but its surface is not yet settled. A preview part is
never removed for being preview, but its surface may change between releases, and such a
change is not a compatibility break; the changelog still names it. The label may name a
feature or an SDK. Like experimental it gates nothing, and unlike experimental nothing
announces it at runtime: a part that stays does not warn on every start. Nothing is preview
today.

**Support tier** says who has run an SDK, and nothing about what it can do.

- **Tier 1**: the maintainer runs this SDK in a live deployment.
- **Tier 2**: the SDK builds and passes the full conformance run in CI exactly as tier 1
  does, but the maintainer does not yet run it in a live deployment, so a defect specific
  to it is likely to be found by its users first.

Every tier is permanently supported, and an SDK is never removed for being tier 2. A tier
never excuses a missing capability: a capability an SDK does not carry is declared in its
runtime's conformance manifest and named in its tier row below, where a reader choosing an
SDK meets it. Today no SDK defers any capability. Nothing announces a tier at runtime.

## How this record is read

Each entry below carries the surfaces that must show its label. A page marks a labelled
part in one fixed form, naming the entry's label and id and linking to its row:

```markdown
[experimental: quic](#quic)
```

with `preview` in place of `experimental` for a preview entry, and states an SDK's tier the
same way, linking to that SDK's row:

```markdown
[tier 1](#sdk-rust)
```

with the link written relative to the page that carries it, or as a full URL from an SDK's
own repository.

`scripts/check-feature-maturity.sh` reads this file on every pull request. It checks that
both tables parse, that every row carries its fields, that ids are unique, that every
entry's status is `experimental` or `preview`, that every surface a row lists exists and
carries the row's marker with the row's label, that every announce site an experimental
entry names exists and points here, that a preview entry's announce site is `none`, that
no tier row uses the experimental label, and that no tracked page calls a feature
experimental outside a marker for an entry listed here. It does not judge the word
preview in prose, which Flezha also uses for a streamed part. It prints how many rows of
each label it read and each entry's id with the date it was marked, so an entry that has
sat here for a long time is visible on every run.

The gate reads only files git tracks: this repository's `README.md`, `CHANGELOG.md` and
`docs/`, and the `README.md` of each SDK's repository under `sdk/`. It judges no page
outside them. The Ruby and PHP SDKs carry no package README yet, so their tier is stated on
their pages under `docs/internal/sdk/` and nowhere else.

## Experimental and preview entries

A preview entry's `announced-by` reads `none`.

| id | feature | status | marked | promise | surfaces | announced-by |
|---|---|---|---|---|---|---|
| <a id="quic"></a>quic | The QUIC transport, between a runner and a controller. The Unix domain socket transport is the default and is stable. | experimental | 2026-09-10 | Its settings and behaviour may change drastically, and it may be removed, without a compatibility break. Until it graduates, it is far less exercised than the socket transport. | `docs/public/configuration/environment_variables.md`, `docs/public/configuration/application_env.md`, `docs/public/configuration/runner_environment.md`, `docs/public/topics/deploying/connecting_a_runner.md`, `docs/public/topics/core/authorization.md`, `docs/public/topics/deploying/deploying_a_controller.md`, `docs/internal/sdk/protocol_spec.md` | `lib/flezha/transport/server.ex` `listen(:quic)`, the controller's QUIC listener; `crates/flz_transport/src/transport/config.rs` `get_transport_type`, the transport selection the Rust, Java, Node, Python, Ruby, PHP, Go and Elixir runners and the `flz` CLI reach |

## SDK support tiers

| sdk | tier | promise | surfaces |
|---|---|---|---|
| <a id="sdk-rust"></a>rust | 1 | Run by the maintainer in a live deployment. Passes the full conformance run in CI and is permanently supported. | `sdk/rust/README.md`, `docs/internal/sdk/rust_relay_architecture.md` |
| <a id="sdk-java"></a>java | 1 | Run by the maintainer in a live deployment. Passes the full conformance run in CI and is permanently supported. | `sdk/java/README.md`, `docs/internal/sdk/java_sdk.md` |
| <a id="sdk-elixir"></a>elixir | 1 | Run by the maintainer in a live deployment. Passes the full conformance run in CI and is permanently supported. | `sdk/elixir/README.md`, `docs/internal/sdk/elixir_sdk.md` |
| <a id="sdk-node"></a>node | 2 | Passes the full conformance run in CI exactly as tier 1 does, with every capability present, and is permanently supported. Not yet run by the maintainer in a live deployment. | `sdk/node/README.md`, `docs/internal/sdk/node_sdk.md` |
| <a id="sdk-python"></a>python | 2 | Passes the full conformance run in CI exactly as tier 1 does, with every capability present, and is permanently supported. Not yet run by the maintainer in a live deployment. | `sdk/python/README.md`, `docs/internal/sdk/python_sdk.md` |
| <a id="sdk-ruby"></a>ruby | 2 | Passes the full conformance run in CI exactly as tier 1 does, with every capability present, and is permanently supported. Not yet run by the maintainer in a live deployment. | `sdk/ruby/README.md`, `docs/internal/sdk/ruby_sdk.md` |
| <a id="sdk-php"></a>php | 2 | Passes the full conformance run in CI exactly as tier 1 does, with every capability present but `local_send`, which its single-threaded serving defers, and is permanently supported. Not yet run by the maintainer in a live deployment. | `sdk/php/README.md`, `docs/internal/sdk/php_sdk.md` |
| <a id="sdk-go"></a>go | 2 | Passes the full conformance run exactly as tier 1 does, with every capability present, and is permanently supported. Not yet run by the maintainer in a live deployment. | `sdk/go/README.md`, `docs/internal/sdk/go_sdk.md` |

## Changing this record

- **Adding an entry.** A feature introduced as experimental adds its row, its markers and
  its runtime warning in the change that introduces it. A part marked preview adds its row
  and its markers in the change that marks it.
- **Graduating an entry.** A part declared stable has its row, every marker the row names
  and any runtime warning removed in the change that hardens it, and the changelog says it
  graduated.
- **Removing a feature.** Removing an experimental feature is not a compatibility break;
  the change that removes it removes its row and its markers too.
- **Demoting a stable part.** Marking a feature experimental after it was published as
  stable is a compatibility break, and needs a proposal of its own. Marking a published
  stable part preview needs a proposal of its own too.
- **Moving an SDK to tier 1.** An SDK moves from tier 2 to tier 1 only in a change that
  records the live deployment that exercised it.
