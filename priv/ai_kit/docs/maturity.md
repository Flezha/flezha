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
compatibility break, announced as one. The operator model, effectively-once delivery, the
correction window, the dead-letter queue, cell timers, state persistence and schema
evolution are all unnamed here, and therefore stable.

## Two labels

The record uses two labels. They answer different questions and are kept apart on purpose.

**Experimental** says whether a feature is settled. An experimental feature's surface may
change drastically, and the feature may be removed. It is outside the compatibility promise,
so changing or removing it is not a compatibility break. The label is used for features and
never for an SDK. It gates nothing: an experimental feature runs exactly as it would if this
record did not list it, and no acknowledgement setting is needed to use it. The process that
switches it on logs one warning, naming the feature and pointing at its row here, when the
feature starts; the warning is not repeated per connection, reconnection, stream or frame.

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

Each entry below carries the surfaces that must show its label. A page marks an
experimental feature in one fixed form, naming the entry's id and linking to its row:

```markdown
[experimental: quic](docs/publishing/maturity.md#quic)
```

and states an SDK's tier the same way, linking to that SDK's row:

```markdown
[tier 1](docs/publishing/maturity.md#sdk-rust)
```

with the link written relative to the page that carries it, or as a full URL from an SDK's
own repository.

`scripts/check-feature-maturity.sh` reads this file on every pull request. It checks that
both tables parse, that every row carries its fields, that ids are unique, that every
surface a row lists exists and carries the row's marker, that every announce site a row
names exists and points here, that no tier row uses the experimental label, and that no
tracked page calls a feature experimental outside a marker for an entry listed here. It
prints how many rows it read and each entry's id with the date it was marked, so an entry
that has sat here for a long time is visible on every run.

The gate reads only files git tracks: this repository's `README.md`, `CHANGELOG.md` and
`docs/`, and the `README.md` of each SDK's repository under `sdk/`. It judges no page
outside them. The Ruby and PHP SDKs carry no package README yet, so their tier is stated on
their pages under `docs/sdk/` and nowhere else.

## Experimental features

| id | feature | status | marked | promise | surfaces | announced-by |
|---|---|---|---|---|---|---|
| <a id="quic"></a>quic | The QUIC transport, between a runner and a controller. The Unix domain socket transport is the default and is stable. | experimental | 2026-09-10 | Its settings and behaviour may change drastically, and it may be removed, without a compatibility break. Until it graduates, it is far less exercised than the socket transport. | `README.md`, `CHANGELOG.md`, `docs/configuration/environment_variables.md`, `docs/configuration/application_env.md`, `docs/configuration/runner_environment.md`, `docs/topics/deploying/connecting_a_runner.md`, `docs/topics/basics/authorization.md`, `docs/topics/deploying/deploying_a_controller.md`, `docs/sdk/protocol_spec.md` | `lib/flezha/transport/server.ex` `listen(:quic)`, the controller's QUIC listener; `flz_transport/src/transport/config.rs` `get_transport_type`, the transport selection the Rust, Java, Node, Python, Ruby and PHP runners and the `flz` CLI reach; `sdk/elixir/lib/flezha/sdk/transport.ex` `announce`, called when the Elixir SDK's runner starts |

## SDK support tiers

| sdk | tier | promise | surfaces |
|---|---|---|---|
| <a id="sdk-rust"></a>rust | 1 | Run by the maintainer in a live deployment. Passes the full conformance run in CI and is permanently supported. | `sdk/rust/README.md`, `docs/sdk/rust_operator_architecture.md` |
| <a id="sdk-java"></a>java | 1 | Run by the maintainer in a live deployment. Passes the full conformance run in CI and is permanently supported. | `sdk/java/README.md`, `docs/sdk/java_sdk.md` |
| <a id="sdk-elixir"></a>elixir | 1 | Run by the maintainer in a live deployment. Passes the full conformance run in CI and is permanently supported. | `sdk/elixir/README.md`, `docs/sdk/elixir_sdk.md` |
| <a id="sdk-node"></a>node | 2 | Passes the full conformance run in CI exactly as tier 1 does, with every capability present, and is permanently supported. Not yet run by the maintainer in a live deployment. | `sdk/node/README.md`, `docs/sdk/node_sdk.md` |
| <a id="sdk-python"></a>python | 2 | Passes the full conformance run in CI exactly as tier 1 does, with every capability present, and is permanently supported. Not yet run by the maintainer in a live deployment. | `sdk/python/README.md`, `docs/sdk/python_sdk.md` |
| <a id="sdk-ruby"></a>ruby | 2 | Passes the full conformance run in CI exactly as tier 1 does, with every capability present, and is permanently supported. Not yet run by the maintainer in a live deployment. | `docs/sdk/ruby_sdk.md` |
| <a id="sdk-php"></a>php | 2 | Passes the full conformance run in CI exactly as tier 1 does, with every capability present, and is permanently supported. Not yet run by the maintainer in a live deployment. | `docs/sdk/php_sdk.md` |

## Changing this record

- **Adding an entry.** A feature introduced as experimental adds its row, its markers and
  its runtime warning in the change that introduces it.
- **Graduating an entry.** A feature declared stable has its row, every marker the row
  names and its runtime warning removed in the change that hardens it, and the changelog
  says it graduated.
- **Removing a feature.** Removing an experimental feature is not a compatibility break;
  the change that removes it removes its row and its markers too.
- **Demoting a stable feature.** Marking a feature experimental after it was published as
  stable is a compatibility break, and needs a proposal of its own.
- **Moving an SDK to tier 1.** An SDK moves from tier 2 to tier 1 only in a change that
  records the live deployment that exercised it.
