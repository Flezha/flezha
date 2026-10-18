# Internal documentation

Pages for people who build Flezha rather than use it. They are not part of the published
site, whose pages live in [`docs/public/`](../public/) and whose sidebar is `mix.exs`. A
published page links one of these by its GitHub URL.

## Top level

- [`glossary.md`](glossary.md): The vocabulary: one entry per term, with where the code for it lives
- [`state_persistence.md`](state_persistence.md): The controller's WAL/snapshot store, flow control, `flz state`

## The consumer API

- [`api/README.md`](api/README.md): The published consumer surface, its three guards, and the manifest that keeps a consumer's calls resolving

## Operations

- [`operations/benchmarking.md`](operations/benchmarking.md): `mix bench`, shapes, recorded floors, CI gates

## Publishing

- [`publishing/readiness.md`](publishing/readiness.md): Every decision the first public release makes irreversible: the blocking/owed test, the record, and the decisions the owner owes

## Resilience mechanisms

- [`resilience/data_durability.md`](resilience/data_durability.md): The mechanism of the guarantees: a frame's guarantee class, the effectively-once tier's parts, the input floor
- [`resilience/steps_relay.md`](resilience/steps_relay.md): The mechanism of a relay that runs in steps: barriers, the durable loop, crash-resume

## SDK internals

- [`sdk/authoring_contract.md`](sdk/authoring_contract.md): The three authoring tiers, signature → type inference, state → Change derivation, cell mode, the cross-SDK reference relay
- [`sdk/calls.md`](sdk/calls.md): Implementation notes for calls: the Response envelope
- [`sdk/cell_timers.md`](sdk/cell_timers.md): Implementation notes for timers: folded state, where the code lives
- [`sdk/conformance.md`](sdk/conformance.md): The cross-SDK conformance suite: manifests, capabilities, the gate, adding an SDK
- [`sdk/elixir_sdk.md`](sdk/elixir_sdk.md): The Elixir SDK on the shared core, wherever its runner runs
- [`sdk/java_sdk.md`](sdk/java_sdk.md): The Java SDK: scope, how its artifacts are built, how it is tested
- [`sdk/multi_input_tasklets.md`](sdk/multi_input_tasklets.md): Implementation notes for merged inputs: replay, where the code lives
- [`sdk/naming_conventions.md`](sdk/naming_conventions.md): The numbered naming rules every SDK surface follows
- [`sdk/node_sdk.md`](sdk/node_sdk.md): The Node SDK (`@flezha/sdk`): supported versions, dispatch configuration, what must never block the event loop
- [`sdk/partitioned_relays.md`](sdk/partitioned_relays.md): Implementation notes for partitioning: the key-group hash and its vectors
- [`sdk/php_sdk.md`](sdk/php_sdk.md): The PHP SDK over `ext-ffi`: supported versions, sizing, the executor seam, accepted limits
- [`sdk/poll_core.md`](sdk/poll_core.md): The shared `flz_core` kernel and its C ABI
- [`sdk/protocol_spec.md`](sdk/protocol_spec.md): The wire protocol: transports, framing, commands, lifecycle, data plane
- [`sdk/python_sdk.md`](sdk/python_sdk.md): The Python SDK (`flezha` on PyPI): supported versions, encoding, concurrency, startup order
- [`sdk/ruby_sdk.md`](sdk/ruby_sdk.md): The Ruby SDK (the `flezha` gem): supported versions, the threading host, parallelism, the runner image
- [`sdk/rust_relay_architecture.md`](sdk/rust_relay_architecture.md): The Rust SDK's dispatch pipeline: channels, tasklets, cells, mailboxes, backpressure, plugin egress
- [`sdk/state_persistence.md`](sdk/state_persistence.md): For SDK implementers: how per-cell state becomes durable in the SDKs
- [`sdk/stream_transforms.md`](sdk/stream_transforms.md): Implementation notes for edge transforms: where the code lives
