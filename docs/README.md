# Flezha documentation

Start with [`architecture/`](architecture/README.md) — the durable-compute kernel, the two
operative profiles and the runner model. Everything else is grouped by who reads it.

| Directory | For | What is in it |
| --- | --- | --- |
| [`architecture/`](architecture/README.md) | everyone | The conceptual model: kernel, stream profile, workflow profile, runner |
| [`sdk/`](#sdk) | operative authors, SDK implementers | Authoring contract, per-SDK surfaces, wire protocol, the shared poll core |
| [`plugins/`](#plugins) | plugin authors and users | The plugin system and every first-party plugin |
| [`operations/`](#operations) | operators | Deploying, configuring, securing, observing and recovering a controller |
| [`resilience/`](#resilience) | everyone | What survives what — the guarantee-level docs |
| [`internals/`](#internals) | controller contributors | How the controller is built |
| [`runbooks/`](#runbooks) | operators, contributors | Step-by-step procedures for one-off operations |
| [`dev_notes/`](#dev-notes) | contributors | Per-change gotchas, indexed in the repo `CLAUDE.md` |

Two references sit at the top level: [`glossary.md`](glossary.md) (vocabulary) and
[`configuration.md`](configuration.md) (every knob, by layer).

[`known_issues.md`](known_issues.md) is the open-defect register rather than a reference:
defects found but not fixed where they were found, each carrying a command that
reproduces it and the output that command gave.

## sdk

| Doc | Covers |
| --- | --- |
| [`authoring_contract.md`](sdk/authoring_contract.md) | The three authoring tiers, signature → type inference, state → Change derivation, entity mode, the cross-SDK reference operative |
| [`naming_conventions.md`](sdk/naming_conventions.md) | The numbered naming rules every SDK surface follows |
| [`rust_operative_architecture.md`](sdk/rust_operative_architecture.md) | The Rust SDK's dispatch pipeline: channels, procedures, entities, mailboxes, backpressure, plugin egress |
| [`state_persistence.md`](sdk/state_persistence.md) | Per-Entity state, Change, snapshot/WAL restore and compaction — Rust and Java surfaces |
| [`elixir_sdk.md`](sdk/elixir_sdk.md) | The Elixir SDK: in-node native mode and the standalone runner |
| [`java_sdk.md`](sdk/java_sdk.md) | The Java SDK: scope, how its artifacts are built, how it is tested |
| [`node_sdk.md`](sdk/node_sdk.md) | The Node SDK (`@flezha/sdk`): supported versions, dispatch configuration, what must never block the event loop |
| [`python_sdk.md`](sdk/python_sdk.md) | The Python SDK (`flezha` on PyPI): supported versions, encoding, concurrency, startup order |
| [`ruby_sdk.md`](sdk/ruby_sdk.md) | The Ruby SDK (the `flezha` gem): supported versions, the threading host, parallelism, the runner image |
| [`php_sdk.md`](sdk/php_sdk.md) | The PHP SDK over `ext-ffi`: supported versions, sizing, the executor seam, accepted limits |
| [`conformance.md`](sdk/conformance.md) | The cross-SDK conformance suite: manifests, capabilities, the gate, adding an SDK |
| [`stream_transforms.md`](sdk/stream_transforms.md) | Edge transforms and `key_by` keying |
| [`multi_input_procedures.md`](sdk/multi_input_procedures.md) | Merged subscriptions: what a merge promises |
| [`entity_timers.md`](sdk/entity_timers.md) | Per-Entity processing-time and event-time timers |
| [`entry_input_idle.md`](sdk/entry_input_idle.md) | The per-entry-channel idle signal (`on_entry_idle`) |
| [`procedure_requested_spawn.md`](sdk/procedure_requested_spawn.md) | `ctx.spawn_workflow`: a running procedure asks for a worker |
| [`partitioned_operatives.md`](sdk/partitioned_operatives.md) | One logical operative, N instances: key groups, sources, rescale |
| [`protocol_spec.md`](sdk/protocol_spec.md) | The wire protocol: transports, framing, commands, lifecycle, data plane |
| [`poll_core.md`](sdk/poll_core.md) | The shared `flz_core` kernel and its C ABI |

## plugins

| Doc | Covers |
| --- | --- |
| [`installation.md`](plugins/installation.md) | `.fpz` packages, manifests, signing, `flz plugin` lifecycle |
| [`lifecycle.md`](plugins/lifecycle.md) | `init_provider/1` vs `activate/1`, host identity, teardown |
| [`kafka_producer.md`](plugins/kafka_producer.md) | The Kafka producer sink: idempotence, batching, the in-flight window |
| [`http_poll.md`](plugins/http_poll.md) | An HTTP resource as an operative input |
| [`websocket_subscription.md`](plugins/websocket_subscription.md) | Changing a live socket's subscription set without reconnecting |
| [`operative_forward.md`](plugins/operative_forward.md) | One operative's output into another's input |
| [`init_state_providers.md`](plugins/init_state_providers.md) | Cold-start state from a provider chain |
| [`message_handlers.md`](plugins/message_handlers.md) | Request/response messages into a running operative |

## operations

| Doc | Covers |
| --- | --- |
| [`deployment.md`](operations/deployment.md) | Docker dev loop, production image, Kubernetes |
| [`authn_authz.md`](operations/authn_authz.md) | Cookie and certificates, bootstrap, tokens, roles, namespaces, recovery |
| [`audit_log.md`](operations/audit_log.md) | The durable JSONL audit log: layout, fsync, rotation, CLI |
| [`observability.md`](operations/observability.md) | Signals, sources, sinks, `flz observe`, Prometheus and OTLP export |
| [`graceful_shutdown.md`](operations/graceful_shutdown.md) | Quiesce → drain → finalise, orchestrator grace periods, `flz controller stop` |
| [`auto_restore_on_boot.md`](operations/auto_restore_on_boot.md) | How a controller restores its operatives after any restart |
| [`remote_durability_tier.md`](operations/remote_durability_tier.md) | Surviving the loss of a controller's disk: backends, RPO, recovery, drills |
| [`reset.md`](operations/reset.md) | `flz reset`: wiping runtime data |
| [`schedules.md`](operations/schedules.md) | `flz schedule`: cron-driven operative spawns |
| [`memory_drift.md`](operations/memory_drift.md) | Localising controller memory growth |
| [`benchmarking.md`](operations/benchmarking.md) | `mix bench`, shapes, recorded floors, CI gates |

## resilience

| Doc | Covers |
| --- | --- |
| [`overview.md`](resilience/overview.md) | The map of every self-healing mechanism and the durability guarantee summary |
| [`data_durability.md`](resilience/data_durability.md) | What can be lost, when, and how to avoid it — delivery semantics, DLQ, late-input correction |
| [`workflow_operative.md`](resilience/workflow_operative.md) | Workflow structure, lifecycle, durable loop, retry, rewind, saga, crash-resume |

## internals

| Doc | Covers |
| --- | --- |
| [`process_hub.md`](internals/process_hub.md) | ProcessHub as the source of truth: hubs per namespace, recovery as reconcile, handover |
| [`overwatch_lifecycle.md`](internals/overwatch_lifecycle.md) | The per-operative Overwatch: boot sequence, ping before `AddOperative`, the retry ladder and parked-error state, ingest entry points |
| [`state_persistence.md`](internals/state_persistence.md) | The controller's WAL/snapshot store, flow control, `flz state` |
| [`convergence_loop.md`](internals/convergence_loop.md) | The self-heal loop: tick pipeline, heal primitives, /healthz |
| [`transport_liveness.md`](internals/transport_liveness.md) | Session liveness and data-plane stall localisation |
| [`runner_identity.md`](internals/runner_identity.md) | Runner identity, binding swap on reconnect, WAL owner fence |
| [`nif_transport.md`](internals/nif_transport.md) | The `flz_nif` UDS/QUIC drivers and their Elixir integration |

## runbooks

| Doc | Covers |
| --- | --- |
| [`purge-build-artifacts.md`](runbooks/purge-build-artifacts.md) | One-time removal of build output from git history: what it removes, the rewrite, verifying by measurement, publishing |

## dev notes

`dev_notes/<change-id>.md` holds the gotchas of one landed change each; the repo `CLAUDE.md`
indexes them. They are deliberately not repeated in the docs above.
