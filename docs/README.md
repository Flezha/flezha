# Flezha documentation

Start with [`architecture/`](architecture/README.md) — the durable-compute kernel, the two
operator profiles and the runner model. Everything else is grouped by who reads it.

| Directory | For | What is in it |
| --- | --- | --- |
| [`guides/`](#guides) | someone deploying Flezha for the first time | End-to-end walkthroughs: build, start, connect, run |
| [`architecture/`](architecture/README.md) | everyone | The conceptual model: kernel, stream profile, bounded profile, runner |
| [`api/`](api/README.md) | authors of a program that drives the controller | The published consumer surface, its three guards, and the manifest that keeps a consumer's calls resolving |
| [`sdk/`](#sdk) | operator authors, SDK implementers | Authoring contract, per-SDK surfaces, wire protocol, the shared poll core |
| [`plugins/`](#plugins) | plugin authors and users | The plugin system and every first-party plugin |
| [`operations/`](#operations) | users | Deploying, configuring, securing, observing and recovering a controller |
| [`resilience/`](#resilience) | everyone | What survives what — the guarantee-level docs |
| [`internals/`](#internals) | controller contributors | How the controller is built |
| [`runbooks/`](#runbooks) | users, contributors | Step-by-step tasklets for one-off operations |
| [`troubleshooting/`](#troubleshooting) | users | A symptom, what it means, and the fix |
| [`publishing/`](#publishing) | the maintainer, before the first public release | The decisions publishing makes irreversible, and which of them block a release |

Three references sit at the top level: [`glossary.md`](glossary.md) (vocabulary),
[`configuration.md`](configuration.md) (every knob, by layer) and
[`code_standards.md`](code_standards.md) (the rules a change to this repository is
held to, in every language it carries).

reproduces it and the output that command gave.

## guides

A guide walks one path from start to finish. The reference docs it points at
answer "what are the options"; a guide answers "what do I type".

| Doc | Covers |
| --- | --- |
| [`what_is_flezha.md`](guides/what_is_flezha.md) | **Start here.** What Flezha is, its pieces, and what it is for |
| [`try_it.md`](guides/try_it.md) | One echo running in Docker, and running an operator without a controller |
| [`guides.md`](guides/guides.md) | How to follow a guide, and what each of the three parts builds |
| [`first_steps.md`](guides/first_steps.md) | Part 1. What it builds, what you need, and its pages |
| [`controller_deployment.md`](guides/controller_deployment.md) | Part 1. Running one controller: the Docker image, the release binaries, or your own Elixir application |
| [`runner_deployment.md`](guides/runner_deployment.md) | Part 1. Reaching the controller, and your own runner registering an operator factory |
| [`running_operators.md`](guides/running_operators.md) | Part 1. Starting an operator, watching it, stopping it, and what survives a restart |
| [`sources_and_sinks.md`](guides/sources_and_sinks.md) | Part 1. Attaching a real input and a real output with the file plugin, and how a plugin is enabled and disabled |
| [`cli.md`](guides/cli.md) | Reference. `flz` itself: reaching a controller, output formats, identity for a gated verb |
| [`going_further.md`](guides/tradedesk/going_further.md) | Part 2. The tradedesk it builds, what you need, and its pages |
| [`authorization.md`](guides/tradedesk/authorization.md) | Part 2. Claiming the admin, tokens, the runner's key and roles |
| [`namespaces.md`](guides/tradedesk/namespaces.md) | Part 2. A namespace of your own, joining it from a runner, and quotas |
| [`building_an_etl_stream.md`](guides/tradedesk/building_an_etl_stream.md) | Part 2. An ETL stream, built as an unbounded operator: chained tasklets, transforms and state |
| [`building_a_workflow.md`](guides/tradedesk/building_a_workflow.md) | Part 2. A workflow, built as a bounded operator: steps, retries and compensation |
| [`partitioning.md`](guides/tradedesk/partitioning.md) | Part 2. Partitions, keys and scaling |
| [`plugins.md`](guides/tradedesk/plugins.md) | Part 2. The plugin system and the plugins that ship |
| [`durability.md`](guides/tradedesk/durability.md) | Part 2. What survives a crash |
| [`advanced_topics.md`](guides/advanced/advanced_topics.md) | Part 3. What it covers, what you need, and its pages |
| [`clustering.md`](guides/advanced/clustering.md) | Part 3. Setting up the example in Docker, and more than one machine |
| [`custom_plugins.md`](guides/advanced/custom_plugins.md) | Part 3. Writing and installing your own plugin |
| [`full_form_tasklets.md`](guides/advanced/full_form_tasklets.md) | Part 3. A tasklet without the annotations |
| [`effectively_once.md`](guides/advanced/effectively_once.md) | Part 3. The delivery guarantee and its cost |
| [`erlang_vm_tuning.md`](guides/advanced/erlang_vm_tuning.md) | Part 3. Settings for a controller under load |
| [`elixir_applications.md`](guides/advanced/elixir_applications.md) | Part 3. Flezha inside an Elixir application of your own |
| [`kubernetes_deployment.md`](guides/advanced/kubernetes_deployment.md) | Part 3. The same images on k3s |

## sdk

| Doc | Covers |
| --- | --- |
| [`authoring_contract.md`](sdk/authoring_contract.md) | The three authoring tiers, signature → type inference, state → Change derivation, cell mode, the cross-SDK reference operator |
| [`naming_conventions.md`](sdk/naming_conventions.md) | The numbered naming rules every SDK surface follows |
| [`rust_operator_architecture.md`](sdk/rust_operator_architecture.md) | The Rust SDK's dispatch pipeline: channels, tasklets, cells, mailboxes, backpressure, plugin egress |
| [`state_persistence.md`](sdk/state_persistence.md) | Per-Cell state, Change, snapshot/WAL restore and compaction — Rust and Java surfaces |
| [`elixir_sdk.md`](sdk/elixir_sdk.md) | The Elixir SDK: in-node native mode and the standalone runner |
| [`java_sdk.md`](sdk/java_sdk.md) | The Java SDK: scope, how its artifacts are built, how it is tested |
| [`node_sdk.md`](sdk/node_sdk.md) | The Node SDK (`@flezha/sdk`): supported versions, dispatch configuration, what must never block the event loop |
| [`python_sdk.md`](sdk/python_sdk.md) | The Python SDK (`flezha` on PyPI): supported versions, encoding, concurrency, startup order |
| [`ruby_sdk.md`](sdk/ruby_sdk.md) | The Ruby SDK (the `flezha` gem): supported versions, the threading host, parallelism, the runner image |
| [`php_sdk.md`](sdk/php_sdk.md) | The PHP SDK over `ext-ffi`: supported versions, sizing, the executor seam, accepted limits |
| [`conformance.md`](sdk/conformance.md) | The cross-SDK conformance suite: manifests, capabilities, the gate, adding an SDK |
| [`stream_transforms.md`](sdk/stream_transforms.md) | Edge transforms and `key_by` keying |
| [`multi_input_tasklets.md`](sdk/multi_input_tasklets.md) | Merged subscriptions: what a merge promises |
| [`cell_timers.md`](sdk/cell_timers.md) | Per-Cell processing-time and event-time timers |
| [`tasklet_requested_spawn.md`](sdk/tasklet_requested_spawn.md) | `ctx.spawn_bounded`: a running tasklet asks for a worker |
| [`admin.md`](sdk/admin.md) | The surface an application administers its own namespace with, and the role that grants it |
| [`partitioned_operators.md`](sdk/partitioned_operators.md) | One logical operator, N instances: key groups, sources, rescale |
| [`protocol_spec.md`](sdk/protocol_spec.md) | The wire protocol: transports, framing, commands, lifecycle, data plane |
| [`poll_core.md`](sdk/poll_core.md) | The shared `flz_core` kernel and its C ABI |

## plugins

| Doc | Covers |
| --- | --- |
| [`installation.md`](plugins/installation.md) | Plugin packages: building, installing, loading at boot, statuses, errors, signing |
| [`lifecycle.md`](plugins/lifecycle.md) | `init_provider/1` vs `activate/1`, host identity, teardown |
| [`kafka_producer.md`](plugins/kafka_producer.md) | The Kafka producer sink: idempotence, batching, the in-flight window |
| [`http_poll.md`](plugins/http_poll.md) | An HTTP resource as an operator input |
| [`file.md`](plugins/file.md) | A file, or an object in an S3 bucket, as an operator input and as its output |
| [`websocket_subscription.md`](plugins/websocket_subscription.md) | Changing a live socket's subscription set without reconnecting |
| [`operator_forward.md`](plugins/operator_forward.md) | One operator's output into another's input |
| [`http_server.md`](plugins/http_server.md) | An operator behind HTTP, and another operator's events as server-sent events (the `flezha_http` package) |
| [`init_state_providers.md`](plugins/init_state_providers.md) | Cold-start state from a provider chain |
| [`message_handlers.md`](plugins/message_handlers.md) | Request/response messages into a running operator |

## operations

| Doc | Covers |
| --- | --- |
| [`deployment/README.md`](operations/deployment/README.md) | Docker dev loop, production image, Kubernetes |
| [`authn_authz.md`](operations/authn_authz.md) | Cookie and certificates, bootstrap, tokens, roles, namespaces, recovery |
| [`audit_log.md`](operations/audit_log.md) | The durable JSONL audit log: layout, fsync, rotation, CLI |
| [`observability.md`](operations/observability.md) | Signals, sources, sinks, `flz observe`, Prometheus and OTLP export |
| [`graceful_shutdown.md`](operations/graceful_shutdown.md) | Quiesce → drain → finalise, orchestrator grace periods, `flz controller stop` |
| [`auto_restore_on_boot.md`](operations/auto_restore_on_boot.md) | How a controller restores its operators after any restart |
| [`reset.md`](operations/reset.md) | `flz reset`: wiping runtime data |
| [`schedules.md`](operations/schedules.md) | `flz schedule`: cron-driven operator spawns |
| [`memory_drift.md`](operations/memory_drift.md) | Localising controller memory growth |
| [`benchmarking.md`](operations/benchmarking.md) | `mix bench`, shapes, recorded floors, CI gates |

## resilience

| Doc | Covers |
| --- | --- |
| [`overview.md`](resilience/overview.md) | The map of every self-healing mechanism and the durability guarantee summary |
| [`data_durability.md`](resilience/data_durability.md) | What can be lost, when, and how to avoid it — delivery semantics, DLQ, late-input correction |
| [`bounded_operator.md`](resilience/bounded_operator.md) | Bounded operator structure, lifecycle, durable loop, retry, rewind, saga, crash-resume |

## internals

| Doc | Covers |
| --- | --- |
| [`process_hub.md`](internals/process_hub.md) | ProcessHub as the source of truth: hubs per namespace, recovery as reconcile, handover |
| [`overwatch_lifecycle.md`](internals/overwatch_lifecycle.md) | The per-operator Overwatch: boot sequence, ping before `AddOperator`, the retry ladder and parked-error state, ingest entry points |
| [`state_persistence.md`](internals/state_persistence.md) | The controller's WAL/snapshot store, flow control, `flz state` |
| [`convergence_loop.md`](internals/convergence_loop.md) | The self-heal loop: tick pipeline, heal primitives, /healthz |
| [`transport_liveness.md`](internals/transport_liveness.md) | Session liveness and data-plane stall localisation |
| [`runner_identity.md`](internals/runner_identity.md) | Runner identity, binding swap on reconnect, WAL owner fence |
| [`nif_transport.md`](internals/nif_transport.md) | The `flz_nif` UDS/QUIC drivers and their Elixir integration |

## runbooks

| Doc | Covers |
| --- | --- |
| [`purge-build-artifacts.md`](runbooks/purge-build-artifacts.md) | One-time removal of build output from git history: what it removes, the rewrite, verifying by measurement, publishing |

## troubleshooting

| Doc | Covers |
| --- | --- |
| [`controller_will_not_start.md`](troubleshooting/controller_will_not_start.md) | A name already in use, a wedged node whose socket still answers, and every boot refusal |
| [`runner_cannot_connect.md`](troubleshooting/runner_cannot_connect.md) | Every `flz runner check` verdict and its fix, the 60-byte socket path cap, and why reaching the socket is not the same as being admitted |

## publishing

| Doc | Covers |
| --- | --- |
| [`readiness.md`](publishing/readiness.md) | Every decision the first public release makes irreversible: the blocking/owed test, the record, and the decisions the owner owes |

## dev notes

indexes them. They are deliberately not repeated in the docs above.
