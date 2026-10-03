# Flezha documentation

New to Flezha: start with [What is Flezha](getting_started/what_is_flezha.md), then
[Try it](getting_started/try_it.md). The user documentation comes in four sections,
in reading order; everything after them is grouped by who reads it.

| Section | What it is |
| --- | --- |
| [Getting started](#getting-started) | What Flezha is, and one echo running in Docker |
| [Tutorials](#tutorials) | Three example applications, run page by page: what to type and what you should see |
| [The basics](#the-basics) | One page per subject a deployment needs, each readable on its own |
| [Advanced topics](#advanced-topics) | One page per subject beyond the basics, each readable on its own |

| Directory | For | What is in it |
| --- | --- | --- |
| [`architecture/`](architecture/README.md) | everyone | The conceptual model: kernel, flow, steps, runner |
| [`api/`](api/README.md) | authors of a program that drives the controller | The published consumer surface, its three guards, and the manifest that keeps a consumer's calls resolving |
| [`sdk/`](#sdk) | operator authors, SDK implementers | Authoring contract, per-SDK surfaces, wire protocol, the shared poll core |
| [`plugins/`](#plugins) | plugin authors and users | The plugin system and every first-party plugin |
| [`operations/`](#operations) | users, contributors | Observability, the audit log, schedules, memory and benchmarking |
| [`resilience/`](#resilience) | contributors | The mechanism behind the guarantees |
| [`internals/`](#internals) | controller contributors | How the controller is built |
| [`runbooks/`](#runbooks) | users, contributors | Step-by-step tasklets for one-off operations |
| [`troubleshooting/`](#troubleshooting) | users | A symptom, what it means, and the fix |
| [`publishing/`](#publishing) | the maintainer before the first public release; every user after it | The decisions publishing makes irreversible, which of them block a release, and which parts are not yet settled |

Three references sit at the top level: [`glossary.md`](glossary.md) (vocabulary),
[`configuration.md`](configuration.md) (every knob, by layer) and
[`code_standards.md`](code_standards.md) (the rules a change to this repository is
held to, in every language it carries).

reproduces it and the output that command gave.

## Getting started

| Doc | Covers |
| --- | --- |
| [`getting_started/what_is_flezha.md`](getting_started/what_is_flezha.md) | **Start here.** What Flezha is, its pieces, and what it is for |
| [`getting_started/try_it.md`](getting_started/try_it.md) | One echo running in Docker |

## Tutorials

A tutorial runs one example application from start to finish. Its pages say what to
type and what you should see; the topic pages say how each piece works.

| Doc | Covers |
| --- | --- |
| [`tutorials.md`](tutorials/tutorials.md) | How to follow a tutorial, and what each one builds |
| [`echo/first_steps.md`](tutorials/echo/first_steps.md) | The echo: a controller, your own runner, an operator, a file on each side |
| [`tradedesk/tradedesk.md`](tutorials/tradedesk/tradedesk.md) | The tradedesk: an ETL stream and a workflow on a demo exchange, partitions, plugins, crashes |
| [`agentdesk/agentdesk.md`](tutorials/agentdesk/agentdesk.md) | The agentdesk: sessions behind HTTP, calls, a cluster, Kubernetes |

## The basics

| Doc | Covers |
| --- | --- |
| [`building_blocks.md`](topics/basics/building_blocks.md) | Tasklets, operators and runners, and the seven SDKs on one engine |
| [`configuring_an_operator.md`](topics/basics/configuring_an_operator.md) | The settings tree: how an operator runs, where its work is kept, as an ETL stream and as a workflow |
| [`streams.md`](topics/basics/streams.md) | A graph of tasklets: channels, edges, transforms and keys |
| [`workflows.md`](topics/basics/workflows.md) | Steps, retries, undo, and one instance per input |
| [`state.md`](topics/basics/state.md) | State per cell, the changes that record it, and its restore |
| [`local_mode.md`](topics/basics/local_mode.md) | An operator run in your own process, with no controller |
| [`deploying_a_controller.md`](topics/basics/deploying_a_controller.md) | The release, the Docker image, an Elixir application; shutdown and its grace periods |
| [`connecting_a_runner.md`](topics/basics/connecting_a_runner.md) | How a runner reaches its controller, and how to run one |
| [`topics/basics/running_operators.md`](topics/basics/running_operators.md) | Starting, watching and stopping operators, from `flz` and from code; what a restart brings back |
| [`topics/basics/plugins.md`](topics/basics/plugins.md) | The plugin model: enabled by name, operators bound, sharing, hooks |
| [`topics/basics/namespaces.md`](topics/basics/namespaces.md) | Namespaces, joining one from a runner, quotas, and administering one from code |
| [`topics/basics/authorization.md`](topics/basics/authorization.md) | The admin, tokens, roles, certificates and recovery |
| [`topics/basics/durability.md`](topics/basics/durability.md) | What survives what, and the durability levels |
| [`topics/basics/cli.md`](topics/basics/cli.md) | `flz` itself: reaching a controller, output formats, identity |

## Advanced topics

| Doc | Covers |
| --- | --- |
| [`topics/advanced/partitioning.md`](topics/advanced/partitioning.md) | One logical operator in N partitions: key groups, sources, rescale |
| [`merged_inputs.md`](topics/advanced/merged_inputs.md) | A tasklet fed by several channels |
| [`topics/advanced/timers.md`](topics/advanced/timers.md) | Per-cell processing-time and event-time timers |
| [`spawning_from_a_tasklet.md`](topics/advanced/spawning_from_a_tasklet.md) | A running tasklet starts or feeds a steps instance |
| [`topics/advanced/operator_calls.md`](topics/advanced/operator_calls.md) | Calling an operator and waiting for its answer |
| [`topics/advanced/sessions.md`](topics/advanced/sessions.md) | A session per user: steps, one pass per message, sleeping when idle |
| [`topics/advanced/rewinding_a_turn.md`](topics/advanced/rewinding_a_turn.md) | Taking iterations back, and a failed turn that fails alone |
| [`topics/advanced/streamed_answers.md`](topics/advanced/streamed_answers.md) | An answer sent in parts while the turn runs |
| [`installing_plugins.md`](topics/advanced/installing_plugins.md) | Plugin packages: installing, loading at boot, versions, signing |
| [`hooks.md`](topics/advanced/hooks.md) | Hooks and controller plugins |
| [`writing_a_plugin.md`](topics/advanced/writing_a_plugin.md) | Writing and packaging a plugin of your own |
| [`topics/advanced/clustering.md`](topics/advanced/clustering.md) | Several controllers as one cluster, and stopping one |
| [`topics/advanced/effectively_once.md`](topics/advanced/effectively_once.md) | The delivery guarantee, what it requires and what it costs |
| [`kubernetes.md`](topics/advanced/kubernetes.md) | Running controllers and runners on Kubernetes |

## sdk

| Doc | Covers |
| --- | --- |
| [`authoring_contract.md`](sdk/authoring_contract.md) | The three authoring tiers, signature → type inference, state → Change derivation, cell mode, the cross-SDK reference operator |
| [`naming_conventions.md`](sdk/naming_conventions.md) | The numbered naming rules every SDK surface follows |
| [`rust_operator_architecture.md`](sdk/rust_operator_architecture.md) | The Rust SDK's dispatch pipeline: channels, tasklets, cells, mailboxes, backpressure, plugin egress |
| [`state_persistence.md`](sdk/state_persistence.md) | For SDK implementers: how per-cell state becomes durable in the SDKs |
| [`elixir_sdk.md`](sdk/elixir_sdk.md) | The Elixir SDK: in-node native mode and the standalone runner |
| [`java_sdk.md`](sdk/java_sdk.md) | The Java SDK: scope, how its artifacts are built, how it is tested |
| [`node_sdk.md`](sdk/node_sdk.md) | The Node SDK (`@flezha/sdk`): supported versions, dispatch configuration, what must never block the event loop |
| [`python_sdk.md`](sdk/python_sdk.md) | The Python SDK (`flezha` on PyPI): supported versions, encoding, concurrency, startup order |
| [`ruby_sdk.md`](sdk/ruby_sdk.md) | The Ruby SDK (the `flezha` gem): supported versions, the threading host, parallelism, the runner image |
| [`php_sdk.md`](sdk/php_sdk.md) | The PHP SDK over `ext-ffi`: supported versions, sizing, the executor seam, accepted limits |
| [`conformance.md`](sdk/conformance.md) | The cross-SDK conformance suite: manifests, capabilities, the gate, adding an SDK |
| [`stream_transforms.md`](sdk/stream_transforms.md) | Implementation notes for edge transforms: where the code lives |
| [`multi_input_tasklets.md`](sdk/multi_input_tasklets.md) | Implementation notes for merged inputs: replay, where the code lives |
| [`cell_timers.md`](sdk/cell_timers.md) | Implementation notes for timers: folded state, where the code lives |
| [`partitioned_operators.md`](sdk/partitioned_operators.md) | Implementation notes for partitioning: the key-group hash and its vectors |
| [`calls.md`](sdk/calls.md) | Implementation notes for calls: the Response envelope |
| [`protocol_spec.md`](sdk/protocol_spec.md) | The wire protocol: transports, framing, commands, lifecycle, data plane |
| [`poll_core.md`](sdk/poll_core.md) | The shared `flz_core` kernel and its C ABI |

## plugins

| Doc | Covers |
| --- | --- |
| [`kafka_producer.md`](plugins/kafka_producer.md) | The Kafka producer sink, a plugin package: partitioning, idempotence, batching, the in-flight window |
| [`kafka_consumer.md`](plugins/kafka_consumer.md) | The Kafka consumer source in the same package: a consumer group's share of topics, commits by input durability, lag |
| [`rabbitmq.md`](plugins/rabbitmq.md) | The RabbitMQ consumer source and producer sink, a plugin package: acknowledgement by input durability, message ids, publisher confirms, reconnects |
| [`http_poll.md`](plugins/http_poll.md) | An HTTP resource as an operator input |
| [`http_push.md`](plugins/http_push.md) | Each output sent as one HTTP request, a guaranteed one held until a 2xx |
| [`postgres_poll.md`](plugins/postgres_poll.md) | The new, and optionally the changed, rows of a Postgres table as an operator input |
| [`postgres_cdc.md`](plugins/postgres_cdc.md) | Every row change of Postgres tables from the write-ahead log, after a first copy, a plugin package: the event, the slot and its lag, confirmation by input durability |
| [`file.md`](plugins/file.md) | A file, or an object in an S3 bucket, as an operator input and as its output |
| [`websocket_subscription.md`](plugins/websocket_subscription.md) | Changing a live socket's subscription set without reconnecting |
| [`operator_forward.md`](plugins/operator_forward.md) | One operator's output into another's input |
| [`hook_webhook.md`](plugins/hook_webhook.md) | What happens in a namespace, POSTed as JSON to a URL: the first controller plugin |
| [`http_server.md`](plugins/http_server.md) | An operator behind HTTP, and another operator's events as server-sent events (the `flezha_http` package) |

## operations

| Doc | Covers |
| --- | --- |
| [`audit_log.md`](operations/audit_log.md) | The durable JSONL audit log: layout, fsync, rotation, CLI |
| [`observability.md`](operations/observability.md) | Signals, sources, sinks, `flz observe`, Prometheus and OTLP export |
| [`schedules.md`](operations/schedules.md) | `flz schedule`: cron-driven operator spawns |
| [`memory_drift.md`](operations/memory_drift.md) | Localising controller memory growth |
| [`benchmarking.md`](operations/benchmarking.md) | `mix bench`, shapes, recorded floors, CI gates |

## resilience

| Doc | Covers |
| --- | --- |
| [`overview.md`](resilience/overview.md) | The map of every self-healing mechanism and the durability guarantee summary |
| [`data_durability.md`](resilience/data_durability.md) | The mechanism of the guarantees: a frame's guarantee class, the effectively-once tier's parts, the input floor |
| [`steps_operator.md`](resilience/steps_operator.md) | The mechanism of an operator that runs in steps: barriers, the durable loop, crash-resume |

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
| [`maturity.md`](publishing/maturity.md) | Which features are not yet settled, each SDK's support tier, and how an entry is added, graduated and removed |

## dev notes

indexes them. They are deliberately not repeated in the docs above.
