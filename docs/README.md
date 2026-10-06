# Flezha documentation

New to Flezha: start with [What is Flezha](getting_started/what_is_flezha.md), then
[Up and running](getting_started/up_and_running.md). The user documentation comes in the sections below,
in reading order, then a section on how Flezha is built and one for contributors; everything after them is grouped by who reads it.

| Section | What it is |
| --- | --- |
| [Getting started](#getting-started) | What Flezha is, and the echo built by hand |
| [Tutorials](#tutorials) | Two example applications, run page by page: what to type and what you should see |
| [Relays](#relays) | What a relay is, how it is set up, and how to start it and talk to it |
| [Kinds of relays](#kinds-of-relays) | Streams, workflows and sessions: what you build with relays |
| [Relay features](#relay-features) | Timers, calls, effects, durability, effectively once, partitions and the rest |
| [Core topics](#core-topics) | Namespaces, authorization and AI assistants: what applies to a whole deployment |
| [Plugin guides](#plugin-guides) | Connecting relays to files, HTTP, Kafka, databases and more |
| [The CLI](#the-cli) | `flz` and every command group |
| [Deploying](#deploying) | Running controllers and runners: the release, Docker, a build of your own, a cluster, Kubernetes |
| [Architecture](#architecture) | How Flezha is built: the controller, the kernel, flow and steps, the runner |
| [Contributing](#contributing) | The rules a contribution follows, the build, the tests and the release |

| Directory | For | What is in it |
| --- | --- | --- |
| [`api/`](api/README.md) | authors of a program that drives the controller | The published consumer surface, its three guards, and the manifest that keeps a consumer's calls resolving |
| [`sdk/`](#sdk) | relay authors, SDK implementers | Authoring contract, per-SDK surfaces, wire protocol, the shared poll core |
| [`plugins/`](#plugins) | plugin authors and users | The plugin system and every first-party plugin |
| [`operations/`](#operations) | users, contributors | Observability, the audit log, schedules, memory and benchmarking |
| [`resilience/`](#resilience) | contributors | The mechanism behind the guarantees |
| [`internals/`](#internals) | controller contributors | How the controller is built |
| [`runbooks/`](#runbooks) | users, contributors | Step-by-step tasklets for one-off operations |
| [`troubleshooting/`](#troubleshooting) | users | A symptom, what it means, and the fix |
| [`publishing/`](#publishing) | the maintainer before the first public release; every user after it | The decisions publishing makes irreversible, which of them block a release, and which parts are not yet settled |

Two references sit at the top level: [`glossary.md`](glossary.md) (vocabulary) and
[`configuration.md`](configuration.md) (every knob, by layer, one page per layer
under `configuration/`).

reproduces it and the output that command gave.

## Getting started

| Doc | Covers |
| --- | --- |
| [`getting_started/what_is_flezha.md`](getting_started/what_is_flezha.md) | **Start here.** What Flezha is, its pieces, and what it is for |
| [`getting_started/up_and_running.md`](getting_started/up_and_running.md) | The echo built by hand: how to follow the steps, a controller, your own runner, a relay |
| [`getting_started/connect_a_plugin.md`](getting_started/connect_a_plugin.md) | A file on each side of the echo, through the file plugins |
| [`getting_started/stop_and_clean_up.md`](getting_started/stop_and_clean_up.md) | Everything the echo started, stopped in reverse order |

## Tutorials

A tutorial runs one example application from start to finish. Its pages say what to
type and what you should see; the topic pages say how each piece works.

| Doc | Covers |
| --- | --- |
| [`tutorials.md`](tutorials/tutorials.md) | What each tutorial builds |
| [`tradedesk/tradedesk.md`](tutorials/tradedesk/tradedesk.md) | The tradedesk: an ETL stream and a workflow on a demo exchange, partitions, plugins, crashes |
| [`agentdesk/agentdesk.md`](tutorials/agentdesk/agentdesk.md) | The agentdesk: sessions behind HTTP, calls, a cluster, Kubernetes |

## Relays

What a relay is, how it is set up, and how to start it and talk to it.

| Doc | Covers |
| --- | --- |
| [`building_blocks.md`](topics/basics/building_blocks.md) | Tasklets, relays and runners, and the seven SDKs on one engine |
| [`state.md`](topics/basics/state.md) | State per cell, the changes that record it, and its restore |
| [`configuring_a_relay.md`](topics/basics/configuring_a_relay.md) | The settings tree: how a relay runs, where its work is kept, as an ETL stream and as a workflow |
| [`running_relays.md`](topics/basics/running_relays.md) | Starting, watching and stopping relays, from `flz` and from code; what a restart brings back |
| [`relay_api.md`](topics/basics/relay_api.md) | A relay's API: describing its channels, reading it with `flz relay info`, and calling it from `flz`, HTTP, an SDK or an AI agent's tools |

## Kinds of relays

What you build with relays.

| Doc | Covers |
| --- | --- |
| [`streams.md`](topics/basics/streams.md) | A graph of tasklets: channels, edges, transforms and keys |
| [`workflows.md`](topics/basics/workflows.md) | Steps, retries, undo, and one instance per input |
| [`sessions.md`](topics/advanced/sessions.md) | A session per user: steps, one pass per message, sleeping when idle |
| [`rewinding_a_turn.md`](topics/advanced/rewinding_a_turn.md) | Taking iterations back, and a failed turn that fails alone |

## Relay features

What a relay can do beyond its graph, and what Flezha guarantees for it.

| Doc | Covers |
| --- | --- |
| [`timers.md`](topics/advanced/timers.md) | Per-cell processing-time and event-time timers |
| [`merged_inputs.md`](topics/advanced/merged_inputs.md) | A tasklet fed by several channels |
| [`recorded_effects.md`](topics/advanced/recorded_effects.md) | Keeping an outside call's result so a re-run reads it instead of calling again |
| [`streamed_answers.md`](topics/advanced/streamed_answers.md) | An answer sent in parts while the turn runs |
| [`spawning_from_a_tasklet.md`](topics/advanced/spawning_from_a_tasklet.md) | A running tasklet starts or feeds a steps instance |
| [`relay_calls.md`](topics/advanced/relay_calls.md) | Calling a relay and waiting for its answer |
| [`sending_from_runner_code.md`](topics/advanced/sending_from_runner_code.md) | Sending to a relay from a runner's own code, through the controller or locally |
| [`durability.md`](topics/basics/durability.md) | What survives what, and the durability levels |
| [`effectively_once.md`](topics/advanced/effectively_once.md) | The delivery guarantee, what it requires and what it costs |
| [`partitioning.md`](topics/advanced/partitioning.md) | One logical relay in N partitions: key groups, sources, rescale |
| [`local_mode.md`](topics/basics/local_mode.md) | Testing a relay in your own process, with no controller |

## Core topics

What applies to a whole deployment rather than one relay.

| Doc | Covers |
| --- | --- |
| [`namespaces.md`](topics/basics/namespaces.md) | Namespaces, joining one from a runner, quotas, and administering one from code |
| [`authorization.md`](topics/basics/authorization.md) | The admin, tokens, roles, certificates and recovery |
| [`working_with_ai_assistants.md`](topics/working_with_ai_assistants.md) | `flz mcp`: an AI assistant gets the rules, the current SDK form and the operations |

## Plugin guides

Connecting relays to the outside; every plugin's own page is under [`plugins/`](#plugins).

| Doc | Covers |
| --- | --- |
| [`plugins.md`](topics/plugins/plugins.md) | The plugin model: installed, enabled by name, relays bound; the plugins that ship |
| [`where_plugins_run.md`](topics/plugins/where_plugins_run.md) | Shared and single copies, partitioned relays, when a copy stops, durability |
| [`enabling_and_binding.md`](topics/plugins/enabling_and_binding.md) | The `flz plugin` commands, their flags, hooks, updates, seeing what runs |
| [`plugins_from_code.md`](topics/plugins/plugins_from_code.md) | Enabling and binding plugins from an SDK's `control()` handle |
| [`plugin_examples.md`](topics/plugins/plugin_examples.md) | The echo's file plugins and the tradedesk's live feed; limits and gotchas |
| [`installing_plugins.md`](topics/plugins/installing_plugins.md) | Plugin packages: installing, loading at boot, versions, signing |
| [`hooks.md`](topics/plugins/hooks.md) | Hooks and controller plugins |
| [`writing_a_plugin.md`](topics/plugins/writing_a_plugin.md) | Writing and packaging a plugin of your own |

## The CLI

| Doc | Covers |
| --- | --- |
| [`cli.md`](topics/cli/cli.md) | `flz` itself: reaching a controller, output formats, identity |
| [`controller_commands.md`](topics/cli/controller_commands.md) | Starting and stopping the controller, checking that it is up, its runners, and clearing its data |
| [`relay_commands.md`](topics/cli/relay_commands.md) | Starting and stopping relays, sending them input, reading their state and their dead letters |
| [`plugin_commands.md`](topics/cli/plugin_commands.md) | Installing plugin packages, enabling plugins and binding relays to them |
| [`access_commands.md`](topics/cli/access_commands.md) | Namespaces and quotas, the first admin, tokens, roles, and the audit trail |
| [`template_commands.md`](topics/cli/template_commands.md) | A new project in any of the seven languages, from `flz template create` |
| [`command_reference.md`](topics/cli/command_reference.md) | Every `flz` verb in one line, by group |

## Deploying

| Doc | Covers |
| --- | --- |
| [`deploying_a_controller.md`](topics/deploying/deploying_a_controller.md) | The release, the Docker image, an Elixir application; shutdown and its grace periods |
| [`docker.md`](topics/deploying/docker.md) | The container's start and stop, what to mount, a controller and a runner in Compose, `flz` in the container |
| [`building_from_source.md`](topics/deploying/building_from_source.md) | A release built from a checkout, and the `make` targets |
| [`connecting_a_runner.md`](topics/deploying/connecting_a_runner.md) | How a runner reaches its controller, and how to run one |
| [`clustering.md`](topics/deploying/clustering.md) | Several controllers as one cluster, and stopping one |
| [`kubernetes.md`](topics/deploying/kubernetes.md) | Running controllers and runners on Kubernetes |

## Architecture

| Doc | Covers |
| --- | --- |
| [`overview.md`](architecture/overview.md) | The roles, the workload model, the kernel invariants, the failure model |
| [`kernel.md`](architecture/kernel.md) | The durable substrate under every relay: the write path, snapshots, delivery, flow control, fencing, health |
| [`flow_profile.md`](architecture/flow_profile.md) | A relay that runs in flow: per-cell recovery, checkpoints, late input, partitions |
| [`steps_profile.md`](architecture/steps_profile.md) | A relay that runs in steps: stages, the task journal, undo, loops, sleeping, rewind |
| [`runner.md`](architecture/runner.md) | Inside a runner: the shared Rust kernel and the host SDK |

## Contributing

| Doc | Covers |
| --- | --- |
| [`contributing/contributing.md`](contributing/contributing.md) | The rules a contribution follows: the CLA, a pull request, commit messages, review |
| [`contributing/setting_up.md`](contributing/setting_up.md) | The tools, the clone, the build, and the development containers |
| [`contributing/testing.md`](contributing/testing.md) | The test suites, one SDK at a time, and CI on your own machine |
| [`contributing/code_standards.md`](contributing/code_standards.md) | The rules a change is held to, the formatters and the gates |
| [`contributing/releasing.md`](contributing/releasing.md) | The release artifacts and how they are built |

## sdk

| Doc | Covers |
| --- | --- |
| [`authoring_contract.md`](sdk/authoring_contract.md) | The three authoring tiers, signature → type inference, state → Change derivation, cell mode, the cross-SDK reference relay |
| [`naming_conventions.md`](sdk/naming_conventions.md) | The numbered naming rules every SDK surface follows |
| [`rust_relay_architecture.md`](sdk/rust_relay_architecture.md) | The Rust SDK's dispatch pipeline: channels, tasklets, cells, mailboxes, backpressure, plugin egress |
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
| [`partitioned_relays.md`](sdk/partitioned_relays.md) | Implementation notes for partitioning: the key-group hash and its vectors |
| [`calls.md`](sdk/calls.md) | Implementation notes for calls: the Response envelope |
| [`protocol_spec.md`](sdk/protocol_spec.md) | The wire protocol: transports, framing, commands, lifecycle, data plane |
| [`poll_core.md`](sdk/poll_core.md) | The shared `flz_core` kernel and its C ABI |

## plugins

| Doc | Covers |
| --- | --- |
| [`kafka_producer.md`](plugins/kafka_producer.md) | The Kafka producer output, a plugin package: partitioning, idempotence, batching, the in-flight window |
| [`kafka_consumer.md`](plugins/kafka_consumer.md) | The Kafka consumer source in the same package: a consumer group's share of topics, commits by input durability, lag |
| [`rabbitmq.md`](plugins/rabbitmq.md) | The RabbitMQ consumer source and producer output, a plugin package: acknowledgement by input durability, message ids, publisher confirms, reconnects |
| [`http_poll.md`](plugins/http_poll.md) | An HTTP resource as a relay input |
| [`http_push.md`](plugins/http_push.md) | Each output sent as one HTTP request, a guaranteed one held until a 2xx |
| [`postgres_poll.md`](plugins/postgres_poll.md) | The new, and optionally the changed, rows of a Postgres table as a relay input |
| [`postgres_cdc.md`](plugins/postgres_cdc.md) | Every row change of Postgres tables from the write-ahead log, after a first copy, a plugin package: the event, the slot and its lag, confirmation by input durability |
| [`file.md`](plugins/file.md) | A file, or an object in an S3 bucket, as a relay input and as its output |
| [`websocket_subscription.md`](plugins/websocket_subscription.md) | Changing a live socket's subscription set without reconnecting |
| [`relay_forward.md`](plugins/relay_forward.md) | One relay's output into another's input |
| [`hook_webhook.md`](plugins/hook_webhook.md) | What happens in a namespace, POSTed as JSON to a URL: the first controller plugin |
| [`http_server.md`](plugins/http_server.md) | A relay behind HTTP, and another relay's events as server-sent events (the `flezha_http` package) |
| [`http_api.md`](plugins/http_api.md) | The controller's operations over HTTP, as MCP tools for AI assistants and as plain JSON, for a token's principal (the `flezha_http` package) |

## operations

| Doc | Covers |
| --- | --- |
| [`audit_log.md`](operations/audit_log.md) | The durable JSONL audit log: layout, fsync, rotation, CLI |
| [`observability.md`](operations/observability.md) | Signals, sources, sinks, `flz observe`, Prometheus and OTLP export |
| [`schedules.md`](operations/schedules.md) | `flz schedule`: cron-driven relay spawns |
| [`memory_drift.md`](operations/memory_drift.md) | Localising controller memory growth |
| [`benchmarking.md`](operations/benchmarking.md) | `mix bench`, shapes, recorded floors, CI gates |

## resilience

| Doc | Covers |
| --- | --- |
| [`overview.md`](resilience/overview.md) | The map of every self-healing mechanism and the durability guarantee summary |
| [`data_durability.md`](resilience/data_durability.md) | The mechanism of the guarantees: a frame's guarantee class, the effectively-once tier's parts, the input floor |
| [`steps_relay.md`](resilience/steps_relay.md) | The mechanism of a relay that runs in steps: barriers, the durable loop, crash-resume |

## internals

| Doc | Covers |
| --- | --- |
| [`process_hub.md`](internals/process_hub.md) | ProcessHub as the source of truth: hubs per namespace, recovery as reconcile, handover |
| [`overwatch_lifecycle.md`](internals/overwatch_lifecycle.md) | The per-relay Overwatch: boot sequence, ping before `AddRelay`, the retry ladder and parked-error state, ingest entry points |
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
