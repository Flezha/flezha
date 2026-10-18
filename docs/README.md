# Flezha documentation

New to Flezha: start with [What is Flezha](public/getting_started/what_is_flezha.md), then
[Up and running](public/getting_started/up_and_running.md). The user documentation comes in the sections below,
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
| [`api/`](internal/api/README.md) | authors of a program that drives the controller | The published consumer surface, its three guards, and the manifest that keeps a consumer's calls resolving |
| [`sdk/`](#sdk) | relay authors, SDK implementers | Authoring contract, per-SDK surfaces, wire protocol, the shared poll core |
| [`plugins/`](#plugins) | plugin authors and users | The plugin system and every first-party plugin |
| [`operations/`](#operations) | users, contributors | Observability, the audit log, schedules, memory and benchmarking |
| [`resilience/`](#resilience) | contributors | The mechanism behind the guarantees |
| [`internals/`](#internals) | controller contributors | How the controller is built |
| [`troubleshooting/`](#troubleshooting) | users | A symptom, what it means, and the fix |
| [`publishing/`](#publishing) | the maintainer before the first public release; every user after it | The decisions publishing makes irreversible, which of them block a release, and which parts are not yet settled |

Two references sit at the top level: [`internal/glossary.md`](internal/glossary.md) (vocabulary) and
[`public/configuration.md`](public/configuration.md) (every knob, by layer, one page per layer
under `configuration/`).

reproduces it and the output that command gave.

## Getting started

| Doc | Covers |
| --- | --- |
| [`public/getting_started/what_is_flezha.md`](public/getting_started/what_is_flezha.md) | **Start here.** What Flezha is, its pieces, and what it is for |
| [`public/getting_started/up_and_running.md`](public/getting_started/up_and_running.md) | The echo built by hand: how to follow the steps, a controller, your own runner, a relay |
| [`public/getting_started/connect_a_plugin.md`](public/getting_started/connect_a_plugin.md) | A file on each side of the echo, through the file plugins |
| [`public/getting_started/stop_and_clean_up.md`](public/getting_started/stop_and_clean_up.md) | Everything the echo started, stopped in reverse order |

## Tutorials

A tutorial runs one example application from start to finish. Its pages say what to
type and what you should see; the topic pages say how each piece works.

| Doc | Covers |
| --- | --- |
| [`tutorials.md`](public/tutorials/tutorials.md) | What each tutorial builds |
| [`tradedesk/tradedesk.md`](public/tutorials/tradedesk/tradedesk.md) | The tradedesk: an ETL stream and a workflow on a demo exchange, partitions, plugins, crashes |
| [`agentdesk/agentdesk.md`](public/tutorials/agentdesk/agentdesk.md) | The agentdesk: sessions behind HTTP, calls, a cluster, Kubernetes |

## Relays

What a relay is, how it is set up, and how to start it and talk to it.

| Doc | Covers |
| --- | --- |
| [`building_blocks.md`](public/topics/core/building_blocks.md) | Tasklets, relays and runners, and the seven SDKs on one engine |
| [`state.md`](public/topics/core/state.md) | State per cell, the changes that record it, and its restore |
| [`relay_settings.md`](public/configuration/relay_settings.md) | The settings tree: how a relay runs, where its work is kept, as an ETL stream and as a workflow |
| [`running_and_testing.md`](public/topics/core/running_and_testing.md) | Starting, watching and stopping relays, from `flz` and from code; what a restart brings back |
| [`sending_and_calling.md`](public/topics/core/sending_and_calling.md#a-relay-s-api) | A relay's API: describing its channels, reading it with `flz relay info`, and calling it from `flz`, HTTP, an SDK or an AI agent's tools |

## Kinds of relays

What you build with relays.

| Doc | Covers |
| --- | --- |
| [`streams.md`](public/topics/core/streams.md) | A graph of tasklets: channels, edges, transforms and keys |
| [`workflows.md`](public/topics/core/workflows.md) | Steps, retries, undo, and one instance per input |
| [`sessions.md`](public/topics/core/sessions.md) | A session per user: steps, one pass per message, sleeping when idle |
| [`sessions.md`](public/topics/core/sessions.md#rewinding-a-turn) | Taking iterations back, and a failed turn that fails alone |

## Relay features

What a relay can do beyond its graph, and what Flezha guarantees for it.

| Doc | Covers |
| --- | --- |
| [`timers.md`](public/topics/core/timers.md) | Per-cell processing-time and event-time timers |
| [`streams.md`](public/topics/core/streams.md#merged-inputs) | A tasklet fed by several channels |
| [`effectively_once.md`](public/topics/core/effectively_once.md#recorded-effects) | Keeping an outside call's result so a re-run reads it instead of calling again |
| [`sessions.md`](public/topics/core/sessions.md#streamed-answers) | An answer sent in parts while the turn runs |
| [`workflows.md`](public/topics/core/workflows.md#starting-one-from-a-tasklet) | A running tasklet starts or feeds a steps instance |
| [`sending_and_calling.md`](public/topics/core/sending_and_calling.md#calls) | Calling a relay and waiting for its answer |
| [`sending_and_calling.md`](public/topics/core/sending_and_calling.md#sending-from-runner-code) | Sending to a relay from a runner's own code, through the controller or locally |
| [`durability.md`](public/topics/core/durability.md) | What survives what, and the durability levels |
| [`effectively_once.md`](public/topics/core/effectively_once.md) | The delivery guarantee, what it requires and what it costs |
| [`partitioning.md`](public/topics/core/partitioning.md) | One logical relay in N partitions: key groups, sources, rescale |
| [`running_and_testing.md`](public/topics/core/running_and_testing.md#local-mode) | Testing a relay in your own process, with no controller |

## Core topics

What applies to a whole deployment rather than one relay.

| Doc | Covers |
| --- | --- |
| [`namespaces.md`](public/topics/core/namespaces.md) | Namespaces, joining one from a runner, quotas, and administering one from code |
| [`authorization.md`](public/topics/core/authorization.md) | The admin, tokens, roles, certificates and recovery |
| [`mcp_server.md`](public/topics/core/mcp_server.md) | `flz mcp`: an AI assistant gets the rules, the current SDK form and the operations |

## Plugin guides

Connecting relays to the outside; every plugin's own page is under [`plugins/`](#plugins).

| Doc | Covers |
| --- | --- |
| [`plugins.md`](public/topics/plugins/plugins.md) | The plugin model: installed, enabled by name, relays bound; the plugins that ship |
| [`where_plugins_run.md`](public/topics/plugins/where_plugins_run.md) | Shared and single copies, partitioned relays, when a copy stops, durability |
| [`enabling_and_binding.md`](public/topics/plugins/enabling_and_binding.md) | The `flz plugin` commands, their flags, hooks, updates, seeing what runs |
| [`plugins_from_code.md`](public/topics/plugins/plugins_from_code.md) | Enabling and binding plugins from an SDK's `control()` handle |
| [`plugin_examples.md`](public/topics/plugins/plugin_examples.md) | The echo's file plugins and the tradedesk's live feed; limits and gotchas |
| [`installing_plugins.md`](public/topics/plugins/installing_plugins.md) | Plugin packages: installing, loading at boot, versions, signing |
| [`hooks.md`](public/topics/plugins/hooks.md) | Hooks and controller plugins |
| [`writing_a_plugin.md`](public/topics/plugins/writing_a_plugin.md) | Writing and packaging a plugin of your own |

## The CLI

| Doc | Covers |
| --- | --- |
| [`cli.md`](public/topics/cli/cli.md) | `flz` itself: reaching a controller, output formats, identity |
| [`controller_commands.md`](public/topics/cli/controller_commands.md) | Starting and stopping the controller, checking that it is up, its runners, and clearing its data |
| [`relay_commands.md`](public/topics/cli/relay_commands.md) | Starting and stopping relays, sending them input, reading their state and their dead letters |
| [`plugin_commands.md`](public/topics/cli/plugin_commands.md) | Installing plugin packages, enabling plugins and binding relays to them |
| [`access_commands.md`](public/topics/cli/access_commands.md) | Namespaces and quotas, the first admin, tokens, roles, and the audit trail |
| [`template_commands.md`](public/topics/cli/template_commands.md) | A new project in any of the seven languages, from `flz template create` |
| [`command_reference.md`](public/topics/cli/command_reference.md) | Every `flz` verb in one line, by group |

## Deploying

| Doc | Covers |
| --- | --- |
| [`deploying_a_controller.md`](public/topics/deploying/deploying_a_controller.md) | The release, the Docker image, an Elixir application; shutdown and its grace periods |
| [`docker.md`](public/topics/deploying/docker.md) | The container's start and stop, what to mount, a controller and a runner in Compose, `flz` in the container |
| [`building_from_source.md`](public/topics/deploying/building_from_source.md) | A release built from a checkout, and the `make` targets |
| [`connecting_a_runner.md`](public/topics/deploying/connecting_a_runner.md) | How a runner reaches its controller, and how to run one |
| [`clustering.md`](public/topics/deploying/clustering.md) | Several controllers as one cluster, and stopping one |
| [`kubernetes.md`](public/topics/deploying/kubernetes.md) | Running controllers and runners on Kubernetes |

## Architecture

| Doc | Covers |
| --- | --- |
| [`overview.md`](public/architecture/overview.md) | The roles, the workload model, the kernel invariants, the failure model |
| [`kernel.md`](public/architecture/kernel.md) | The durable substrate under every relay: the write path, snapshots, delivery, flow control, fencing, health |
| [`flow_profile.md`](public/architecture/flow_profile.md) | A relay that runs in flow: per-cell recovery, checkpoints, late input, partitions |
| [`steps_profile.md`](public/architecture/steps_profile.md) | A relay that runs in steps: stages, the task journal, undo, loops, sleeping, rewind |
| [`runner.md`](public/architecture/runner.md) | Inside a runner: the shared Rust kernel and the host SDK |

## Contributing

| Doc | Covers |
| --- | --- |
| [`public/contributing/contributing.md`](public/contributing/contributing.md) | The rules a contribution follows: the CLA, a pull request, commit messages, review |
| [`public/contributing/setting_up.md`](public/contributing/setting_up.md) | The tools, the clone, the build, and the development containers |
| [`public/contributing/testing.md`](public/contributing/testing.md) | The test suites, one SDK at a time, and CI on your own machine |
| [`public/contributing/code_standards.md`](public/contributing/code_standards.md) | The rules a change is held to, the formatters and the gates |
| [`public/contributing/releasing.md`](public/contributing/releasing.md) | The release artifacts and how they are built |

## sdk

| Doc | Covers |
| --- | --- |
| [`authoring_contract.md`](internal/sdk/authoring_contract.md) | The three authoring tiers, signature → type inference, state → Change derivation, cell mode, the cross-SDK reference relay |
| [`naming_conventions.md`](internal/sdk/naming_conventions.md) | The numbered naming rules every SDK surface follows |
| [`rust_relay_architecture.md`](internal/sdk/rust_relay_architecture.md) | The Rust SDK's dispatch pipeline: channels, tasklets, cells, mailboxes, backpressure, plugin egress |
| [`state_persistence.md`](internal/sdk/state_persistence.md) | For SDK implementers: how per-cell state becomes durable in the SDKs |
| [`elixir_sdk.md`](internal/sdk/elixir_sdk.md) | The Elixir SDK on the shared core, wherever its runner runs |
| [`java_sdk.md`](internal/sdk/java_sdk.md) | The Java SDK: scope, how its artifacts are built, how it is tested |
| [`node_sdk.md`](internal/sdk/node_sdk.md) | The Node SDK (`@flezha/sdk`): supported versions, dispatch configuration, what must never block the event loop |
| [`python_sdk.md`](internal/sdk/python_sdk.md) | The Python SDK (`flezha` on PyPI): supported versions, encoding, concurrency, startup order |
| [`ruby_sdk.md`](internal/sdk/ruby_sdk.md) | The Ruby SDK (the `flezha` gem): supported versions, the threading host, parallelism, the runner image |
| [`php_sdk.md`](internal/sdk/php_sdk.md) | The PHP SDK over `ext-ffi`: supported versions, sizing, the executor seam, accepted limits |
| [`conformance.md`](internal/sdk/conformance.md) | The cross-SDK conformance suite: manifests, capabilities, the gate, adding an SDK |
| [`stream_transforms.md`](internal/sdk/stream_transforms.md) | Implementation notes for edge transforms: where the code lives |
| [`multi_input_tasklets.md`](internal/sdk/multi_input_tasklets.md) | Implementation notes for merged inputs: replay, where the code lives |
| [`cell_timers.md`](internal/sdk/cell_timers.md) | Implementation notes for timers: folded state, where the code lives |
| [`partitioned_relays.md`](internal/sdk/partitioned_relays.md) | Implementation notes for partitioning: the key-group hash and its vectors |
| [`calls.md`](internal/sdk/calls.md) | Implementation notes for calls: the Response envelope |
| [`protocol_spec.md`](internal/sdk/protocol_spec.md) | The wire protocol: transports, framing, commands, lifecycle, data plane |
| [`poll_core.md`](internal/sdk/poll_core.md) | The shared `flz_core` kernel and its C ABI |

## plugins

| Doc | Covers |
| --- | --- |
| [`kafka_producer.md`](public/plugins/kafka_producer.md) | The Kafka producer output, a plugin package: partitioning, idempotence, batching, the in-flight window |
| [`kafka_consumer.md`](public/plugins/kafka_consumer.md) | The Kafka consumer source in the same package: a consumer group's share of topics, commits by input durability, lag |
| [`rabbitmq.md`](public/plugins/rabbitmq.md) | The RabbitMQ consumer source and producer output, a plugin package: acknowledgement by input durability, message ids, publisher confirms, reconnects |
| [`http_poll.md`](public/plugins/http_poll.md) | An HTTP resource as a relay input |
| [`http_push.md`](public/plugins/http_push.md) | Each output sent as one HTTP request, a guaranteed one held until a 2xx |
| [`postgres_poll.md`](public/plugins/postgres_poll.md) | The new, and optionally the changed, rows of a Postgres table as a relay input |
| [`postgres_cdc.md`](public/plugins/postgres_cdc.md) | Every row change of Postgres tables from the write-ahead log, after a first copy, a plugin package: the event, the slot and its lag, confirmation by input durability |
| [`file.md`](public/plugins/file.md) | A file, or an object in an S3 bucket, as a relay input and as its output |
| [`websocket_subscription.md`](public/plugins/websocket_subscription.md) | Changing a live socket's subscription set without reconnecting |
| [`relay_forward.md`](public/plugins/relay_forward.md) | One relay's output into another's input |
| [`hook_webhook.md`](public/plugins/hook_webhook.md) | What happens in a namespace, POSTed as JSON to a URL: the first controller plugin |
| [`http_server.md`](public/plugins/http_server.md) | A relay behind HTTP, and another relay's events as server-sent events (the `flezha_http` package) |
| [`http_api.md`](public/plugins/http_api.md) | The controller's operations over HTTP, as MCP tools for AI assistants and as plain JSON, for a token's principal (the `flezha_http` package) |

## operations

| Doc | Covers |
| --- | --- |
| [`benchmarking.md`](internal/operations/benchmarking.md) | `mix bench`, shapes, recorded floors, CI gates |

## resilience

| Doc | Covers |
| --- | --- |
| [`overview.md`](public/architecture/overview.md) | The map of every self-healing mechanism and the durability guarantee summary |
| [`data_durability.md`](internal/resilience/data_durability.md) | The mechanism of the guarantees: a frame's guarantee class, the effectively-once tier's parts, the input floor |
| [`steps_relay.md`](internal/resilience/steps_relay.md) | The mechanism of a relay that runs in steps: barriers, the durable loop, crash-resume |

## internals

| Doc | Covers |
| --- | --- |
| [`state_persistence.md`](internal/state_persistence.md) | The controller's WAL/snapshot store, flow control, `flz state` |

## troubleshooting

| Doc | Covers |
| --- | --- |
| [`controller_will_not_start.md`](public/troubleshooting/controller_will_not_start.md) | A name already in use, a wedged node whose socket still answers, and every boot refusal |
| [`runner_cannot_connect.md`](public/troubleshooting/runner_cannot_connect.md) | Every `flz runner check` verdict and its fix, the 60-byte socket path cap, and why reaching the socket is not the same as being admitted |

## publishing

| Doc | Covers |
| --- | --- |
| [`readiness.md`](internal/publishing/readiness.md) | Every decision the first public release makes irreversible: the blocking/owed test, the record, and the decisions the owner owes |
| [`maturity.md`](public/publishing/maturity.md) | Which features are not yet settled, each SDK's support tier, and how an entry is added, graduated and removed |

## dev notes

indexes them. They are deliberately not repeated in the docs above.
