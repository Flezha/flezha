# The guides

Each part walks through a set of topics, one page at a time. The pages share a
small example, so if you follow the steps you have a working toy at the end of
the part. It is there to try things on, not to deploy.

The parts do not share their example, and nothing needs to be left running from
one part to the next. What carries over is what you learned: each part assumes
you have done the ones before it, so read them in order.

## Part 1: Introduction

Starting a controller, connecting a runner of your own, running an operative,
and attaching a real input and output to it. The example is an echo that
answers whatever you send it. The pages share one running controller, and the
last one stops everything.

| Guide | What you end up with |
|---|---|
| [`controller_deployment.md`](controller_deployment.md) | A controller running, four ways to choose from, and a check that it is up |
| [`runner_deployment.md`](runner_deployment.md) | Your own runner connected to it, offering an operative class |
| [`running_operatives.md`](running_operatives.md) | An operative started on that runner, taking messages and answering them |
| [`sources_and_sinks.md`](sources_and_sinks.md) | Data arriving from somewhere real, and results going somewhere real |
| [`stop_everything.md`](stop_everything.md) | Everything above stopped, and what comes back when you start again |

## Part 2: The basics

Namespaces, streams and workflows, authorization, durability, plugins,
partitioning and clustering. The example is `tradedesk`, a toy trader: a stream
turns trades into candles, and when a candle gives a signal a workflow places an
order with paper money, so nothing reaches a real exchange. It runs in Docker
Compose, with a controller, a testnet standing in for the exchange, and a runner
with your code.

| Guide | What you do |
|---|---|
| [`namespaces.md`](tradedesk/namespaces.md) | Give tradedesk a namespace of its own |
| [`stream_basics.md`](tradedesk/stream_basics.md) | Write the stream operative by hand: chained procedures, `map`, `filter`, `key_by`, a merge, and state per symbol |
| [`workflow_basics.md`](tradedesk/workflow_basics.md) | Write the paper-order workflow by hand: steps, retries and compensation, started by the stream operative |
| [`authorization.md`](tradedesk/authorization.md) | Turn enforcement on, claim the admin, give the runner a credential and the spawn role |
| [`durability.md`](tradedesk/durability.md) | Kill the runner mid-order: the order continues from its step, is never placed twice, and candles keep coming |
| [`plugins.md`](tradedesk/plugins.md) | Read the testnet's trades with the WebSocket plugin, and record filled orders with a file sink |
| [`partitioning.md`](tradedesk/partitioning.md) | `--partitions`, `key_by`, how trades are routed, and `flz operative scale` |
| [`clustering.md`](tradedesk/clustering.md) | Run several controllers in Docker, then stop one |
| [`kubernetes_deployment.md`](tradedesk/kubernetes_deployment.md) | Run the same images on k3s |

## Part 3: Advanced topics

Subjects that go deeper, to read in any order. The example, not written yet, is
an AI agent behind an HTTP service, where every user talks to a session of
their own and many run at once.

| Guide | Covers |
|---|---|
| [`custom_plugins.md`](advanced/custom_plugins.md) | Writing and installing a plugin of your own |
| [`effectively_once.md`](advanced/effectively_once.md) | The delivery guarantee, what it requires, and what it costs |
| [`erlang_vm_tuning.md`](advanced/erlang_vm_tuning.md) | Scheduler, memory and IO settings for a controller under load |
| [`elixir_applications.md`](advanced/elixir_applications.md) | Flezha inside an Elixir application of your own |

## Also useful

- [`cli.md`](cli.md): `flz` itself, how it reaches a controller, its output
  formats, and how a gated verb is identified.
- [`../troubleshooting/`](../troubleshooting/runner_cannot_connect.md): from a
  symptom to its fix.
- [`../quickstart.md`](../quickstart.md): run an operative's graph in your own
  process, with no controller and no deployment. The fastest way to see one
  work, and to test one.
