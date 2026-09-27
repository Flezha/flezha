# The guides

Each part walks through a set of topics, one page at a time. The pages share a
small example, so if you follow the steps you have a working toy at the end of
the part. It is there to try things on, not to deploy.

The parts do not share their example, and nothing needs to be left running from
one part to the next. What carries over is what you learned: each part assumes
you have done the ones before it, so read them in order.

## How to follow a guide

A page mixes explanation with the steps you take. Each step has a purple box
with a pencil, right above the command to run or the code to write. The steps
are not optional: to try things for real and reach the working example at the
end of the part, do every one of them, in the order the page gives. The box says
what to do and what it does, and names anything that has to be in place first:

> Check that the controller is up and ready to take commands. It needs a
> controller started first.
> {: .step}

```sh
$ flz boot await
```

```output
boot await: ready
```

A command or a piece of code with no purple box above it is there to read: it
shows an option or a variant, and the guide does not need it.

The copy button on a command copies the command alone, without the `$` prompt.
What the command prints is shown right below it, and your ids, tokens and times
will differ from the ones on the page.

## Part 1: First steps

Starting a controller, connecting a runner of your own, running an operator,
and attaching a real input and output to it. The example is an echo that
answers whatever you send it. The pages share one running controller, and the
last one stops everything.

> #### What you need {: .tip}
>
> - **For the controller**, a Linux machine on x86-64 to run the release, or
>   Docker to run its image. [`controller_deployment.md`](controller_deployment.md)
>   shows both.
> - **For your runner**, one language: pick the one you want to write it in,
>   and install only what its row lists.
>
> | Language | You need |
> |---|---|
> | Java | Java 21 and Maven |
> | Rust | Rust with Cargo |
> | Node | Node.js 20 or later, with npm |
> | Python | Python 3.11 or later, with pip |
> | Ruby | Ruby 3.3 or later, with Bundler |
> | PHP | PHP 8.4 or later with `ext-ffi`, and Composer |
> | Elixir | Elixir 1.17 or later |
>
> - **git**, to get the examples.

| Guide | What you end up with |
|---|---|
| [`controller_deployment.md`](controller_deployment.md) | A controller running, four ways to choose from, and a check that it is up |
| [`runner_deployment.md`](runner_deployment.md) | Your own runner connected to it, registering an operator factory |
| [`running_operators.md`](running_operators.md) | An operator started on that runner, taking messages and answering them |
| [`sources_and_sinks.md`](sources_and_sinks.md) | Data arriving from somewhere real, and results going somewhere real |
| [`stop_everything.md`](stop_everything.md) | Everything above stopped, and what comes back when you start again |

## Part 2: Going further

Authorization, namespaces, an ETL stream and a workflow, partitioning, plugins and
durability. The example is `tradedesk`, a toy trader: an ETL stream, built as an
unbounded operator, compares each trade's price with its symbol's recent average, and
when a price is far enough above it, a workflow, built as a bounded operator, places an
order with paper money on a demo exchange.

> #### What you need {: .tip}
>
> Everything Part 1 needed, and Node.js with npm to run the demo exchange,
> which [`plugins.md`](tradedesk/plugins.md) starts.

| Guide | What you do |
|---|---|
| [`authorization.md`](tradedesk/authorization.md) | Keep authorization on, claim the admin, and issue the key tradedesk's runner connects with |
| [`namespaces.md`](tradedesk/namespaces.md) | Give tradedesk a namespace of its own |
| [`building_an_etl_stream.md`](tradedesk/building_an_etl_stream.md) | Build the desk's ETL stream as an unbounded operator: chained tasklets, `flat_map`, `filter`, `key_by`, and state per symbol |
| [`building_a_workflow.md`](tradedesk/building_a_workflow.md) | Build the `order_executor` workflow as a bounded operator: steps, retries and compensation, started by hand |
| [`partitioning.md`](tradedesk/partitioning.md) | Start the desk: `--partitions`, `key_by`, how trades are routed, and `flz operator scale` |
| [`plugins.md`](tradedesk/plugins.md) | Read the demo exchange's trades with the WebSocket plugin, and write the buy signals to a file |
| [`durability.md`](tradedesk/durability.md) | Stop and crash the runner and the controller, read the desk's state back, finish an order a crash interrupted, and handle a message that cannot be read |

## Part 3: Advanced topics

Subjects that go deeper. The example, not written yet, is an AI agent behind an
HTTP service, where every user talks to a session of their own and many run at
once. It runs in Docker Compose, which the first page sets up; the rest can be
read in any order.

> #### What you need {: .tip}
>
> Everything Part 1 needed, and:
>
> - **Docker, with Docker Compose**, from [`clustering.md`](advanced/clustering.md) on.
> - **A Kubernetes cluster, such as k3s, and `kubectl`**, for
>   [`kubernetes_deployment.md`](advanced/kubernetes_deployment.md).
> - **Elixir 1.17 or later**, for
>   [`elixir_applications.md`](advanced/elixir_applications.md).

| Guide | Covers |
|---|---|
| [`clustering.md`](advanced/clustering.md) | Set up the example in Docker, run several controllers, then stop one |
| [`custom_plugins.md`](advanced/custom_plugins.md) | Writing and installing a plugin of your own |
| [`full_form_tasklets.md`](advanced/full_form_tasklets.md) | Writing a tasklet without the sugar, and what only that form declares |
| [`effectively_once.md`](advanced/effectively_once.md) | The delivery guarantee, what it requires, and what it costs |
| [`erlang_vm_tuning.md`](advanced/erlang_vm_tuning.md) | Scheduler, memory and IO settings for a controller under load |
| [`elixir_applications.md`](advanced/elixir_applications.md) | Flezha inside an Elixir application of your own |
| [`kubernetes_deployment.md`](advanced/kubernetes_deployment.md) | Run the same images on k3s |

## Also useful

- [`cli.md`](cli.md): `flz` itself, how it reaches a controller, its output
  formats, and how a gated verb is identified.
- [`../troubleshooting/`](../troubleshooting/runner_cannot_connect.md): from a
  symptom to its fix.
- [`../quickstart.md`](../quickstart.md): run an operator's graph in your own
  process, with no controller and no deployment. The fastest way to see one
  work, and to test one.
