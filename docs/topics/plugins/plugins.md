# Plugins

A plugin connects an operator to something outside Flezha: a file, a websocket, an HTTP
endpoint, Kafka, a database. It runs in the controller, at the edge of the operator. A
**source** plugin feeds one of the operator's entry channels, and a **sink** plugin takes
what the operator sends out on one of its sinks. A third sort, the controller plugin, is
bound to no operator at all: it is told what happens in a namespace.

Plugins exist so that an operator's code never names where its data comes from or goes.
An operator declares where data comes in and goes out, and a user decides what is on the
other side. The same operator reads from a file today and from Kafka tomorrow with no
rebuild, and a plugin can be added, changed or removed while the operator runs.

**Contents**

1. [How it works](#how-it-works)
2. [Two types](#two-types)
3. [In this section](#in-this-section)
4. [The plugins that ship](#the-plugins-that-ship)
5. [See also](#see-also)

## How it works

A plugin goes through three steps, each its own act:

1. **Installed.** A controller has a provider when its release ships it (the built-in
   plugins) or when a plugin package that provides it is installed on that controller. A
   package is installed per controller, through `FLEZHA_PLUGINS` or `flz plugin install`,
   and loaded when the controller starts; [`installing_plugins.md`](installing_plugins.md)
   covers it.
2. **Enabled.** `flz plugin enable <name> <provider>` records a plugin under a name of its
   own in a namespace, with its options, for the whole cluster. No operator is named and
   nothing starts: the plugin is listed `idle`. It survives a controller restart and a
   whole-cluster cold start.
3. **Bound.** `flz plugin bind <name> --operator_id <op>` connects an operator to the
   plugin. The plugin starts where that operator runs. A binding travels with its
   operator through a move, a restart and a cold start.

```mermaid
flowchart LR
    E["flz plugin enable trades ..."] --> P(["trades<br/>enabled, idle"])
    P -- "bind a" --> A["operator a<br/>controller 1"]
    P -- "bind b" --> B["operator b<br/>controller 1"]
    P -- "bind c" --> C["operator c<br/>controller 3"]
    A & B --- W1["one copy of trades<br/>on controller 1"]
    C --- W3["one copy of trades<br/>on controller 3"]
```

## Two types

| Type | Bound to | Runs |
| --- | --- | --- |
| operator plugin | one or more operators | on each controller where an operator bound to it runs, and nowhere else; stops when no bound operator runs there, and stays enabled |
| controller plugin | no operator | on every controller that holds its namespace, from its enable until it is disabled; it never stops for want of operators |

Every catalogue page in [`docs/plugins/`](https://github.com/flezha/flezha/tree/main/docs/plugins) says which type a plugin is. A
controller plugin is enabled with its hooks and is told what happened through them;
[`hooks.md`](hooks.md) covers it. The other pages of this section are
about operator plugins.

## In this section

1. [Where a plugin runs](where_plugins_run.md): shared and `single` copies, partitioned
   operators, when a copy stops, a controller without the provider, and durability
2. [Enabling and binding](enabling_and_binding.md): the commands, their flags, hooks,
   changing a running plugin and seeing what runs
3. [Plugins from your own code](plugins_from_code.md): the same acts from an SDK's admin
   handle
4. [Plugin examples](plugin_examples.md): a file in and a file out, a live feed into a
   partitioned operator, limits and gotchas
5. [Installing plugins](installing_plugins.md): plugin packages on a controller
6. [Hooks](hooks.md): every hook, and controller plugins
7. [Writing a plugin](writing_a_plugin.md): a plugin of your own

## The plugins that ship

| Plugin | Provider id | Connects | Type | Ships | Shareable | Partitioned operators |
|---|---|---|---|---|---|---|
| [File](../../plugins/file.md) | `operator_file_source`, `operator_file_sink` | lines of a file in, one line per value out | operator | built in | no | allowed |
| [WebSocket](../../plugins/websocket_subscription.md) | `operator_websocket` | a live socket, both directions | operator | built in | yes | allowed |
| [HTTP poll](../../plugins/http_poll.md) | `operator_http_poll` | an HTTP resource as an input, whenever its answer changes | operator | built in | yes | allowed |
| [HTTP push](../../plugins/http_push.md) | `operator_http_push` | an HTTP endpoint as an output | operator | built in | yes | allowed |
| [Operator forward](../../plugins/operator_forward.md) | `operator_forward` | one operator's output into another's input | operator | built in | no | allowed |
| [Postgres sink](../../plugins/postgres_sink.md) | `operator_postgres` | rows in a table as an output | operator | built in | no | allowed |
| [Postgres poll](../../plugins/postgres_poll.md) | `operator_postgres_poll` | a table as an input, read by polling | operator | built in | no | refused |
| [Webhook](../../plugins/hook_webhook.md) | `hook_webhook` | hook events to an HTTP endpoint | controller | built in | no | not applicable |
| [HTTP server](../../plugins/http_server.md) | `http_server` | an operator behind HTTP | operator | package `flezha_http` | no | refused |
| [HTTP API](../../plugins/http_api.md) | `http_api` | the controller's operations over HTTP, as MCP tools and plain JSON | controller | package `flezha_http` | no | not applicable |
| [Kafka producer](../../plugins/kafka_producer.md) | `kafka_producer` | a Kafka topic as an output | operator | package `flezha_kafka` | yes | allowed |
| [Kafka consumer](../../plugins/kafka_consumer.md) | `kafka_consumer` | Kafka topics as an input, read by a consumer group | operator | package `flezha_kafka` | no | refused |
| [RabbitMQ](../../plugins/rabbitmq.md) | `rabbitmq_consumer`, `rabbitmq_producer` | a queue as an input, an exchange as an output | operator | package `flezha_rabbitmq` | consumer no, producer yes | consumer refused, producer allowed |
| [Postgres change capture](../../plugins/postgres_cdc.md) | `postgres_cdc` | every row change of Postgres tables, from the write-ahead log | operator | package `flezha_postgres_cdc` | no | refused |

The release also carries providers Flezha uses for its own mechanisms, such as
`operator_stdout` and `operator_io_filter`. `flz plugin list --providers` lists every
provider a controller can enable, and ends with a line naming how many packages were
refused at boot.

## See also

- [`partitioning.md`](../advanced/partitioning.md): partitioned operators and their plugins
- [`durability.md`](../basics/durability.md) and [`effectively_once.md`](../advanced/effectively_once.md): what a sink can promise
- [Controller environment variables](../../configuration/environment_variables.md): `FLEZHA_PLUGIN_IDLE_STOP_MS`, `FLEZHA_FILE_PLUGIN_ROOT` and the other plugin settings
- [`../../errors.md`](../../errors.md): every `plugin.*` code
- Tutorials: [`feed_the_echo_a_file.md`](../../tutorials/echo/feed_the_echo_a_file.md) and [`watch_the_trades.md`](../../tutorials/tradedesk/watch_the_trades.md)
