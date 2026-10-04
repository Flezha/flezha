# Plugin options

A plugin's options are its own settings: the URL it polls, the topic it publishes
to, the file it writes. They belong to one enabled plugin, are given when it is
enabled, and are changed with `flz plugin update`; nothing else on the node moves.
How plugins work is taught in [Plugins](../topics/plugins/plugins.md).

## Setting options

On the command line each option is one `--X<option>=<value>` flag. The `X` keeps
it apart from the flags of `flz` itself:

```sh
$ flz plugin enable signal-config operator_http_poll --operator_id=detector \
  --Xurl=https://api.internal/signal-config \
  --Xchannel=config_in --Xinterval_ms=30000
$ flz plugin update signal-config --Xinterval_ms=10000
```

When an operator is started with its plugins, the same options go in the `opts`
object of a `--plugins` spec:

```sh
$ flz operator start order_executor --id order_1 --namespace trading \
  --text '{"symbol": "EUR/USD", "quantity": 10}' --channel order \
  --plugins '[{"name": "order_1_file", "provider": "operator_file_sink", "opts": {"path": "order_1.txt"}, "hooks": [["on_output", {}]]}]'
```

An option the provider does not declare is refused at enable rather than
ignored, so a typo fails loudly. JSON values and quoting are covered in
[Three kinds of flags](../topics/plugins/enabling_and_binding.md#three-kinds-of-flags).

## Providers and their options

> #### Your controller may offer a different list {: .info}
>
> These are the providers documented for this release. A controller offers the
> built-in providers of its own version plus those of the plugin packages
> installed on it, so yours can have fewer or more, and a package's options can
> change between versions. Ask the controller itself:
>
> ```sh
> $ flz plugin list --providers
> $ flz plugin help kafka_producer
> ```
>
> The first lists every provider it can enable, the second one provider's
> options as the running version declares them. A package not listed here
> documents its own options; installing one is covered in
> [Installing plugin packages](../topics/plugins/installing_plugins.md).

| Provider | Comes with | What it does | Options |
|---|---|---|---|
| `operator_file_source` | built in | Reads a file onto an operator's input, one line per input. | [file](../plugins/file.md#options) |
| `operator_file_sink` | built in | Writes an operator's records to a file, one line each. | [file](../plugins/file.md#options) |
| `operator_http_poll` | built in | Polls a URL on an interval and injects the body when it changed. | [http_poll](../plugins/http_poll.md#options) |
| `operator_http_push` | built in | Sends each output, or each input, of an operator as an HTTP request. | [http_push](../plugins/http_push.md#options) |
| `operator_websocket` | built in | Holds a websocket open, injects its frames and can send outputs back on it. | [websocket](../plugins/websocket_subscription.md#options) |
| `operator_postgres` | built in | Writes an operator's outputs into Postgres, one transaction per batch. | [postgres_sink](../plugins/postgres_sink.md#options) |
| `operator_postgres_poll` | built in | Reads the new, and optionally changed, rows of a Postgres table by polling. | [postgres_poll](../plugins/postgres_poll.md#options) |
| `operator_forward` | built in | Sends one operator's outputs into another operator's input. | [operator_forward](../plugins/operator_forward.md#options) |
| `hook_webhook` | built in | Posts the hook events it is enabled for as JSON to a URL; a controller plugin. | [hook_webhook](../plugins/hook_webhook.md#options) |
| `kafka_consumer` | package `flezha_kafka` | Delivers the records of Kafka topics to an operator, as a consumer group. | [kafka_consumer](../plugins/kafka_consumer.md#options) |
| `kafka_producer` | package `flezha_kafka` | Publishes what an operator ingests or emits to a Kafka topic. | [kafka_producer](../plugins/kafka_producer.md#options) |
| `rabbitmq_consumer` | package `flezha_rabbitmq` | Delivers the messages of a RabbitMQ queue to an operator. | [rabbitmq](../plugins/rabbitmq.md#options) |
| `rabbitmq_producer` | package `flezha_rabbitmq` | Publishes an operator's outputs to a RabbitMQ exchange. | [rabbitmq](../plugins/rabbitmq.md#options) |
| `postgres_cdc` | package `flezha_postgres_cdc` | Delivers every row change of Postgres tables, read from the write-ahead log. | [postgres_cdc](../plugins/postgres_cdc.md#options) |
| `http_server` | package `flezha_http` | Puts an operator behind HTTP and streams outputs as server-sent events. | [http_server](../plugins/http_server.md#options) |
| `http_api` | package `flezha_http` | Serves the controller's operations over HTTP, as MCP tools and plain JSON; a controller plugin. | [http_api](../plugins/http_api.md#options) |

The release also carries providers Flezha uses for its own mechanisms and tests.
They have no page; `flz plugin help <provider>` lists their options.

| Provider | What it does |
|---|---|
| `operator_stdout` | Prints each payload to the controller's standard output and passes it on unchanged. |
| `operator_io_filter` | Passes or drops each payload by a regular expression. |
| `operator_message_forward` | Forwards an `on_message` request to the operator's SDK side and returns the reply. |
| `operator_beam_msg`, `monitored_io_forward` | Send each payload to a BEAM process; they take a process id, so they serve tests and embedded nodes. |
