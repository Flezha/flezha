# Plugin options

Passed in the plugin's `opts` map at enable time; changing one means
re-installing the plugin. Every option a provider takes
is a row of that provider's own declaration, so the option table on its page is
rendered from the declaration and gated against it by the conformance test:
there is no second list to keep in step, and this page does not carry one.

| Provider name | Options |
|---|---|
| `kafka_producer` (the Kafka plugin package) | [`plugins/kafka_producer.md`](../plugins/kafka_producer.md) |
| `kafka_consumer` (the Kafka plugin package) | [`plugins/kafka_consumer.md`](../plugins/kafka_consumer.md) |
| `rabbitmq_consumer`, `rabbitmq_producer` (the RabbitMQ plugin package) | [`plugins/rabbitmq.md`](../plugins/rabbitmq.md) |
| `operator_postgres` | [`plugins/postgres_sink.md`](../plugins/postgres_sink.md) |
| `operator_postgres_poll` | [`plugins/postgres_poll.md`](../plugins/postgres_poll.md) |
| `operator_http_poll` | [`plugins/http_poll.md`](../plugins/http_poll.md) |
| `operator_http_push` | [`plugins/http_push.md`](../plugins/http_push.md) |
| `operator_file_source`, `operator_file_sink` | [`plugins/file.md`](../plugins/file.md) |
| `operator_websocket` | [`plugins/websocket_subscription.md`](../plugins/websocket_subscription.md) |
| `operator_forward` | [`plugins/operator_forward.md`](../plugins/operator_forward.md) |
| `postgres_cdc` (the Postgres CDC plugin package) | [`plugins/postgres_cdc.md`](../plugins/postgres_cdc.md) |
| `http_server` (the HTTP plugin package) | [`plugins/http_server.md`](../plugins/http_server.md) |
| `hook_webhook` | [`plugins/hook_webhook.md`](../plugins/hook_webhook.md) |
| `operator_stdout`, `operator_io_filter`, `operator_beam_msg`, `monitored_io_forward`, `operator_message_forward` | `flz plugin help <provider>`, which lists the provider's options |

An option a provider does not declare is refused at enable rather than ignored,
on the CLI path and on the `--plugins` spec path alike, so a config asking for a
capability the provider does not offer fails loudly.
