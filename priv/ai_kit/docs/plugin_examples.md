# Plugin examples

Two setups from the tutorials: the echo's file plugins, and the tradedesk's live feed
into a partitioned operator. The commands they use are on
[Enabling and binding](enabling_and_binding.md).

**Contents**

1. [A file in, a file out](#a-file-in-a-file-out)
2. [A live feed into a partitioned operator](#a-live-feed-into-a-partitioned-operator)
3. [Limits and gotchas](#limits-and-gotchas)

## A file in, a file out

The echo example declares one entry channel and one sink, and both ends are open to
plugins:

<!-- tabs-open -->

### Java

```java
// EchoOperator.java: the echo operator's channels
c.input(String.class, "in").into(Echo.class).sink("out");
```

[Full file](https://github.com/flezha/flezha_java_sdk/blob/main/examples/echo/src/main/java/com/flezha/examples/echo/EchoOperator.java)

### Rust

```rust
// operator.rs: the echo operator's channels
c.input::<String>("in").into::<Echo>().sink_named("out");
```

[Full file](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/echo/src/operator.rs)

### Node

```javascript
// echo_operator.mjs: the echo operator's channels
c.input('in', { codec: 'text' }).into(EchoTasklet).sink('out');
```

[Full file](https://github.com/flezha/flezha_node_sdk/blob/main/examples/echo/echo_operator.mjs)

### Python

```python
# echo_operator.py: the echo operator's channels
c.input("in", codec="text").into(Echo).sink("out")
```

[Full file](https://github.com/flezha/flezha_python_sdk/blob/main/examples/echo/echo_operator.py)

### Ruby

```ruby
# echo_operator.rb: the echo operator's channels
c.input("in", codec: :text)
 .into(EchoTasklet)
 .sink("out")
```

[Full file](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/echo/echo_operator.rb)

### PHP

```php
// EchoOperator.php: the echo operator's channels
$c->input('in', codec: ChannelCodec::Text)
    ->into(EchoTasklet::class)
    ->sink('out');
```

[Full file](https://github.com/flezha/flezha_php_sdk/blob/main/examples/echo/src/EchoOperator.php)

### Elixir

```elixir
# operator.ex: the echo operator's channels
c |> input("in") |> into(Echo.Tasklet) |> sink("out")
```

[Full file](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/echo/lib/echo/operator.ex)

<!-- tabs-close -->

`in` names the channel messages come in on, and `out` the channel they leave on. A file
sink named `writer` writes everything `my_echo` sends out to `echo-out.txt`:

```sh
$ flz plugin enable writer operator_file_sink \
  --operator_id=my_echo \
  --hook=on_output \
  --Xpath=echo-out.txt
```

Read that as four things: `writer` is the plugin's name in the namespace,
`operator_file_sink` is which plugin, `--operator_id` is the operator it is bound to, and
`--hook=on_output` means **send this plugin everything the operator emits**.
`flz plugin bind writer --operator_id=<op> --hook=on_output` binds another operator later.

File plugins may touch one folder and nothing outside it: `app_data/files` inside the
directory the controller was started from by default (`/flezha/app_data/files`, inside the
data volume, under Docker), created the first time a file plugin is enabled.
`flz controller status | grep file_plugin_root` prints it, the boot prints the same path,
and `flz operator info` shows each file plugin's full path. A relative `path` is taken
under that folder. [`file.md`](../../plugins/file.md#the-root-folder) has the rules and
`FLEZHA_FILE_PLUGIN_ROOT`, and the warning that on several controllers each has its own
folder unless they share one or use an S3 bucket.

The sink creates `echo-out.txt` when the plugin is enabled, readable only by the user the
controller runs as. Its folder has to exist: the default `app_data/files` is made for you,
a subfolder such as `out/` in `--Xpath=out/echo-out.txt` is not. The sink never looks at
the extension. It writes in small batches, so a line lands a moment after the send:

```sh
$ flz operator send my_echo --text hello
```

```output
Delivered to 'my_echo'.
```

```sh
$ cat app_data/files/echo-out.txt
```

```output
echo: hello
```

One record, one line: the message bytes and a newline. `--Xchannel_out=out` writes only
the records from one channel; without it the sink writes every record its hook hands it.

A file source named `reader` sends every line added to `echo-in.txt` in on the `in`
channel:

```sh
$ flz plugin enable reader operator_file_source \
  --operator_id=my_echo \
  --Xpath=echo-in.txt \
  --Xchannel_in=in
```

`--Xchannel_in=in` is the entry channel each line is injected on. There is no `--hook`: a
source drives itself, reading the file and pushing lines in. It looks for new lines once a
second, remembers the byte offset of the last line the operator took, and resumes there
after a plugin restart, a move to another runner, or a controller restart.

```sh
$ echo "from a file" >> app_data/files/echo-in.txt
```

```sh
$ cat app_data/files/echo-out.txt
```

```output
echo: hello
echo: from a file
```

To take a plugin away from one operator, unbind it; to turn it off for every operator,
disable it by its name:

```sh
$ flz plugin unbind reader --operator_id=my_echo
```

```output
Plugin reader unbound from my_echo; it stays enabled.
```

```sh
$ flz plugin disable reader
```

```output
Plugin reader disabled.
```

## A live feed into a partitioned operator

In the tradedesk example, a websocket source reads the demo exchange's trades into the
`trades` channel of `trade_watcher`, a group of three partitions, and a file sink writes
its buy signals:

```sh
$ flz plugin enable exchange operator_websocket \
  --operator_id=trade_watcher --namespace trading \
  --Xuri=ws://localhost:4500/ \
  --Xchannel=trades \
  --Xsubscription_items='["EUR/USD", "GBP/USD", "USD/JPY"]' \
  --Xsubscribe_template='{"method": "subscribe", "params": {"channel": "trade", "symbol": {{items}}}, "req_id": {{req_id}}}' \
  --Xunsubscribe_template='{"method": "unsubscribe", "params": {"channel": "trade", "symbol": {{items}}}, "req_id": {{req_id}}}'
```

```output
Enabling plugin...
Plugin exchange enabled.
Plugin exchange bound to trade_watcher.
```

| Setting | What it does |
|---|---|
| `uri` | where the exchange is |
| `channel` | the entry channel every message is handed to |
| `subscription_items` | the symbols to subscribe to, a JSON list |
| `subscribe_template` | the message that subscribes. `{{items}}` becomes a list of symbols, and `{{req_id}}` a number the exchange echoes back to confirm it |
| `unsubscribe_template` | the message that drops symbols, so a list that changes later is followed without reconnecting |

Every exchange has its own message for subscribing, so the plugin is given the exchange's
as a template and fills in the symbols itself. Bound to a group, it runs once per member,
and each copy subscribes only the symbols its partition owns, by the same hash that routes
a trade, so no trade is received only to be thrown away. `flz operator info` shows each
copy's subscriptions:

```sh
$ flz operator info trade_watcher --namespace trading
```

```output
...
Operator: trade_watcher#p1
  ...
  Plugins:
    - exchange#p1: Flezha.Plugin.Provider.Websocket
        ...
        applied: EUR/USD, USD/JPY
        desired: EUR/USD, USD/JPY
...
```

`desired` is what the plugin asked for, `applied` what the exchange confirmed. The sink
keeps only what leaves on the `signals` sink:

```sh
$ flz plugin enable signals operator_file_sink \
  --operator_id=trade_watcher --namespace trading \
  --hook=on_output \
  --Xpath=signals.txt \
  --Xchannel_out=signals
```

```output
Enabling plugin...
Plugin signals enabled.
Plugin signals bound to trade_watcher.
```

## Limits and gotchas

- **Output only reaches a plugin through a sink.** An operator decides in its code what
  leaves it, with `sink(...)`, and a plugin can only take what does. The tradedesk
  `order_executor` declares no sink, so the `--plugins` start on
  [Enabling and binding](enabling_and_binding.md#at-an-operator-s-start) leaves `order_1.txt`
  empty: its steps hand their values to each other and its last step ends the order with a
  result, but nothing leaves on a sink for `on_output` to hand the plugin.
- **A source hands over every message.** A websocket plugin hands the operator every
  message the upstream sends, subscription confirmations included. The tradedesk
  example's first transform keeps only messages whose `channel` is `trade`; without such a
  filter a message the tasklets cannot read fails, and after its retries it is set aside in
  the dead-letter queue.
- **A sink on a group runs on every member.** A file sink bound to a group of three writes
  from three copies; two sinks on one file can mix their batches, so give each writer a
  file of its own.
- **A source picks up where it was.** A websocket source keeps the subscriptions the
  upstream confirmed, and a file source its offset, across a reconnect, a move and a
  controller restart.
- **A sink can see a value twice.** After a crash, what the operator sent out just before
  it can be handed to a sink again, unless the sink honours `effectively_once`; its
  catalogue page says which.
- **A shared copy does not wait.** A shared HTTP poll hands each operator the content
  without waiting for it, so a parked operator loses that content until it changes. An
  operator whose input is durable runs its own copy instead.
