# Plugin examples

Two setups from the guides: the echo's file plugins from
[Connect a plugin](../../getting_started/connect_a_plugin.md), and the tradedesk's live feed
into a partitioned relay. The commands they use are on
[Enabling and binding](enabling_and_binding.md).

## A file in, a file out

The echo example declares one entry channel and one output plugin, and both ends are open to
plugins:

<!-- tabs-open -->

### Java

```java
// EchoRelay.java: the echo relay's channels
c.input(String.class, "in").into(Echo.class).output("out");
```

[Full file](https://github.com/flezha/flezha_java_sdk/blob/main/examples/echo/src/main/java/com/flezha/examples/echo/EchoRelay.java)

### Rust

```rust
// relay.rs: the echo relay's channels
c.input::<String>("in").into::<Echo>().output_named("out");
```

[Full file](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/echo/src/relay.rs)

### Node

```javascript
// echo_relay.mjs: the echo relay's channels
c.input('in', { codec: 'text' }).into(EchoTasklet).output('out');
```

[Full file](https://github.com/flezha/flezha_node_sdk/blob/main/examples/echo/echo_relay.mjs)

### Python

```python
# echo_relay.py: the echo relay's channels
c.input("in", codec="text").into(Echo).output("out")
```

[Full file](https://github.com/flezha/flezha_python_sdk/blob/main/examples/echo/echo_relay.py)

### Ruby

```ruby
# echo_relay.rb: the echo relay's channels
c.input("in", codec: :text)
 .into(EchoTasklet)
 .output("out")
```

[Full file](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/echo/echo_relay.rb)

### PHP

```php
// EchoRelay.php: the echo relay's channels
$c->input('in', codec: ChannelCodec::Text)
    ->into(EchoTasklet::class)
    ->output('out');
```

[Full file](https://github.com/flezha/flezha_php_sdk/blob/main/examples/echo/src/EchoRelay.php)

### Elixir

```elixir
# relay.ex: the echo relay's channels
c |> input("in", codec: :text) |> into(Echo.Tasklet) |> output("out")
```

[Full file](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/echo/lib/echo/relay.ex)

### Go

```go
// relay.go: the echo relay's channels
in := flezha.Input[string](c, "in", flezha.InputOptions{Codec: flezha.Text})
flezha.Into(in, Echo{}).Output("out")
```

[Full file](https://github.com/flezha/flezha_go_sdk/blob/main/examples/echo/relay.go)

<!-- tabs-close -->

`in` names the channel messages come in on, and `out` the channel they leave on. A file
output plugin named `writer` writes everything `my_echo` sends out to `echo-out.txt`:

```sh
$ flz plugin enable writer relay_file_output \
  --relay_id=my_echo \
  --hook=on_output \
  --Xpath=echo-out.txt
```

Read that as four things: `writer` is the plugin's name in the namespace,
`relay_file_output` is which plugin, `--relay_id` is the relay it is bound to, and
`--hook=on_output` means **send this plugin everything the relay emits**.
`flz plugin bind writer --relay_id=<op> --hook=on_output` binds another relay later.

File plugins may touch one folder and nothing outside it: `storage/app/files` inside the
directory the controller was started from by default (`/flezha/storage/app/files`, inside the
data volume, under Docker), created the first time a file plugin is enabled, and in it only
the folder of their namespace, here `storage/app/files/default`.
`flz controller status | grep file_plugin_root` prints it, the boot prints the same path,
and `flz relay info` shows each file plugin's full path. A relative `path` is taken
under the namespace's folder. [`file.md`](../../plugins/file.md#the-root-folder) has the rules and
`FLEZHA_FILE_PLUGIN_ROOT`, and the warning that on several controllers each has its own
folder unless they share one or use an S3 bucket.

The output plugin creates `echo-out.txt` when the plugin is enabled, readable only by the user the
controller runs as. Its folder has to exist: the default `storage/app/files/default` is made for you,
a subfolder such as `out/` in `--Xpath=out/echo-out.txt` is not. The output plugin never looks at
the extension. It writes in small batches, so a line lands a moment after the send:

```sh
$ flz relay send my_echo --text hello
```

```output
Delivered to 'my_echo'.
```

```sh
$ cat storage/app/files/default/echo-out.txt
```

```output
echo: hello
```

One record, one line: the message bytes and a newline. `--Xchannel_out=out` writes only
the records from one channel; without it the output plugin writes every record its hook hands it.

A file source named `reader` sends every line added to `echo-in.txt` in on the `in`
channel:

```sh
$ flz plugin enable reader relay_file_input \
  --relay_id=my_echo \
  --Xpath=echo-in.txt \
  --Xchannel_in=in
```

`--Xchannel_in=in` is the entry channel each line is injected on. There is no `--hook`: a
source drives itself, reading the file and pushing lines in. It looks for new lines once a
second, remembers the byte offset of the last line the relay took, and resumes there
after a plugin restart, a move to another runner, or a controller restart.

```sh
$ echo "from a file" >> storage/app/files/default/echo-in.txt
```

```sh
$ cat storage/app/files/default/echo-out.txt
```

```output
echo: hello
echo: from a file
```

To take a plugin away from one relay, unbind it; to turn it off for every relay,
disable it by its name:

```sh
$ flz plugin unbind reader --relay_id=my_echo
```

```output
Plugin reader unbound from my_echo.
```

```sh
$ flz plugin disable reader
```

```output
Plugin reader disabled.
```

## A live feed into a partitioned relay

In the tradedesk example, a websocket source reads the demo exchange's trades into the
`trades` channel of `trade_watcher`, a group of three partitions, and a file output plugin writes
its buy signals:

```sh
$ flz plugin enable exchange relay_websocket \
  --relay_id=trade_watcher --namespace trading \
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
a trade, so no trade is received only to be thrown away. `flz relay info` shows each
copy's subscriptions:

```sh
$ flz relay info trade_watcher --namespace trading
```

```output
...
Relay: trade_watcher#p1
  ...
  Plugins:
    - exchange#p1: Flezha.Plugin.Provider.Websocket
        ...
        applied: EUR/USD, USD/JPY
        desired: EUR/USD, USD/JPY
...
```

`desired` is what the plugin asked for, `applied` what the exchange confirmed. The output plugin
keeps only what leaves on the `signals` output plugin:

```sh
$ flz plugin enable signals relay_file_output \
  --relay_id=trade_watcher --namespace trading \
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

- **Output only reaches a plugin through an output plugin.** A relay decides in its code what
  leaves it, with `output(...)`, and a plugin can only take what does. The tradedesk
  `order_executor` declares no output plugin, so the `--plugins` start on
  [Enabling and binding](enabling_and_binding.md#at-a-relay-s-start) leaves `order_1.txt`
  empty: its steps hand their values to each other and its last step ends the order with a
  result, but nothing leaves on an output plugin for `on_output` to hand the plugin.
- **A source hands over every message.** A websocket plugin hands the relay every
  message the upstream sends, subscription confirmations included. The tradedesk
  example's first transform keeps only messages whose `channel` is `trade`; without such a
  filter a message the tasklets cannot read fails, and after its retries it is set aside in
  the dead-letter queue.
- **An output plugin on a group runs on every member.** A file output plugin bound to a group of three writes
  from three copies; two output plugins on one file can mix their batches, so give each writer a
  file of its own.
- **A source picks up where it was.** A websocket source keeps the subscriptions the
  upstream confirmed, and a file source its offset, across a reconnect, a move and a
  controller restart.
- **An output plugin can see a value twice.** After a crash, what the relay sent out just before
  it can be handed to an output plugin again, unless the output plugin honours `effectively_once`; its
  catalogue page says which.
- **A shared copy does not wait.** A shared HTTP poll hands each relay the content
  without waiting for it, so a halted relay loses that content until it changes. A
  relay whose input is durable runs its own copy instead.
