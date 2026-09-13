# Sources and sinks

**Before you start:** finish [`running_operatives.md`](running_operatives.md).
You need `my_echo` running on a connected runner.

TODO: a draft. The commands are taken from
[`../plugins/file.md`](../plugins/file.md) but have not been run end to end yet.

So far you have been the input and the output. `flz operative send` put a
message in and `flz operative listen` read what came out, and both of them are
you, at a keyboard, for as long as you keep typing. A deployment needs something
that does that on its own. That something is a **plugin**, and attaching one is
all this page is about.

## You have been using one all along

Look at the declaration again:

```java
c.input(String.class, "in").into(EchoProcedure.class).sink("out");
```

`sink("out")` terminates that channel **on the plugin layer**. The operative has
always been emitting to a plugin-bound channel; there was simply nothing attached
to it, and `operative listen` borrowed the output for the length of a window.
Attaching a plugin is what makes it permanent, and it changes nothing in your
code.

That is the shape to remember: **an operative declares where its edges are, and
an operator decides what is on the other side.** The same `echo` runs against a
file today and Kafka tomorrow with no rebuild.

## Name the folder plugins may touch

File plugins read and write real paths, so a controller that let a plugin name
any path would hand whoever enables plugins its certificates, its state and its
tokens. One variable names the only folder they may touch, and **while it is
unset every file plugin enable is refused**:

```sh
export FLEZHA_FILE_PLUGIN_ROOT=/srv/flezha/files
mkdir -p /srv/flezha/files
```

It is read at boot, so set it and restart the controller. A relative `path` in a
plugin is then taken under that folder, an absolute one must resolve inside it,
and every symbolic link is followed before the check, so `../certs/key` and a
link pointing out both fail the same way.

## Write the output to a file

```sh
flz plugin enable writer operative_file_sink \
  --operative_id=my_echo \
  --extension=on_output \
  --Xpath=echo-out.ndjson
```

Read that as four things: `writer` is the id you will use to disable it,
`operative_file_sink` is which plugin, `--operative_id` is which operative it
attaches to, and `--extension=on_output` is the subscription, meaning **send this
plugin everything the operative emits**. Options belonging to the plugin itself
carry an `X` prefix, which is what separates them from the command's own flags.

Send something and the file grows:

```sh
flz operative send my_echo --text hello
cat /srv/flezha/files/echo-out.ndjson
```

```
echo: hello
```

One record, one line, the message bytes and a newline. Add
`--Xchannel_out=out` to write only the records from one channel; without it the
sink writes everything it is subscribed to.

## Read the input from a file

```sh
flz plugin enable reader operative_file_source \
  --operative_id=my_echo \
  --Xpath=echo-in.txt \
  --Xchannel_in=in
```

`--Xchannel_in=in` is the entry channel each line is injected on, and `in` is the
name the operative gave it. There is no `--extension` here: a source drives
itself, reading the file and pushing lines in, rather than being sent anything.

```sh
echo "from a file" >> /srv/flezha/files/echo-in.txt
cat /srv/flezha/files/echo-out.ndjson
```

```
echo: hello
echo: from a file
```

That is the whole loop, and nothing is holding it open. The reader polls the file
for growth, remembers the byte offset of the last line the operative took, and
resumes there after a plugin restart, a move to another runner, or a controller
restart.

## Turn one off

```sh
flz plugin disable reader
```

The id is the one you chose at enable. `flz operative info my_echo` lists what is
attached, with each plugin's path and health underneath it, which is where to
look when an operative is `degraded` and you want to know which side is unhappy.

## This is a single machine feature

The root folder is a folder **on the controller the operative is running on**.
One controller, or the same shared mount at the same path on every controller,
and that is fine. Otherwise an operative that moves to a controller without the
file reports `{:unhealthy, {:file_missing, path}}` within one health probe rather
than quietly reading nothing. There is no object storage in this plugin.

So a file source is the right tool for replaying a log, loading an export, or
seeing an operative work with your own eyes. It is not how a cluster ingests
production traffic.

## Or from your own code

Enabling and disabling are on the same admin handle
[`running_operatives.md`](running_operatives.md#or-from-your-own-code) used, so
an application can attach its own plugins:

```java
Admin admin = runner.admin();

ObjectNode sink = JsonNodeFactory.instance.objectNode();
sink.put("id", "writer");
sink.put("provider", "operative_file_sink");
sink.put("operative_id", "my_echo");
sink.putObject("opts").put("path", "echo-out.ndjson");
sink.putArray("subscriptions").addArray().add("on_output").addObject();

admin.enablePlugin("writer", sink);
admin.disablePlugin("writer");
```

The object carries what the command line carries: the plugin's id, its provider,
the operative it attaches to, the provider's own options under `opts`, and the
subscription that `--extension=on_output` spells on the command line. The same
block sits commented out in
[the echo example](https://github.com/alfetahe/flezha/tree/main/sdk/java/examples/echo),
ready to uncomment. [`../sdk/admin.md`](../sdk/admin.md) is the whole surface,
and the other six SDKs spell the same two methods their own way.

What it cannot do is worth knowing: installing, upgrading and uninstalling a
plugin package are node-wide acts, so they stay with the operator. An
application may attach and detach the plugins its own namespace already has.

## Other sources and sinks

The file plugin is one of a family, and they attach exactly the same way: an id,
a provider, an operative and its options.

| Plugin | What it connects |
|---|---|
| [`http_poll.md`](../plugins/http_poll.md) | an HTTP resource as an input |
| [`websocket_subscription.md`](../plugins/websocket_subscription.md) | a live socket, both directions |
| [`kafka_producer.md`](../plugins/kafka_producer.md) | Kafka as an output |
| [`postgres_sink.md`](../plugins/postgres_sink.md) | Postgres as an output |
| [`operative_forward.md`](../plugins/operative_forward.md) | one operative's output into another's input |

## Next

That is Part 1. You have a controller, a runner, an operative and real data
moving through it. [`introduction.md`](introduction.md) lists what Part 2 takes
apart, and [`authorization.md`](authorization.md) is the one to read first,
because Part 1 left it switched off.
