# Sources and sinks

**Before you start:** finish [`running_operatives.md`](running_operatives.md),
with the controller, your runner and `my_echo` all still running. Check:

```sh
flz operative list    # my_echo, with your runner under RUNNER
```

If it is not there, go back to the guide that starts the missing piece.

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

## The folder plugins may touch

File plugins read and write real paths, so a controller that let a plugin name
any path would hand whoever enables plugins its certificates, its state and its
tokens. They get one folder instead, and nothing outside it.

**By default the folder is `app_data/files`, inside the directory the controller
was started from.** Started the way
[`controller_deployment.md`](controller_deployment.md) shows, that is
`app_data/files` inside the release directory. Under Docker it is
`/flezha/app_data/files`, inside the data volume. The controller creates the
folder the first time a file plugin is enabled.

**Ask the controller for the exact path:**

```sh
$ flz controller status | grep file_plugin_root
file_plugin_root:              /opt/flezha/app_data/files
```

The boot prints the same path, and once a file plugin is attached,
`flz operative info` shows its full path too.

A relative `path` in a plugin is taken under that folder, an absolute one must
resolve inside it, and every symbolic link is followed before the check, so
`../certs/key` and a link pointing out both fail the same way. The commands on
this page use relative paths, run from the directory the controller was started
in.

To use another folder, name it with `FLEZHA_FILE_PLUGIN_ROOT` when the
controller starts. It is read at boot, so a running controller keeps its folder
until it is restarted, and a folder you name has to exist already:

```sh
mkdir -p "$HOME/flezha-files"
FLEZHA_AUTHZ_ENABLED=false FLEZHA_FILE_PLUGIN_ROOT="$HOME/flezha-files" flz controller start
```

## Write the output to a file

```sh
flz plugin enable writer operative_file_sink \
  --operative_id=my_echo \
  --hook=on_output \
  --Xpath=echo-out.txt
```

Read that as four things: `writer` is the id you will use to disable it,
`operative_file_sink` is which plugin, `--operative_id` is which operative it
attaches to, and `--hook=on_output` is the hook, meaning **send this plugin
everything the operative emits**. Options belonging to the plugin itself carry an
`X` prefix, which is what separates them from the command's own flags.

You do not create the file: the sink creates `echo-out.txt` when the plugin is
enabled, readable only by the user the controller runs as. Its folder has to
exist, though. The default `app_data/files` is made for you; a subfolder such as
`out/` in `--Xpath=out/echo-out.txt` is not. The name is yours to pick; the sink
never looks at the extension.

Send something and the file grows. The sink writes in small batches, so the line
lands a moment after the send rather than with it:

```sh
flz operative send my_echo --text hello
cat app_data/files/echo-out.txt
```

```
echo: hello
```

One record, one line, the message bytes and a newline. Add
`--Xchannel_out=out` to write only the records from one channel; without it the
sink writes every record its hook hands it.

## Read the input from a file

```sh
flz plugin enable reader operative_file_source \
  --operative_id=my_echo \
  --Xpath=echo-in.txt \
  --Xchannel_in=in
```

`--Xchannel_in=in` is the entry channel each line is injected on, and `in` is the
name the operative gave it. There is no `--hook` here: a source drives
itself, reading the file and pushing lines in, rather than being sent anything.

It looks for new lines once a second, so give it that second before reading the
output:

```sh
echo "from a file" >> app_data/files/echo-in.txt
cat app_data/files/echo-out.txt
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
than quietly reading nothing. Or set the root to an S3 bucket instead, so an operative
can move freely: see [An S3 bucket instead of a folder](../plugins/file.md#an-s3-bucket-instead-of-a-folder).

So a file source is the right tool for replaying a log, loading an export, or
seeing an operative work with your own eyes. It is not how a cluster ingests
production traffic.

## Or from your own code

Enabling and disabling are on the same admin handle
[`running_operatives.md`](running_operatives.md#or-from-your-own-code) used, so
an application can attach its own plugins:

<!-- tabs-open -->

### Java

```java
Admin admin = runner.admin();

ObjectNode sink = JsonNodeFactory.instance.objectNode();
sink.put("id", "writer");
sink.put("provider", "operative_file_sink");
sink.put("operative_id", "my_echo");
sink.putObject("opts").put("path", "echo-out.txt");
sink.putArray("hooks").addArray().add("on_output").addObject();

admin.enablePlugin("writer", sink);
admin.disablePlugin("writer");
```

### Rust

```rust
let admin = runner.admin();

admin.enable_plugin(
    "writer",
    serde_json::json!({
        "id": "writer",
        "provider": "operative_file_sink",
        "operative_id": "my_echo",
        "opts": { "path": "echo-out.txt" },
        "hooks": [["on_output", {}]],
    }),
)?;
admin.disable_plugin("writer")?;
```

### Node

```javascript
const admin = runner.admin();

await admin.enablePlugin('writer', {
  id: 'writer',
  provider: 'operative_file_sink',
  operative_id: 'my_echo',
  opts: { path: 'echo-out.txt' },
  hooks: [['on_output', {}]],
});
await admin.disablePlugin('writer');
```

### Python

```python
admin = runner.admin()

admin.enable_plugin(
    "writer",
    {
        "id": "writer",
        "provider": "operative_file_sink",
        "operative_id": "my_echo",
        "opts": {"path": "echo-out.txt"},
        "hooks": [["on_output", {}]],
    },
)
admin.disable_plugin("writer")
```

### Ruby

```ruby
admin = runner.admin

admin.enable_plugin("writer", {
  "id" => "writer",
  "provider" => "operative_file_sink",
  "operative_id" => "my_echo",
  "opts" => { "path" => "echo-out.txt" },
  "hooks" => [["on_output", {}]]
})
admin.disable_plugin("writer")
```

### PHP

```php
$admin = $runner->admin();

$admin->enablePlugin('writer', [
    'id' => 'writer',
    'provider' => 'operative_file_sink',
    'operative_id' => 'my_echo',
    'opts' => ['path' => 'echo-out.txt'],
    'hooks' => [['on_output', new stdClass()]],
]);
$admin->disablePlugin('writer');
```

### Elixir

```elixir
alias Flezha.SDK.Admin
runner = Flezha.SDK.RunnerServer

{:ok, _} =
  Admin.enable_plugin(runner, "writer", %{
    "id" => "writer",
    "provider" => "operative_file_sink",
    "operative_id" => "my_echo",
    "opts" => %{"path" => "echo-out.txt"},
    "hooks" => [["on_output", %{}]]
  })

{:ok, _} = Admin.disable_plugin(runner, "writer")
```

<!-- tabs-close -->

The object carries what the command line carries: the plugin's id, its provider,
the operative it attaches to, the provider's own options under `opts`, and the
hook that `--hook=on_output` spells on the command line. Every echo example
carries its enable commented out right after the operative's start, ready to
uncomment, and a message sent after it lands in the file.
[`../sdk/admin.md`](../sdk/admin.md) is the whole surface.

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

You have a controller, a runner, an operative and real data moving through it.
[`stop_everything.md`](stop_everything.md), the last page of Part 1, stops all of
it and says what comes back when you start again.
