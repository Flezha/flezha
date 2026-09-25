# Sources and sinks

> Check that the controller, your runner and `my_echo` from
> [`running_operators.md`](running_operators.md) are all still running:
> `my_echo` should be listed with your runner under `RUNNER`. If it is not, go
> back to the guide that starts the missing piece.
> {: .step}

```sh
$ flz operator list
```

TODO: a draft. The commands are taken from
[`../plugins/file.md`](../plugins/file.md) but have not been run end to end yet.

So far you have been the input and the output. `flz operator send` put a
message in and `flz operator listen` read what came out, and both of them are
you, at a keyboard, for as long as you keep typing. A deployment needs something
that does that on its own. That something is a **plugin**, and attaching one is
all this page is about.

## You have been using one all along

Look at the declaration again:

```java
c.input(String.class, "in").into(EchoTasklet.class).sink("out");
```

Both ends of that line are open to plugins. `input(String.class, "in")` names
the channel messages come in on, and `sink("out")` names the channel they leave
on. Until now nothing was attached to either: `flz operator send` put your
messages in, and `flz operator listen` watched what came out for as long as it
ran. A plugin takes over one of those ends for good: a source feeds `in`, and a
sink receives what leaves on `out`. Your code does not change.

That is the shape to remember: **an operator declares where its data comes in
and goes out, and a user decides what is on the other side.** The same
`echo` reads from a file today and Kafka tomorrow with no rebuild.

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

You can ask the controller for the exact path:

```sh
$ flz controller status | grep file_plugin_root
```

```output
file_plugin_root:              /opt/flezha/app_data/files
```

The boot prints the same path, and once a file plugin is attached,
`flz operator info` shows its full path too.

A relative `path` in a plugin is taken under that folder, an absolute one must
resolve inside it, and every symbolic link is followed before the check, so
`../certs/key` and a link pointing out both fail the same way. The commands on
this page use relative paths, run from the directory the controller was started
in.

To use another folder, name it with `FLEZHA_FILE_PLUGIN_ROOT` when the
controller starts. It is read at boot, so a running controller keeps its folder
until it is restarted, and a folder you name has to exist already:

```sh
$ mkdir -p "$HOME/flezha-files"
$ FLEZHA_AUTHZ_ENABLED=false FLEZHA_FILE_PLUGIN_ROOT="$HOME/flezha-files" flz controller start
```

> #### Warning {: .warning}
>
> This folder is on the machine that runs the controller. With one controller
> that is all there is to it. With several controllers, each has its own folder,
> and an operator that moves to another controller reads and writes the folder
> on that machine: a file source no longer finds its file, and a file sink starts
> a new one there. To avoid that, give every controller the same shared folder at
> the same path, or keep the files in an S3 bucket, as
> [An S3 bucket instead of a folder](../plugins/file.md#an-s3-bucket-instead-of-a-folder)
> shows.

## Write the output to a file

> Attach a file sink named `writer` to `my_echo`, so everything the operator
> sends out is written to `echo-out.txt` in the plugin folder.
> {: .step}

```sh
$ flz plugin enable writer operator_file_sink \
  --operator_id=my_echo \
  --hook=on_output \
  --Xpath=echo-out.txt
```

Read that as four things: `writer` is the id you will use to disable it,
`operator_file_sink` is which plugin, `--operator_id` is which operator it
attaches to, and `--hook=on_output` is the hook, meaning **send this plugin
everything the operator emits**. Options belonging to the plugin itself carry an
`X` prefix, which is what separates them from the command's own flags.

You do not create the file: the sink creates `echo-out.txt` when the plugin is
enabled, readable only by the user the controller runs as. Its folder has to
exist, though. The default `app_data/files` is made for you; a subfolder such as
`out/` in `--Xpath=out/echo-out.txt` is not. The name is yours to pick; the sink
never looks at the extension.

Send something and the file grows. The sink writes in small batches, so the line
lands a moment after the send rather than with it:

> Send a message to `my_echo`, then print the file a moment later: its echo is
> a line in `echo-out.txt`. Run both from the directory the controller was
> started in.
> {: .step}

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

One record, one line, the message bytes and a newline. Add
`--Xchannel_out=out` to write only the records from one channel; without it the
sink writes every record its hook hands it.

## Read the input from a file

> Attach a file source named `reader` to `my_echo`, so every line added to
> `echo-in.txt` in the plugin folder is sent in on the `in` channel.
> {: .step}

```sh
$ flz plugin enable reader operator_file_source \
  --operator_id=my_echo \
  --Xpath=echo-in.txt \
  --Xchannel_in=in
```

`--Xchannel_in=in` is the entry channel each line is injected on, and `in` is the
name the operator gave it. There is no `--hook` here: a source drives
itself, reading the file and pushing lines in, rather than being sent anything.

It looks for new lines once a second, so give it that second before reading the
output:

> Add a line to the input file, then print the output file a second later: the
> source picks the line up, and its echo lands at the end of `echo-out.txt`.
> {: .step}

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

That is the whole loop, and nothing is holding it open. The reader polls the file
for growth, remembers the byte offset of the last line the operator took, and
resumes there after a plugin restart, a move to another runner, or a controller
restart.

## See what is attached

You can list every operator in the namespace, with the plugins attached to it:

```sh
$ flz operator info
```

```output
Fetching operator information for namespace 'default'...
Operator information for namespace 'default':

Operator: my_echo
  Kind: stream
  Phase: running
  Health: healthy
  Runner ID: default_b2e389ab1c13
  Group ID: default
  Uptime (ms): 7249
  Default Input Type: 2
  Plugins:
    - reader: Flezha.Plugin.Provider.File.Source
        path: /opt/flezha/app_data/files/echo-in.txt
        health: :healthy
    - writer: Flezha.Plugin.Provider.File.Sink
        hook: on_output []
        path: /opt/flezha/app_data/files/echo-out.txt
        health: :healthy
```

It lists every operator in the namespace, and under each one the plugins
attached to it: the id you chose, which plugin it is, the hook it is on, the
full path of its file, and whether it is healthy. `reader` has no hook line,
because a source is not sent anything. When an operator shows `degraded`, this
is where to look for the plugin that is not healthy.

## Turn one off

To detach a plugin, disable it by its id:

```sh
$ flz plugin disable reader
```

The id is the one you chose at enable, the same one `flz operator info` lists
under the operator.

## Or from your own code

Enabling and disabling are on the same admin handle
[`running_operators.md`](running_operators.md#or-from-your-own-code) used, so
an application can attach its own plugins:

<!-- tabs-open -->

### Java

```java
Admin admin = runner.admin();

ObjectNode sink = JsonNodeFactory.instance.objectNode();
sink.put("id", "writer");
sink.put("provider", "operator_file_sink");
sink.put("operator_id", "my_echo");
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
        "provider": "operator_file_sink",
        "operator_id": "my_echo",
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
  provider: 'operator_file_sink',
  operator_id: 'my_echo',
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
        "provider": "operator_file_sink",
        "operator_id": "my_echo",
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
  "provider" => "operator_file_sink",
  "operator_id" => "my_echo",
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
    'provider' => 'operator_file_sink',
    'operator_id' => 'my_echo',
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
    "provider" => "operator_file_sink",
    "operator_id" => "my_echo",
    "opts" => %{"path" => "echo-out.txt"},
    "hooks" => [["on_output", %{}]]
  })

{:ok, _} = Admin.disable_plugin(runner, "writer")
```

<!-- tabs-close -->

The object carries what the command line carries: the plugin's id, its provider,
the operator it attaches to, the provider's own options under `opts`, and the
hook that `--hook=on_output` spells on the command line. Every echo example
carries its enable commented out right after the operator's start, ready to
uncomment, and a message sent after it lands in the file.
[`../sdk/admin.md`](../sdk/admin.md) is the whole surface.

What it cannot do is worth knowing: installing, upgrading and uninstalling a
plugin package are node-wide acts, so they stay with the user. An
application may attach and detach the plugins its own namespace already has.

## Other sources and sinks

The file plugin is one of a family, and they attach exactly the same way: an id,
a provider, an operator and its options.

| Plugin | What it connects |
|---|---|
| [`http_poll.md`](../plugins/http_poll.md) | an HTTP resource as an input |
| [`websocket_subscription.md`](../plugins/websocket_subscription.md) | a live socket, both directions |
| [`kafka_producer.md`](../plugins/kafka_producer.md) | Kafka as an output |
| [`postgres_sink.md`](../plugins/postgres_sink.md) | Postgres as an output |
| [`operator_forward.md`](../plugins/operator_forward.md) | one operator's output into another's input |

## Next

You have a controller, a runner, an operator and real data moving through it.
[`stop_everything.md`](stop_everything.md), the last page of Part 1, stops all of
it and says what comes back when you start again.
