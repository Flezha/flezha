# Connect a plugin

On this page you attach a file to each side of `my_echo`: lines added to one
file go in, and their echoes land in another. The relay declared where its
data comes in (`in`) and goes out (`out`); a plugin decides what is on the
other side, so the code does not change.
[Plugins](../topics/plugins/plugins.md) explains how plugins are enabled and
bound, and [the file plugin](../plugins/file.md) lists its options.

> #### What you need {: .tip}
>
> [Up and running](up_and_running.md) done first, with its controller, your
> runner and `my_echo` still running.

```mermaid
flowchart LR
    I(["echo-in.txt"]) --> R["file plugin<br/>reader"]
    R --> E["my_echo<br/>on your runner"]
    E --> W["file plugin<br/>writer"] --> O(["echo-out.txt"])
```

File plugins read and write only inside one folder, `storage/app/files` under the
directory the controller was started from (`/flezha/storage/app/files` in the
Docker volume), and in it only in their namespace's folder: `storage/app/files/default`
here. Run the commands below from the directory the controller was
started in, or with Docker in the container's shell.

## Write the output to a file

> Attach a file output named `writer` to `my_echo`, so everything it sends out is
> written to `echo-out.txt`. The controller, your runner and `my_echo` have to
> be running.
> {: .step}

```sh
$ ./bin/flz plugin enable writer relay_file_output \
  --relay_id=my_echo \
  --hook=on_output \
  --Xpath=echo-out.txt
```

```output
Enabling plugin...
Plugin writer enabled.
Plugin writer bound to my_echo.
```

The command did two things. It enabled a plugin named `writer` from the
provider `relay_file_output`, and it bound `my_echo` to it. Its flags are of
three kinds:

| Flag | Belongs to | Here |
|---|---|---|
| `--relay_id` | the command: which relay to bind | `my_echo` |
| `--hook` | the binding: when the controller calls the plugin | `on_output`, with everything the relay sends out |
| `--X<option>` | the plugin: its own settings, one flag each | `--Xpath`, the file to write |

The `X` keeps a plugin's settings apart from the command's own flags.
`flz plugin help relay_file_output` lists every setting and hook the provider
takes, and `flz plugin list --providers` the providers there are.

Send a message, and a moment later it is in the file:

```sh
$ ./bin/flz relay send my_echo --text hello
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

Each record the relay sends out becomes one line, appended to the file.

## Read the input from a file

> Attach a file source named `reader`, so every line added to `echo-in.txt` is
> sent in on the channel `in`.
> {: .step}

```sh
$ ./bin/flz plugin enable reader relay_file_input \
  --relay_id=my_echo \
  --Xpath=echo-in.txt \
  --Xchannel_in=in
```

```output
Enabling plugin...
Plugin reader enabled.
Plugin reader bound to my_echo.
```

A source takes no hook: it is not sent anything, and pushes its input in by
itself. `--Xchannel_in` names the entry channel each line arrives on.

> Add a line to the input file, then print the output file a second later.
> {: .step}

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

That is the whole loop. How the reader handles the file:

- **It looks for new lines once a second**, `poll_interval_ms` by default.
- **A line counts once its newline is written.** A half-written last line waits,
  so a writer that is mid-line is never read too early.
- **It keeps its place.** It remembers the byte offset of the last line it
  delivered, and resumes from there after a restart or a move to another
  controller.
- **A line can arrive twice, never not at all.** The offset is saved in
  batches, so a crash can send again the lines delivered since the last save.

## See what is attached

`flz plugin list` shows the plugins enabled in the namespace and the relays
each one is bound to:

```sh
$ ./bin/flz plugin list
```

```output
reader  file_source  bound: my_echo  runs on: flezha@127.0.0.1
writer  file_sink  bound: my_echo  runs on: flezha@127.0.0.1
```

`runs on` names the controllers where a plugin is running: the file plugins run
inside the relay itself, so they show the relay's controller, and a plugin
running nowhere shows `idle`. `--json` prints the same rows as JSON.

## Unbind and bind again

A binding can be taken away while the plugin stays enabled:

```sh
$ ./bin/flz plugin unbind writer --relay_id=my_echo
```

```output
Plugin writer unbound from my_echo.
```

Until it is bound again, what `my_echo` sends out is not written to the file.
The hook belongs to the binding, so binding again names it again:

```sh
$ ./bin/flz plugin bind writer --relay_id=my_echo --hook=on_output
```

```output
Plugin writer bound to my_echo.
```

One plugin can be bound to several relays this way, each with its own hooks.

## Change a plugin's settings

`flz plugin update` changes an enabled plugin's settings without disabling it.
Here `writer` is told to write only what `my_echo` sends on its output channel
`out`, which is all it sends:

```sh
$ ./bin/flz plugin update writer --Xchannel_out=out
```

```output
Plugin writer updated; its running copies restarted.
warning: restarted under my_echo
```

A setting the provider can apply while it runs changes in place, and the
command says `updated in place`. The file plugins have none, so `writer`
restarts with the new setting, and the warning names the relays it restarted
under. An update that changes nothing prints `Plugin writer is unchanged.`

[Enabling and binding](../topics/plugins/enabling_and_binding.md) covers every
`flz plugin` command, and [Hooks](../topics/plugins/hooks.md) every hook.
