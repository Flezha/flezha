# Feed the echo a file

On this page you attach a file to each side of `my_echo`: lines added to one
file go in, and their echoes land in another. The operator declared where its
data comes in (`in`) and goes out (`out`); a plugin decides what is on the
other side, so the code does not change.
[Plugins](../../topics/basics/plugins.md) explains how plugins are enabled and
bound, and [the file plugin](../../plugins/file.md) lists its options.

File plugins read and write only inside one folder, `app_data/files` under the
directory the controller was started from (`/flezha/app_data/files` in the
Docker volume). Run the commands below from the directory the controller was
started in.

> Attach a file sink named `writer` to `my_echo`, so everything it sends out is
> written to `echo-out.txt`. The controller, your runner and `my_echo` have to
> be running.
> {: .step}

```sh
$ flz plugin enable writer operator_file_sink \
  --operator_id=my_echo \
  --hook=on_output \
  --Xpath=echo-out.txt
```

`writer` is the plugin's name, `operator_file_sink` is which plugin,
`--hook=on_output` sends it everything the operator emits, and `--Xpath` is the
plugin's own setting. Send a message, and a moment later it is in the file:

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

> Attach a file source named `reader`, so every line added to `echo-in.txt` is
> sent in on the channel `in`.
> {: .step}

```sh
$ flz plugin enable reader operator_file_source \
  --operator_id=my_echo \
  --Xpath=echo-in.txt \
  --Xchannel_in=in
```

> Add a line to the input file, then print the output file a second later.
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

That is the whole loop. The reader looks for new lines once a second and
resumes from where it stopped after a restart or a move.

`flz operator info` lists every operator with the plugins attached to it, their
files and their health:

```sh
$ flz operator info
```

```output
Operator: my_echo
  Run: flow
  Durable: local (sleep_after: false)
  Phase: running
  Health: healthy
  ...
  Plugins:
    - reader: Flezha.Plugin.Provider.File.Source
        path: /opt/flezha/app_data/files/echo-in.txt
        health: :healthy
    - writer: Flezha.Plugin.Provider.File.Sink
        hook: on_output []
        path: /opt/flezha/app_data/files/echo-out.txt
        health: :healthy
```

## Next

[Stop everything](stop_everything.md) takes it all down again.
