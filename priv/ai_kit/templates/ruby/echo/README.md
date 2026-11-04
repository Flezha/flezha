# Echo: the Ruby hello world

The Ruby hello world, as a standalone project. Nothing here depends on the
Flezha source tree: the `Gemfile` names one released version of `flezha`, the
way your own project would. It needs Ruby 3.3 or later.

Three files, one for each of the [building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md):

| File | Part | What it does |
|---|---|---|
| `echo_tasklet.rb` | the tasklet | the processing stage: prefixes each message |
| `echo_relay.rb` | the relay | the graph: `in` feeds the tasklet, output lands on `out` |
| `runner.rb` | the runner | this process: registers the echo factory and waits |

## Run it

You need a controller running and reachable first
([Up and running](https://github.com/flezha/flezha/blob/main/docs/public/getting_started/up_and_running.md)).
From this folder:

```sh
bundle install
bundle exec ruby runner.rb
```

Then, from another terminal:

```sh
flz relay start echo --id my_echo
flz relay send my_echo --text 'hello world' --await 2000
```

```
Delivered to 'my_echo'.
out	echo: hello world
```

It keeps running until a SIGTERM, Ctrl-C, or its standard input closing, so do
not start it with standard input closed. The namespace is the first argument
and defaults to `default`: `bundle exec ruby runner.rb my_app`.

## Building from a checkout

To run the example against a checkout of the SDK instead of the published gem, build
a gem for this platform from the SDK's repository root, with the Ruby you will run
the runner with, and install it once:

```sh
rake native gem
gem install pkg/flezha-0.1.1-*.gem
```

`rake native gem` compiles the SDK's native library from the core's git tag, so
it needs cargo and git. Once the gem is installed, `bundle install` in this
folder finds it.

## Where to go next

[Building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md)
walks through what each of the three parts is and why the split is the way it
is. The [Ruby SDK guide](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/ruby_sdk.md) covers the rest of
the SDK.
