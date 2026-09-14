# Echo: the Ruby hello world

The Ruby hello world, as a standalone project. Nothing here depends on the
Flezha source tree: the `Gemfile` names one released version of `flezha`, the
way your own project would. It needs Ruby 3.3 or later.

Three files, one for each part the guides describe:

| File | Part | What it does |
|---|---|---|
| `echo_operative.rb` | the operative | the graph: `in` feeds the procedure, output lands on `out` |
| `echo_procedure.rb` | the procedure | the processing stage: prefixes each message |
| `runner.rb` | the runner | this process: offers the operative and waits |

## Run it

You need a controller running and reachable first
([`docs/guides/controller_deployment.md`](../../../../docs/guides/controller_deployment.md)).
From this folder:

```sh
bundle install
bundle exec ruby runner.rb
```

Then, from another terminal:

```sh
flz operative start my_echo echo
flz operative send my_echo --text 'hello world' --await 2000
```

```
Delivered to 'my_echo'.
out	echo: hello world
```

It keeps running until a SIGTERM, Ctrl-C, or its standard input closing, so do
not start it with standard input closed. The namespace is the first argument
and defaults to `default`: `bundle exec ruby runner.rb my_app`.

## Building from a checkout

The gem is not on RubyGems yet. From the repository root, with the Ruby you will
run the runner with, build a gem for this platform and install it once:

```sh
cd sdk/ruby
rake native_gem
gem install pkg/flezha-0.1.0-*.gem
```

`rake native_gem` compiles the SDK's native library, so it needs a Rust
toolchain; `scripts/build-ruby-sdk.sh` names what else it looks for. Once the gem
is installed, `bundle install` in this folder finds it, and once it is published
the step goes away.

## Where to go next

[`docs/guides/runner_deployment.md`](../../../../docs/guides/runner_deployment.md)
walks through what each of the three parts is and why the split is the way it
is. [`docs/sdk/ruby_sdk.md`](../../../../docs/sdk/ruby_sdk.md) covers the rest of
the SDK.
