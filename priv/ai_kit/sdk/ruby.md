# The ruby SDK's echo example

Every file of `sdk/ruby/examples/echo`, the runner Up and running builds. Its dependency file names the SDK package and the version this release pins.

## examples/echo/Gemfile

````
# frozen_string_literal: true

source "https://rubygems.org"

gem "flezha", "0.1.0"
````

## examples/echo/README.md

````markdown
# Echo: the Ruby hello world

The Ruby hello world, as a standalone project. Nothing here depends on the
Flezha source tree: the `Gemfile` names one released version of `flezha`, the
way your own project would. It needs Ruby 3.3 or later.

Three files, one for each part the guides describe:

| File | Part | What it does |
|---|---|---|
| `echo_tasklet.rb` | the tasklet | the processing stage: prefixes each message |
| `echo_relay.rb` | the relay | the graph: `in` feeds the tasklet, output lands on `out` |
| `runner.rb` | the runner | this process: registers the echo factory and waits |

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

The gem is not on RubyGems yet. From the SDK's repository root, with the Ruby
you will run the runner with, build a gem for this platform and install it once:

```sh
rake native gem
gem install pkg/flezha-0.1.0-*.gem
```

`rake native gem` compiles the SDK's native library from the core's git tag, so
it needs cargo and git. Once the gem is installed, `bundle install` in this
folder finds it, and once it is published the step goes away.

## Where to go next

[`docs/guides/runner_deployment.md`](../../../../docs/guides/runner_deployment.md)
walks through what each of the three parts is and why the split is the way it
is. [`docs/sdk/ruby_sdk.md`](../../../../docs/sdk/ruby_sdk.md) covers the rest of
the SDK.
````

## examples/echo/echo_relay.rb

````ruby
# frozen_string_literal: true

require "flezha"
require_relative "echo_tasklet"

# The deployable unit. A relay declares the graph a message travels: the
# entry channel it arrives on, the steps it passes through, and the channel its
# result leaves on.
#
# The graph is checked as a whole when the runner declares it, so one that
# cannot work is refused at startup. The entry names the text codec, so the
# tasklet is handed a String, and the output names none, so a String result
# leaves as its own bytes.
class EchoRelay < Flezha::Relay
  # The relay's stable identity, and the name `flz relay start` takes.
  factory_id "echo" # the name `flz relay start` takes

  channels do |c|
    c.input("in", codec: :text) # messages arrive on the entry channel "in"
     .into(EchoTasklet)          # each one runs through the echo tasklet
     .output("out")              # the result leaves on "out", for a plugin to pick up
  end
end
````

## examples/echo/echo_tasklet.rb

````ruby
# frozen_string_literal: true

require "flezha"

# A processing stage of the relay's graph, run once per message. This one
# prefixes the input and emits it through the context, the one way a value
# leaves a tasklet; what `run` returns is not output.
#
# `tasklet_name` is required. State is filed under it, so renaming the class
# keeps it, and renaming `tasklet_name` declares a different tasklet. With no
# `init_state` and no `apply_change`, nothing is kept between messages and
# nothing is journaled. Where the result goes is not decided here: the relay
# decides that, so the graph can be rewired without touching this class.
class EchoTasklet < Flezha::Tasklet
  tasklet_name "echo" # state is saved under this name, so choose it once

  def run(input, ctx) = ctx.emit("echo: #{input}") # where it goes is the relay's business
end
````

## examples/echo/runner.rb

````ruby
# frozen_string_literal: true

# Starts a runner: a process that connects to the controller and serves your
# relays.
#
# Declare every relay you want served before `boot`. One runner can serve
# several, and the same call takes a steps relay.

require "flezha"
require_relative "echo_relay"

namespace = ARGV[0] || "default"

# the relays this runner serves
runner = Flezha.create_runner(namespace).register_factory(EchoRelay)

# `run` is `boot` followed by `await_termination`. Splitting them leaves room
# for the calls below, and still blocks until shutdown.
runner.boot # connects to the controller

# Everything `flz relay` and `flz plugin` do is also a method of `control`,
# and `send_to` hands a message to a relay. Uncomment to drive this
# namespace from the application instead of from a terminal:
#
# control = runner.control
# control.start_relay("echo", id: "my_echo")
# control.enable_plugin("writer", "relay_file_output", opts: { "path" => "echo-out.txt" })
# control.bind_plugin("writer", "my_echo", hooks: [["on_output", {}]])
# runner.send_to("my_echo", "hello world", "in", msg_id: "hello-1")
#
# docs/topics/advanced/sending_from_runner_code.md covers sending. An enforced
# controller refuses all of it until `flz role bind runner@default app default`.

# Blocks until shutdown: a SIGTERM, or standard input closing.
runner.await_termination
````
