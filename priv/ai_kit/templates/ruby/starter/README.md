# Starter: a relay that counts visits per page

A small, complete Flezha project in Ruby, to grow into your own: one relay, one
tasklet that keeps state, and a test. The `Gemfile` names the SDK by its
released version, the way your own project does. It needs Ruby 3.3 or later.

| File | What it is |
|---|---|
| `count.rb` | the tasklet `count`: keeps a page's total in state, changes it only through the change `run` records, and answers the new count |
| `starter_relay.rb` | the relay `starter`: the entry channel `hit`, described and keyed by page, into `count`, out on `counts` |
| `runner.rb` | the runner: serves the relay until shutdown |
| `count_test.rb` | the test: four hits with no controller, and the replay check |

## Run it

You need a controller running and reachable, as
[Start the controller](https://github.com/flezha/flezha/blob/main/docs/public/getting_started/up_and_running.md#start-the-controller)
starts one. From this folder, install the SDK, run the test and start the
runner:

```sh
bundle install
bundle exec ruby count_test.rb
bundle exec ruby runner.rb
```

Then, from another terminal, start the relay and call it:

```sh
flz relay start starter --id starter
flz relay call starter --channel hit --arg page=home
```

```
{"page":"home","total":1}
```

Each call for the same page answers the next total. `flz` is the release's
`bin/flz`. The runner keeps running until a SIGTERM, Ctrl-C or its standard
input closing. The namespace is its first argument and defaults to `default`.

## Make it yours

Nothing inside the files is renamed for you. `starter` is the name
`flz relay start` takes. `count` is the name the tasklet's state is saved under,
so choose it before the first run.

## Building from a checkout

To run the project against a Flezha checkout instead of the published gem, build
a gem for this platform in the checkout's `sdk/ruby`, with the Ruby you run the
runner with (it needs cargo and git), and install it once:

```sh
rake native gem
gem install pkg/flezha-0.1.0-*.gem
```

`bundle install` in this folder then finds it.
