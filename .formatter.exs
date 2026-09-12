# Used by "mix format". Every tracked `.ex`/`.exs` in the repo is listed here: a
# script the formatter never sees is a script that drifts, which is how
# `bench/acceptance/summarize.exs` came to need a 102-line reflow.
# `examples/` is narrowed to `plugins` because `examples/coding-fleet/` is a
# gitignored prototype with its own repo — `examples/**` would turn this check
# red on a file CI cannot see.
[
  locals_without_parens: [verb: 2, verb: 3],
  inputs: [
    "{mix,.formatter,.credo}.exs",
    "{config,lib,test}/**/*.{ex,exs}",
    "{bench,scripts}/**/*.exs",
    "examples/plugins/**/*.exs",
    "examples/elixir/**/*.{ex,exs}"
  ]
]
