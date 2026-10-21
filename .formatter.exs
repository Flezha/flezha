# Used by "mix format". Every tracked `.ex`/`.exs` in the repo is listed here: a
# script the formatter never sees is a script that drifts.
[
  locals_without_parens: [verb: 2, verb: 3],
  inputs: [
    "{mix,.formatter,.credo}.exs",
    "{config,lib,test}/**/*.{ex,exs}",
    "scripts/**/*.exs"
  ]
]
