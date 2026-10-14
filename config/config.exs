import Config

config :flezha,
  # The environment the application is running in. The one key `Mix.env()`
  # answers; every other knob is a row of `Flezha.Config.Keys` and its
  # per-environment default is that row's `env_default`.
  env: Mix.env(),

  # Validator hooks to run before and after boot: function bodies, not values,
  # so they are written where they are read.
  pre_hooks: [],
  post_hooks: []

# The controller runs no Elixir SDK runner in its own VM, so the SDK's native
# library is neither downloaded nor compiled. `config/test.exs` builds it: a
# test starts a runner in the controller's own BEAM.
config :flezha_sdk, socket_nif: false

config :logger,
  handle_sasl_reports: true,
  handle_otp_reports: true,
  level: :warning

config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id, :module, :function]

# The path, level and rotation are rows of `Flezha.Config.Keys`, written here
# by `config/runtime.exs`. The format and the metadata are the logger's own.
config :logger, :file_log,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id, :module, :function, :line]

import_config "#{Mix.env()}.exs"
