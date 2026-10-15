import Config

alias Flezha.Config.Environment

# Every knob is a row of `Flezha.Config.Keys`, and this file is the one place
# the OS environment is turned into the application env: one call per
# application, no name, default or derivation of its own. A knob is added by
# writing a row — this file is not edited for one.
#
# `Keys` is a project module, and this file names it deliberately: it is
# evaluated after `lib/` is compiled on every path that reaches it, which the
# release boot and every `mix` task have done since the durations table landed.
environment = System.get_env()

config :flezha, Environment.runtime_config(environment, config_env())

# `:logger`'s own application env, from the four rows carrying this tree's
# log-file names. The format and the metadata are the logger's business and
# stay in `config/config.exs`.
config :logger, :file_log,
  path: Environment.env_value(environment, config_env(), :log_path),
  level: Environment.env_value(environment, config_env(), :log_level),
  rotate: %{
    max_bytes: Environment.env_value(environment, config_env(), :log_max_bytes),
    keep: Environment.env_value(environment, config_env(), :log_keep_files)
  }

# Not a knob: a VM `flzvm eval` starts answers one `flz` verb, and its stderr
# is the verb's. It logs warnings and errors only, from `:logger`'s start, so
# OTP's boot progress reports, which the application controller logs after the
# boot has moved on, never race the verb's halt onto it.
if environment["RELEASE_COMMAND"] == "eval", do: config(:logger, level: :warning)
