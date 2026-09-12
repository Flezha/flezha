import Config

# A host application configures the controller through its own application
# env. `FLEZHA_*` variables are read by the flezha repository's own
# `config/runtime.exs`, which a dependency does not evaluate, so they do
# nothing here unless this application translates them itself.
config :flezha, healthz_enabled: true

# Required, not optional. `:kafka_ex` is an unconditional dependency of flezha
# and starts a default worker when its application starts; with no brokers
# configured that worker raises and takes the whole boot with it. flezha
# disables it in its own `config/config.exs`, which this application does not
# inherit, so it repeats the setting here.
config :kafka_ex, disable_default_worker: true, kafka_version: "kayrock"

import_config "#{config_env()}.exs"
