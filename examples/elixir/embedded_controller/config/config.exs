import Config

# A host application configures the controller through its own application
# env. `FLEZHA_*` variables are read by the flezha repository's own
# `config/runtime.exs`, which a dependency does not evaluate, so they do
# nothing here unless this application translates them itself.
config :flezha, healthz_enabled: true

import_config "#{config_env()}.exs"
