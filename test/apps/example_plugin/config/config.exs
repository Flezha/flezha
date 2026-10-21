import Config

# Compile flezha's SDK the way the controller release does: a controller runs
# no runner, so the SDK's native library is neither downloaded nor built.
config :flezha_sdk, socket_nif: false

# The bundled library's own setting: the package carries it in `stamp_lib`'s `.app`.
config :stamp_lib, prefix: ">>"
