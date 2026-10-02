import Config

# Compile flezha's SDK the way the controller release does: a controller dials
# no socket, so the SDK's socket client is neither downloaded nor built.
config :flezha_sdk, socket_nif: false

# The bundled library's own setting: the package carries it in `stamp_lib`'s `.app`.
config :stamp_lib, prefix: ">>"
