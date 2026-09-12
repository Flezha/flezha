import Config

# Keep the test run off any controller already running on this machine: its
# own socket directory, and its own state root to take the exclusive lock on.
# The socket directory is short on purpose, because a runner's data socket
# address has to fit beside it.
config :flezha,
  socket_files_path: "/tmp/flezha_example_ctl",
  state_root: Path.expand("../app_data/state", __DIR__)
