defmodule Flezha.Transport.Native do
  @moduledoc """
  Rustler NIF module wrapping the `flz_nif` crate.

  Provides transport operations for UDS (and later QUIC) servers.
  The NIF handles transport I/O and binary frame parsing in Rust,
  delivering complete `{:flz_data, stream_id, payload_id, message, metadata}`
  tuples to Elixir processes.

  ## Messages sent to owning process

    * `{:flz_connection, stream_ref}` — new connection accepted
    * `{:flz_data, stream_id, payload_id, message, metadata}` — complete parsed frame
    * `{:flz_closed, stream_id, reason}` — stream/connection closed
  """

  use Rustler,
    otp_app: :flezha,
    crate: "flz_nif",
    path: "flz_nif",
    skip_compilation?: System.get_env("RUSTLER_SKIP_COMPILATION") == "true"

  @behaviour Flezha.Transport.Behaviour

  # --- Server lifecycle ---

  @doc "Start a UDS server listening on `socket_path`. Returns `{:ok, server_ref}`."
  def start_uds_server(_socket_path, _acceptor_pid), do: :erlang.nif_error(:not_loaded)

  @doc "Start a QUIC server listening on `bind_address`. Returns `{:ok, server_ref}`."
  def start_quic_server(_bind_address, _cert_path, _key_path, _acceptor_pid),
    do: :erlang.nif_error(:not_loaded)

  @doc "Stop a running transport server."
  def stop_server(_server_ref), do: :erlang.nif_error(:not_loaded)

  @doc """
  Self-reported driver liveness for a server. Returns a map
  (`%{now_ms, loop_iterations, last_loop_ms, cmd_queue_depth, streams: [...]}`)
  for UDS servers, or `:undefined` for QUIC. A frozen `loop_iterations` with a
  stale `last_loop_ms` means the driver thread is wedged.
  """
  def driver_stats(_server_ref), do: :erlang.nif_error(:not_loaded)

  # --- Behaviour callbacks (stream operations) ---

  @impl true
  def send_data(_stream_ref, _data), do: :erlang.nif_error(:not_loaded)

  @impl true
  def set_active(_stream_ref), do: :erlang.nif_error(:not_loaded)

  @impl true
  def close(_stream_ref), do: :erlang.nif_error(:not_loaded)

  @impl true
  def controlling_process(_stream_ref, _pid), do: :erlang.nif_error(:not_loaded)

  # --- Additional NIF functions ---

  @doc "Build a frame in Rust and send it. Avoids Elixir-side frame building."
  def send_frame(_stream_ref, _payload_id, _message, _metadata),
    do: :erlang.nif_error(:not_loaded)

  @doc "Disable active mode — the driver buffers incoming frames."
  def set_passive(_stream_ref), do: :erlang.nif_error(:not_loaded)

  @doc """
  Return the driver-assigned stream id for a handle. Matches the id carried by
  `{:flz_data, stream_id, ...}` and `{:flz_closed, stream_id, ...}`, so an owner
  can correlate a `{:flz_connection, handle}` with that stream's later frames.
  """
  def stream_id(_stream_ref), do: :erlang.nif_error(:not_loaded)
end
