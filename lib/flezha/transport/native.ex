defmodule Flezha.Transport.Native do
  @moduledoc """
  Rustler NIF module wrapping the `flz_nif` crate.

  Provides transport procedures for UDS (and later QUIC) servers.
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

  @doc """
  Start a QUIC server listening on `bind_address`. Returns `{:ok, server_ref}`.

  A non-empty `client_ca_path` requires every peer to present a client cert
  chaining to that trust anchor; `""` disables client auth (explicit opt-out).
  """
  def start_quic_server(_bind_address, _cert_path, _key_path, _client_ca_path, _acceptor_pid),
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

  # --- Behaviour callbacks (stream procedures) ---

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

  # --- Client entry points (standalone SDK deployment) ---
  #
  # Thin wrappers over flz_transport's client + payloader; they return the same
  # StreamHandle resource the server drivers produce, so every per-stream op and
  # the `Flezha.Transport.Stream` seam covers a client handle with no new code.

  @doc """
  Where this process's controller is, from `flz_transport`: the resolved
  absolute socket directory for `role` (`"controller"` or `"runner"`) and the
  QUIC candidate list. `"controller"` creates the first ladder candidate it can;
  `"runner"` takes the first holding a live control socket and refuses when two
  do. Returns `{:ok, socket_dir, quic_addresses} | {:error, reason}`.

  `socket_dir` and `quic_addresses` pass this node's own reading of
  `FLEZHA_SOCKET_FILES_PATH` and `FLEZHA_QUIC_ADDRESSES`, `nil` when unset: the
  BEAM keeps its own environment table, so a value set with `System.put_env/2`
  never reaches `std::env::var` in the NIF.
  """
  def resolve_connect(_role, _socket_dir, _quic_addresses), do: :erlang.nif_error(:not_loaded)

  @doc """
  `resolve_connect/3` with this node's environment view supplied. Every Elixir
  caller wants this one; the arity-3 NIF exists so the view can be passed at
  all.
  """
  @spec resolve_connect(String.t()) ::
          {:ok, String.t(), [String.t()]} | {:error, String.t()}
  def resolve_connect(role), do: resolve_connect(role, socket_dir_env(), quic_env())

  @doc """
  Why this node cannot reach a controller over `transport` (`"uds"` or
  `"quic"`), as one line: for UDS the resolved directory, the candidates walked,
  which of the four situations it is (directory absent, socket absent, access
  denied, stale socket) and the fix; for QUIC the candidate list. The same text
  a Rust runner logs and `flz runner check` prints. `socket_dir` and
  `quic_addresses` carry this node's environment view, as in
  `resolve_connect/3`.
  """
  def connect_diagnosis(_transport, _socket_dir, _quic_addresses),
    do: :erlang.nif_error(:not_loaded)

  @doc """
  `connect_diagnosis/3` with this node's environment view supplied.
  """
  @spec connect_diagnosis(:uds | :quic) :: String.t()
  def connect_diagnosis(transport),
    do: connect_diagnosis(to_string(transport), socket_dir_env(), quic_env())

  # The BEAM keeps its own environment table, so a value set with
  # `System.put_env/2` never reaches `std::env::var` in the NIF. Reading it here
  # is what lets this node's view win without a second copy of the ladder.
  defp socket_dir_env, do: System.get_env("FLEZHA_SOCKET_FILES_PATH")
  defp quic_env, do: System.get_env("FLEZHA_QUIC_ADDRESSES")

  @doc """
  Connect a UDS client stream to `socket_path`, retrying the connect on a
  deadline (exponential from 10 ms, cap 500 ms, ~3 s total). Frames arrive at
  `owner_pid` as `{:flz_data, ...}` / `{:flz_closed, ...}`. Returns
  `{:ok, stream_handle} | {:error, reason}`.
  """
  def client_connect_uds(_socket_path, _owner_pid), do: :erlang.nif_error(:not_loaded)

  @doc """
  Open a QUIC client connection to `address` — one `resolve_connect/1` entry,
  re-resolved here on this attempt — verifying the server against
  `ca_cert_path` (and presenting the client identity when the cert/key env
  points at readable files). Returns `{:ok, conn} | {:error, reason}`.
  """
  def client_connect_quic(_address, _ca_cert_path), do: :erlang.nif_error(:not_loaded)

  @doc """
  Open a bidirectional QUIC stream on `conn`. `stream_type` 1/2 writes the 2-byte
  `[type, index]` preamble the server driver keys `{:flz_stream, ...}` on; 0 means
  the control stream and writes no preamble. Frames arrive at `owner_pid`.
  Returns `{:ok, stream_handle}`.
  """
  def client_open_stream(_conn, _stream_type, _index, _owner_pid),
    do: :erlang.nif_error(:not_loaded)
end
