defmodule Flezha.Transport.Native do
  @moduledoc """
  Rustler NIF module wrapping the `flz_nif` crate.

  Provides the controller's UDS and QUIC servers. A runner's client side is the
  Elixir SDK's own NIF, `Flezha.SDK.Native`.
  The NIF handles transport I/O and binary frame parsing in Rust,
  delivering complete `{:flz_data, stream_id, payload_id, message, metadata}`
  tuples to Elixir processes.

  ## Messages sent to owning process

    * `{:flz_connection, stream_ref}` — new connection accepted
    * `{:flz_data, stream_id, payload_id, message, metadata}` — complete parsed frame
    * `{:flz_closed, stream_id, reason}` — stream/connection closed
  """

  alias Flezha.Config

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

  @doc "Stop a running transport server. Its socket is released when this returns."
  def stop_server(_server_ref), do: :erlang.nif_error(:not_loaded)

  @doc """
  Self-reported driver liveness for a server. Returns a map
  (`%{now_ms, loop_iterations, last_loop_ms, cmd_queue_depth, streams: [...]}`),
  one shape for both the UDS and the QUIC driver. A frozen `loop_iterations`
  with a stale `last_loop_ms` means the driver thread is wedged.
  """
  def driver_stats(_server_ref), do: :erlang.nif_error(:not_loaded)

  # --- Behaviour callbacks (flow tasklets) ---

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
  Deliver the next incoming frame, then buffer again until the stream is
  activated. What arrives behind that frame waits for the next activation, which
  is how a stream changes owner without a frame reaching the wrong one.
  """
  def set_active_once(_stream_ref), do: :erlang.nif_error(:not_loaded)

  @doc """
  Return the driver-assigned stream id for a handle. Matches the id carried by
  `{:flz_data, stream_id, ...}` and `{:flz_closed, stream_id, ...}`, so an owner
  can correlate a `{:flz_connection, handle}` with that stream's later frames.
  """
  def stream_id(_stream_ref), do: :erlang.nif_error(:not_loaded)

  @doc """
  The DER certificate the peer presented on this stream's connection, captured
  once at the handshake, or `nil` when there was none — every UDS stream, and a
  QUIC one whose peer authenticated with no client certificate.
  """
  @spec peer_certificate(reference()) :: binary() | nil
  def peer_certificate(_stream_ref), do: :erlang.nif_error(:not_loaded)

  @doc """
  The process id of the peer on this stream's connection, as the kernel recorded
  it at connect, or `nil` for a QUIC peer and where the platform cannot say.
  """
  @spec peer_pid(reference()) :: non_neg_integer() | nil
  def peer_pid(_stream_ref), do: :erlang.nif_error(:not_loaded)

  # --- Where this controller's sockets are ---

  @doc """
  Where this controller's sockets are, from `flz_transport`: the resolved
  absolute socket directory, creating the first ladder candidate it can, and
  the QUIC candidate list. Returns `{:ok, socket_dir, quic_addresses} |
  {:error, reason}`.

  `socket_dir` and `quic_addresses` pass this node's own reading of
  `FLEZHA_SOCKET_FILES_PATH` and `FLEZHA_QUIC_ADDRESSES`, `nil` when unset: the
  BEAM keeps its own environment table, so a value set with `System.put_env/2`
  never reaches `std::env::var` in the NIF.
  """
  def resolve_connect(_socket_dir, _quic_addresses), do: :erlang.nif_error(:not_loaded)

  @doc "`resolve_connect/2` with this node's environment view supplied."
  @spec resolve_connect() :: {:ok, String.t(), [String.t()]} | {:error, String.t()}
  def resolve_connect, do: resolve_connect(socket_dir_env(), quic_env())

  # The NIF reads `std::env::var` in its own thread, which never sees the BEAM's
  # environment table. Handing it this node's answers is what lets the
  # controller's view win without a second copy of the ladder.
  defp socket_dir_env, do: Config.get(:socket_files_path)
  defp quic_env, do: Config.get(:quic_addresses)

  # --- Instance claims ---
  #
  # What one controller instance owns exclusively on a host: the socket
  # directory it listens in, asked of `flz_transport`'s probe, and the state
  # root it writes, asked of the kernel. They live on this module because
  # rustler binds one Elixir module per NIF library.

  @doc """
  Whether a controller is listening in `socket_dir` right now — `flz_transport`'s
  probe, the same one a runner's diagnosis asks, from the controller's side. A
  directory a live peer answers in belongs to that peer; one holding a socket
  nobody answers was left by a controller that died hard.
  """
  @spec socket_dir_live(String.t()) :: boolean()
  def socket_dir_live(_socket_dir), do: :erlang.nif_error(:not_loaded)

  @doc """
  A runner's data-socket path in `socket_dir` — `flz_transport`'s own
  composition, the one every other SDK reaches through `flz_core`, asked for
  here rather than spelled so one runner cannot end up with two names. The
  runner-id field is bounded, and a path a Unix domain socket address cannot
  hold is refused by name and length.
  """
  @spec stream_socket_path(String.t(), String.t(), String.t()) ::
          {:ok, String.t()} | {:error, String.t()}
  def stream_socket_path(_socket_dir, _runner_id, _stream_type),
    do: :erlang.nif_error(:not_loaded)

  @doc """
  The control-socket path in `socket_dir` — the composer beside
  `stream_socket_path/3`. The controller that listens and the Elixir SDK
  runner that dials both ask for the name here, so neither spells it.
  """
  @spec control_socket_path_in(String.t()) :: {:ok, String.t()} | {:error, String.t()}
  def control_socket_path_in(_socket_dir), do: :erlang.nif_error(:not_loaded)

  @doc """
  The runner-id ceiling a data-socket name applies, in bytes. Read so the side
  that MINTS a runner id can bound what it mints: a longer id is shortened by
  the transport into a name the minting side never reports.
  """
  @spec stream_socket_id_max() :: non_neg_integer()
  def stream_socket_id_max(), do: :erlang.nif_error(:not_loaded)

  @doc """
  Claim `state_root` for this controller with an exclusive advisory lock on a
  file inside it, held for the OS process lifetime and released by the kernel
  when it exits. `{:ok, lock_file}`, or `{:error, holder}` naming the process
  that holds it.
  """
  @spec lock_state_root(String.t()) :: {:ok, String.t()} | {:error, String.t()}
  def lock_state_root(_state_root), do: :erlang.nif_error(:not_loaded)

  @doc """
  The lock file this OS process holds, `""` before it has claimed one — asked
  of the claim itself rather than of a copy kept beside it.
  """
  @spec held_state_root() :: String.t()
  def held_state_root(), do: :erlang.nif_error(:not_loaded)
end
