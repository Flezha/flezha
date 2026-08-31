defmodule Flezha.AuthZ.AuthState do
  @moduledoc """
  Durable `:enabled` flag for the authn/authz subsystem.

  Lives in `<auth_dir>/state.dets`. The only writer is the
  bootstrap-claim flow, which flips the flag from `false` to `true`
  after a successful `flz auth bootstrap`. Once `true`, the flag is
  durable across restarts; it never flips back unless the file is
  removed manually.

  The runtime `enabled?/0` accessor is the source of truth consumed
  by `Flezha.AuthZ.enabled?/0`. Before the GenServer has been
  started (very early boot) it returns the conservatively-safe
  `false`.
  """

  use GenServer

  alias Flezha.AuthZ.{Config, DetsStore}

  @table :flz_authz_state
  @filename "state.dets"
  @key :enabled

  ## ----- Public API -----

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "True when the durable flag is set."
  @spec enabled?() :: boolean()
  def enabled?() do
    case DetsStore.cache_lookup(@table, @key) do
      {:ok, value} when is_boolean(value) -> value
      _ -> false
    end
  end

  @doc """
  Flip the durable flag to `true`. Idempotent. The fsync completes
  before the call returns.
  """
  @spec set_enabled() :: :ok | {:error, term()}
  def set_enabled() do
    GenServer.call(__MODULE__, :set_enabled)
  end

  @doc """
  Force the flag back to `false`. Used by the break-glass rebootstrap
  recovery (`Flezha.AuthZ.Bootstrap.consume_recovery_marker/0`) and by tests.
  """
  @spec reset() :: :ok
  def reset() do
    GenServer.call(__MODULE__, :reset)
  end

  @doc "Absolute path of the backing DETS file."
  @spec path() :: String.t()
  def path() do
    Path.join(Config.auth_dir(), @filename)
  end

  ## ----- GenServer -----

  @impl true
  def init(_opts) do
    # The ETS cache shares the DETS table's name (separate namespaces).
    DetsStore.init_cached(@table, @table, path())
  end

  @impl true
  def handle_call(:set_enabled, _from, state) do
    :ok = DetsStore.put(@table, @table, @key, true)
    {:reply, :ok, state}
  end

  def handle_call(:reset, _from, state) do
    :ok = DetsStore.put(@table, @table, @key, false)
    {:reply, :ok, state}
  end

  @impl true
  def terminate(_reason, _state) do
    _ = :dets.close(@table)
    :ok
  end
end
