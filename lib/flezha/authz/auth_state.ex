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

  require Logger

  alias Flezha.AuthZ.{Config, Store}

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
    case :ets.whereis(@table) do
      :undefined ->
        false

      _ ->
        case :ets.lookup(@table, @key) do
          [{@key, value}] when is_boolean(value) -> value
          _ -> false
        end
    end
  rescue
    _ -> false
  end

  @doc """
  Flip the durable flag to `true`. Idempotent. The fsync completes
  before the call returns.
  """
  @spec set_enabled() :: :ok | {:error, term()}
  def set_enabled() do
    GenServer.call(__MODULE__, :set_enabled)
  end

  @doc "Force the flag back to `false`. Test-support only."
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
    file_path = path()
    :ok = Store.ensure_auth_dir!(Path.dirname(file_path))

    case Store.open(@table, file_path) do
      {:ok, _} ->
        :ok = ensure_ets!()
        :ok = load_into_cache()
        {:ok, %{path: file_path}}

      {:error, reason} ->
        {:stop, {:dets_open_failed, file_path, reason}}
    end
  end

  @impl true
  def handle_call(:set_enabled, _from, state) do
    :ok = :dets.insert(@table, {@key, true})
    :ok = :dets.sync(@table)
    :ets.insert(@table, {@key, true})
    {:reply, :ok, state}
  end

  def handle_call(:reset, _from, state) do
    :ok = :dets.insert(@table, {@key, false})
    :ok = :dets.sync(@table)
    :ets.insert(@table, {@key, false})
    {:reply, :ok, state}
  end

  @impl true
  def terminate(_reason, _state) do
    _ = :dets.close(@table)
    :ok
  end

  ## ----- private -----

  defp ensure_ets!() do
    case :ets.whereis(@table) do
      :undefined ->
        :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])

      _ ->
        :ok
    end

    :ok
  end

  defp load_into_cache() do
    value =
      case :dets.lookup(@table, @key) do
        [{@key, v}] when is_boolean(v) -> v
        _ -> false
      end

    :ets.insert(@table, {@key, value})
    :ok
  end
end
