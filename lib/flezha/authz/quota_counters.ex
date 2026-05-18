defmodule Flezha.AuthZ.QuotaCounters do
  @moduledoc """
  Durable per-namespace counters backed by
  `<auth_dir>/quota_counters.dets`, fronted by an ETS read cache.

  Counters tracked:

    * `:relays`           — count of active Relays
    * `:partitions`       — count of active Partitions
    * `:mailbox_bytes`    — total allocated mailbox bytes
    * `:executors`        — count of connected SDK Executors
    * `:disk_bytes`       — cached disk footprint snapshot (refreshed
      separately by `Flezha.AuthZ.Quotas`)

  ETS reads are direct (no GenServer hop). Mutations go through the
  GenServer, write DETS with `:dets.sync/1`, then refresh the cache.
  """

  use GenServer

  alias Flezha.AuthZ.{Config, Store}

  @table :flz_authz_quota_counters
  @cache :flz_authz_quota_counters_cache
  @filename "quota_counters.dets"

  @kinds [:relays, :partitions, :mailbox_bytes, :executors, :disk_bytes]

  ## ----- Public API -----

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Get the current counter for `(namespace, kind)`. Returns 0 when absent."
  @spec get(binary(), atom()) :: non_neg_integer()
  def get(namespace, kind) when is_binary(namespace) and is_atom(kind) do
    case :ets.whereis(@cache) do
      :undefined ->
        0

      _ ->
        case :ets.lookup(@cache, {namespace, kind}) do
          [{_, value}] when is_integer(value) -> value
          _ -> 0
        end
    end
  rescue
    _ -> 0
  end

  @doc "Increment the counter by `delta` (default 1)."
  @spec incr(binary(), atom(), integer()) :: :ok
  def incr(namespace, kind, delta \\ 1)
      when is_binary(namespace) and is_atom(kind) and is_integer(delta) do
    GenServer.cast(__MODULE__, {:adjust, namespace, kind, delta})
  end

  @doc "Decrement the counter by `delta` (default 1)."
  @spec decr(binary(), atom(), integer()) :: :ok
  def decr(namespace, kind, delta \\ 1)
      when is_binary(namespace) and is_atom(kind) and is_integer(delta) do
    incr(namespace, kind, -delta)
  end

  @doc "Overwrite the counter with `value` (used by the disk-footprint refresh)."
  @spec set(binary(), atom(), non_neg_integer()) :: :ok
  def set(namespace, kind, value)
      when is_binary(namespace) and is_atom(kind) and is_integer(value) and value >= 0 do
    GenServer.call(__MODULE__, {:set, namespace, kind, value})
  end

  @doc "Drop every counter for a namespace (used by `flz ns delete`)."
  @spec drop_namespace(binary()) :: :ok
  def drop_namespace(namespace) when is_binary(namespace) do
    GenServer.call(__MODULE__, {:drop_namespace, namespace})
  end

  @doc "Path of the backing DETS file."
  @spec path() :: String.t()
  def path(), do: Path.join(Config.auth_dir(), @filename)

  @doc "Documented counter kinds."
  @spec kinds() :: [atom()]
  def kinds(), do: @kinds

  ## ----- GenServer -----

  @impl true
  def init(_opts) do
    file_path = path()
    :ok = Store.ensure_auth_dir!(Path.dirname(file_path))

    case Store.open(@table, file_path) do
      {:ok, _} ->
        :ok = ensure_cache!()
        :ok = warm_cache_from_dets()
        {:ok, %{path: file_path}}

      {:error, reason} ->
        {:stop, {:dets_open_failed, file_path, reason}}
    end
  end

  @impl true
  def handle_cast({:adjust, namespace, kind, delta}, state) do
    {:noreply, do_adjust(namespace, kind, delta, state)}
  end

  @impl true
  def handle_call({:set, namespace, kind, value}, _from, state) do
    key = {namespace, kind}
    :ok = :dets.insert(@table, {key, value})
    :ok = :dets.sync(@table)
    :ets.insert(@cache, {key, value})
    {:reply, :ok, state}
  end

  def handle_call({:drop_namespace, namespace}, _from, state) do
    Enum.each(@kinds, fn kind ->
      key = {namespace, kind}
      :ok = :dets.delete(@table, key)
      :ets.delete(@cache, key)
    end)

    :ok = :dets.sync(@table)
    {:reply, :ok, state}
  end

  @impl true
  def terminate(_reason, _state) do
    _ = :dets.close(@table)
    :ok
  end

  ## ----- private -----

  defp do_adjust(namespace, kind, delta, state) do
    key = {namespace, kind}

    current =
      case :dets.lookup(@table, key) do
        [{^key, v}] when is_integer(v) -> v
        _ -> 0
      end

    new = max(current + delta, 0)

    :ok = :dets.insert(@table, {key, new})
    :ok = :dets.sync(@table)
    :ets.insert(@cache, {key, new})

    state
  end

  defp ensure_cache!() do
    case :ets.whereis(@cache) do
      :undefined ->
        :ets.new(@cache, [:set, :public, :named_table, read_concurrency: true])

      _ ->
        :ets.delete_all_objects(@cache)
    end

    :ok
  end

  defp warm_cache_from_dets() do
    :dets.match_object(@table, {:_, :_})
    |> Enum.each(fn {key, value} -> :ets.insert(@cache, {key, value}) end)

    :ok
  end
end
