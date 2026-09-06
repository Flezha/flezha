defmodule Flezha.AuthZ.QuotaCounters do
  @moduledoc """
  Durable per-namespace counters backed by
  `<auth_dir>/quota_counters.dets`, fronted by an ETS read cache.

  Counters tracked:

    * `:operatives`           — count of active Operatives
    * `:entities`       — count of active Entities
    * `:mailbox_bytes`    — total allocated mailbox bytes
    * `:runners`        — count of connected SDK Runners
    * `:disk_bytes`       — cached disk footprint snapshot (refreshed
      separately by `Flezha.AuthZ.Quotas`)

  ETS reads are direct (no GenServer hop). Mutations go through the
  GenServer, write DETS with `:dets.sync/1`, then refresh the cache.
  """

  use GenServer
  use Flezha.State.Store

  alias Flezha.DetsStore

  @table :flz_authz_quota_counters
  @cache :flz_authz_quota_counters_cache
  @filename "quota_counters.dets"

  @kinds [:operatives, :entities, :mailbox_bytes, :runners, :disk_bytes]

  ## ----- Public API -----

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Get the current counter for `(namespace, kind)`. Returns 0 when absent."
  @spec get(binary(), atom()) :: non_neg_integer()
  def get(namespace, kind) when is_binary(namespace) and is_atom(kind) do
    case DetsStore.cache_lookup(@cache, {namespace, kind}) do
      {:ok, value} when is_integer(value) -> value
      _ -> 0
    end
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

  @doc "Documented counter kinds."
  @spec kinds() :: [atom()]
  def kinds(), do: @kinds

  ## ----- store declaration -----

  @impl Flezha.State.Store
  def subtree(), do: :auth

  @impl Flezha.State.Store
  def filename(), do: @filename

  @impl Flezha.State.Store
  def table(), do: @table

  @impl Flezha.State.Store
  def cache(), do: @cache

  ## ----- GenServer -----

  @impl true
  def handle_cast({:adjust, namespace, kind, delta}, state) do
    {:noreply, do_adjust(namespace, kind, delta, state)}
  end

  @impl true
  def handle_call({:set, namespace, kind, value}, _from, state) do
    :ok = DetsStore.put(@table, @cache, {namespace, kind}, value)
    {:reply, :ok, state}
  end

  def handle_call({:drop_namespace, namespace}, _from, state) do
    Enum.each(@kinds, fn kind ->
      key = {namespace, kind}
      :ok = DetsStore.delete(@table, @cache, key)
    end)

    {:reply, :ok, state}
  end

  ## ----- private -----

  defp do_adjust(namespace, kind, delta, state) do
    key = {namespace, kind}

    current =
      case DetsStore.fetch(@table, key) do
        {:ok, v} when is_integer(v) -> v
        _ -> 0
      end

    new = max(current + delta, 0)

    :ok = DetsStore.put(@table, @cache, key, new)

    state
  end
end
