defmodule Flezha.AuthZ.QuotaCounters do
  @moduledoc """
  Durable per-namespace counters backed by
  `<auth_dir>/quota_counters.dets`, fronted by an ETS read cache.

  Counters tracked:

    * `:operatives`           — count of active Operatives
    * `:cells`       — count of active Cells
    * `:mailbox_bytes`    — total allocated mailbox bytes
    * `:runners`        — count of connected SDK Runners
    * `:disk_bytes`       — cached disk footprint snapshot (refreshed
      separately by `Flezha.AuthZ.Quotas`)

  ETS reads are direct (no GenServer hop). Mutations go through the
  GenServer, write DETS with `:dets.sync/1`, then refresh the cache.

  The process also keeps, per counter, when it was last written from the truth
  behind it. `claim_refresh/3` reads that stamp so concurrent callers rebuild a
  counter once between them rather than once each; `set/3` writes it, being the
  write that establishes a counter from its source.
  """

  use GenServer
  use Flezha.State.Store

  alias Flezha.DetsStore

  @table :flz_authz_quota_counters
  @cache :flz_authz_quota_counters_cache
  @filename "quota_counters.dets"

  @kinds [:operatives, :cells, :mailbox_bytes, :runners, :disk_bytes]

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

  @doc """
  Claim the right to rebuild `(namespace, kind)` from the truth behind it.

  Answers `:claimed` to one caller per `ttl_seconds` window and `:fresh` to
  every other, so a burst of callers costs one rebuild between them. The window
  is stamped on the claim, before the caller does the work, which is what makes
  the burst collapse: stamping after the work leaves every caller in the burst
  reading a window nobody has claimed yet.
  """
  @spec claim_refresh(binary(), atom(), non_neg_integer()) :: :claimed | :fresh
  def claim_refresh(namespace, kind, ttl_seconds)
      when is_binary(namespace) and is_atom(kind) and is_integer(ttl_seconds) and
             ttl_seconds >= 0 do
    GenServer.call(__MODULE__, {:claim_refresh, namespace, kind, ttl_seconds})
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
    {:reply, :ok, stamp(state, {namespace, kind}, System.monotonic_time(:second))}
  end

  def handle_call({:claim_refresh, namespace, kind, ttl_seconds}, _from, state) do
    key = {namespace, kind}
    now = System.monotonic_time(:second)

    # Monotonic seconds carry an arbitrary (typically negative) offset, so
    # "never stamped" has to be the absent key rather than a 0 default, which
    # would read as freshly written.
    case Map.get(stamps(state), key) do
      at when is_integer(at) and now - at < ttl_seconds -> {:reply, :fresh, state}
      _ -> {:reply, :claimed, stamp(state, key, now)}
    end
  end

  def handle_call({:drop_namespace, namespace}, _from, state) do
    Enum.each(@kinds, fn kind ->
      key = {namespace, kind}
      :ok = DetsStore.delete(@table, @cache, key)
    end)

    kept = Map.reject(stamps(state), fn {{ns, _kind}, _at} -> ns == namespace end)

    {:reply, :ok, Map.put(state, :refreshed_at, kept)}
  end

  ## ----- private -----

  # The stamps ride the store's state map and are volatile by design: they say
  # when this uptime last wrote a counter from its source, so a restart rebuilds
  # rather than trusting a window it cannot have observed.
  defp stamps(state), do: Map.get(state, :refreshed_at, %{})

  defp stamp(state, key, at), do: Map.put(state, :refreshed_at, Map.put(stamps(state), key, at))

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
