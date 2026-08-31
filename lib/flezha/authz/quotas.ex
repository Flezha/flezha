defmodule Flezha.AuthZ.Quotas do
  @moduledoc """
  Per-namespace quota check engine.

  Hot-path is `check_allocation/3`: one ETS lookup against
  `NamespaceStore`'s cache plus one ETS lookup against
  `QuotaCounters`'s cache. Documented latency budget < 50µs p99
  against a warm cache.

  Enforcement points:

    * `OperativeManager.start_operative/2`   → `:operatives`, `:entities`,
      `:disk_bytes` (refuse new allocation while over `max_state_bytes`)
    * `FleetManager.connect_handler/4`       → `:runners` (`max_runners`)
    * Bounded-mailbox capacity provisioning  → `:mailbox_bytes`
    * `Flezha.State.Watchdog`                → disk-quota breach ALARM for a
      running namespace (never a write-path kill)

  Allocation order: caller asks `check_allocation/3` BEFORE touching
  the resource; on success the caller increments the counter via
  `record_allocation/3`. On dealloc the caller calls
  `record_deallocation/3`. The split lets the caller skip the
  counter touch entirely when the allocation itself fails for an
  unrelated reason.

  While `:authn_authz, :enabled` is `false`, every check returns
  `:ok` and no counter is touched.
  """

  alias Flezha.AuthZ
  alias Flezha.AuthZ.{NamespaceStore, QuotaCounters}
  alias Flezha.State.Paths

  @type kind ::
          :max_operatives
          | :max_entities_per_operative
          | :max_mailbox_bytes_total
          | :max_message_rate_per_second
          | :max_state_bytes
          | :max_runners

  @type quota_error ::
          {:error,
           {:quota_exceeded, kind(), %{current: non_neg_integer(), limit: non_neg_integer()}}}
          | {:error, :namespace_unknown}

  @doc """
  Check whether allocating `requested` units of `kind` for
  `namespace` would exceed its quota.

  `kind` is one of the documented atoms above. `requested` is the
  size of the allocation (1 for a operative/entity/runner; bytes
  for mailbox/disk).
  """
  @spec check_allocation(binary(), atom(), non_neg_integer()) :: :ok | quota_error()
  def check_allocation(namespace, kind, requested \\ 1)
      when is_binary(namespace) and is_atom(kind) and is_integer(requested) and requested >= 0 do
    if AuthZ.enabled?() do
      do_check(namespace, kind, requested)
    else
      :ok
    end
  end

  defp do_check(namespace, kind, requested) do
    case NamespaceStore.get(namespace) do
      :error ->
        {:error, :namespace_unknown}

      {:ok, %{quotas: quotas}} ->
        case Map.get(quotas, kind) do
          # No quota configured for this kind → stream.
          nil ->
            :ok

          limit when is_integer(limit) ->
            current = QuotaCounters.get(namespace, counter_for(kind))

            if current + requested <= limit do
              :ok
            else
              {:error, {:quota_exceeded, kind, %{current: current, limit: limit}}}
            end
        end
    end
  end

  @doc """
  Record an allocation against the counter for `kind`. Called by the
  caller AFTER the resource has actually been created.
  """
  @spec record_allocation(binary(), atom(), non_neg_integer()) :: :ok
  def record_allocation(namespace, kind, n \\ 1) do
    if AuthZ.enabled?() do
      QuotaCounters.incr(namespace, counter_for(kind), n)
    else
      :ok
    end
  end

  @doc """
  Record a deallocation against the counter for `kind`.
  """
  @spec record_deallocation(binary(), atom(), non_neg_integer()) :: :ok
  def record_deallocation(namespace, kind, n \\ 1) do
    if AuthZ.enabled?() do
      QuotaCounters.decr(namespace, counter_for(kind), n)
    else
      :ok
    end
  end

  @doc """
  Rebuild the disk-bytes counter for `namespace` from the on-disk truth,
  bypassing the cache — the boot-time recovery shape (drift is bounded by
  one uptime).
  """
  @spec rebuild_disk_bytes(binary()) :: non_neg_integer()
  def rebuild_disk_bytes(namespace) when is_binary(namespace) do
    bytes = compute_disk_bytes(namespace)
    :ok = QuotaCounters.set(namespace, :disk_bytes, bytes)
    :persistent_term.put({namespace, :disk_bytes_cached_at}, System.monotonic_time(:second))
    bytes
  end

  @doc """
  Snapshot the disk-bytes counter for `namespace` from the on-disk
  state tree. Cached internally for 30s — calls within the cache
  window read the stored value rather than re-walking.
  """
  @spec refresh_disk_bytes(binary()) :: non_neg_integer()
  def refresh_disk_bytes(namespace) when is_binary(namespace) do
    now = System.monotonic_time(:second)

    # Monotonic time has an arbitrary (typically negative) offset, so "never
    # cached" must be the nil sentinel — a 0 default made every first read
    # look freshly cached and serve the empty counter instead of the disk.
    case :persistent_term.get({namespace, :disk_bytes_cached_at}, nil) do
      cached_at when is_integer(cached_at) and now - cached_at < 30 ->
        QuotaCounters.get(namespace, :disk_bytes)

      _ ->
        rebuild_disk_bytes(namespace)
    end
  end

  ## ----- private -----

  # Map quota fields → counter kinds. Naming kept distinct so the
  # quota field reads natural in the namespace catalog while the
  # counter kind is a short atom.
  defp counter_for(:max_operatives), do: :operatives
  defp counter_for(:max_entities_per_operative), do: :entities
  defp counter_for(:max_mailbox_bytes_total), do: :mailbox_bytes
  defp counter_for(:max_message_rate_per_second), do: :messages
  defp counter_for(:max_state_bytes), do: :disk_bytes
  defp counter_for(:max_runners), do: :runners
  defp counter_for(other), do: other

  defp compute_disk_bytes(namespace) do
    base = Paths.namespace_dir(namespace)

    if File.exists?(base) do
      walk_size(base)
    else
      0
    end
  rescue
    _ -> 0
  end

  defp walk_size(path) do
    case File.stat(path) do
      {:ok, %File.Stat{type: :regular, size: size}} ->
        size

      {:ok, %File.Stat{type: :directory}} ->
        case File.ls(path) do
          {:ok, entries} ->
            entries
            |> Enum.map(fn entry -> walk_size(Path.join(path, entry)) end)
            |> Enum.sum()

          _ ->
            0
        end

      _ ->
        0
    end
  rescue
    _ -> 0
  end
end
