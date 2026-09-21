defmodule Flezha.AuthZ.Quotas do
  @moduledoc """
  Per-namespace quota check engine.

  Hot-path is `check_allocation/3`: one ETS lookup against
  `NamespaceStore`'s cache plus one ETS lookup against
  `QuotaCounters`'s cache. Documented latency budget < 50µs p99
  against a warm cache.

  Enforcement points:

    * `OperativeManager.start_operative/2`   → `:operatives`, `:cells`,
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

  # How long a disk-bytes snapshot stands before the next caller rebuilds it.
  @disk_bytes_window_seconds 30

  @type kind ::
          :max_operatives
          | :max_cells_per_operative
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
  size of the allocation (1 for a operative/cell/runner; bytes
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
    bytes
  end

  @doc """
  Snapshot the disk-bytes counter for `namespace` from the on-disk
  state tree. Cached internally for 30s — calls within the cache
  window read the stored value rather than re-walking.

  Concurrent callers collapse onto one walk: the window is claimed in
  `QuotaCounters` BEFORE the tree is walked, so a namespace starting a thousand
  operatives at once pays one walk and one counter write rather than a thousand
  of each, and none of them queues behind the other 999 to be refused on a call
  timeout. A claim whose walk then fails leaves the counter as it was until the
  window lapses, which is the staleness the window already allows.
  """
  @spec refresh_disk_bytes(binary()) :: non_neg_integer()
  def refresh_disk_bytes(namespace) when is_binary(namespace) do
    case QuotaCounters.claim_refresh(namespace, :disk_bytes, @disk_bytes_window_seconds) do
      :claimed -> rebuild_disk_bytes(namespace)
      :fresh -> QuotaCounters.get(namespace, :disk_bytes)
    end
  end

  ## ----- private -----

  # Map quota fields → counter kinds. Naming kept distinct so the
  # quota field reads natural in the namespace catalog while the
  # counter kind is a short atom.
  defp counter_for(:max_operatives), do: :operatives
  defp counter_for(:max_cells_per_operative), do: :cells
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
