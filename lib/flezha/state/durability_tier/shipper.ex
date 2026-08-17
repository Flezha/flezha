defmodule Flezha.State.DurabilityTier.Shipper do
  @moduledoc """
  Ships artifacts to the configured backend, and is the only writer of the
  remote manifest.

  One process, one queue, one reference at a time — per-reference ordering is
  then structural rather than a lock, and a garbage collection can never race an
  upload it is about to declare orphaned. Nothing here is on an ack path: an
  enqueue is a cast, a failing backend retries on a timer with the shared
  `ProviderBehaviour.retry_backoff_ms/3` ladder, and local operation continues
  at full speed throughout.

  The ship itself is manifest-last. Blobs go up first, under content-addressed
  keys so no two writers can ever address one another's bytes; the per-reference
  manifest entry naming them is written last, through the backend's
  claim-conditional write. A crash in between leaves the previous entry
  authoritative and the new blobs unreferenced — invisible to recovery, and
  collected later by `gc/0`.

  A conditional write refused because another writer holds the claim is *not*
  retried: that writer owns this reference's remote lineage now, so shipping
  stops here. Retrying would be a split-brain writing over a claimant that
  legitimately superseded it — and since an ordinary migration produces exactly
  this outcome, it is reported as a routine handover rather than an alarm.
  """

  use GenServer

  require Logger

  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.State.DurabilityTier
  alias Flezha.State.DurabilityTier.{Artifact, Backend, Manifest}

  @table :flezha_durability_status
  @retry_base_ms 500
  @retry_max_ms 30_000
  # Under the SDK's 30 s commit-ack budget: a synchronous barrier must fail
  # loudly here rather than have the driver time out first.
  @call_timeout_ms 25_000

  # `queued` dedups the queue and `manifests` caches the last entry written per
  # reference; everything else about a reference's schedule — superseded,
  # retry deadline, pending age — lives in the status row, which is one record
  # readable from any process.
  defstruct backend: nil,
            opts: [],
            queue: :queue.new(),
            queued: MapSet.new(),
            manifests: %{},
            writer: nil,
            adopted: MapSet.new()

  # ---------- API ----------

  def child_spec(opts), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Queue `ref` for shipping. Never blocks; a no-op when the tier is not running."
  @spec enqueue(Manifest.ref()) :: :ok
  def enqueue(ref) do
    case GenServer.whereis(__MODULE__) do
      nil -> :ok
      pid -> GenServer.cast(pid, {:enqueue, ref})
    end
  end

  # Every synchronous entry point answers the same way when the tier is not
  # running, and none of them may bring down its caller.
  defp call(request) do
    case GenServer.whereis(__MODULE__) do
      nil -> {:error, :durability_tier_not_running}
      pid -> GenServer.call(pid, request, @call_timeout_ms)
    end
  catch
    :exit, reason -> {:error, {:shipper_unavailable, reason}}
  end

  @doc """
  Ship `ref` now and report the backend's verdict — the synchronous-durability
  path. Runs on this process like every other ship, so it cannot interleave with
  a queued ship of the same reference.
  """
  @spec ship_now(Manifest.ref()) :: :ok | {:error, term()}
  def ship_now(ref), do: call({:ship_now, ref})

  @doc "Run one orphan-blob collection synchronously. Used by the sweep and by tests."
  @spec gc() :: {:ok, non_neg_integer()} | {:error, term()}
  def gc(), do: call(:gc)

  @doc "Run one backstop sweep synchronously. Used by tests."
  @spec sweep_now() :: :ok | {:error, term()}
  def sweep_now(), do: call(:sweep_now)

  @doc """
  Per-reference shipping state, straight out of ETS. Readable from any process
  and while the shipper is busy — which is what makes it safe for `/healthz`.
  """
  @spec status() :: [map()]
  def status() do
    case :ets.whereis(@table) do
      :undefined -> []
      _ -> :ets.select(@table, [{{:_, :"$1"}, [], [:"$1"]}])
    end
  end

  # ---------- GenServer ----------

  @impl true
  def init(_opts) do
    case Backend.resolve() do
      :none ->
        :ignore

      {module, opts} ->
        ensure_table()
        schedule(:sweep_tick, DurabilityTier.ship_interval_ms())
        schedule(:gc_tick, DurabilityTier.gc_interval_ms())

        # Read once and held: a writer identity that changed mid-lifetime would
        # split one node's lineage in two and make it bid against itself.
        state = %__MODULE__{backend: module, opts: opts, writer: DurabilityTier.writer_id()}

        {:ok, state, {:continue, :initial_sweep}}
    end
  end

  @impl true
  def handle_continue(:initial_sweep, state), do: {:noreply, sweep(state)}

  @impl true
  def handle_cast({:enqueue, ref}, state), do: {:noreply, push(state, ref)}

  # This node now runs `state_key`'s operative, so it is entitled to take over
  # that lineage remotely. Without this gate a node that merely still has the
  # files — every source of a migration, since the copy is never deleted —
  # would bid its way back in on its next sweep and fence the real owner.
  def handle_cast({:adopt, state_key}, state) do
    refs = DurabilityTier.refs_of(state_key)
    Enum.each(refs, &clear_supersession/1)

    {:noreply,
     refs
     |> Enum.reduce(%{state | adopted: MapSet.put(state.adopted, state_key)}, &push(&2, &1))}
  end

  @impl true
  def handle_call({:ship_now, ref}, _from, state) do
    if status_of(ref).superseded_by do
      {:reply, {:error, :superseded}, state}
    else
      {result, state} = ship(state, ref)
      {:reply, result, state}
    end
  end

  def handle_call(:gc, _from, state), do: {:reply, collect_orphans(state), state}

  # Sweep AND drain, so the caller has a real barrier: the queue is empty when
  # this returns. The async `:drain` messages the sweep queued then find nothing
  # to do.
  def handle_call(:sweep_now, _from, state) do
    {:reply, :ok, state |> sweep() |> drain_all()}
  end

  @impl true
  def handle_info(:drain, state) do
    case drain_one(state) do
      :empty -> {:noreply, state}
      {:ok, state} -> {:noreply, drain_soon(state)}
    end
  end

  def handle_info({:retry, ref}, state), do: {:noreply, push(state, ref)}

  def handle_info(:sweep_tick, state) do
    schedule(:sweep_tick, DurabilityTier.ship_interval_ms())
    {:noreply, sweep(state)}
  end

  def handle_info(:gc_tick, state) do
    schedule(:gc_tick, DurabilityTier.gc_interval_ms())
    _ = collect_orphans(state)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------- queue ----------

  # The retry deadline is consulted here, not only by the retry timer: the
  # backstop sweep re-offers every unshipped reference, so without this it would
  # jump the backoff ladder and hammer a failing backend once per sweep.
  defp push(state, ref) do
    status = status_of(ref)

    if status.superseded_by != nil or MapSet.member?(state.queued, ref) or cooling_off?(status) do
      state
    else
      mark_pending(ref)

      drain_soon(%{
        state
        | queue: :queue.in(ref, state.queue),
          queued: MapSet.put(state.queued, ref)
      })
    end
  end

  defp cooling_off?(%{retry_after_ms: nil}), do: false
  defp cooling_off?(%{retry_after_ms: at}), do: System.monotonic_time(:millisecond) < at

  defp drain_soon(state) do
    unless :queue.is_empty(state.queue), do: send(self(), :drain)
    state
  end

  defp drain_one(state) do
    case :queue.out(state.queue) do
      {:empty, _} ->
        :empty

      {{:value, ref}, rest} ->
        state = %{state | queue: rest, queued: MapSet.delete(state.queued, ref)}
        {_result, state} = ship(state, ref)
        {:ok, state}
    end
  end

  defp drain_all(state) do
    case drain_one(state) do
      :empty -> state
      {:ok, state} -> drain_all(state)
    end
  end

  # The backstop: every reference with local durable state, so an entity that
  # reaches no compaction, checkpoint or commit boundary is still bounded by one
  # sweep period. The cspec rides the same sweep — it changes rarely and a
  # whole-file ship whose content is unchanged costs one hash.
  defp sweep(state), do: Enum.reduce(DurabilityTier.all_refs(), state, &push(&2, &1))

  # ---------- ship ----------

  defp ship(state, ref) do
    # Ownership is decided from the cached entry before a single byte is
    # uploaded, so a node that lost a lineage stops spending on it immediately.
    # This is an economy, not the guard — the cache can be stale, and the
    # conditional write below is what actually decides.
    case entitled?(state, ref) do
      {:no, holder} -> {{:error, {:superseded, holder}}, supersede(state, ref, holder)}
      :yes -> do_ship_and_record(state, ref)
    end
  end

  defp do_ship_and_record(state, ref) do
    case do_ship(state, ref) do
      {:ok, manifest, caught_up?} ->
        record_success(ref, caught_up?)
        {:ok, %{state | manifests: Map.put(state.manifests, ref, manifest)}}

      :nothing_to_ship ->
        record_success(ref, true)
        {:ok, state}

      # The backend answers with the counter it holds, because a store must
      # never parse a manifest. Who holds it is a fact of the entry, so it is
      # read back from there — and the stale cache that let this node try at all
      # is dropped with it.
      {:error, {:claim_held, _counter}} ->
        state = %{state | manifests: Map.delete(state.manifests, ref)}
        holder = fetch_manifest(state, ref).owner
        {{:error, {:superseded, holder}}, supersede(state, ref, holder)}

      {:error, reason} ->
        {{:error, reason}, retry_later(state, ref, reason)}
    end
  end

  # A writer extends its own lineage always, and takes one over only when this
  # node has adopted the operative.
  #
  # An unclaimed lineage needs no adoption: two nodes first-shipping it both bid
  # `{:claim, 1}` against a stored 0, and the strict compare-and-set already
  # resolves that to one winner. Adoption exists for the other case — a node
  # that LOST a lineage and still holds the files, which every migration
  # produces, since the source copy is never deleted. Without the gate its next
  # backstop sweep would bid the lineage back and fence the real owner.
  defp entitled?(state, ref) do
    case manifest_for(state, ref) do
      %{owner: nil} -> :yes
      %{owner: owner} when owner == state.writer -> :yes
      %{owner: owner} -> if adopted?(state, ref), do: :yes, else: {:no, owner}
    end
  end

  defp adopted?(state, ref), do: MapSet.member?(state.adopted, state_key_of(ref))

  defp state_key_of({:entity, {class, _procedure, _entity_key}}), do: class
  defp state_key_of({:input_log, class}), do: class
  defp state_key_of({:cspec, {hub_id, _writer}}), do: hub_id

  defp do_ship(state, {:cspec, _} = ref), do: ship_whole_file(state, ref)
  defp do_ship(state, ref), do: ship_seq_addressed(state, ref)

  # Whole-file artifact (the ProcessHub registry cspec). Content-addressed, so a
  # re-ship of unchanged bytes writes nothing and a torn upload is orphaned
  # under a key nothing references.
  defp ship_whole_file(state, ref) do
    with path when is_binary(path) <- Artifact.file_path(ref),
         {:ok, bytes} <- File.read(path) do
      sha = sha256(bytes)
      manifest = manifest_for(state, ref)

      if match?(%{sha: ^sha}, manifest.file) do
        :nothing_to_ship
      else
        blob = Manifest.blob_key(ref, Manifest.file_name(sha))
        shipped = %{manifest | file: %{blob: blob, size: byte_size(bytes), sha: sha}}

        with :ok <- state.backend.put(blob, bytes, state.opts) do
          # `local_top` is -1: a whole file has no seq, so an entry naming it is
          # by definition caught up.
          commit_cut(state, ref, manifest, shipped, %{bytes: byte_size(bytes), count: 0}, -1)
        end
      end
    else
      nil -> :nothing_to_ship
      {:error, :enoent} -> :nothing_to_ship
      {:error, _} = err -> err
    end
  end

  # Seq-addressed artifact (Entity WAL + snapshots, proc-journal, durable inbox,
  # input log). The manifest is reconciled against local truth first — a rewind
  # dropped records above a seq, a compaction dropped them below one — and only
  # then extended with the cut being shipped.
  defp ship_seq_addressed(state, ref) do
    snapshots = Artifact.snapshots(ref)
    bounds = Artifact.seq_bounds(ref)

    if snapshots == [] and bounds == :empty do
      :nothing_to_ship
    else
      epoch = Artifact.epoch(ref)
      manifest = manifest_for(state, ref)
      local_top = local_top(snapshots, bounds)

      local = %{epoch: epoch, markers: Artifact.markers(ref)}

      with {:ok, shipped} <- ship_snapshots(state, ref, manifest, snapshots, local_top),
           {:ok, shipped, records} <- ship_records(state, ref, shipped, bounds, epoch) do
        commit_cut(state, ref, manifest, Map.merge(shipped, local), records, local_top)
      end
    end
  end

  # The manifest write is skipped when the cut is unchanged: a quiet entity
  # reached by every backstop sweep must not rewrite its entry forever, and a
  # skipped write is still fully caught up.
  defp commit_cut(state, ref, previous, shipped, records, local_top) do
    caught_up? = Manifest.shipped_through(shipped) >= local_top

    # Ownership is stamped BEFORE the same-cut test, because a takeover changes
    # no data: without this, `same_cut?/2` would report nothing to write and the
    # new owner's claim would never land.
    guard = Manifest.guard_for(previous, state.writer)
    shipped = Manifest.claimed_by(shipped, state.writer, guard)

    if Manifest.same_cut?(previous, shipped) do
      {:ok, previous, caught_up?}
    else
      shipped = %{shipped | shipped_at_ms: System.system_time(:millisecond)}

      case write_manifest(state, ref, shipped, guard) do
        :ok ->
          emit(:artifact_shipped, %{bytes: records.bytes, records: records.count}, ref)
          {:ok, shipped, caught_up?}

        {:error, _} = err ->
          err
      end
    end
  end

  defp local_top(snapshots, bounds) do
    top_snapshot = Enum.reduce(snapshots, -1, fn {seq, _path}, acc -> max(seq, acc) end)

    case bounds do
      {_lo, hi} -> max(hi, top_snapshot)
      :empty -> top_snapshot
    end
  end

  # Mirror the local snapshot set: upload the ones the manifest does not name,
  # then let the manifest hold exactly what is on disk. A snapshot retention
  # deleted locally stops being referenced, so hydration never asks for a
  # baseline whose successor already subsumed it.
  defp ship_snapshots(state, ref, manifest, snapshots, local_top) do
    manifest = Manifest.prune_above(manifest, local_top)
    # Keyed by seq to its already-shipped blob: a content-addressed name cannot
    # be reconstructed without the bytes, so a snapshot the entry already names
    # is carried over rather than re-derived.
    known = Map.new(manifest.snapshots, &{&1.seq, &1.blob})

    Enum.reduce_while(snapshots, {:ok, []}, fn {seq, path}, {:ok, acc} ->
      case Map.fetch(known, seq) do
        {:ok, blob} -> {:cont, {:ok, [{seq, blob} | acc]}}
        :error -> upload_snapshot(state, ref, seq, path, acc)
      end
    end)
    |> case do
      {:ok, seq_blobs} -> {:ok, Manifest.put_snapshots(manifest, Enum.reverse(seq_blobs))}
      {:error, _} = err -> err
    end
  end

  defp upload_snapshot(state, ref, seq, path, acc) do
    case File.read(path) do
      {:ok, bytes} ->
        blob = Manifest.blob_key(ref, Manifest.snapshot_name(seq, sha256(bytes)))

        case state.backend.put(blob, bytes, state.opts) do
          :ok -> {:cont, {:ok, [{seq, blob} | acc]}}
          {:error, _} = err -> {:halt, err}
        end

      # Retention deleted it between the scan and the read. It drops out of this
      # cut rather than being named: an entry may only reference what was
      # actually uploaded, or recovery halts on a blob that never existed.
      {:error, :enoent} ->
        {:cont, {:ok, acc}}

      {:error, _} = err ->
        {:halt, err}
    end
  end

  defp ship_records(_state, _ref, manifest, :empty, _epoch),
    do: {:ok, manifest, %{bytes: 0, count: 0}}

  defp ship_records(state, ref, manifest, {min_seq, max_seq}, epoch) do
    manifest = Manifest.prune_below(manifest, snapshot_floor(manifest, min_seq))
    from = max(Manifest.shipped_through(manifest) + 1, min_seq)

    if from > max_seq do
      {:ok, manifest, %{bytes: 0, count: 0}}
    else
      {:ok, bytes, info} = Artifact.read_range(ref, from, max_seq)
      blob = Manifest.blob_key(ref, Manifest.range_name(epoch, from, max_seq, sha256(bytes)))

      case state.backend.put(blob, bytes, state.opts) do
        :ok ->
          manifest =
            manifest
            |> Manifest.put_range(%{from: from, to: max_seq, epoch: epoch, blob: blob})
            |> Map.update!(:source_offsets, &Map.merge(&1, info.source_offsets))

          {:ok, manifest, %{bytes: byte_size(bytes), count: info.count}}

        {:error, _} = err ->
          err
      end
    end
  end

  # A shipped range is superseded only when the local WAL no longer holds it AND
  # a retained snapshot covers it. Both halves matter: dropping ranges on the
  # WAL floor alone would strand a rewound Entity whose snapshot was deleted
  # with the records above it.
  defp snapshot_floor(manifest, min_seq) do
    floor = min_seq - 1
    if Enum.any?(manifest.snapshots, &(&1.seq >= floor)), do: floor, else: -1
  end

  defp write_manifest(state, ref, manifest, guard) do
    state.backend.put_if(Manifest.key(ref), Manifest.encode(manifest), guard, state.opts)
  end

  defp manifest_for(state, ref) do
    Map.get_lazy(state.manifests, ref, fn -> fetch_manifest(state, ref) end)
  end

  defp fetch_manifest(state, ref) do
    with {:ok, blob} <- state.backend.get(Manifest.key(ref), state.opts),
         {:ok, manifest} <- Manifest.decode(blob) do
      manifest
    else
      _ -> Manifest.new(ref)
    end
  end

  # ---------- failure handling ----------

  defp retry_later(state, ref, reason) do
    attempt = status_of(ref).failures + 1
    delay = ProviderBehaviour.retry_backoff_ms(@retry_base_ms, @retry_max_ms, attempt)
    Process.send_after(self(), {:retry, ref}, delay)
    record_failure(ref, reason, attempt, delay)
    emit(:ship_failed, %{attempt: attempt, backoff_ms: delay}, ref, %{reason: inspect(reason)})
    state
  end

  # Losing a lineage is ORDINARY now — every migration produces a loser — so
  # this is not an alarm and must not latch. `pending_since_ms` is cleared with
  # it: a reference this node no longer owns cannot be behind on shipping, and
  # leaving it set would report a permanent RPO breach (and a permanently
  # degraded `/healthz`) for every handover the cluster performs.
  #
  # Nothing local is touched. The tier never deletes local files, and the node
  # that lost the claim is usually the one that still has the only other copy.
  defp supersede(state, ref, holder) do
    Logger.info(
      "[durability] #{inspect(ref)} is now shipped by #{inspect(holder)}; this node stopped. " <>
        "Its local state is untouched."
    )

    emit(:claim_superseded, %{}, ref, %{holder: inspect(holder)})
    record_superseded(ref, holder)
    state
  end

  # ---------- orphan collection ----------

  # Unreferenced is not the same fact as orphaned. It is safe to read one as the
  # other only for uploads this process made, because ships and collection run
  # here in one mailbox — and every OTHER controller sharing this backend is
  # mid-ship as far as this pass can tell. So an artifact must fail two
  # independent tests before it is removed:
  #
  #   * age — nothing younger than the grace is ever collected, whoever owns it,
  #     which covers an in-flight upload without needing to know whose it is;
  #   * scope — a reference this node holds local state for is one it is the
  #     sole shipper of, so it is also the only node entitled to collect it.
  #
  # Scope alone would leak a reference no node holds any more (a stopped
  # operative, one whose state moved), so out-of-scope artifacts are still
  # reclaimed — at a far longer threshold, where no ship can plausibly still be
  # in flight. Collection may leak; it must never lose.
  defp collect_orphans(state) do
    with {:ok, manifest_entries} <- state.backend.list("manifest/", state.opts),
         {:ok, blob_entries} <- state.backend.list(Manifest.blob_prefix(), state.opts) do
      live = live_blob_keys(state, manifest_entries)
      owned = MapSet.new(DurabilityTier.all_refs())

      {orphans, retained} =
        blob_entries
        |> Enum.reject(fn {key, _age} -> MapSet.member?(live, key) end)
        |> Enum.split_with(fn {key, age} -> collectable?(key, age, owned) end)

      Enum.each(orphans, fn {key, _age} -> state.backend.delete(key, state.opts) end)

      if orphans != [] or retained != [] do
        :telemetry.execute(
          [:flezha, :state, :durability_gc],
          %{collected: length(orphans), retained: length(retained)},
          %{}
        )
      end

      {:ok, length(orphans)}
    end
  end

  defp live_blob_keys(state, manifest_entries) do
    manifest_entries
    |> Enum.flat_map(fn {key, _age} ->
      with {:ok, blob} <- state.backend.get(key, state.opts),
           {:ok, manifest} <- Manifest.decode(blob) do
        Manifest.blob_keys(manifest)
      else
        _ -> []
      end
    end)
    |> MapSet.new()
  end

  defp collectable?(key, age, owned) do
    cond do
      age < DurabilityTier.gc_grace_ms() -> false
      in_scope?(key, owned) -> true
      true -> age >= DurabilityTier.gc_abandoned_after_ms()
    end
  end

  # A key this build did not write reads as out of scope, so it is only ever
  # reclaimed on the long threshold — never on this node's say-so.
  defp in_scope?(key, owned) do
    case Manifest.ref_of_blob_key(key) do
      {:ok, ref} -> MapSet.member?(owned, ref)
      :error -> false
    end
  end

  # ---------- status table ----------

  defp ensure_table() do
    case :ets.whereis(@table) do
      :undefined -> :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])
      _ -> @table
    end
  end

  defp mark_pending(ref) do
    now = System.system_time(:millisecond)

    update_status(ref, fn entry ->
      %{entry | pending_since_ms: entry.pending_since_ms || now}
    end)
  end

  defp record_success(ref, caught_up?) do
    now = System.system_time(:millisecond)

    update_status(ref, fn entry ->
      %{
        entry
        | last_shipped_ms: now,
          pending_since_ms: if(caught_up?, do: nil, else: entry.pending_since_ms || now),
          failures: 0,
          retry_after_ms: nil,
          last_error: nil
      }
    end)
  end

  defp record_failure(ref, reason, attempt, delay) do
    update_status(ref, fn entry ->
      %{
        entry
        | failures: attempt,
          retry_after_ms: System.monotonic_time(:millisecond) + delay,
          last_error: inspect(reason),
          pending_since_ms: entry.pending_since_ms || System.system_time(:millisecond)
      }
    end)
  end

  defp record_superseded(ref, holder) do
    update_status(ref, fn entry ->
      %{
        entry
        | superseded_by: holder,
          pending_since_ms: nil,
          retry_after_ms: nil,
          last_error: nil
      }
    end)
  end

  defp clear_supersession(ref) do
    update_status(ref, &%{&1 | superseded_by: nil})
  end

  defp update_status(ref, fun) do
    ensure_table()
    :ets.insert(@table, {ref, fun.(status_of(ref))})
    :ok
  end

  defp status_of(ref) do
    case :ets.whereis(@table) do
      :undefined ->
        blank_status(ref)

      _ ->
        case :ets.lookup(@table, ref) do
          [{^ref, entry}] -> entry
          [] -> blank_status(ref)
        end
    end
  end

  defp blank_status(ref) do
    %{
      ref: ref,
      last_shipped_ms: nil,
      pending_since_ms: nil,
      failures: 0,
      retry_after_ms: nil,
      superseded_by: nil,
      last_error: nil
    }
  end

  # ---------- helpers ----------

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  defp schedule(message, interval), do: Process.send_after(self(), message, interval)

  defp emit(event, measurements, ref, extra \\ %{}) do
    :telemetry.execute(
      [:flezha, :state, event],
      Map.put(measurements, :count, 1),
      Map.merge(%{ref: inspect(ref)}, extra)
    )
  end
end
