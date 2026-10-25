defmodule Flezha.State.DurabilityTier.Shipper do
  @moduledoc """
  Ships artifacts to the configured backend, and is the only writer of the
  remote manifest.

  One process, one queue, one reference at a time — per-reference ordering is
  then structural rather than a lock, and a garbage collection can never race an
  upload it is about to declare orphaned. Nothing here is on an ack path: an
  enqueue is a cast, a failing backend retries on the shared
  `:durability_ship` row of the backoff table, and local operation continues
  at full speed.

  The ship itself is manifest-last. Blobs go up first, under content-addressed
  keys so no two writers can address one another's bytes; the per-reference
  manifest entry naming them is written last, through the backend's
  claim-conditional write. A refused conditional write is *not* retried: that
  writer owns this reference's remote lineage, so shipping stops here.
  """

  use GenServer
  use Flezha.Process.Timer, keys: [:sweep, :gc]

  alias Flezha.Config.Durations
  alias Flezha.Config.Backoff
  alias Flezha.SDK.SafeCall
  alias Flezha.State.{DurabilityTier, Lineage, Ref}
  alias Flezha.State.DurabilityTier.{Artifact, Manifest}
  alias Flezha.State.DurabilityTier.Backend.Config, as: BackendConfig

  @table :flezha_durability_status

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
            adopted: MapSet.new(),
            releasing: MapSet.new(),
            timers: %{}

  # ---------- API ----------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Queue `ref` for shipping. Never blocks; a no-op when the tier is not running."
  @spec enqueue(Manifest.ref()) :: :ok
  def enqueue(ref), do: cast({:enqueue, ref})

  @doc "This node now runs `state_key`'s relay (`DurabilityTier.adopt_relay/1`)."
  @spec adopt(binary()) :: :ok
  def adopt(state_key), do: cast({:adopt, state_key})

  @doc "`state_key`'s relay left this node (`DurabilityTier.release_relay/2`)."
  @spec release(binary(), :moved | :stopped) :: :ok
  def release(state_key, departure), do: cast({:release, state_key, departure})

  # `GenServer.cast/2` answers `:ok` whether or not the tier is running.
  defp cast(request), do: GenServer.cast(__MODULE__, request)

  # Every synchronous entry point answers the same way when the tier is not
  # running, and none of them may bring down its caller.
  defp call(request) do
    case GenServer.whereis(__MODULE__) do
      nil ->
        {:error, :durability_tier_not_running}

      pid ->
        SafeCall.bounded(pid, request, Durations.get(:durability_ship_call_ms),
          on_error: &{:error, {:shipper_unavailable, &1}}
        )
    end
  end

  @doc """
  Ship `ref` now and report the backend's verdict — the synchronous-durability
  path. Runs on this process like every other ship, so it cannot interleave with
  a queued ship of the same reference.
  """
  @spec ship_now(Manifest.ref()) :: :ok | {:error, term()}
  def ship_now(ref), do: call({:ship_now, ref})

  @doc """
  Nothing waits on `ref`'s ship any more: if another writer holds its lineage,
  its RPO clock stops, as it does when an asynchronous ship finds it superseded.
  """
  @spec end_wait(Manifest.ref()) :: :ok
  def end_wait(ref), do: cast({:end_wait, ref})

  @doc "The highest seq `ref`'s cached, else fetched, entry covers; `-1` for none or no answer."
  @spec shipped_through(Manifest.ref()) :: integer()
  def shipped_through(ref), do: with({:error, _} <- call({:shipped_through, ref}), do: -1)

  @doc "Run one orphan-blob collection synchronously. Used by the sweep and by tests."
  @spec gc() :: {:ok, non_neg_integer()} | {:error, term()}
  def gc(), do: call(:gc)

  @doc "Run one backstop sweep synchronously. Used by tests."
  @spec sweep_now() :: :ok | {:error, term()}
  def sweep_now(), do: call(:sweep_now)

  @doc """
  Which of `refs` have every change announced for them landed remotely, or
  belong to a lineage another writer holds. Asked of this process rather than
  read from the status table: a ship already running when a change was
  announced can land before the announcement is even dequeued.
  """
  @spec caught_up([Manifest.ref()]) :: [Manifest.ref()]
  def caught_up(refs), do: with({:error, _} <- call({:caught_up, refs}), do: [])

  @doc """
  Per-reference shipping state, straight out of ETS. Readable from any process
  and while the shipper is busy — which is what makes it safe for `/healthz`.
  Each row adds `retry_in_ms`, how long until a pending retry (`nil` when none
  is pending), so a reader on another node never compares this node's
  monotonic `retry_after_ms` with its own clock.
  """
  @spec status() :: [map()]
  def status() do
    case :ets.whereis(@table) do
      :undefined ->
        []

      _ ->
        now = System.monotonic_time(:millisecond)

        @table
        |> :ets.select([{{:_, :"$1"}, [], [:"$1"]}])
        |> Enum.map(&Map.put(&1, :retry_in_ms, retry_in(&1, now)))
    end
  end

  defp retry_in(%{retry_after_ms: at}, now) when is_integer(at), do: max(at - now, 0)
  defp retry_in(_entry, _now), do: nil

  # ---------- GenServer ----------

  @impl true
  def init(_opts) do
    case BackendConfig.resolve() do
      :none ->
        :ignore

      {module, opts} ->
        ensure_table()

        # Read once and held: a writer identity that changed mid-lifetime would
        # split one node's lineage in two and make it bid against itself.
        state =
          %__MODULE__{backend: module, opts: opts, writer: DurabilityTier.writer_id()}
          |> arm_sweep()
          |> arm_gc()

        {:ok, state, {:continue, :initial_sweep}}
    end
  end

  @impl true
  def handle_continue(:initial_sweep, state), do: {:noreply, sweep(state)}

  @impl true
  def handle_cast({:enqueue, ref}, state), do: {:noreply, push(state, ref)}

  def handle_cast({:end_wait, ref}, state) do
    if status_of(ref).superseded_by, do: not_owed(ref)
    {:noreply, state}
  end

  # This node now runs `state_key`'s relay (`entitled?/2`): every reference
  # ships, an extend that waited included. The cached entries go too: one this
  # node wrote before the relay ran elsewhere still names it owner, and would
  # make it extend a claim another writer has since moved past.
  def handle_cast({:adopt, state_key}, state) do
    refs = DurabilityTier.refs_of_state_key(state_key)
    Enum.each(refs, &clear_supersession/1)

    state = %{
      state
      | adopted: MapSet.put(state.adopted, state_key),
        releasing: MapSet.delete(state.releasing, state_key),
        manifests: Map.drop(state.manifests, refs)
    }

    {:noreply, Enum.reduce(refs, state, &push(&2, &1))}
  end

  # The relay left this node. A reference it never shipped has no cached entry
  # to fail an extend against, so a standing adoption would let the next ship
  # bid above the writer that runs the relay now and roll its cut back.
  def handle_cast({:release, state_key, :moved}, state),
    do: {:noreply, withdraw(state, state_key)}

  # Stopped here, its last turns have no other way to the backend: every
  # reference ships once more, and the adoption ends when none is owed.
  def handle_cast({:release, state_key, :stopped}, state) do
    refs = DurabilityTier.refs_of_state_key(state_key)
    state = %{state | releasing: MapSet.put(state.releasing, state_key)}
    {:noreply, refs |> Enum.reduce(state, &push(&2, &1)) |> settle_release(state_key)}
  end

  @impl true
  def handle_call({:ship_now, ref}, _from, state) do
    {result, state} =
      if status_of(ref).superseded_by,
        do: {{:error, :superseded}, state},
        else: ship(state, ref)

    # A caller withholding an acknowledgement on this ship is behind however
    # the lineage moved, so a refusal keeps its RPO clock running.
    if match?({:error, _}, result), do: mark_pending(ref)
    {:reply, result, state}
  end

  def handle_call(:gc, _from, state), do: {:reply, collect_orphans(state), state}

  def handle_call({:shipped_through, ref}, _from, state),
    do: {:reply, Manifest.shipped_through(manifest_for(state, ref)), state}

  # Every enqueue marks its reference pending, so one that is not pending has
  # nothing announced left to ship — once something has landed for it at all.
  def handle_call({:caught_up, refs}, _from, state) do
    landed =
      Enum.filter(refs, fn ref ->
        case status_of(ref) do
          %{superseded_by: holder} when holder != nil -> true
          %{pending_since_ms: nil, last_shipped_ms: at} -> at != nil
          _pending -> false
        end
      end)

    {:reply, landed, state}
  end

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
    {:noreply, state |> arm_sweep() |> sweep()}
  end

  def handle_info(:gc_tick, state) do
    state = arm_gc(state)
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

  # The backstop: every reference with local durable state, so a cell that
  # reaches no compaction, checkpoint or commit boundary is still bounded by one
  # sweep period. The whole-payload artifacts — the cspec, the parked failures,
  # the control-plane stores — ride the same sweep: they change rarely, and a
  # re-ship whose content is unchanged costs one hash.
  defp sweep(state),
    do: Enum.reduce(DurabilityTier.all_refs(state.writer), state, &push(&2, &1))

  # ---------- ship ----------

  defp ship(state, ref) do
    # Ownership is decided from the cached entry before a single byte is
    # uploaded, so a node that lost a lineage stops spending on it immediately.
    # This is an economy, not the guard — the cache can be stale, and the
    # conditional write below is what actually decides.
    {result, state} =
      case entitled?(state, ref) do
        {:no, holder} -> {{:error, {:superseded, holder}}, supersede(state, ref, holder)}
        :wait -> {{:error, :not_adopted}, tap(state, fn _ -> not_owed(ref) end)}
        :yes -> ship_and_record(state, ref)
      end

    {result, settle_release(state, Ref.state_key(ref))}
  end

  # A reference is owed while it has unshipped progress and nobody else holds
  # its lineage; a failing one stays owed through its retries.
  defp settle_release(state, state_key) do
    if MapSet.member?(state.releasing, state_key) and
         not Enum.any?(DurabilityTier.refs_of_state_key(state_key), &owed?/1),
       do: withdraw(state, state_key),
       else: state
  end

  defp owed?(ref),
    do: match?(%{pending_since_ms: at, superseded_by: nil} when at != nil, status_of(ref))

  defp withdraw(state, state_key) do
    %{
      state
      | adopted: MapSet.delete(state.adopted, state_key),
        releasing: MapSet.delete(state.releasing, state_key)
    }
  end

  defp ship_and_record(state, ref) do
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

  # A relay's turns ship only while this node runs it: one that LOST it keeps the
  # files (a migration keeps the source copy) and would extend its entry with
  # abandoned turns, or bid back over the owner or an entry continuing its own
  # lineage. Until a bind adopts, an extend waits, not owed. Two first ships
  # both bid `{:claim, 1}` and the strict compare-and-set picks one.
  defp entitled?(state, ref) do
    entry = manifest_for(state, ref)
    adopted? = MapSet.member?(state.adopted, Ref.state_key(ref))

    # `guard_for/2` is the one place the owner comparison is made, so the
    # decision to ship and the guard the write carries can never disagree.
    case Manifest.guard_for(entry, state.writer) do
      {:extend, _} when adopted? -> :yes
      {:extend, _} -> if Artifact.whole_payload?(ref), do: :yes, else: :wait
      {:claim, _} when entry.owner == nil -> :yes
      {:claim, _} when adopted? -> if continued?(entry, ref), do: {:no, entry.owner}, else: :yes
      {:claim, _} -> {:no, entry.owner}
    end
  end

  defp continued?(entry, ref),
    do: Lineage.relation(entry.lineage, Ref.lineage(ref)) == :descendant

  # A file that refuses to be read fails this reference alone, never the shipper.
  defp do_ship(state, ref) do
    ship = if Artifact.whole_payload?(ref), do: &ship_whole_payload/2, else: &ship_seq_addressed/2
    ship.(state, ref)
  rescue
    e in RuntimeError -> {:error, {:raised, Exception.message(e)}}
  end

  # Whole-payload artifact (the registry cspec, a parked-failure set, a
  # control-plane store). Content-addressed, so a re-ship of unchanged bytes
  # uploads nothing and a torn upload is orphaned under a key nothing references.
  #
  # `local_top` is -1 throughout: a whole payload has no seq, so an entry naming
  # it is by definition caught up.
  defp ship_whole_payload(state, ref) do
    manifest = manifest_for(state, ref)

    case Artifact.pack(ref) do
      {:ok, bytes} ->
        sha = sha256(bytes)
        blob = Manifest.blob_key(ref, Manifest.file_name(sha))

        with :ok <- upload_payload(state, Manifest.blob_keys(manifest), blob, bytes) do
          shipped = %{manifest | file: %{blob: blob, size: byte_size(bytes), sha: sha}}
          commit_cut(state, ref, manifest, shipped, %{bytes: byte_size(bytes), count: 0}, -1)
        end

      # Nothing on disk any more — the user resolved the last parked record
      # of a set. The entry must stop naming the old blob, or a replacement
      # controller would hydrate entries that were deliberately deleted.
      :empty ->
        commit_cut(state, ref, manifest, %{manifest | file: nil}, %{bytes: 0, count: 0}, -1)

      {:error, _} = err ->
        err
    end
  end

  # The blob name IS the hash, so a blob the entry already names means the bytes
  # are up. The manifest write is deliberately not skipped with it: a takeover
  # changes no bytes at all, and `commit_cut` is the only place allowed to
  # conclude that a cut is unchanged — it compares the owner and the claim too.
  defp upload_payload(state, named, blob, bytes) do
    if blob in named, do: :ok, else: state.backend.put(blob, bytes, state.opts)
  end

  # Seq-addressed artifact (Cell WAL + snapshots, task-journal, input log). The manifest is reconciled against local truth first — a rewind
  # dropped records above a seq, a compaction dropped them below one — and only
  # then extended with the cut being shipped.
  defp ship_seq_addressed(state, ref) do
    snapshots = Ref.snapshots(ref)
    bounds = Artifact.seq_bounds(ref)

    if snapshots == [] and bounds == :empty do
      :nothing_to_ship
    else
      epoch = Ref.epoch(ref)
      local = %{epoch: epoch, markers: Ref.markers(ref), lineage: Ref.lineage(ref)}
      manifest = state |> manifest_for(ref) |> Manifest.rebase(local.lineage)
      local_top = Artifact.top_seq(snapshots, bounds)

      with {:ok, shipped} <- ship_snapshots(state, ref, manifest, snapshots, local_top),
           {:ok, shipped, records} <- ship_records(state, ref, shipped, bounds, epoch) do
        commit_cut(state, ref, manifest, Map.merge(shipped, local), records, local_top)
      end
    end
  end

  # The manifest write is skipped when the cut is unchanged: a quiet cell
  # reached by every backstop sweep must not rewrite its entry forever, and a
  # skipped write is still fully caught up.
  defp commit_cut(state, ref, previous, shipped, records, local_top) do
    caught_up? = Manifest.shipped_through(shipped) >= local_top

    # Ownership is stamped BEFORE the same-cut test, because a takeover changes
    # no data: without this, `same_cut?/2` would report nothing to write and the
    # new owner's claim would never land.
    {shipped, guard} = Manifest.claim_as(shipped, previous, state.writer)

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

  # Mirror the local snapshot set: upload the ones the manifest does not name,
  # then let the manifest hold exactly what is on disk. A snapshot retention
  # deleted locally stops being referenced, so hydration never asks for a
  # baseline whose successor already subsumed it.
  defp ship_snapshots(state, ref, manifest, snapshots, local_top) do
    manifest = Manifest.prune_above(manifest, local_top)
    # What the entry already names is carried over rather than re-derived: by
    # seq, since a content-addressed name cannot be rebuilt without the bytes,
    # and by content (`Manifest.snapshot_blob/4`), so equal bytes upload nothing.
    known = Map.new(manifest.snapshots, &{&1.seq, &1.blob})

    Enum.reduce_while(snapshots, {:ok, []}, fn {seq, path}, {:ok, acc} ->
      case Map.fetch(known, seq) do
        {:ok, blob} -> {:cont, {:ok, [{seq, blob} | acc]}}
        :error -> upload_snapshot(state, ref, {seq, path}, Map.values(known), acc)
      end
    end)
    |> case do
      {:ok, seq_blobs} -> {:ok, Manifest.put_snapshots(manifest, Enum.reverse(seq_blobs))}
      {:error, _} = err -> err
    end
  end

  defp upload_snapshot(state, ref, {seq, path}, shipped, acc) do
    case File.read(path) do
      {:ok, bytes} ->
        blob = Manifest.snapshot_blob(ref, seq, sha256(bytes), shipped)

        case upload_payload(state, shipped, blob, bytes) do
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

  # A shipped range is superseded only when the local WAL has dropped it AND
  # a retained snapshot covers it. Both halves matter: dropping ranges on the
  # WAL floor alone would strand a rewound Cell whose snapshot was deleted
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
    delay = Backoff.arm(:durability_ship, attempt, ref)
    Process.send_after(self(), {:retry, ref}, delay)
    record_failure(ref, reason, attempt, delay)
    emit(:ship_failed, %{attempt: attempt, backoff_ms: delay}, ref, %{reason: inspect(reason)})
    state
  end

  # Losing a lineage is ORDINARY now — every migration produces a loser — so
  # this is not an alarm and must not latch. `pending_since_ms` is cleared with
  # it: a reference this node does not own cannot be behind on shipping, and
  # leaving it set would report a permanent RPO breach (and a permanently
  # degraded `/healthz`) for every handover the cluster performs.
  #
  # Nothing local is touched: the node that lost the claim is usually the one
  # that still has the only other copy.
  defp supersede(state, ref, holder) do
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
  # relay, one whose state moved), so out-of-scope artifacts are still
  # reclaimed — at a far longer threshold, where no ship can plausibly still be
  # in flight. Collection may leak; it must never lose.
  defp collect_orphans(state) do
    with {:ok, blob_entries} <- state.backend.list(Manifest.blob_prefix(), state.opts) do
      owned = MapSet.new(DurabilityTier.all_refs(state.writer))
      abandoned = Durations.get(:durability_gc_abandoned_after_ms)

      # The age test comes first and costs nothing, so nothing younger than the
      # grace is ever fetched for, let alone deleted.
      grace = Durations.get(:durability_gc_grace_ms)
      {candidates, retained} = Enum.split_with(blob_entries, fn {_key, age} -> age >= grace end)

      orphans =
        candidates
        |> Enum.group_by(fn {key, _age} -> Manifest.ref_of_blob_key(key) end)
        |> Enum.flat_map(&orphans_of_ref(state, &1, owned, abandoned))

      Enum.each(orphans, &state.backend.delete(&1, state.opts))

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

  # One manifest answers for one reference's artifacts, and only for those:
  # `blob_key/2` builds every key under its own reference's prefix, so a blob
  # under `blob/<path>/` can be named by `manifest/<path>` and by nothing else.
  # A pass therefore costs one fetch per reference that actually has a candidate,
  # rather than one per reference in the whole store.
  #
  # Always fetched, never read from the ship cache: a cached entry can be behind
  # what another writer has since committed, and deleting against a stale live
  # set is the whole failure being closed.
  defp orphans_of_ref(state, {ref_result, entries}, owned, abandoned) do
    {live, min_age} =
      case ref_result do
        {:ok, ref} ->
          scope_floor = if MapSet.member?(owned, ref), do: 0, else: abandoned
          {MapSet.new(Manifest.blob_keys(fetch_manifest(state, ref))), scope_floor}

        # A key this build did not write has no reference to consult, so nothing
        # can vouch for it and it is only ever reclaimed on the long threshold.
        :error ->
          {MapSet.new(), abandoned}
      end

    for {key, age} <- entries, age >= min_age, not MapSet.member?(live, key), do: key
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
          failures: Backoff.reset(:durability_ship, ref),
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

  defp not_owed(ref), do: update_status(ref, &%{&1 | pending_since_ms: nil})

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

  defp arm_sweep(state),
    do: arm_timer(state, :sweep, Durations.get(:durability_ship_interval_ms), :sweep_tick)

  defp arm_gc(state),
    do: arm_timer(state, :gc, Durations.get(:durability_gc_interval_ms), :gc_tick)

  defp emit(event, measurements, ref, extra \\ %{}) do
    :telemetry.execute(
      [:flezha, :state, event],
      Map.put(measurements, :count, 1),
      Map.merge(%{ref: inspect(ref)}, extra)
    )
  end
end
