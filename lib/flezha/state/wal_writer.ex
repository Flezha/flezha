defmodule Flezha.State.WalWriter do
  @moduledoc """
  Per-Cell `GenServer` wrapping a single open WAL file handle.

  Each appended record is framed by `Flezha.State.RecordFrame`, which owns the
  layout and its crc; nothing here restates either.

  fsync is decoupled from append: the writer registers as dirty with
  `Flezha.State.FsyncBatcher` which drives the actual `:file.datasync/1`
  on its tick cadence. High-watermark crossings emit a telemetry event
  (`[:flezha, :state, :wal, :backpressure]`) that section-9 wiring picks
  up to ship the wire-level `CellMailboxStatus`.
  """

  use GenServer, restart: :transient
  use Flezha.Process.Timer, keys: [:outbox_retry, :remote_retry]

  alias Flezha.Config.Durations
  alias Flezha.State.Frame
  alias Flezha.SDK.SafeCall
  alias Flezha.State.{Metrics, RecordFrame, SeqIndex, WriterRegistry}
  alias Flezha.State.WalWriter.{Admission, Boot, Epoch, RemoteGate, Settlement, State}

  @type t :: State.t()

  @typedoc """
  Liveness view consumed by `Flezha.State.Watchdog`. `last_advance_at`
  moves only when `last_persisted_seq` increases; `last_change_seen_at`
  moves on every received Change frame (append, duplicate, or gap). A
  Cell is *stalled* (vs idle) when Changes are still arriving but
  the persisted seq has stopped advancing. `wal_size_bytes` /
  `oldest_unsnapshotted_at` / `owning_runner` feed the Watchdog's
  size+age compaction triggers without a second disk scan.
  """
  @type stall_status :: %{
          last_persisted_seq: integer(),
          last_advance_at: integer(),
          last_change_seen_at: integer(),
          wal_size_bytes: non_neg_integer(),
          oldest_unsnapshotted_at: integer() | nil,
          owning_runner: binary() | nil,
          out_pending_seq: integer(),
          outbox_flushed_seq: integer(),
          outbox_unconfirmed_ms: non_neg_integer()
        }

  # ---------- Public API ----------

  def start_link(cell_id) do
    GenServer.start_link(__MODULE__, cell_id, name: via(cell_id))
  end

  @doc """
  Append a Change at `seq`. `runner_id` identifies the runner that emitted
  it; a Change from a *different* runner than the one that wrote the current
  epoch realigns the WAL to the new runner's seq base (runner-restart
  recovery — see `handle_call/3`) — unless a takeover superseded that session
  (`Flezha.Runner.FleetManager.Marks.superseded_session?/1`), in which case the
  frame is refused with `{:error, :stale_writer}` instead of realigning. Pass
  `nil` (the default) to disable epoch tracking — the single-runner / test
  path keeps the prior dedup/gap/append semantics.
  """
  @spec append(GenServer.server(), non_neg_integer(), binary(), binary(), binary() | nil) ::
          :ok | :duplicate | :awaiting_baseline | {:error, term()}
  def append(server, seq, header, change_bytes, runner_id \\ nil) do
    call(server, {:append, seq, header, change_bytes, runner_id})
  end

  @doc """
  Non-blocking append. The caller (a `StreamChannel`) hands off the Change and
  returns immediately, so WAL/recovery latency never stalls the data-plane
  channel. On a seq gap the writer emits `[:flezha, :state, :seq_gap]` and
  drives resync itself. Past the hard in-memory bound (un-fsynced bytes or
  mailbox depth) the Change is shed (dropped, `wal_shed_total` incremented),
  and the resulting gap drives resync — back-pressure sheds, never blocks.

  `credit_opts` names the stream channel that handed the Change over:
  `%{channel: pid, session: id | nil, wire_size: bytes}`, `session` set on a
  credit-speaking stream. The writer reports every attributed byte back to
  `channel` exactly once as a `{:credit_drained, session, bytes, cell_id}`
  message — on fsync for written records (durable drain, mints credit and
  confirms processed-prefix markers), or immediately for a Change that never
  occupies the un-fsynced buffer (dedup drop, gap rejection, shed,
  stale-writer refusal, epoch realign, truncation). The session tag lets the
  channel drop credit for bytes debited under a pre-reconnect session. An
  empty map (the default) accounts nothing.
  """
  @spec append_async(
          GenServer.server(),
          non_neg_integer(),
          binary(),
          binary(),
          binary() | nil,
          map()
        ) :: :ok
  def append_async(server, seq, header, change_bytes, runner_id \\ nil, credit_opts \\ %{}) do
    GenServer.cast(server, {:append_async, seq, header, change_bytes, runner_id, credit_opts})
  end

  @spec fsync(GenServer.server()) :: :ok
  def fsync(server), do: call(server, :fsync)

  @doc "Fire-and-forget `fsync/1`: how the batcher's tick syncs every dirty writer at once."
  @spec fsync_async(GenServer.server()) :: :ok
  def fsync_async(server), do: GenServer.cast(server, :fsync)

  @doc """
  Append one record at the log's own next seq, answering the seq it was
  assigned once it is durable. Seq assignment and write are one call because
  they are one decision: two processes appending to the same log would
  otherwise read the same tail and collide, and the loser's record would be
  refused as a duplicate.

  `sync: :sync` (the default) fsyncs before answering. `sync: :batched` marks
  the writer dirty and answers when the batch that holds the record has synced
  — the `FsyncBatcher` tick, a prefix cut, or a clean close — so every append
  that lands in one tick shares one fsync, and the reply is never earlier than
  the sync. A writer that dies in between never answers, which fails the
  caller's call (or its `send_request`, see `append_next_async/6`).

  `dedup: true` refuses a record whose 16-byte header matches one still in the
  file, answering `{:duplicate, seq}` with the existing record's seq — the
  idempotency a re-sent barrier needs. The index is built from the file on the
  first keyed append and trimmed by every prefix cut, so a log that never
  dedups pays nothing.
  """
  @spec append_next(GenServer.server(), binary(), binary(), keyword()) ::
          {:ok, non_neg_integer()} | {:duplicate, non_neg_integer()} | {:error, term()}
  def append_next(server, header, body, opts \\ []),
    do: call(server, {:append_next, header, body, opts})

  @doc """
  `append_next/4` issued with `:gen_server.send_request/4` under `label` into
  `reqids`: the caller carries on and collects the answer — the same reply, or
  the writer's exit — with `:gen_server.check_response/3`. With
  `sync: :batched` the answer IS the durability token: it arrives when the
  batch has synced, never before.
  """
  @spec append_next_async(
          GenServer.server(),
          binary(),
          binary(),
          keyword(),
          :gen_server.request_id_collection(),
          term()
        ) :: :gen_server.request_id_collection()
  def append_next_async(server, header, body, opts, reqids, label),
    do: :gen_server.send_request(server, {:append_next, header, body, opts}, label, reqids)

  @doc """
  Drop the records at or below `seq`. `min_bytes: n` defers the file rewrite
  while fewer than `n` bytes lie below the cut — the records stay on disk until
  a later request finds enough of them, so a log cut at every turn pays its
  fsync'd rewrite once per `n` bytes rather than once per turn. A keyed log's
  dedup window releases the records either way.
  """
  @spec truncate_up_to(GenServer.server(), non_neg_integer(), keyword()) :: :ok | {:error, term()}
  def truncate_up_to(server, seq, opts \\ []),
    do: call(server, {:truncate_up_to, seq, opts})

  @doc "Fire-and-forget `truncate_up_to/3`, for a cut whose caller has no use for the answer."
  @spec truncate_up_to_async(GenServer.server(), non_neg_integer(), keyword()) :: :ok
  def truncate_up_to_async(server, seq, opts \\ []),
    do: GenServer.cast(server, {:truncate_up_to, seq, opts})

  @doc """
  Rewind the Cell to `seq` under `epoch`: keep the records at or below it,
  drop the snapshots above it, and re-seat the outbox on the survivors.

  The non-destructive sibling of the realign wipe. A realign discards a stale
  runner's whole lineage; a rewind discards only the history above a boundary
  the controller means to rebuild, which is what a late-input correction needs.

  `epoch` is explicit rather than `current + 1` so that re-running an
  interrupted correction re-asserts the epoch it already committed to. A
  correction that crashed after this call and before its replay finished resumes
  by rewinding to the same boundary under the same epoch — a no-op on the WAL,
  and crucially not a second bump, which would make the replay's outputs miss
  the rows the first attempt already wrote.
  """
  @spec rewind_to(GenServer.server(), non_neg_integer(), non_neg_integer()) ::
          :ok | {:error, term()}
  def rewind_to(server, seq, epoch), do: call(server, {:rewind_to, seq, epoch})

  @doc "Current outbox epoch — the base a deliberate correction bumps from."
  @spec outbox_epoch(GenServer.server()) :: non_neg_integer()
  def outbox_epoch(server), do: call(server, :outbox_epoch)

  @doc """
  Re-dispatch now every relay call this Cell's outbox still holds from before
  `started` (monotonic ms); how many entries hold call `id` with a chain run
  yet to report, or the writer's refusal.
  """
  @spec redrive_calls(GenServer.server(), integer(), binary()) ::
          non_neg_integer() | {:error, term()}
  def redrive_calls(server, started, id), do: call(server, {:redrive_calls, started, id})

  @doc """
  Where a correction for `target_event_time` must rewind to: the last seq whose
  history entirely precedes that event time, plus the input offset consumed as
  of that seq.

  The boundary is the record *before the first one at or after the target* — not
  the last record whose own event time precedes it. The difference matters
  exactly when the WAL already holds an out-of-order straggler (a beyond-window
  input processed as an arrival): scanning backwards for the first earlier event
  time would stop at the straggler and leave the records that overtook the late
  input in place, correcting nothing. Scanning forward folds the straggler into
  the replay too, which is what re-ordering it means.

  `{:error, :nothing_to_correct}` when no record is at or after the target —
  the input is not actually late for this Cell.
  """
  @spec correction_baseline(GenServer.server(), non_neg_integer()) ::
          {:ok, %{seq: integer(), consumed_offset: integer()}} | {:error, term()}
  def correction_baseline(server, target_event_time),
    do: call(server, {:correction_baseline, target_event_time})

  @doc """
  Highest input-log offset consumed at or below `seq` (`-1` when none) — the
  point an manual replay from that baseline must re-forward the input log
  from.
  """
  @spec consumed_offset_at(GenServer.server(), non_neg_integer()) :: integer() | {:error, term()}
  def consumed_offset_at(server, seq), do: call(server, {:consumed_offset_at, seq})

  @doc "`cell_id`'s input floor, read from disk alone: `Epoch.floor/2` at `:latest`."
  def consumed_offset_on_disk(cell_id), do: Epoch.floor(cell_id, :latest)

  @doc "The inter-tasklet values `cell_id`'s WAL records kept above `floor`; see `Epoch`."
  defdelegate edges_above(cell_id, floor), to: Epoch

  @spec last_persisted_seq(GenServer.server()) :: integer()
  def last_persisted_seq(server), do: call(server, :last_persisted_seq)

  @doc """
  Ask for the record at `seq` to be durable, with `:gen_server.send_request/4`
  under `label` into `reqids`: `{:ok, seq}` comes back at once when it already
  is, else from the sync that makes it so, and a writer whose file refused a
  record answers `{:error, refusal}`. A record that never arrives is never
  answered; the caller bounds the wait.
  """
  @spec await_durable_async(
          GenServer.server(),
          non_neg_integer(),
          :gen_server.request_id_collection(),
          term()
        ) :: :gen_server.request_id_collection()
  def await_durable_async(server, seq, reqids, label),
    do: :gen_server.send_request(server, {:await_durable, seq}, label, reqids)

  @doc """
  A steps relay Cell's records `first..last` are named by a stage marker that is
  now durable: release their outputs (`Flezha.State.WalWriter.Settlement`).
  """
  @spec commit_records(GenServer.server(), non_neg_integer(), non_neg_integer()) :: :ok
  def commit_records(server, first, last),
    do: GenServer.cast(server, {:commit_records, first, last})

  @doc """
  At a steps relay bind, re-derive which of this Cell's queued outputs are
  committed from the instance's `TaskJournal.committed_records/1`, dropping the
  rest: what the rebound runner interrupted, and what a rewind unwound.
  """
  @spec seed_commits(GenServer.server(), %{
          optional(binary()) => [{non_neg_integer(), non_neg_integer()}]
        }) :: :ok
  def seed_commits(server, committed), do: GenServer.cast(server, {:seed_commits, committed})

  @doc """
  Live outbox status (`flz state outbox`): the cursors, the entries owed a
  dispatch (`pending`), those held for a steps stage marker (`held`) and
  those in the plugin chain awaiting confirmation (`awaiting`).
  """
  @spec outbox_status(GenServer.server(), timeout()) :: map()
  def outbox_status(server, timeout \\ 5_000),
    do: call(server, :outbox_status, timeout)

  @spec stall_status(GenServer.server()) :: stall_status()
  def stall_status(server), do: call(server, :stall_status)

  # Two closers can race for one writer, a relay's teardown and its move; the
  # one that finds it gone has closed it all the same.
  @spec close(GenServer.server()) :: :ok
  def close(server) do
    GenServer.stop(server, :normal)
  catch
    :exit, {:noproc, _stop} -> :ok
  end

  defp via(cell_id) do
    {:via, Elixir.Registry, {WriterRegistry.name(), cell_id}}
  end

  # ---------- GenServer ----------

  # One writer per cell, so `server` is a resolved pid or registered name that
  # can be mid-restart or gone. Every entry point declares the same refusal
  # rather than waiting out the default and dying with it.
  defp call(server, request, timeout \\ nil) do
    SafeCall.bounded(server, request, timeout || Durations.get(:wal_writer_call_ms),
      on_error: &{:error, {:wal_writer_unavailable, &1}}
    )
  end

  @impl true
  def init(cell_id) do
    Process.flag(:trap_exit, true)
    {:ok, Boot.new(cell_id), {:continue, :open}}
  end

  @impl true
  def handle_continue(:open, state), do: {:noreply, Boot.open(state)}

  # Everything that writes to the WAL file or cuts it. A refused writer answers
  # each of them with its refusal; the read-only status calls still answer.
  @writing_requests [:append, :append_next, :rewind_to, :truncate_up_to, :await_durable]

  # A writer whose file refused a record answers every request that would write
  # to it, cut it, or wait on it, with that refusal, and stays alive doing so. Raising here
  # would be restarted straight back into the same file (`restart: :transient`),
  # and the read-only status calls below still answer, so the watchdog and the
  # convergence loop keep seeing the Cell. The refusal is what leaves the
  # relay unbound: the restore assembled from this file refuses too.
  @impl true
  def handle_call(request, _from, %{refusal: {_, _, _} = refusal} = state)
      when request == :fsync
      when is_tuple(request) and elem(request, 0) in @writing_requests do
    {:reply, {:error, refusal}, state}
  end

  def handle_call({:append, seq, header, change_bytes, runner_id}, _from, state) do
    # Every received Change frame — accepted, deduped, realigned, or rejected
    # as a gap — proves the SDK is still emitting, which distinguishes a
    # stalled Cell from a genuinely idle one for the watchdog.
    now = State.now_ms()
    state = %{state | last_change_seen_at: now}

    cond do
      Epoch.stale_writer?(state, runner_id) ->
        # A takeover superseded this session — its channels were unbound
        # before the new owner was bound, so this frame is late in-flight
        # residue (typically a wedged channel flushing after the swap).
        # Refuse instead of realigning onto the stale lineage; the owner's
        # restore/resync handshake re-covers anything durable.
        {:reply, {:error, :stale_writer}, Epoch.refuse_stale(state, runner_id)}

      Epoch.realign?(state, runner_id, seq) ->
        # A *different* runner than the one that wrote the current epoch is
        # emitting a non-contiguous seq — the signature of a runner pod
        # restart whose post-rebuild seq base has diverged from our WAL. The
        # rebuilt runner is the live source of truth, so adopt its epoch
        # instead of dedup-dropping its Changes as stale retransmits, which
        # would freeze the WAL while the runner keeps producing.
        case Epoch.realign_epoch(state, seq, runner_id, now) do
          {:ok, state} -> write_record(state, seq, header, change_bytes, runner_id, now)
          {:error, _reason} = refused -> {:reply, refused, state}
        end

      seq <= state.last_persisted_seq ->
        Metrics.add(state.metrics, :dedup_drops_total, 1)
        {:reply, :duplicate, state}

      seq != state.last_persisted_seq + 1 and state.last_persisted_seq == -1 ->
        # Empty-WAL gap: a cold takeover — this controller holds no baseline
        # for a Cell the live runner already has state for. Not a
        # protocol violation: shed the frame and pull a full-state baseline
        # via CompactCell instead of disconnecting the SDK.
        {:reply, :awaiting_baseline, Epoch.await_baseline(state, seq, runner_id)}

      seq != state.last_persisted_seq + 1 ->
        {:reply, {:error, {:gap, expected: state.last_persisted_seq + 1, got: seq}}, state}

      true ->
        write_record(state, seq, header, change_bytes, runner_id, now)
    end
  end

  def handle_call({:append_next, header, body, opts}, from, state) do
    dedup? = Keyword.get(opts, :dedup, false)
    state = if dedup?, do: ensure_header_index(state), else: state

    case dedup? and Map.fetch(state.header_index, header) do
      {:ok, seq} ->
        answer(state, from, {:duplicate, seq}, opts)

      _ ->
        seq = state.last_persisted_seq + 1

        case handle_call({:append, seq, header, body, nil}, from, state) do
          {:reply, :ok, state} -> answer(state, from, {:ok, seq}, opts)
          {:reply, error, state} -> {:reply, error, state}
        end
    end
  end

  def handle_call(:fsync, _from, state), do: {:reply, :ok, do_fsync(state)}

  def handle_call({:await_durable, seq}, _from, state) when seq <= state.outbox_durable_seq,
    do: {:reply, {:ok, seq}, state}

  def handle_call({:await_durable, seq}, from, state),
    do: {:noreply, %{state | durable_waiters: [{from, seq} | state.durable_waiters]}}

  def handle_call({:rewind_to, seq, epoch}, _from, state) do
    case Epoch.rewind(state, seq, epoch) do
      {:ok, state} -> {:reply, :ok, state}
      {:error, refusal, state} -> {:reply, {:error, refusal}, state}
    end
  end

  # Drop the records at or below `seq` (compaction's prefix, a retention
  # cut). The cut runs in this loop — under the effectively-once tier every
  # output rides it — so it costs the tail, never the file. A cut with nothing
  # below it (a retention tick on a writer compaction already cut), or with too
  # little below it to be worth a rewrite yet, answers at once; a seq ahead of
  # the writer is still recorded, since that snapshot is its new baseline. The
  # dedup window releases the records in every case. What the dropped records
  # consumed is folded first (`Epoch.cut_below/3`).
  def handle_call({:truncate_up_to, seq, opts}, _from, state) do
    case Epoch.cut_below(trim_header_index(state, seq), seq, Keyword.get(opts, :min_bytes, 0)) do
      {:ok, state} ->
        {:reply, :ok, after_prefix_cut(state, seq)}

      {:none, state} ->
        {:reply, :ok, Epoch.establish_baseline(state, seq)}

      # Nothing cut, the handle untouched: the file went away under a queued cut, the
      # lineage install failed, or a record was refused, which stops this writer serving.
      {{:error, reason}, state} ->
        {:reply, {:error, reason}, note_refusal(state, reason)}
    end
  end

  def handle_call(:last_persisted_seq, _from, state) do
    {:reply, state.last_persisted_seq, state}
  end

  def handle_call(:outbox_epoch, _from, state) do
    {:reply, state.outbox_epoch, state}
  end

  def handle_call({:redrive_calls, started, id}, _from, state) do
    {count, state} = Settlement.redrive_calls(state, started, id)
    {:reply, count, state}
  end

  def handle_call({:correction_baseline, target}, _from, state) do
    {:reply, Epoch.scan_correction_baseline(state, target), state}
  end

  def handle_call({:consumed_offset_at, seq}, _from, state) do
    {:reply, Epoch.floor(state.cell_id, seq), state}
  end

  def handle_call(:outbox_status, _from, state) do
    {pending, held} = Settlement.queued(state)

    status =
      Map.merge(State.progress(state), %{
        pending: pending,
        held: held,
        awaiting: map_size(state.outbox_awaiting)
      })

    {:reply, status, state}
  end

  def handle_call(:stall_status, _from, state) do
    status = %{
      last_persisted_seq: state.last_persisted_seq,
      last_advance_at: state.last_advance_at,
      last_change_seen_at: state.last_change_seen_at,
      wal_size_bytes: Metrics.get(state.metrics, :wal_size_bytes),
      oldest_unsnapshotted_at: state.oldest_unsnapshotted_at,
      owning_runner: state.owning_runner,
      out_pending_seq: state.outbox_last_seq,
      outbox_flushed_seq: state.outbox_flushed_seq,
      outbox_unconfirmed_ms: Settlement.unconfirmed_age_ms(state)
    }

    {:reply, status, state}
  end

  # A refusal seen by a live writer stops it serving, the way a refused open
  # does. Any other error — a missing file under a queued cut — leaves it alone.
  defp note_refusal(state, {_reason, _last_seq, _offset} = refusal),
    do: %{state | refusal: refusal}

  defp note_refusal(state, _other), do: state

  # The append's reply under its sync mode. Synchronous: fsync now. Batched: owed
  # until the next sync — unless nothing is un-synced (a duplicate against a
  # clean writer), in which case the record it names is durable already.
  defp answer(state, from, reply, opts) do
    case Keyword.get(opts, :sync, :sync) do
      :sync -> {:reply, reply, do_fsync(state)}
      :batched when state.pending_since_us == nil -> {:reply, reply, state}
      :batched -> {:noreply, %{state | sync_waiters: [{from, reply} | state.sync_waiters]}}
    end
  end

  @impl true
  def handle_cast({:append_async, seq, header, change_bytes, runner_id, credit_opts}, state) do
    state = %{state | credit_active?: Map.get(credit_opts, :session) != nil}

    if Admission.over_hard_bound?(state) do
      state = RemoteGate.released(state, credit_opts)
      {:noreply, Admission.shed(state, seq, runner_id)}
    else
      do_append_async(
        seq,
        header,
        change_bytes,
        runner_id,
        %{state | shedding?: false},
        credit_opts
      )
    end
  end

  def handle_cast(:fsync, state), do: {:noreply, do_fsync(state)}
  def handle_cast({:fence, credit}, state), do: {:noreply, RemoteGate.released(state, credit)}

  def handle_cast({:truncate_up_to, seq, opts}, state) do
    {:reply, _reply, state} = handle_call({:truncate_up_to, seq, opts}, nil, state)
    {:noreply, state}
  end

  def handle_cast({:commit_records, first, last}, state),
    do: {:noreply, Settlement.commit_records(state, first, last)}

  def handle_cast({:seed_commits, committed}, state),
    do: {:noreply, Settlement.seed_commits(state, committed, :drop)}

  # Delivery confirmation (`Flezha.State.Outbox`): sinks report against the
  # entry the dispatch registered.

  def handle_cast({:outbox_hold, key}, state),
    do: {:noreply, Settlement.tally(state, key, &%{&1 | holds: &1.holds + 1})}

  def handle_cast({:outbox_settled, key, holds}, state),
    do: {:noreply, Settlement.settled(state, {key, holds})}

  def handle_cast({:outbox_settled_many, entries}, state),
    do: {:noreply, Enum.reduce(entries, state, &Settlement.settled(&2, &1))}

  def handle_cast({:outbox_confirmed, key}, state),
    do: {:noreply, Settlement.confirmed(state, key)}

  def handle_cast({:outbox_confirmed_many, keys}, state),
    do: {:noreply, Enum.reduce(keys, state, &Settlement.confirmed(&2, &1))}

  def handle_cast({:outbox_delivery_hold, key}, state),
    do: {:noreply, Settlement.delivery(state, key, :held)}

  def handle_cast({:outbox_delivery_confirmed, key}, state),
    do: {:noreply, Settlement.delivery(state, key, :confirmed)}

  defp do_append_async(seq, header, change_bytes, runner_id, state, credit_opts) do
    case handle_call({:append, seq, header, change_bytes, runner_id}, nil, state) do
      {:reply, {:error, {:gap, expected: expected, got: got}}, new_state} ->
        new_state = RemoteGate.released(new_state, credit_opts)
        Admission.emit_seq_gap(new_state.cell_id, expected, got)
        {:noreply, Admission.open_gap(new_state, runner_id, :seq_gap)}

      {:reply, :ok, new_state} ->
        {:noreply, Admission.track_credit(new_state, credit_opts)}

      {:reply, _result, new_state} ->
        {:noreply, RemoteGate.released(new_state, credit_opts)}
    end
  end

  # Overwatch was not yet resolvable when durable outbox entries arrived
  # (cold-boot re-drive before the relay bound): retry the gated flush.
  @impl true
  def handle_info(:outbox_retry, state) do
    before = state.outbox_flushed_seq
    state = Settlement.resettle_outbox(timer_fired(state, :outbox_retry))
    state = Settlement.try_dispatch_outbox(state)

    state =
      if state.outbox_flushed_seq != before, do: Settlement.persist_dedup(state), else: state

    state =
      if state.outbox_awaiting == %{}, do: state, else: Settlement.schedule_outbox_retry(state)

    {:noreply, state}
  end

  def handle_info({:remote_committed, generation, result}, state),
    do: {:noreply, state |> RemoteGate.shipped(generation, result) |> Settlement.persist_dedup()}

  def handle_info(:remote_retry, state),
    do: {:noreply, state |> timer_fired(:remote_retry) |> RemoteGate.retry()}

  def handle_info(_msg, state), do: {:noreply, state}

  # A clean close syncs what it holds and answers the batched appends it owes;
  # a crash answers nobody, so a waiter fails instead of reading as durable.
  # Records synced here stay owed to their streams until a ship covers them.
  @impl true
  def terminate(reason, %{file_handle: file} = state) when not is_nil(file) do
    synced = :file.datasync(file)
    _ = :file.close(file)
    _ = if synced == :ok, do: RemoteGate.close(state), else: RemoteGate.reset(state)
    _ = Settlement.persist_dedup(state)

    if synced == :ok and clean_exit?(reason), do: reply_synced(state)

    :ok
  end

  def terminate(_reason, state) do
    _ = RemoteGate.reset(state)
    :ok
  end

  defp clean_exit?(reason), do: reason in [:normal, :shutdown] or match?({:shutdown, _}, reason)

  defp do_fsync(state) do
    t0 = System.monotonic_time(:microsecond)
    :ok = :file.datasync(state.file_handle)
    t1 = System.monotonic_time(:microsecond)
    Metrics.add(state.metrics, :fsync_total, 1)

    Admission.record_latency_sample(state.metrics, state.latency_reservoir, div(t1 - t0, 1000))
    |> case do
      {p99_ms, new_reservoir} ->
        Metrics.put(state.metrics, :flush_latency_ms_p99, p99_ms)

        Metrics.put(
          state.metrics,
          :fsync_durability_lag_ms,
          fsync_lag_ms(state.pending_since_us, t1)
        )

        new_state = %{
          state
          | pending_bytes: 0,
            pending_since_us: nil,
            latency_reservoir: new_reservoir
        }

        new_state = Admission.maybe_signal_backpressure(new_state, 0)

        # Durable drain: report the fsynced wire bytes back to each owning
        # stream's ledger — the only point that mints credit for written
        # records (append alone never does) — and dispatch the gated
        # `:on_output` flush; behind the remote gate, once the ship lands.
        # The dedup file advances once per batch (never per record).
        new_state = RemoteGate.synced(new_state)

        new_state |> Settlement.persist_dedup() |> reply_synced()
    end
  end

  # Everything appended is durable: answer the batched appends, oldest first,
  # and every caller waiting on a record the sync covered.
  defp reply_synced(state) do
    state.sync_waiters
    |> Enum.reverse()
    |> Enum.each(fn {from, reply} -> GenServer.reply(from, reply) end)

    {due, waiting} =
      Enum.split_with(state.durable_waiters, fn {_from, seq} ->
        seq <= state.last_persisted_seq
      end)

    Enum.each(due, fn {from, seq} -> GenServer.reply(from, {:ok, seq}) end)
    %{state | sync_waiters: [], durable_waiters: waiting}
  end

  # ---------- Header dedup ----------

  # `header => seq` for every record in the file, built once per writer
  # lifetime by the first keyed append. The file is the set a keyed log dedups
  # against: a prefix cut trims it, a rewrite of the lineage drops it for a
  # rebuild.
  defp ensure_header_index(%{header_index: nil} = state) do
    # The boot scan walked this file to its end before any append reached it,
    # so a record that does not decode here is impossible, not a case.
    {:ok, index} =
      RecordFrame.index_by(state.file_handle, state.wal_path, fn {_seq, header, _} -> header end)

    %{state | header_index: index}
  end

  defp ensure_header_index(state), do: state

  defp note_header(%{header_index: nil} = state, _header, _seq), do: state

  defp note_header(state, header, seq),
    do: %{state | header_index: Map.put(state.header_index, header, seq)}

  defp trim_header_index(%{header_index: nil} = state, _seq), do: state

  defp trim_header_index(state, seq),
    do: %{state | header_index: Map.reject(state.header_index, fn {_, s} -> s <= seq end)}

  # The bookkeeping a prefix cut owes once the file is shorter: the rewrite
  # fsynced the kept tail, so everything pending is durable.
  defp after_prefix_cut(state, seq) do
    size = RecordFrame.file_size(state.wal_path)
    Metrics.put(state.metrics, :wal_size_bytes, size)

    # Surviving tail records post-date the snapshot's baseline by at most the
    # compaction round-trip, so "now" is a safe age floor for the next trigger.
    oldest = if size > 0, do: State.now_ms()

    state = RemoteGate.drained(state)
    state = Epoch.establish_baseline(state, seq)

    state = %{state | pending_bytes: 0, pending_since_us: nil, oldest_unsnapshotted_at: oldest}

    # Rotation/compaction is one of the dedup file's advance points.
    state |> Settlement.persist_dedup() |> reply_synced()
  end

  # ---------- Append / realign ----------

  defp write_record(state, seq, header, change_bytes, runner_id, now) do
    # One open per fsync window: `pending_since_us` is nil exactly when
    # nothing written since the last sync is waiting on the batcher.
    state = if state.pending_since_us == nil, do: Epoch.open_window(state), else: state
    record = Frame.encode_record(seq, header, change_bytes)
    :ok = :file.write(state.file_handle, record)
    size = byte_size(record)

    Metrics.add(state.metrics, :persisted_changes_total, 1)
    Metrics.add(state.metrics, :persisted_bytes_total, size)
    Metrics.put(state.metrics, :wal_seq, seq)
    Metrics.add(state.metrics, :wal_size_bytes, size)

    # The WAL append is the only per-record durable write; the dedup file
    # is a rebuildable cache advanced at fsync-batch cadence (and repaired
    # from the WAL scan on boot).
    state = Settlement.maybe_queue_effect(state, seq, header, change_bytes)
    new_pending = state.pending_bytes + size

    new_state = %{
      note_header(state, header, seq)
      | last_persisted_seq: seq,
        seq_index: SeqIndex.note(state.seq_index, seq, size),
        last_advance_at: now,
        owning_runner: runner_id || state.owning_runner,
        pending_bytes: new_pending,
        pending_since_us: state.pending_since_us || System.monotonic_time(:microsecond),
        oldest_unsnapshotted_at: state.oldest_unsnapshotted_at || now
    }

    new_state = Admission.maybe_signal_backpressure(new_state, new_pending)

    # A contiguous append is the evidence a gap episode healed, whichever path
    # wrote it: the episode ends and its counter-fact releases the loop's
    # ladder, so the next gap is declared at once.
    {:reply, :ok, Admission.close_gap(new_state)}
  end

  # ---------- Metrics helpers ----------

  defp fsync_lag_ms(nil, _now_us), do: 0

  defp fsync_lag_ms(pending_since_us, now_us) do
    div(max(now_us - pending_since_us, 0), 1000)
  end
end
