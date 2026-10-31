defmodule Flezha.State.WalWriter.Admission do
  @moduledoc """
  What the writer lets in, and what it tells the sender about it.

  Four decisions over one writer struct: the hard in-memory bound and the shed
  that answers it, the soft-watermark back-pressure signal, the per-session
  credit attribution a durable drain releases, and the bounds themselves,
  resolved once at boot. A shed is the last resort — a conformant
  credit-speaking sender can never reach it, so one is a protocol anomaly
  rather than back-pressure. It also answers the callers a sync settles
  (`reply_synced/1`) and measures how long written records waited for it (`fsync_lag_ms/2`).
  """

  alias Flezha.Config

  alias Flezha.Config.Durations
  alias Flezha.Convergence.Facts
  alias Flezha.State.{Commands, Metrics, PersistenceResolver}
  alias Flezha.State.WalWriter.State

  @reservoir_capacity 1024

  # ---------- Hard bound (shed) ----------

  # Last-resort safety net above the soft watermark: cooperative SDKs throttle on
  # `CellMailboxStatus` first; only a slow disk or a non-cooperating SDK
  # reaches here.
  #
  # The un-fsynced byte bound always applies. The mailbox-depth cap applies only
  # to an un-credited stream, where nothing else stops a cast flood: it counts
  # MESSAGES while credit counts BYTES, so on a credit-speaking stream it fires
  # far below the window it is meant to backstop — at 128-byte Changes, 5_000
  # messages is ~0.6 MB against a 16 MB window, shedding a conformant sender's
  # Changes 25x early. Credit already bounds mailbox + un-fsynced bytes to the
  # window (the channel debits at admit, before the cast).
  def over_hard_bound?(state) do
    state.pending_bytes >= state.limits.hard_watermark or
      (not state.credit_active? and message_queue_len() >= state.limits.pending_msg_cap)
  end

  defp message_queue_len() do
    case Process.info(self(), :message_queue_len) do
      {:message_queue_len, n} -> n
      nil -> 0
    end
  end

  # A seq gap is one latched fact per episode: the first gap declares
  # `{:seq_gap, cell_id}` to the convergence loop, which plans the resync on
  # its own table; every further gap until the next contiguous append rides
  # the open episode. Shared by the three gap-driven triggers — a gap, a shed,
  # and a cold-takeover baseline pull — so a sustained flood declares once.
  @spec open_gap(State.t(), binary(), atom()) :: State.t()
  def open_gap(%{gap_open?: true} = state, _runner_id, _trigger), do: state

  def open_gap(state, runner_id, trigger) do
    Facts.declare({:seq_gap, state.cell_id, %{runner_id: runner_id, trigger: trigger}})
    %{state | gap_open?: true}
  end

  # A contiguous append, or the pulled baseline, is the evidence the gap
  # healed: the episode ends, its counter-fact releases the loop's ladder, and
  # the next gap may declare at once. A Cell pulling its baseline heals only
  # once its log reaches the highest seq the episode lost, nothing kept left.
  @spec close_gap(State.t()) :: State.t()
  def close_gap(%{gap_open?: false} = state), do: state

  def close_gap(%{awaiting_baseline?: true, kept: {_bytes, kept, lost}} = state)
      when state.last_persisted_seq < lost or kept != [],
      do: state

  def close_gap(state) do
    Facts.declare({:seq_gap_closed, state.cell_id, %{}})
    %{state | gap_open?: false, awaiting_baseline?: false, kept: {0, [], -1}}
  end

  # Drop the Change without writing; the resulting seq gap is declared so the
  # loop drives the resync handshake and the SDK (authoritative) re-sends. The
  # shed episode latches on the false->true transition, so a sustained flood
  # declares once; `append_async` clears it on any successful append.
  def shed(state, seq, runner_id) do
    Metrics.add(state.metrics, :wal_shed_total, 1)

    if state.shedding? do
      state
    else
      report_credit_shed(state, runner_id)
      emit_seq_gap(state.cell_id, state.last_persisted_seq + 1, seq)
      %{open_gap(state, runner_id, :shed) | shedding?: true}
    end
  end

  # A conformant credit-speaking sender can never reach the hard bound (the
  # stream ledger refuses the overrun first): a shed here is a protocol
  # anomaly, not back-pressure.
  defp report_credit_shed(%{credit_active?: true, cell_id: {rc, sh, pk}}, runner_id) do
    :telemetry.execute(
      [:flezha, :state, :credit_anomaly],
      %{count: 1},
      %{kind: :shed, relay: rc, tasklet: sh, key: pk, runner: runner_id}
    )
  end

  defp report_credit_shed(_state, _runner_id), do: :ok

  # ---------- Backpressure ----------

  def maybe_signal_backpressure(%{backpressured?: false} = state, pending)
      when pending >= state.limits.high_watermark do
    emit_backpressure(state, :full, pending)
    %{state | backpressured?: true}
  end

  def maybe_signal_backpressure(%{backpressured?: true} = state, pending)
      when pending <= state.limits.low_watermark do
    emit_backpressure(state, :open, pending)
    %{state | backpressured?: false}
  end

  def maybe_signal_backpressure(state, _), do: state

  # Emit the telemetry event AND ship the wire signal. The telemetry
  # event is for in-process observers (metrics, debug); the wire signal
  # routes through the producing Tasklet's `OverflowPolicy` machinery on
  # the SDK side. A credit-speaking session gets no wire signal: its
  # back-pressure is carried entirely by the credit ledger (the sender parks
  # on window exhaustion), so only the telemetry event is emitted.
  defp emit_backpressure(state, status, pending) do
    :telemetry.execute(
      [:flezha, :state, :wal, :backpressure],
      %{pending_bytes: pending},
      %{cell_id: state.cell_id, status: status}
    )

    # Routing needs the owning runner (`nil` until the first Change lands,
    # in which case there is no session to signal).
    with false <- state.credit_active?,
         {rc, sh, pk} <- state.cell_id,
         runner_id when is_binary(runner_id) <- state.owning_runner do
      _ =
        Commands.notify_cell_backpressure(
          {runner_id, rc, sh, pk},
          status
        )
    end

    :ok
  end

  # ---------- Drain attribution ----------

  # Accumulate a written record's wire bytes against the stream channel that
  # handed it over; reported back, naming this Cell, on the next durable drain
  # (fsync/truncate). The channel frees a credit session's debit with it and
  # confirms processed-prefix markers against it, so it is reported whether
  # or not the stream speaks credit (`session` is then `nil`).
  def track_credit(state, opts),
    do: %{state | credit_pending: add_credit(state.credit_pending, opts)}

  # `credit`, a `credit_pending` map, with one record's wire bytes added.
  def add_credit(credit, %{channel: ch, session: session, wire_size: n})
      when is_pid(ch) and n > 0,
      do: Map.update(credit, {ch, session}, n, &(&1 + n))

  def add_credit(credit, _), do: credit

  # Everything appended is durable: answer the batched appends, oldest first,
  # and every caller waiting on a record the sync covered.
  def reply_synced(state) do
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

  # A Change that will never occupy the un-fsynced buffer (dedup drop, gap
  # rejection, shed): it is drained at once; one never written, released.
  def release_credit(state, opts, tag \\ :credit_drained),
    do: drain_credit(state.cell_id, add_credit(%{}, opts), tag)

  def flush_credit_pending(%{credit_pending: cp} = state, _tag) when map_size(cp) == 0, do: state

  def flush_credit_pending(state, tag) do
    drain_credit(state.cell_id, state.credit_pending, tag)
    %{state | credit_pending: %{}}
  end

  # Report `credit`, a `credit_pending` map, to each stream it names: drained
  # frees the debit and counts toward the stream's processed-prefix markers;
  # released frees the debit only, for bytes no drain will ever make durable
  # where they must be.
  def drain_credit(cell_id, credit, tag \\ :credit_drained)
      when tag in [:credit_drained, :credit_released] do
    Enum.each(credit, fn {{ch, session}, bytes} -> send(ch, {tag, session, bytes, cell_id}) end)
  end

  def emit_seq_gap({rc, sh, pk}, expected, got) do
    :telemetry.execute(
      [:flezha, :state, :seq_gap],
      %{expected: expected, got: got},
      %{relay: rc, tasklet: sh, key: pk}
    )
  end

  def fsync_lag_ms(nil, _now_us), do: 0
  def fsync_lag_ms(pending_since_us, now_us), do: div(max(now_us - pending_since_us, 0), 1000)

  def record_latency_sample(_metrics, reservoir, sample_ms) do
    capped =
      if :queue.len(reservoir) >= @reservoir_capacity do
        {_, q} = :queue.out(reservoir)
        q
      else
        reservoir
      end

    new_reservoir = :queue.in(sample_ms, capped)
    {p99(new_reservoir), new_reservoir}
  end

  defp p99(reservoir) do
    list = :queue.to_list(reservoir)

    case length(list) do
      0 ->
        0

      n ->
        sorted = Enum.sort(list)
        idx = max(0, ceil(n * 0.99) - 1)
        Enum.at(sorted, idx, 0)
    end
  end

  # Resolved once at `init/1`. The hard bound sits strictly above the soft
  # watermark (default 4x) so cooperative back-pressure takes effect before
  # anything is shed; the mailbox-depth cap backstops an un-credited stream,
  # where a cast flood grows the mailbox even when each record is small;
  # `outbox_inflight_max` is how many effect records may be inside the plugin
  # chain unconfirmed before the flush stops handing over more (the durable
  # entries keep queueing behind it, only the flush waits); and
  # `outbox_resettle_ms` is how long a dispatched entry may go without its chain
  # run reporting back before it is re-dispatched — long enough that a merely
  # backlogged Overwatch is not duplicated into, and a re-dispatch is safe
  # either way. `kept_cap` bounds the credit the Changes kept for a pulled
  # baseline hold at a sixteenth of the soft watermark, the default size of the
  # runner's credit window; a pull the exhausted window refuses releases them
  # (`Epoch.pull_refused/2`).
  @spec limits() :: State.limits()
  def limits() do
    high = PersistenceResolver.wal_high_watermark_bytes()

    %{
      high_watermark: high,
      kept_cap: div(high, 16),
      low_watermark: Config.get(:wal_low_watermark_bytes),
      hard_watermark: Config.get(:wal_hard_watermark_bytes),
      pending_msg_cap: Config.get(:wal_pending_msg_cap),
      outbox_inflight_max: Config.get(:outbox_inflight_max),
      outbox_resettle_ms: Durations.get(:outbox_resettle_ms)
    }
  end
end
