defmodule Flezha.State.WalWriter.Admission do
  @moduledoc """
  What the writer lets in, and what it tells the sender about it.

  Four decisions over one writer struct: the hard in-memory bound and the shed
  that answers it, the soft-watermark back-pressure signal, the per-session
  credit attribution a durable drain releases, and the bounds themselves,
  resolved once at boot. A shed is the last resort — a conformant
  credit-speaking sender can never reach it, so one is a protocol anomaly
  rather than back-pressure.
  """

  alias Flezha.Config

  alias Flezha.Config.Durations
  alias Flezha.Convergence.Facts
  alias Flezha.State.{Commands, Metrics, PersistenceResolver}
  alias Flezha.State.WalWriter.State

  @reservoir_capacity 1024

  # ---------- Hard bound (shed) ----------

  # Last-resort safety net above the soft watermark: cooperative SDKs throttle on
  # `EntityMailboxStatus` first; only a slow disk or a non-cooperating SDK
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
  # `{:seq_gap, entity_id}` to the convergence loop, which plans the resync on
  # its own table; every further gap until the next contiguous append rides
  # the open episode. Shared by the three gap-driven triggers — a gap, a shed,
  # and a cold-takeover baseline pull — so a sustained flood declares once.
  @spec open_gap(State.t(), binary(), atom()) :: State.t()
  def open_gap(%{gap_open?: true} = state, _runner_id, _trigger), do: state

  def open_gap(state, runner_id, trigger) do
    Facts.declare({:seq_gap, state.entity_id, %{runner_id: runner_id, trigger: trigger}})
    %{state | gap_open?: true}
  end

  # A contiguous append, or the pulled baseline, is the evidence the gap
  # healed: the episode ends, its counter-fact releases the loop's ladder, and
  # the next gap may declare at once.
  @spec close_gap(State.t()) :: State.t()
  def close_gap(%{gap_open?: false} = state), do: state

  def close_gap(state) do
    Facts.declare({:seq_gap_closed, state.entity_id, %{}})
    %{state | gap_open?: false}
  end

  # Drop the Change without writing; the resulting seq gap is declared so the
  # loop drives the resync handshake and the SDK (authoritative) re-sends. The
  # shed episode latches on the false->true transition, so a sustained flood
  # declares once; `append_async` clears it on any successful append.
  def shed(state, seq, runner_id) do
    Metrics.add(state.metrics, :wal_shed_total, 1)
    {rc, sh, pk} = state.entity_id

    # A conformant credit-speaking sender can never reach the hard bound
    # (the stream ledger refuses the overrun first) — a shed here is a
    # protocol anomaly, not back-pressure.
    if state.credit_active? do
      :telemetry.execute(
        [:flezha, :state, :credit_anomaly],
        %{count: 1},
        %{kind: :shed, operative: rc, procedure: sh, key: pk, runner: runner_id}
      )
    end

    if state.shedding? do
      state
    else
      emit_seq_gap(state.entity_id, state.last_persisted_seq + 1, seq)
      %{open_gap(state, runner_id, :shed) | shedding?: true}
    end
  end

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
  # routes through the producing Procedure's `OverflowPolicy` machinery on
  # the SDK side. A credit-speaking session gets no wire signal: its
  # back-pressure is carried entirely by the credit ledger (the sender parks
  # on window exhaustion), so only the telemetry event is emitted.
  defp emit_backpressure(state, status, pending) do
    :telemetry.execute(
      [:flezha, :state, :wal, :backpressure],
      %{pending_bytes: pending},
      %{entity_id: state.entity_id, status: status}
    )

    # Routing needs the owning runner (`nil` until the first Change lands,
    # in which case there is no session to signal).
    with false <- state.credit_active?,
         {rc, sh, pk} <- state.entity_id,
         runner_id when is_binary(runner_id) <- state.owning_runner do
      _ =
        Commands.notify_entity_backpressure(
          {runner_id, rc, sh, pk},
          status
        )
    end

    :ok
  end

  # ---------- Credit-ledger attribution ----------

  # Accumulate a written record's wire bytes against the stream channel that
  # debited them; reported back on the next durable drain (fsync/truncate).
  def track_credit(state, %{channel: ch, session: session, wire_size: n})
      when is_pid(ch) and n > 0 do
    %{state | credit_pending: Map.update(state.credit_pending, {ch, session}, n, &(&1 + n))}
  end

  def track_credit(state, _), do: state

  # A Change that will never occupy the un-fsynced buffer (dedup drop, gap
  # rejection, shed): its debit is released immediately.
  def release_credit(%{channel: ch, session: session, wire_size: n})
      when is_pid(ch) and n > 0 do
    send(ch, {:credit_drained, session, n})
    :ok
  end

  def release_credit(_), do: :ok

  def flush_credit_pending(%{credit_pending: cp} = state) when map_size(cp) == 0, do: state

  def flush_credit_pending(state) do
    Enum.each(state.credit_pending, fn {{ch, session}, bytes} ->
      send(ch, {:credit_drained, session, bytes})
    end)

    %{state | credit_pending: %{}}
  end

  def emit_seq_gap({rc, sh, pk}, expected, got) do
    :telemetry.execute(
      [:flezha, :state, :seq_gap],
      %{expected: expected, got: got},
      %{operative: rc, procedure: sh, key: pk}
    )
  end

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
  # either way.
  @spec limits() :: State.limits()
  def limits() do
    high = PersistenceResolver.wal_high_watermark_bytes()

    %{
      high_watermark: high,
      low_watermark: Config.get(:wal_low_watermark_bytes),
      hard_watermark: Config.get(:wal_hard_watermark_bytes),
      pending_msg_cap: Config.get(:wal_pending_msg_cap),
      outbox_inflight_max: Config.get(:outbox_inflight_max),
      outbox_resettle_ms: Durations.get(:outbox_resettle_ms)
    }
  end
end
