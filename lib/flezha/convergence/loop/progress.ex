defmodule Flezha.Convergence.Loop.Progress do
  @moduledoc """
  The tick's producing-liveness pass: per-Cell fed/advance evidence from the
  `Watchdog` and `InputLog`, rotated against a grace-window baseline and handed
  to `StallClassifier.classify_producing/3`.

  Report-only — healing stays on the output-stall ladder. The input-log read is
  per RELAY, not per Cell, so a relay with 200 Cells costs one
  lookup per tick. A failure here returns the state untouched.
  """

  alias Flezha.Convergence.Loop.State
  alias Flezha.Convergence.StallClassifier
  alias Flezha.State.{InputLog, Watchdog}

  require Logger

  def classify(%State{} = state, now) do
    markers = progress_markers()

    {baseline, history} =
      rotate_progress_history(
        state.progress_history,
        Enum.filter(markers, &tier_cursors?/1),
        now,
        state.fact_grace_ms
      )

    verdicts =
      StallClassifier.classify_producing(markers, baseline, %{
        now: now,
        grace_ms: state.fact_grace_ms
      })

    stuck =
      for {id, {:stuck, detail, ctx}} <- verdicts, into: %{} do
        {sh, pk} = ctx.cell
        {id, %{detail: detail, cell: "#{sh}/#{pk}"}}
      end

    note_stuck_transitions(state.stuck_relays, stuck)
    %{state | progress_history: history, stuck_relays: stuck}
  rescue
    e ->
      Logger.warning("[convergence] producing-liveness skipped: #{Exception.message(e)}")
      state
  catch
    _, _ -> state
  end

  # Per-Cell fed/advance evidence from the existing Watchdog/WalWriter state,
  # plus the tier cursors when the relay runs the effectively-once tier. The
  # input-log read is per RELAY, not per Cell — a relay with 200
  # Cells costs one lookup per tick, not 200. A tier-off relay yields no
  # cursor fields, and classification degrades to WAL-seq-only.
  @doc false
  def progress_markers() do
    Watchdog.cell_statuses()
    |> Enum.group_by(fn {{rc, _sh, _pk}, _s} -> rc end)
    |> Enum.flat_map(fn {rc, statuses} ->
      cursors = InputLog.cursors(rc)

      Enum.map(statuses, fn {{_rc, sh, pk} = cell_id, s} ->
        %{
          relay_id: rc,
          cell: {sh, pk},
          last_change_seen_at: s.last_change_seen_at,
          last_advance_at: s.last_advance_at,
          out_pending_seq: s.out_pending_seq,
          outbox_flushed_seq: s.outbox_flushed_seq,
          outbox_unconfirmed_ms: s.outbox_unconfirmed_ms
        }
        |> merge_input_cursors(cursors, cell_id)
      end)
    end)
  end

  # A Cell that has never reported a consumed offset has no entry, so no key is
  # merged and the watermark guard stays false — a Cell with no durable progress
  # has nothing to freeze.
  defp merge_input_cursors(marker, nil, _cell_id), do: marker

  defp merge_input_cursors(marker, %{tail: tail, consumed: consumed}, cell_id) do
    marker = Map.put(marker, :input_log_tail, tail)

    case Map.fetch(consumed, cell_id) do
      {:ok, offset} -> Map.put(marker, :consumed_offset, offset)
      :error -> marker
    end
  end

  defp tier_cursors?(m),
    do: is_integer(m[:consumed_offset]) or is_integer(m[:outbox_flushed_seq])

  # Keep the newest snapshot at least one grace window old as the
  # cursor-delta baseline (plus everything younger); nil until one has aged
  # enough, so cursor checks stay off during the first grace after boot.
  @doc false
  def rotate_progress_history(history, snapshot, now, grace_ms) do
    {aged, young} =
      Enum.split_with(history ++ [{now, snapshot}], fn {at, _} -> now - at >= grace_ms end)

    case List.last(aged) do
      nil -> {nil, young}
      {_at, baseline} = entry -> {baseline, [entry | young]}
    end
  end

  # Telemetry + user log on stuck-set transitions only; steady state is
  # visible in /healthz, not re-emitted every tick.
  @doc false
  def note_stuck_transitions(same, same), do: :ok

  def note_stuck_transitions(prev, new) do
    fresh = Map.drop(new, Map.keys(prev))
    cleared = Map.keys(prev) -- Map.keys(new)

    Enum.each(fresh, fn {id, %{detail: detail, cell: cell}} ->
      :telemetry.execute(
        [:flezha, :convergence, :relay_stuck],
        %{count: 1},
        %{relay_id: id, detail: detail, cell: cell}
      )
    end)

    if cleared != [], do: Logger.info("[convergence] stuck cleared for #{inspect(cleared)}")
    :ok
  end

  # Durable evidence of prior intent (state dirs + durable-registry rows)
  # contradicting the live intended set — the wiped-registry witness.
  # Report-only; a user resumes the relay or deletes its state
  # (`flz state delete` / `flz reset`), which clears the entry.
end
