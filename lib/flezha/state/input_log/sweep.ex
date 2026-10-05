defmodule Flezha.State.InputLog.Sweep do
  @moduledoc """
  Reclaiming an instance's durable input log, and saying so when it cannot be.

  The floor — not the watermark — decides whether there is work: under a hold
  window it advances with the clock even while every reporter's consumed-offset
  stands still, so gating on the watermark would leave a fully-consumed,
  fully-aged-out log unreclaimed for ever. The truncate itself is a cast; the
  sweeper is housekeeping and must never die because a log is busy.

  A log that grows past the size threshold while its watermark stands still is
  not silently accumulating: the frozen-watermark event names the reporter (an
  flow Cell, or a steps relay's journal) that is holding it. Two memos are kept
  per instance, because they answer two questions — the floor "is there
  anything left to reclaim?", the watermark "is a reporter refusing to
  consume?".
  """

  alias Flezha.State.{InputLog, Paths, PersistenceResolver, Supervisor}

  @doc """
  Sweep one instance: truncate its input log strictly below
  `min(watermark, now - hold_window)`, and below the lowest record a live call
  still holds (`Flezha.State.InputLog.CallRetention`), and emit the frozen-watermark warning
  when the log grows past the size threshold without the watermark advancing.

  Only a log open on this node is swept, and the sweep never opens one: the log
  of a relay that moved away stays closed, so its return can set the stale
  copy aside (`Flezha.State.Adoption`) with nothing writing into it.
  """
  @spec run(binary()) :: :ok
  def run(relay_class) do
    case Supervisor.lookup({relay_class, :input_log}) do
      {:ok, pid} -> do_sweep(relay_class, pid)
      :error -> :ok
    end
  end

  @doc "Every state key with an input log on disk (for the retention sweep)."
  @spec all_classes() :: [binary()]
  def all_classes() do
    Enum.filter(Paths.relay_classes(), &File.regular?(Paths.input_log_path(&1)))
  end

  defp do_sweep(relay_class, pid) do
    watermark = InputLog.watermark(relay_class)
    floor = relay_class |> truncation_floor(watermark) |> below_live_calls(relay_class)

    # The *floor* decides whether there is work, not the watermark: under a hold
    # window the floor advances with the clock even while every reporter's
    # consumed-offset stands still, and gating on the watermark would mean a
    # fully-consumed, fully-aged-out log is never reclaimed.
    advanced? = floor != last_sweep_floor(relay_class)

    if is_integer(floor) and floor > 0 and advanced? do
      # A cast: the sweeper is housekeeping and must never die because a log is
      # busy. The log truncates in its turn; the floor remembered below is what
      # the next sweep compares against.
      GenServer.cast(pid, {:truncate_below, floor})
    else
      maybe_warn_frozen(relay_class, Paths.input_log_path(relay_class), watermark)
    end

    remember_sweep(relay_class, watermark, floor)
    :ok
  end

  defp last_sweep_floor(relay_class), do: last_sweep(relay_class, :last_sweep_floor)

  defp last_sweep_watermark(relay_class),
    do: last_sweep(relay_class, :last_sweep_watermark)

  defp last_sweep(relay_class, key) do
    InputLog.ensure_watermark_table()

    case :ets.lookup(InputLog.watermark_table(), {relay_class, key}) do
      [{_, value}] -> value
      [] -> :none
    end
  end

  # A record carrying a live call is kept whatever the watermark allows.
  defp below_live_calls(nil, _relay_class), do: nil

  defp below_live_calls(floor, relay_class) do
    case InputLog.call_floor(relay_class) do
      nil -> floor
      held -> min(floor, held)
    end
  end

  # `min(watermark, now - hold_window)` in offset terms — a consumed record is
  # only reclaimable once it has also aged out of the hold window, because a
  # correction replays consumed inputs.
  #
  # With the default `hold_window = 0` the time bound is "now", which every
  # retained record predates, so the watermark alone decides. A raised window
  # holds the tail: `nil` from the scan means nothing has aged out yet, which is
  # "hold everything", and it clears itself: the first record to age out
  # releases the tail.
  defp truncation_floor(_relay_class, nil), do: nil

  defp truncation_floor(relay_class, watermark) do
    case InputLog.hold_window_ms(relay_class) do
      0 ->
        watermark

      hold_window ->
        case InputLog.offset_before_time(
               relay_class,
               System.system_time(:millisecond) - hold_window
             ) do
          aged_out when is_integer(aged_out) -> min(watermark, aged_out)
          # No record is that old, or the log refused one: either way there is
          # no floor to truncate to, which is the "hold everything" answer.
          _none_or_refused -> nil
        end
    end
  end

  defp frozen_size_threshold(), do: PersistenceResolver.wal_high_watermark_bytes()

  defp maybe_warn_frozen(relay_class, path, watermark) do
    size =
      case File.stat(path) do
        {:ok, %{size: size}} -> size
        {:error, _} -> 0
      end

    frozen? = last_sweep_watermark(relay_class) == watermark

    if frozen? and size >= frozen_size_threshold() do
      :telemetry.execute(
        [:flezha, :state, :input_log_frozen_watermark],
        %{size_bytes: size},
        %{
          relay: relay_class,
          watermark: watermark,
          lagging_cell: lagging_cell(relay_class)
        }
      )
    end

    :ok
  end

  # Two memos, two questions: the floor answers "is there anything left to
  # reclaim?" (it moves with the clock under a hold window); the watermark
  # answers "is a reporter refusing to consume?" (the frozen-watermark warning).
  defp remember_sweep(relay_class, watermark, floor) do
    InputLog.ensure_watermark_table()
    :ets.insert(InputLog.watermark_table(), {{relay_class, :last_sweep_floor}, floor})
    :ets.insert(InputLog.watermark_table(), {{relay_class, :last_sweep_watermark}, watermark})
  end

  defp lagging_cell(relay_class) do
    case InputLog.cell_offsets(relay_class) do
      [] -> nil
      offsets -> offsets |> Enum.min_by(&elem(&1, 1)) |> elem(0)
    end
  end
end
