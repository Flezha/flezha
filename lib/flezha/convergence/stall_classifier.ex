defmodule Flezha.Convergence.StallClassifier do
  @moduledoc """
  Pure per-layer classifier for a silent data-plane stall.

  Given a current and previous liveness snapshot (driver self-report +
  `Flezha.Transport.LivenessCounters`), it walks the controller→executor path
  top-down and returns the single most-fundamental stalled layer, or `:ok`.
  Detection only — it changes no behaviour and triggers no remediation; the
  convergence Loop emits its verdict as a `[:flezha, :data_plane,
  :stall_localized]` event so the *next* silent freeze self-localises instead
  of needing a live post-mortem.

  Verdicts, in priority order (a lower layer being wedged explains every layer
  above it, so the deepest wins):

    * `:driver_loop_frozen`     — the flz-uds-driver thread stopped looping
    * `:nif_queue_backing_up`   — commands enqueued but the driver isn't draining them
    * `:socket_write_blocked`   — driver writing into a buffer the peer won't drain
    * `:executor_not_consuming` — controller sending fine, executor reads nothing back
    * `:executor_pool_wedged`   — executor alive (heartbeats flowing) and fed, but its
      self-reported dispatch counter is frozen — the SDK pool stopped processing
    * `:channel_not_sending`    — channel fed but not calling send_frame
    * `:operative_not_forwarding`   — operative fed but not forwarding to its channel

  Each check has a built-in "is it actually being fed?" precondition (a count
  that must be *advancing* between ticks), so an idle operative never reads as a
  stall.
  """

  @type snapshot :: %{drivers: [map()], channels: [map()], operatives: [map()]}
  @type verdict :: {atom(), map()} | :ok

  @defaults %{driver_freeze_ms: 5_000, stall_ms: 30_000, queue_high: 256}

  @doc """
  Classify the stall, if any. `previous` is `nil` on the first tick (no deltas
  yet → always `:ok`). `opts` overrides `#{inspect(@defaults)}`.
  """
  @spec classify(snapshot(), snapshot() | nil, map()) :: verdict()
  def classify(current, previous, opts \\ %{})

  def classify(_current, nil, _opts), do: :ok

  def classify(current, previous, opts) do
    o = Map.merge(@defaults, opts)

    driver_loop_frozen(current, previous, o) ||
      nif_queue_backing_up(current, previous, o) ||
      socket_write_blocked(current, previous, o) ||
      executor_not_consuming(current, previous, o) ||
      executor_pool_wedged(current, previous) ||
      channel_not_sending(current, previous) ||
      operative_not_forwarding(current, previous) ||
      :ok
  end

  # ---------- driver-global checks ----------

  defp driver_loop_frozen(current, previous, o) do
    find_driver(current, previous, fn d, prev ->
      age = age(d.now_ms, d.last_loop_ms)

      if d.loop_iterations == prev.loop_iterations and age > o.driver_freeze_ms do
        {:driver_loop_frozen, %{transport: d.transport, loop_age_ms: age}}
      end
    end)
  end

  defp nif_queue_backing_up(current, previous, o) do
    find_driver(current, previous, fn d, prev ->
      written = total_written(d.streams)
      prev_written = total_written(prev.streams)

      if d.cmd_queue_depth > o.queue_high and d.cmd_queue_depth > prev.cmd_queue_depth and
           written == prev_written do
        {:nif_queue_backing_up, %{transport: d.transport, cmd_queue_depth: d.cmd_queue_depth}}
      end
    end)
  end

  # ---------- driver per-stream checks ----------

  defp socket_write_blocked(current, previous, o) do
    find_stream(current, previous, fn d, s, prev_s ->
      if s.write_buffer_len > 0 and s.write_buffer_len > prev_s.write_buffer_len and
           s.frames_written > prev_s.frames_written and
           age(d.now_ms, s.last_write_ms) > o.stall_ms do
        {:socket_write_blocked,
         %{transport: d.transport, stream_id: s.stream_id, write_buffer_len: s.write_buffer_len}}
      end
    end)
  end

  defp executor_not_consuming(current, previous, o) do
    find_stream(current, previous, fn d, s, prev_s ->
      # `prev_s.frames_read > 0` exempts one-directional streams: a primary
      # data stream is write-only on the controller side (the executor never
      # answers on it), so "reads flat" is its normal state, not a stall.
      if s.frames_written > prev_s.frames_written and s.frames_read == prev_s.frames_read and
           prev_s.frames_read > 0 and
           age(d.now_ms, s.last_read_ms) > o.stall_ms do
        {:executor_not_consuming, %{transport: d.transport, stream_id: s.stream_id}}
      end
    end)
  end

  # ---------- executor self-report check ----------

  # The executor's heartbeat thread outlives a wedged dispatch pool, so an
  # executor can look perfectly alive while processing nothing (the 2026-06-10
  # freeze). Heartbeat metadata carries a dispatch-progress counter; flag the
  # executor when it is being fed and heartbeating but that counter is frozen.
  defp executor_pool_wedged(current, previous) do
    prev_by_exec = group_channels_by_executor(previous.channels)

    current.channels
    |> group_channels_by_executor()
    |> find_first(fn {executor_id, cur} ->
      case Map.get(prev_by_exec, executor_id) do
        nil ->
          nil

        prev ->
          if cur.send_frame_calls > prev.send_frame_calls and cur.hb_count > prev.hb_count and
               cur.hb_dispatches > 0 and cur.hb_dispatches == prev.hb_dispatches do
            {:executor_pool_wedged, %{executor_id: executor_id, dispatches: cur.hb_dispatches}}
          end
      end
    end)
  end

  # Aggregate per-channel counters to one row per executor: sends and
  # heartbeats sum across its channels; the self-reported dispatch counter is
  # process-global on the executor, so max() collapses duplicates.
  defp group_channels_by_executor(channels) do
    Enum.reduce(channels, %{}, fn c, acc ->
      row = %{
        send_frame_calls: Map.get(c, :send_frame_calls, 0),
        hb_count: Map.get(c, :hb_count, 0),
        hb_dispatches: Map.get(c, :hb_dispatches, 0)
      }

      Map.update(acc, c.executor_id, row, fn v ->
        %{
          send_frame_calls: v.send_frame_calls + row.send_frame_calls,
          hb_count: v.hb_count + row.hb_count,
          hb_dispatches: max(v.hb_dispatches, row.hb_dispatches)
        }
      end)
    end)
  end

  # ---------- BEAM checks ----------

  defp channel_not_sending(current, previous) do
    prev_index = index_by(previous.channels, fn c -> {c.executor_id, c.name} end)

    find_first(current.channels, fn c ->
      case Map.get(prev_index, {c.executor_id, c.name}) do
        nil ->
          nil

        prev ->
          # Fed but not sending, and not merely an unbound channel dropping
          # (nil_drops flat) — that is a binding problem, not a send wedge.
          if c.io_in_received > prev.io_in_received and
               c.send_frame_calls == prev.send_frame_calls and
               c.nil_drops == prev.nil_drops do
            {:channel_not_sending, %{executor_id: c.executor_id, channel: c.name}}
          end
      end
    end)
  end

  defp operative_not_forwarding(current, previous) do
    prev_index = index_by(previous.operatives, fn r -> r.operative_id end)

    find_first(current.operatives, fn r ->
      case Map.get(prev_index, r.operative_id) do
        nil ->
          nil

        prev ->
          io_delta = r.io_in_received - prev.io_in_received
          filtered_delta = r.filtered_nil - prev.filtered_nil

          # Fed, not forwarding, and the gap isn't explained by the on_input
          # filter dropping everything.
          if io_delta > 0 and r.forwarded == prev.forwarded and filtered_delta < io_delta do
            {:operative_not_forwarding, %{operative_id: r.operative_id}}
          end
      end
    end)
  end

  # ---------- helpers ----------

  defp find_driver(current, previous, fun) do
    prev_index = index_by(previous.drivers, fn d -> d.transport end)

    find_first(current.drivers, fn d ->
      case Map.get(prev_index, d.transport) do
        nil -> nil
        prev -> fun.(d, prev)
      end
    end)
  end

  defp find_stream(current, previous, fun) do
    prev_index = index_by(previous.drivers, fn d -> d.transport end)

    find_first(current.drivers, fn d ->
      case Map.get(prev_index, d.transport) do
        nil ->
          nil

        prev ->
          prev_streams = index_by(prev.streams, fn s -> s.stream_id end)

          find_first(d.streams, fn s ->
            case Map.get(prev_streams, s.stream_id) do
              nil -> nil
              prev_s -> fun.(d, s, prev_s)
            end
          end)
      end
    end)
  end

  defp find_first(list, fun) do
    Enum.find_value(list, fun)
  end

  defp index_by(list, key_fun) do
    Map.new(list, fn item -> {key_fun.(item), item} end)
  end

  defp total_written(streams) do
    Enum.reduce(streams, 0, fn s, acc -> acc + s.frames_written end)
  end

  # Driver-elapsed age; `last_ms == 0` means "never", reported as a large age.
  defp age(_now_ms, 0), do: :infinity
  defp age(now_ms, last_ms), do: max(now_ms - last_ms, 0)
end
