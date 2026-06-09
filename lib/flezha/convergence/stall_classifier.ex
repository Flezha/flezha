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
    * `:channel_not_sending`    — channel fed but not calling send_frame
    * `:relay_not_forwarding`   — relay fed but not forwarding to its channel

  Each check has a built-in "is it actually being fed?" precondition (a count
  that must be *advancing* between ticks), so an idle relay never reads as a
  stall.
  """

  @type snapshot :: %{drivers: [map()], channels: [map()], relays: [map()]}
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
      channel_not_sending(current, previous) ||
      relay_not_forwarding(current, previous) ||
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
      if s.frames_written > prev_s.frames_written and s.frames_read == prev_s.frames_read and
           age(d.now_ms, s.last_read_ms) > o.stall_ms do
        {:executor_not_consuming, %{transport: d.transport, stream_id: s.stream_id}}
      end
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

  defp relay_not_forwarding(current, previous) do
    prev_index = index_by(previous.relays, fn r -> r.relay_id end)

    find_first(current.relays, fn r ->
      case Map.get(prev_index, r.relay_id) do
        nil ->
          nil

        prev ->
          io_delta = r.io_in_received - prev.io_in_received
          filtered_delta = r.filtered_nil - prev.filtered_nil

          # Fed, not forwarding, and the gap isn't explained by the on_input
          # filter dropping everything.
          if io_delta > 0 and r.forwarded == prev.forwarded and filtered_delta < io_delta do
            {:relay_not_forwarding, %{relay_id: r.relay_id}}
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
