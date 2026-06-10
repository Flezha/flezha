defmodule Flezha.Convergence.StallClassifierTest do
  @moduledoc """
  Unit coverage for the pure per-layer stall classifier. Each test feeds a
  crafted (previous, current) snapshot pair that exhibits exactly one layer's
  stall signature and asserts the matching verdict, plus the idle/no-prev
  cases that must stay `:ok`.
  """

  use ExUnit.Case, async: true

  alias Flezha.Convergence.StallClassifier

  @now 1_000_000

  test "no previous snapshot → :ok (first tick has no deltas)" do
    assert StallClassifier.classify(snapshot(), nil) == :ok
  end

  test "idle data plane (nothing advancing) → :ok" do
    prev = snapshot(drivers: [driver(streams: [stream()])])
    curr = snapshot(drivers: [driver(streams: [stream()])])
    assert StallClassifier.classify(curr, prev) == :ok
  end

  test "driver_loop_frozen: loop counter frozen and heartbeat stale" do
    prev = snapshot(drivers: [driver(loop_iterations: 500)])
    curr = snapshot(drivers: [driver(loop_iterations: 500, last_loop_ms: 0)])

    assert {:driver_loop_frozen, ctx} = StallClassifier.classify(curr, prev)
    assert ctx.transport == :uds
    assert ctx.loop_age_ms > 5_000
  end

  test "nif_queue_backing_up: queue high and rising, no frames drained" do
    prev =
      snapshot(
        drivers: [
          driver(loop_iterations: 1, cmd_queue_depth: 300, streams: [stream(frames_written: 10)])
        ]
      )

    curr =
      snapshot(
        drivers: [
          driver(loop_iterations: 2, cmd_queue_depth: 900, streams: [stream(frames_written: 10)])
        ]
      )

    assert {:nif_queue_backing_up, ctx} = StallClassifier.classify(curr, prev)
    assert ctx.cmd_queue_depth == 900
  end

  test "socket_write_blocked: buffer rising, frames submitted, writes stale" do
    prev =
      snapshot(
        drivers: [
          driver(loop_iterations: 1, streams: [stream(write_buffer_len: 100, frames_written: 10)])
        ]
      )

    curr =
      snapshot(
        drivers: [
          driver(
            loop_iterations: 2,
            streams: [stream(write_buffer_len: 5_000, frames_written: 25, last_write_ms: 0)]
          )
        ]
      )

    assert {:socket_write_blocked, ctx} = StallClassifier.classify(curr, prev)
    assert ctx.stream_id == 1
  end

  test "executor_not_consuming: sending fine, nothing read back" do
    prev =
      snapshot(
        drivers: [
          driver(loop_iterations: 1, streams: [stream(frames_written: 10, frames_read: 7)])
        ]
      )

    curr =
      snapshot(
        drivers: [
          driver(
            loop_iterations: 2,
            streams: [stream(frames_written: 40, frames_read: 7, last_read_ms: 0)]
          )
        ]
      )

    assert {:executor_not_consuming, ctx} = StallClassifier.classify(curr, prev)
    assert ctx.stream_id == 1
  end

  test "executor_pool_wedged: fed and heartbeating but dispatch counter frozen" do
    # The 2026-06-10 prod signature: controller sends ~100 frames/s, the
    # executor's heartbeat thread stays alive, but its dispatch pool is
    # deadlocked so the self-reported counter never moves.
    prev =
      snapshot(
        channels: [
          channel(name: :scp, send_frame_calls: 100),
          channel(name: :scs, hb_count: 5, hb_dispatches: 9_000)
        ]
      )

    curr =
      snapshot(
        channels: [
          channel(name: :scp, send_frame_calls: 200),
          channel(name: :scs, hb_count: 6, hb_dispatches: 9_000)
        ]
      )

    assert {:executor_pool_wedged, ctx} = StallClassifier.classify(curr, prev)
    assert ctx.executor_id == "exec-1"
    assert ctx.dispatches == 9_000
  end

  test "executor_pool_wedged does NOT fire while dispatches advance" do
    prev =
      snapshot(
        channels: [
          channel(name: :scp, send_frame_calls: 100),
          channel(name: :scs, hb_count: 5, hb_dispatches: 9_000)
        ]
      )

    curr =
      snapshot(
        channels: [
          channel(name: :scp, send_frame_calls: 200),
          channel(name: :scs, hb_count: 6, hb_dispatches: 9_500)
        ]
      )

    assert StallClassifier.classify(curr, prev) == :ok
  end

  test "executor_pool_wedged does NOT fire for executors that don't self-report (old SDK)" do
    prev =
      snapshot(
        channels: [
          channel(name: :scp, send_frame_calls: 100),
          channel(name: :scs, hb_count: 5, hb_dispatches: 0)
        ]
      )

    curr =
      snapshot(
        channels: [
          channel(name: :scp, send_frame_calls: 200),
          channel(name: :scs, hb_count: 6, hb_dispatches: 0)
        ]
      )

    assert StallClassifier.classify(curr, prev) == :ok
  end

  test "executor_pool_wedged does NOT fire when heartbeats stopped (channel-level stall instead)" do
    prev =
      snapshot(
        channels: [
          channel(name: :scp, send_frame_calls: 100),
          channel(name: :scs, hb_count: 5, hb_dispatches: 9_000)
        ]
      )

    curr =
      snapshot(
        channels: [
          channel(name: :scp, send_frame_calls: 200),
          channel(name: :scs, hb_count: 5, hb_dispatches: 9_000)
        ]
      )

    assert StallClassifier.classify(curr, prev) == :ok
  end

  test "channel_not_sending: channel fed but send_frame flat (stream bound)" do
    prev = snapshot(channels: [channel(io_in_received: 10, send_frame_calls: 10)])
    curr = snapshot(channels: [channel(io_in_received: 30, send_frame_calls: 10)])

    assert {:channel_not_sending, ctx} = StallClassifier.classify(curr, prev)
    assert ctx.executor_id == "exec-1"
  end

  test "channel_not_sending does NOT fire when the channel is merely unbound (nil_drops rising)" do
    prev = snapshot(channels: [channel(io_in_received: 10, send_frame_calls: 10, nil_drops: 10)])
    curr = snapshot(channels: [channel(io_in_received: 30, send_frame_calls: 10, nil_drops: 30)])

    assert StallClassifier.classify(curr, prev) == :ok
  end

  test "relay_not_forwarding: relay fed but forwarded flat (not all filtered)" do
    prev = snapshot(relays: [relay(io_in_received: 10, forwarded: 10, filtered_nil: 0)])
    curr = snapshot(relays: [relay(io_in_received: 30, forwarded: 10, filtered_nil: 2)])

    assert {:relay_not_forwarding, ctx} = StallClassifier.classify(curr, prev)
    assert ctx.relay_id == "relay-1"
  end

  test "relay_not_forwarding does NOT fire when on_input filters everything" do
    prev = snapshot(relays: [relay(io_in_received: 10, forwarded: 0, filtered_nil: 10)])
    curr = snapshot(relays: [relay(io_in_received: 30, forwarded: 0, filtered_nil: 30)])

    assert StallClassifier.classify(curr, prev) == :ok
  end

  test "deepest layer wins: a frozen driver outranks a BEAM-side gap" do
    prev =
      snapshot(
        drivers: [driver(loop_iterations: 5)],
        channels: [channel(io_in_received: 10, send_frame_calls: 10)]
      )

    curr =
      snapshot(
        drivers: [driver(loop_iterations: 5, last_loop_ms: 0)],
        channels: [channel(io_in_received: 30, send_frame_calls: 10)]
      )

    assert {:driver_loop_frozen, _} = StallClassifier.classify(curr, prev)
  end

  # ---------- builders ----------

  defp snapshot(opts \\ []) do
    %{
      drivers: Keyword.get(opts, :drivers, []),
      channels: Keyword.get(opts, :channels, []),
      relays: Keyword.get(opts, :relays, [])
    }
  end

  defp driver(opts) do
    %{
      transport: Keyword.get(opts, :transport, :uds),
      loop_iterations: Keyword.get(opts, :loop_iterations, 0),
      now_ms: @now,
      last_loop_ms: Keyword.get(opts, :last_loop_ms, @now),
      cmd_queue_depth: Keyword.get(opts, :cmd_queue_depth, 0),
      streams: Keyword.get(opts, :streams, [])
    }
  end

  defp stream(opts \\ []) do
    %{
      stream_id: Keyword.get(opts, :stream_id, 1),
      write_buffer_len: Keyword.get(opts, :write_buffer_len, 0),
      frames_written: Keyword.get(opts, :frames_written, 0),
      frames_read: Keyword.get(opts, :frames_read, 0),
      last_write_ms: Keyword.get(opts, :last_write_ms, @now),
      last_read_ms: Keyword.get(opts, :last_read_ms, @now)
    }
  end

  defp channel(opts) do
    %{
      executor_id: Keyword.get(opts, :executor_id, "exec-1"),
      name: Keyword.get(opts, :name, :chan_1),
      io_in_received: Keyword.get(opts, :io_in_received, 0),
      send_frame_calls: Keyword.get(opts, :send_frame_calls, 0),
      nil_drops: Keyword.get(opts, :nil_drops, 0),
      flz_data_received: Keyword.get(opts, :flz_data_received, 0),
      last_send_ms: Keyword.get(opts, :last_send_ms, 0),
      hb_count: Keyword.get(opts, :hb_count, 0),
      hb_dispatches: Keyword.get(opts, :hb_dispatches, 0)
    }
  end

  defp relay(opts) do
    %{
      relay_id: Keyword.get(opts, :relay_id, "relay-1"),
      io_in_received: Keyword.get(opts, :io_in_received, 0),
      forwarded: Keyword.get(opts, :forwarded, 0),
      filtered_nil: Keyword.get(opts, :filtered_nil, 0)
    }
  end
end
