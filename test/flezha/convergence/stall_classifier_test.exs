defmodule Flezha.Convergence.StallClassifierTest do
  @moduledoc """
  Unit coverage for the pure per-layer stall classifier. Each test feeds a
  crafted (previous, current) snapshot pair that exhibits exactly one layer's
  stall signature and asserts the matching verdict, plus the idle/no-prev
  cases that must stay `:ok`.
  """

  use ExUnit.Case, async: true

  alias Flezha.Convergence.StallClassifier
  alias Flezha.Transport.LivenessCounters

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
    curr = snapshot(drivers: [driver(loop_iterations: 500, last_loop_ms: @now - 20_000)])

    assert {:driver_loop_frozen, ctx} = StallClassifier.classify(curr, prev)
    assert ctx.transport == :uds
    assert ctx.loop_age_ms > 5_000
  end

  test "driver_loop_frozen: a driver that has never looped is not frozen" do
    # `last_loop_ms == 0` means "no iteration has completed", whose age is
    # `:infinity` — the signature of a driver registered moments ago, not of
    # one that stopped. Two consecutive polls of it must stay quiet.
    prev = snapshot(drivers: [driver(loop_iterations: 0, last_loop_ms: 0)])
    curr = snapshot(drivers: [driver(loop_iterations: 0, last_loop_ms: 0)])

    assert StallClassifier.classify(curr, prev) == :ok
  end

  test "driver_loop_frozen: a re-created driver at the same key is not frozen" do
    # A reconnect registers a fresh driver under the same transport key; its
    # counters restart. A count that went backwards is a different thread.
    prev = snapshot(drivers: [driver(loop_iterations: 500)])
    curr = snapshot(drivers: [driver(loop_iterations: 3, last_loop_ms: 0)])

    assert StallClassifier.classify(curr, prev) == :ok
  end

  test "driver_loop_frozen can never report an infinite loop age" do
    # The value that identified the false positive is unreachable by
    # construction: an established baseline plus a counter that has not gone
    # backwards is the same driver instance, and every iteration stamps the
    # clock. Sweep the whole (iterations, stamp) space that could produce one.
    for prev_iters <- [0, 1, 500],
        prev_stamp <- [0, @now],
        curr_iters <- [0, 1, 500, 999] do
      prev = snapshot(drivers: [driver(loop_iterations: prev_iters, last_loop_ms: prev_stamp)])
      curr = snapshot(drivers: [driver(loop_iterations: curr_iters, last_loop_ms: 0)])

      refute match?(
               {:driver_loop_frozen, %{loop_age_ms: :infinity}},
               StallClassifier.classify(curr, prev)
             )
    end
  end

  test "nif_queue_backing_up: a driver that has never written is not backed up" do
    # Fleet bring-up: the command queue fills past the high-water mark while
    # the stream set is still empty, so "nothing written since" is vacuous.
    prev = snapshot(drivers: [driver(loop_iterations: 1, cmd_queue_depth: 0, streams: [])])

    curr =
      snapshot(drivers: [driver(loop_iterations: 2, cmd_queue_depth: 808, streams: [stream()])])

    assert StallClassifier.classify(curr, prev) == :ok
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

  test "send_buffer_dropping: a drop counter that moved outranks the block it happened under" do
    prev =
      snapshot(
        drivers: [
          driver(
            loop_iterations: 1,
            streams: [stream(write_buffer_len: 100, frames_written: 10, write_buffer_dropped: 3)]
          )
        ]
      )

    curr =
      snapshot(
        drivers: [
          driver(
            loop_iterations: 2,
            streams: [
              stream(
                write_buffer_len: 5_000,
                frames_written: 25,
                last_write_ms: 0,
                write_buffer_dropped: 9
              )
            ]
          )
        ]
      )

    # The same snapshot pair satisfies `socket_write_blocked`; the loss wins,
    # because a frame the transport discarded is the more specific fact.
    assert {:send_buffer_dropping, ctx} = StallClassifier.classify(curr, prev)
    assert ctx.stream_id == 1
    assert ctx.dropped == 6

    # It mints no heal op: the frame is already gone, and the congestion it
    # reports is what the verdict below heals.
    assert StallClassifier.layer_scope(:send_buffer_dropping) == nil
  end

  test "send_buffer_dropping does NOT fire on a counter that merely stands non-zero" do
    prev =
      snapshot(drivers: [driver(loop_iterations: 1, streams: [stream(write_buffer_dropped: 9)])])

    curr =
      snapshot(drivers: [driver(loop_iterations: 2, streams: [stream(write_buffer_dropped: 9)])])

    assert StallClassifier.classify(curr, prev) == :ok
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

  test "runner_not_consuming: sending fine, nothing read back" do
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

    assert {:runner_not_consuming, ctx} = StallClassifier.classify(curr, prev)
    assert ctx.stream_id == 1
  end

  test "runner_not_consuming does NOT fire on a one-directional (write-only) stream" do
    # A primary data stream never carries runner→controller frames, so
    # frames_read stays 0 forever — that is its normal state, not a stall.
    prev =
      snapshot(
        drivers: [
          driver(
            transport: {:uds_data, "exec-1", :primary},
            loop_iterations: 1,
            streams: [stream(frames_written: 10, frames_read: 0, last_read_ms: 0)]
          )
        ]
      )

    curr =
      snapshot(
        drivers: [
          driver(
            transport: {:uds_data, "exec-1", :primary},
            loop_iterations: 2,
            streams: [stream(frames_written: 500, frames_read: 0, last_read_ms: 0)]
          )
        ]
      )

    assert StallClassifier.classify(curr, prev) == :ok
  end

  # The production freeze signature: the controller keeps sending, the runner's
  # heartbeat thread stays alive, and its dispatch pool is deadlocked. The
  # latest send is always newer than the latest heartbeat, so the frame that
  # proves it is the previous sample's: sent after that sample's heartbeat, and
  # answered by one since.
  test "runner_pool_wedged: fed without pause and heartbeating, dispatch counter frozen" do
    {prev, curr} =
      fed_runner(
        [hb_dispatches: 9_000, last_send_ms: @now - 4_000],
        hb_dispatches: 9_000,
        last_send_ms: @now - 100
      )

    assert {:runner_pool_wedged, ctx} = StallClassifier.classify(curr, prev)
    assert ctx.runner_id == "exec-1"
    assert ctx.dispatches == 9_000
  end

  test "runner_pool_wedged: a frame sent before the window's heartbeat, dispatch counter frozen" do
    {prev, curr} = fed_runner([hb_dispatches: 9_000], hb_dispatches: 9_000)

    assert {:runner_pool_wedged, _ctx} = StallClassifier.classify(curr, prev)
  end

  # A heartbeat reports the count as it stood when the runner sent it, so no
  # heartbeat has counted a frame sent after the latest one.
  test "runner_pool_wedged does NOT fire on a send newer than the latest heartbeat" do
    {prev, curr} =
      fed_runner([hb_dispatches: 9_000], hb_dispatches: 9_000, last_send_ms: @now - 500)

    assert StallClassifier.classify(curr, prev) == :ok
  end

  test "runner_pool_wedged does NOT fire while the block counter climbs (backpressure)" do
    # A producer held on a full cell mailbox freezes `dispatches` exactly as
    # a wedge does — this was byte-for-byte the same picture. The block counter
    # advancing per wait iteration is what separates them: the system is
    # working, the consumer is behind, and a rebind would only move the jam.
    {prev, curr} =
      fed_runner(
        [hb_dispatches: 9_000, hb_blocked_parks: 40],
        hb_dispatches: 9_000,
        hb_blocked_parks: 91
      )

    assert StallClassifier.classify(curr, prev) == :ok
  end

  test "runner_pool_wedged DOES fire when dispatches and blocks are both frozen" do
    {prev, curr} =
      fed_runner(
        [hb_dispatches: 9_000, hb_blocked_parks: 40],
        hb_dispatches: 9_000,
        hb_blocked_parks: 40
      )

    assert {:runner_pool_wedged, _ctx} = StallClassifier.classify(curr, prev)
  end

  test "runner_pool_wedged does NOT fire while dispatches advance" do
    {prev, curr} = fed_runner([hb_dispatches: 9_000], hb_dispatches: 9_500)

    assert StallClassifier.classify(curr, prev) == :ok
  end

  test "runner_pool_wedged does NOT fire for runners that don't self-report (old SDK)" do
    {prev, curr} = fed_runner([hb_dispatches: 0], hb_dispatches: 0)

    assert StallClassifier.classify(curr, prev) == :ok
  end

  test "runner_pool_wedged does NOT fire when heartbeats stopped (channel-level stall instead)" do
    {prev, curr} =
      fed_runner(
        [hb_dispatches: 9_000],
        hb_dispatches: 9_000,
        hb_count: 5,
        last_heartbeat_ms: @now - 8_000
      )

    assert StallClassifier.classify(curr, prev) == :ok
  end

  # A quiet runner: H1 at -6 s, a frame sent at -4 s and handed out at once, H2
  # at -2 s while that handler still runs (out 2 s), so no dispatch completed
  # between the two. The handout moved, so the runner took the frame up.
  test "a heartbeat landing mid-handler is not a wedge (KB-328)" do
    {prev, curr} =
      fed_runner(
        [hb_dispatches: 7, hb_handouts: 11, last_heartbeat_ms: @now - 6_000],
        hb_dispatches: 7,
        hb_handouts: 12,
        hb_inflight: 1,
        hb_inflight_oldest_ms: 2_000,
        last_send_ms: @now - 4_000,
        last_heartbeat_ms: @now - 2_000
      )

    assert StallClassifier.classify(curr, prev) == :ok
  end

  # The frame was handed out, failed and waits out its backoff: nothing is
  # outstanding and the dispatch count moves only at the final attempt.
  test "a retry waiting out its backoff is not a wedge (KB-539)" do
    {prev, curr} =
      fed_runner([hb_dispatches: 5, hb_handouts: 20],
        hb_dispatches: 5,
        hb_handouts: 21,
        hb_inflight: 0
      )

    assert StallClassifier.classify(curr, prev) == :ok
  end

  test "runner_pool_wedged DOES fire when handouts are frozen too, a handout outstanding" do
    {prev, curr} =
      fed_runner([hb_dispatches: 9_000, hb_handouts: 9_100, hb_inflight: 1],
        hb_dispatches: 9_000,
        hb_handouts: 9_100,
        hb_inflight: 1,
        hb_inflight_oldest_ms: 9_000
      )

    assert {:runner_pool_wedged, %{runner_id: "exec-1"}} = StallClassifier.classify(curr, prev)
  end

  test "channel_not_sending: channel fed but send_frame flat (stream bound)" do
    prev = snapshot(channels: [channel(io_in_received: 10, send_frame_calls: 10)])
    curr = snapshot(channels: [channel(io_in_received: 30, send_frame_calls: 10)])

    assert {:channel_not_sending, ctx} = StallClassifier.classify(curr, prev)
    assert ctx.runner_id == "exec-1"
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
        drivers: [driver(loop_iterations: 5, last_loop_ms: @now - 20_000)],
        channels: [channel(io_in_received: 30, send_frame_calls: 10)]
      )

    assert {:driver_loop_frozen, _} = StallClassifier.classify(curr, prev)
  end

  test "every verdict layer declares the scope that decides its consumer" do
    # The scope is what routes a verdict: `:fabric` reaches /healthz as a 503
    # (only a restart clears it), everything else is healed by the pipeline.
    scopes = %{
      driver_loop_frozen: :fabric,
      nif_queue_backing_up: :fabric,
      socket_write_blocked: :connection,
      runner_not_consuming: :runner,
      runner_pool_wedged: :runner,
      channel_not_sending: :connection,
      relay_not_forwarding: :relay
    }

    for {layer, scope} <- scopes do
      assert StallClassifier.layer_scope(layer) == scope
    end

    assert StallClassifier.layer_scope(nil) == nil
    assert StallClassifier.layer_scope(:not_a_verdict) == nil
    assert StallClassifier.layer(:ok) == nil
    assert StallClassifier.layer({:runner_pool_wedged, %{}}) == :runner_pool_wedged
  end

  describe "cell_dispatch_stalled" do
    @stall_opts %{cell_stall_ms: 60_000}

    # One activation the runner took and never answered. `dispatches` keeps
    # climbing on every other Cell, so the runner-wide verdict is blind to
    # it — and per-cell exclusivity means the stalled Cell simply never
    # appears in a poll result again.
    test "fires for an aged handout on a runner that is still completing work" do
      prev =
        snapshot(
          channels: [
            channel(name: :scp, send_frame_calls: 100),
            channel(name: :scs, hb_count: 5, hb_dispatches: 9_000, hb_inflight: 1)
          ]
        )

      curr =
        snapshot(
          channels: [
            channel(name: :scp, send_frame_calls: 200),
            channel(
              name: :scs,
              hb_count: 6,
              hb_dispatches: 9_500,
              hb_inflight: 1,
              hb_inflight_oldest_ms: 600_000
            )
          ]
        )

      assert {:cell_dispatch_stalled, ctx} =
               StallClassifier.classify(curr, prev, @stall_opts)

      assert ctx.runner_id == "exec-1"
      assert ctx.inflight == 1
      # The reported age rides the verdict so a false positive — a genuinely
      # long Tasklet — is diagnosable from the log line alone.
      assert ctx.oldest_ms == 600_000
      assert StallClassifier.layer_scope(:cell_dispatch_stalled) == :relay
    end

    test "does NOT fire below the threshold, nor with nothing outstanding" do
      prev =
        snapshot(
          channels: [
            channel(name: :scp, send_frame_calls: 100),
            channel(name: :scs, hb_count: 5, hb_dispatches: 9_000)
          ]
        )

      for outstanding <- [
            [hb_inflight: 1, hb_inflight_oldest_ms: 10_000],
            [hb_inflight: 0, hb_inflight_oldest_ms: 600_000]
          ] do
        curr =
          snapshot(
            channels: [
              channel(name: :scp, send_frame_calls: 200),
              channel([name: :scs, hb_count: 6, hb_dispatches: 9_500] ++ outstanding)
            ]
          )

        assert StallClassifier.classify(curr, prev, @stall_opts) == :ok
      end
    end

    # An SDK that has not shipped the self-report sends neither key, which the
    # snapshot reads as zero. Zero must never look like a stall.
    test "an SDK that reports neither key never reads as stalled" do
      prev = snapshot(channels: [channel(name: :scs, hb_count: 5, hb_dispatches: 9_000)])

      curr =
        snapshot(
          channels: [
            channel(name: :scp, send_frame_calls: 200),
            channel(name: :scs, hb_count: 6, hb_dispatches: 9_500)
          ]
        )

      assert StallClassifier.classify(curr, prev, @stall_opts) == :ok
    end

    # A wedged runner explains a wedged Cell, not the other way round.
    test "a wedged runner outranks a stalled cell" do
      {prev, curr} =
        fed_runner(
          [hb_dispatches: 9_000, hb_inflight: 1],
          hb_dispatches: 9_000,
          hb_inflight: 1,
          hb_inflight_oldest_ms: 600_000
        )

      assert {:runner_pool_wedged, _} = StallClassifier.classify(curr, prev, @stall_opts)
    end

    # The fed precondition every check carries: a runner nobody is sending to
    # is idle, not stalled, however long its handout has been out.
    test "an unfed runner yields nothing" do
      prev =
        snapshot(
          channels: [
            channel(name: :scp, send_frame_calls: 100),
            channel(name: :scs, hb_count: 5, hb_dispatches: 9_000, hb_inflight: 1)
          ]
        )

      curr =
        snapshot(
          channels: [
            channel(name: :scp, send_frame_calls: 100),
            channel(
              name: :scs,
              hb_count: 6,
              hb_dispatches: 9_500,
              hb_inflight: 1,
              hb_inflight_oldest_ms: 600_000
            )
          ]
        )

      assert StallClassifier.classify(curr, prev, @stall_opts) == :ok
    end
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
      write_buffer_dropped: Keyword.get(opts, :write_buffer_dropped, 0),
      frames_written: Keyword.get(opts, :frames_written, 0),
      frames_read: Keyword.get(opts, :frames_read, 0),
      last_write_ms: Keyword.get(opts, :last_write_ms, @now),
      last_read_ms: Keyword.get(opts, :last_read_ms, @now)
    }
  end

  # A whole layout row, as `LivenessCounters.snapshot_all/0` returns one.
  defp channel(opts) do
    LivenessCounters.channel_slots()
    |> Map.new(&{&1, Keyword.get(opts, &1, 0)})
    |> Map.merge(%{
      runner_id: Keyword.get(opts, :runner_id, "exec-1"),
      name: Keyword.get(opts, :name, :chan_1)
    })
  end

  # One runner fed across its heartbeats, as two samples: a frame sent, then a
  # heartbeat, in each. `prev` and `curr` override any slot, and win over the
  # defaults. The sending channel takes `send_frame_calls` and `last_send_ms`;
  # the heartbeat channel takes everything else.
  defp fed_runner(prev, curr) do
    {runner_sample(prev,
       send_frame_calls: 100,
       last_send_ms: @now - 9_000,
       hb_count: 5,
       last_heartbeat_ms: @now - 8_000
     ),
     runner_sample(curr,
       send_frame_calls: 200,
       last_send_ms: @now - 2_000,
       hb_count: 6,
       last_heartbeat_ms: @now - 1_000
     )}
  end

  defp runner_sample(opts, defaults) do
    {sent, beat} = Keyword.split(opts ++ defaults, [:send_frame_calls, :last_send_ms])
    snapshot(channels: [channel([name: :scp] ++ sent), channel([name: :scs] ++ beat)])
  end

  defp relay(opts) do
    %{
      namespace: Keyword.get(opts, :namespace, "ns"),
      relay_id: Keyword.get(opts, :relay_id, "relay-1"),
      io_in_received: Keyword.get(opts, :io_in_received, 0),
      forwarded: Keyword.get(opts, :forwarded, 0),
      filtered_nil: Keyword.get(opts, :filtered_nil, 0)
    }
  end

  describe "classify_producing/3 (producing-liveness from progress markers)" do
    @grace 60_000
    @opts %{now: @now, grace_ms: @grace}

    defp marker(overrides) do
      Map.merge(
        %{
          relay_id: "btc",
          cell: {"task", "k1"},
          last_change_seen_at: @now,
          last_advance_at: @now
        },
        Map.new(overrides)
      )
    end

    test "idle relay (no input, flat markers) classifies IDLE" do
      m =
        marker(
          last_change_seen_at: @now - 2 * @grace,
          last_advance_at: @now - 2 * @grace
        )

      assert StallClassifier.classify_producing([m], nil, @opts) == %{"btc" => :idle}
    end

    test "fed but frozen WAL past grace is STUCK wal_frozen" do
      m = marker(last_change_seen_at: @now - 1_000, last_advance_at: @now - 2 * @grace)

      assert StallClassifier.classify_producing([m], nil, @opts) ==
               %{"btc" => {:stuck, :wal_frozen, %{cell: {"task", "k1"}}}}
    end

    test "advancing markers classify advancing; absent tier cursors degrade to WAL-only" do
      # No cursor fields and no baseline: only the WAL check runs.
      m = marker(last_change_seen_at: @now - 500, last_advance_at: @now - 500)
      assert StallClassifier.classify_producing([m], nil, @opts) == %{"btc" => :advancing}
    end

    test "growing input-log with a flat consumed offset is STUCK watermark_frozen (tier on)" do
      base = marker(input_log_tail: 100, consumed_offset: 40)
      # WAL itself is fed and advancing — only the watermark cursor is wedged.
      cur = marker(input_log_tail: 250, consumed_offset: 40)

      assert %{"btc" => {:stuck, :watermark_frozen, %{cell: {"task", "k1"}}}} =
               StallClassifier.classify_producing([cur], [base], @opts)
    end

    test "output-bearing records accumulating with flat flush cursor is STUCK outbox_flat" do
      base = marker(out_pending_seq: 10, outbox_flushed_seq: 10)
      cur = marker(out_pending_seq: 50, outbox_flushed_seq: 10)

      assert %{"btc" => {:stuck, :outbox_flat, %{cell: {"task", "k1"}}}} =
               StallClassifier.classify_producing([cur], [base], @opts)
    end

    test "output unconfirmed past the grace window is STUCK outbox_flat, with no baseline" do
      # Nothing new is emitted, so no accumulation comparison can see this: the
      # evidence is that a sink has not answered for what is already dispatched.
      cur = marker(out_pending_seq: 10, outbox_flushed_seq: 10, outbox_unconfirmed_ms: 90_000)

      assert %{"btc" => {:stuck, :outbox_flat, %{cell: {"task", "k1"}}}} =
               StallClassifier.classify_producing([cur], nil, @opts)

      fresh = marker(out_pending_seq: 10, outbox_flushed_seq: 10, outbox_unconfirmed_ms: 100)
      assert %{"btc" => :advancing} = StallClassifier.classify_producing([fresh], nil, @opts)
    end

    test "one root cause reports one deepest verdict (frozen WAL wins)" do
      base =
        marker(
          input_log_tail: 100,
          consumed_offset: 40,
          out_pending_seq: 10,
          outbox_flushed_seq: 10
        )

      # WAL frozen while fed; the frozen watermark and flat outbox are its
      # consequences and must not stack.
      cur =
        marker(
          last_change_seen_at: @now - 1_000,
          last_advance_at: @now - 2 * @grace,
          input_log_tail: 250,
          consumed_offset: 40,
          out_pending_seq: 50,
          outbox_flushed_seq: 10
        )

      assert %{"btc" => {:stuck, :wal_frozen, _ctx}} =
               StallClassifier.classify_producing([cur], [base], @opts)
    end

    test "multi-cell collapse: any STUCK cell wins over advancing and idle" do
      ok = marker(cell: {"task", "healthy"})

      frozen =
        marker(
          cell: {"task", "wedged"},
          last_change_seen_at: @now - 1_000,
          last_advance_at: @now - 2 * @grace
        )

      idle_other =
        marker(
          relay_id: "eth",
          last_change_seen_at: @now - 2 * @grace,
          last_advance_at: @now - 2 * @grace
        )

      assert %{
               "btc" => {:stuck, :wal_frozen, %{cell: {"task", "wedged"}}},
               "eth" => :idle
             } = StallClassifier.classify_producing([ok, frozen, idle_other], nil, @opts)
    end
  end
end
