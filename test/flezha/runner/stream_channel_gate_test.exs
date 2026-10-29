defmodule FlezhaTest.Runner.StreamChannelGateTest do
  @moduledoc """
  The send-buffer gate, end to end: a real UDS driver, a real peer that stops
  reading, a real `StreamChannel` owning the stream, and a real sender on
  `io_in/3`.

  Nothing here is stubbed, because the thing under test is a race between a
  driver thread and a BEAM process. The driver raises the gate on its own
  buffer depth, reports the crossing on the same route it delivers frames on,
  and lowers it when the peer drains — a stub for any one of those would prove
  only that the test agrees with itself.

  What is asserted here is the hold: a congested stream refuses its sender,
  the sender waits in its own process rather than on a scheduler, and it is
  released — or told the stream went — instead of being left.

  And what the hold is *for*: `write_buffer_dropped == 0`, beside a peer that
  read every frame. That counter is the invariant the whole gate exists to
  keep, so it is asserted where the loss would happen — at the crossing, and
  again over a completed run of every frame. It is the assertion that catches a gate raised one frame too late:
  the bit is read by the sender and written by both the sender and a driver
  thread behind a queue, so a gate that weighs only what the driver has
  already buffered passes every other assertion in this file and still lets a
  fifth of a burst reach the ceiling under an `:ok` (KI-73).
  """

  alias Flezha.Runner.StreamChannel
  alias Flezha.State.TestHelpers
  alias Flezha.Transport.LivenessCounters
  alias Flezha.Transport.Native
  alias FlezhaTest.Helper.NodeTestHelper

  # async: false: registered channel names, a real driver thread and a socket.
  use FlezhaTest.Case, fixtures: [:tmp_dir], async: false

  # Comfortably past the 8 MiB default cap at 64 KiB a frame, so the gate has
  # to engage well before the last one.
  @frames 400
  @frame_bytes 64 * 1024

  setup context do
    name =
      "send_gate_#{context.test}"
      |> String.replace(~r/[^a-zA-Z0-9_]/, "_")
      |> String.to_atom()

    path = Path.join(context.tmp, "send_gate.sock")

    {:ok, server} = Native.start_uds_server(path, self())

    {:ok, peer} =
      :gen_tcp.connect({:local, String.to_charlist(path)}, 0, [
        :binary,
        active: false,
        packet: :raw
      ])

    stream =
      receive do
        {:flz_connection, s} -> s
      after
        5_000 -> flunk("the driver never reported the peer's connection")
      end

    {:ok, pid} = StreamChannel.start_link({"_system", "send_gate_runner", name, :primary})
    :ok = Native.controlling_process(stream, pid)
    :ok = GenServer.call(pid, {:bind, stream})

    on_exit(fn ->
      :gen_tcp.close(peer)
      NodeTestHelper.ensure_stopped(pid)
      Native.stop_server(server)
    end)

    %{pid: pid, name: name, peer: peer, server: server, stream: stream}
  end

  describe "a peer that stops reading" do
    test "holds the sender, then delivers its frames when the peer drains", ctx do
      # The hold must outlast the checks below, which a loaded machine can
      # stretch past the 5 s default; a lost wake still fails the 10 s wait.
      on_exit(TestHelpers.override_env(:send_gate_hold_ms, 60_000))
      spawn_sender(ctx.name)

      # The hold is the point: with the frames merely refused the task would
      # finish at once, and with them dropped it would finish reporting :ok.
      await_hold(ctx.name)
      counters = channel_counters(ctx.name)
      assert counters.send_frame_calls > 0, "the channel is sending, not wedged"
      assert counters.nil_drops == 0, "and holding is not dropping"
      # The driver publishes its counters once per loop iteration.
      assert NodeTestHelper.wait_until(fn -> written(ctx.server) > 0 end, 5_000),
             "and the driver is writing, not wedged"

      assert dropped(ctx.server) == 0, "a held sender is a sender nothing is discarded for"

      drain_peer(ctx.peer)

      assert_receive {:sender_done, results}, 10_000
      assert results == %{ok: @frames}, "the held sender resumed and finished its run"

      # The invariant, over the whole burst, proved at the far end of the
      # socket: the peer read every frame the sender was told `:ok` for.
      assert_receive {:peer_drained, _}, 10_000
      assert dropped(ctx.server) == 0
    end

    test "wakes the held sender with an error when the stream closes under it", ctx do
      spawn_sender(ctx.name)

      await_hold(ctx.name)

      # The stream goes while the sender is still holding on its gate. It must
      # be told, not left waiting out a hold on a binding that has gone.
      :gen_tcp.close(ctx.peer)

      assert_receive {:sender_done, results}, 10_000
      assert Map.get(results, :ok, 0) < @frames, "the run did not complete normally"

      # The `assert_receive` above is the proof it was answered rather than
      # left: a sender that was not woken never reaches this send at all.
      assert Map.get(results, {:error, :no_stream}, 0) > 0,
             "a held sender whose stream closed is answered, never left: #{inspect(results)}"
    end
  end

  describe "the control" do
    test "a peer that keeps reading finishes the run rather than wedging on the gate", ctx do
      reader = spawn_drainer(ctx.peer)
      spawn_sender(ctx.name)

      # Without this the "held" cases prove nothing: a gate that never lowers
      # and a gate that works both leave a sender that has not come back.
      assert_receive {:sender_done, results}, 20_000
      assert results == %{ok: @frames}

      assert_receive {:peer_drained, _}, 10_000
      assert dropped(ctx.server) == 0, "a clean result is one this file can report"

      Process.exit(reader, :kill)
    end
  end

  # One sender, in its own process, because `io_in/3` holds the CALLING one —
  # which is the whole mechanism under test.
  defp spawn_sender(name) do
    test_pid = self()
    payload = :binary.copy("x", @frame_bytes)

    spawn(fn ->
      results =
        Enum.map(1..@frames, fn i ->
          StreamChannel.io_in(name, "relay_#{i}", {payload, %{}})
        end)

      send(test_pid, {:sender_done, Enum.frequencies(results)})
    end)
  end

  # Read until the peer has taken the whole burst, and say so once. The
  # threshold is the burst's payload bytes, which is a count 399 framed frames
  # cannot reach even with their headers — so the message is the delivery
  # proof, and it is read off the socket rather than off a driver counter the
  # driver refreshes once per loop iteration.
  defp spawn_drainer(peer) do
    test_pid = self()
    spawn(fn -> drain_forever(peer, 0, test_pid) end)
  end

  @burst_bytes @frames * @frame_bytes

  defp drain_forever(peer, seen, test_pid) do
    case :gen_tcp.recv(peer, 0, 5_000) do
      {:ok, bytes} ->
        total = seen + byte_size(bytes)

        if seen < @burst_bytes and total >= @burst_bytes,
          do: send(test_pid, {:peer_drained, total})

        drain_forever(peer, total, test_pid)

      _ ->
        :ok
    end
  end

  # Read until the sender reports; the driver flushes on the writable event the
  # drain produces, and lowers the gate on the depth that flush leaves behind.
  defp drain_peer(peer) do
    spawn_drainer(peer)
  end

  defp channel_counters(name) do
    %{channels: channels} = LivenessCounters.snapshot_all()
    Enum.find(channels, fn c -> c.name == name end) || flunk("no counters for #{inspect(name)}")
  end

  # The sender is held on its gate, the counter a credit-parked sender is
  # observed by. Its peer reads nothing, so nothing can release it.
  defp await_hold(name) do
    assert NodeTestHelper.wait_until(fn -> channel_counters(name).send_gate_held > 0 end, 5_000),
           "the sender is recorded as held, not merely slow"

    refute_received {:sender_done, _}
  end

  defp written(server), do: driver_sum(server, :frames_written)

  # The invariant, not a metric: a bounded send buffer signals congestion, so
  # every increment here is a frame the sender was never told about.
  defp dropped(server), do: driver_sum(server, :write_buffer_dropped)

  defp driver_sum(server, slot) do
    case Native.driver_stats(server) do
      %{streams: streams} -> Enum.reduce(streams, 0, &(Map.fetch!(&1, slot) + &2))
      _ -> 0
    end
  end
end
