defmodule FlezhaTest.Runner.StreamChannelSendGateTest do
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
  released — or told the stream went — instead of being left. What is NOT
  asserted is `write_buffer_dropped == 0` after a burst, because it is not
  true yet: the gate bit is set by the driver behind an unbounded command
  channel, so a sender queues frames it has not been refused for and the
  ceiling discards them under an `:ok` (KI-73). That assertion is what the
  fix for KI-73 owes; adding it here now would only make this file red.
  """

  alias Flezha.Runner.StreamChannel
  alias Flezha.Transport.LivenessCounters
  alias Flezha.Transport.Native
  alias FlezhaTest.Helper.NodeTestHelper

  # async: false: registered channel names, a real driver thread and a socket.
  use ExUnit.Case, async: false

  # Comfortably past the 8 MiB default cap at 64 KiB a frame, so the gate has
  # to engage well before the last one.
  @frames 400
  @frame_bytes 64 * 1024

  setup context do
    name =
      "send_gate_#{context.test}"
      |> String.replace(~r/[^a-zA-Z0-9_]/, "_")
      |> String.to_atom()

    path =
      Path.join(System.tmp_dir!(), "flz_send_gate_#{System.unique_integer([:positive])}.sock")

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

    {:ok, pid} = StreamChannel.start_link({"send_gate_runner", name, :primary})
    :ok = Native.controlling_process(stream, pid)
    :ok = GenServer.call(pid, {:bind, stream})

    on_exit(fn ->
      :gen_tcp.close(peer)
      NodeTestHelper.ensure_stopped(pid)
      Native.stop_server(server)
      File.rm(path)
    end)

    %{pid: pid, name: name, peer: peer, server: server, stream: stream}
  end

  describe "a peer that stops reading" do
    test "holds the sender, then delivers its frames when the peer drains", ctx do
      spawn_sender(ctx.name)

      # The hold is the point: with the frames merely refused the task would
      # finish at once, and with them dropped it would finish reporting :ok.
      refute_receive {:sender_done, _}, 500
      assert held(ctx.name) > 0, "the sender is recorded as held, not merely slow"
      assert written(ctx.server) > 0, "and the driver is writing, not wedged"

      drain_peer(ctx.peer)

      assert_receive {:sender_done, results}, 10_000
      assert results == %{ok: @frames}, "the held sender resumed and finished its run"
    end

    test "wakes the held sender with an error when the stream closes under it", ctx do
      spawn_sender(ctx.name)
      refute_receive {:sender_done, _}, 500
      assert held(ctx.name) > 0

      # The stream goes while the sender is still holding on its gate. It must
      # be told, not left waiting out a hold on a binding that no longer exists.
      :gen_tcp.close(ctx.peer)

      assert_receive {:sender_done, results}, 10_000
      assert Map.get(results, :ok, 0) < @frames, "the run did not complete normally"

      # The `assert_receive` above is the proof it was answered rather than
      # left: a sender that was not woken never reaches this send at all.
      assert Map.get(results, {:error, :no_stream}, 0) > 0,
             "a held sender whose stream closed is answered, never left: #{inspect(results)}"
    end

    test "the hold is reported as held while nothing reads as dropped", ctx do
      spawn_sender(ctx.name)
      refute_receive {:sender_done, _}, 500

      counters = channel_counters(ctx.name)

      assert counters.send_gate_held > 0,
             "a held sender is observable the way a credit-parked one is"

      assert counters.nil_drops == 0, "and holding is not dropping"
      assert counters.send_frame_calls > 0, "the channel is sending, not wedged"

      drain_peer(ctx.peer)
      assert_receive {:sender_done, _}, 10_000
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
          StreamChannel.io_in(name, "operative_#{i}", {payload, %{}})
        end)

      send(test_pid, {:sender_done, Enum.frequencies(results)})
    end)
  end

  defp spawn_drainer(peer) do
    spawn(fn -> drain_forever(peer) end)
  end

  defp drain_forever(peer) do
    case :gen_tcp.recv(peer, 0, 5_000) do
      {:ok, _} -> drain_forever(peer)
      _ -> :ok
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

  defp held(name), do: channel_counters(name).send_gate_held

  defp written(server) do
    case Native.driver_stats(server) do
      %{streams: streams} -> Enum.reduce(streams, 0, &(&1.frames_written + &2))
      _ -> 0
    end
  end
end
