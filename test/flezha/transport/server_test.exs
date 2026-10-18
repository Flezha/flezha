defmodule Flezha.Transport.ServerTest do
  @moduledoc """
  `Transport.Server`'s control-channel behaviour: stream correlation and
  close-on-reject.

  The NIF delivers `{:flz_connection, handle}` with an opaque handle but
  `{:flz_data, stream_id, ...}` / `{:flz_closed, stream_id, ...}` with the
  driver's integer stream id, so pending streams are keyed by that integer id —
  a `connect_control_channel` frame must bind the ControlChannel to the exact
  stream it arrived on, and closed streams (e.g. `nc -zU` liveness probes) must
  be cleaned up instead of accumulating.

  When `FleetManager.connect_handler/4` refuses a connect (`:shutting_down`
  during drain) the transport closes the stream and emits a counted
  `[:flezha, :transport, :connect_rejected]` signal, so the runner detects the
  dead controller and reconnects to a live one instead of dangling. `Native` is
  a NIF and cannot be mocked, so the close is observed through the signal and
  through the handler's robustness to a bogus stream ref.
  """

  # async: false: Mimic stubs; the live controller and its fleet; the primary Logger.
  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Config
  alias Flezha.Runner.FleetManager
  alias Flezha.Transport.Native
  alias Flezha.Transport.Server
  alias FlezhaTest.Helper.ControlFrameTestHelper
  alias FlezhaTest.Helper.MessageTestHelper
  alias FlezhaTest.Helper.MimicGlobal
  alias FlezhaTest.Helper.NodeTestHelper

  defp connect_frame(stream_id \\ 0) do
    message = Jason.encode!(%{"namespace" => "default", "factories" => []})
    {:flz_data, stream_id, "connect_control_channel", message, %{}}
  end

  defp attach_rejected_handler() do
    MessageTestHelper.forward_telemetry([:flezha, :transport, :connect_rejected], :rejected)
  end

  describe "stream correlation" do
    test "connect_control_channel binds the handle matching the frame's stream_id" do
      test_pid = self()
      want = make_ref()
      other = make_ref()

      stub(FleetManager, :connect_handler, fn ref, _ns, _payload, _transport ->
        send(test_pid, {:connected, ref})
        {:ok, "ce_ok"}
      end)

      # Two streams pending; the frame arrives on id 7. Only id 7's handle must be
      # used, and only id 7 must be removed from the pending map.
      state = %{
        transport: :uds,
        pending_streams: %{7 => want, 9 => other}
      }

      assert {:noreply, new_state} = Server.handle_info(connect_frame(7), state)
      assert_receive {:connected, ^want}, 500
      assert new_state.pending_streams == %{9 => other}
    end

    test "connect_control_channel for an unknown stream_id does not crash or bind" do
      stub(FleetManager, :connect_handler, fn _ref, _ns, _payload, _transport ->
        flunk("connect_handler must not be called for an unknown stream_id")
      end)

      state = %{transport: :uds, pending_streams: %{1 => make_ref()}}

      assert {:noreply, new_state} = Server.handle_info(connect_frame(42), state)
      assert new_state.pending_streams == state.pending_streams
    end

    test "flz_closed removes the matching pending stream (probe-churn cleanup)" do
      keep = make_ref()
      state = %{transport: :uds, pending_streams: %{3 => make_ref(), 4 => keep}}

      assert {:noreply, s1} = Server.handle_info({:flz_closed, 3, "connection_closed"}, state)
      assert s1.pending_streams == %{4 => keep}

      # Unknown / already-handed-off id is a harmless no-op.
      assert {:noreply, s2} = Server.handle_info({:flz_closed, 99, "connection_closed"}, s1)
      assert s2.pending_streams == %{4 => keep}
    end
  end

  describe "close on reject" do
    # Per test, not per module: the reject path logs at error by design, and a
    # module-wide capture would also silence the correlation tests above.
    @tag :capture_log
    test "rejected connect closes the stream, emits connect_rejected, and does not crash" do
      attach_rejected_handler()
      stream_ref = make_ref()

      stub(FleetManager, :connect_handler, fn ^stream_ref, _ns, _payload, _transport ->
        {:error, :shutting_down}
      end)

      state = %{transport: :uds, pending_streams: %{0 => stream_ref}}

      # safe_close tolerates the bogus (non-NIF) ref, so the server does not crash.
      assert {:noreply, new_state} = Server.handle_info(connect_frame(), state)
      assert new_state.pending_streams == %{}

      assert_receive {:rejected, %{count: 1}, %{reason: :shutting_down, transport: :uds}}, 500
    end

    @tag :capture_log
    test "successful connect does not emit connect_rejected" do
      attach_rejected_handler()
      stream_ref = make_ref()

      stub(FleetManager, :connect_handler, fn _ref, _ns, _payload, _transport ->
        {:ok, "ce_ok"}
      end)

      state = %{transport: :uds, pending_streams: %{0 => stream_ref}}

      assert {:noreply, _} = Server.handle_info(connect_frame(), state)
      refute_received {:rejected, _, _}
    end
  end

  describe "a frame a runner sends while its connect is still running" do
    setup {MimicGlobal, :set_mimic_global_until_exit}

    # A runner's start returns once its connect frame is written, and the connect
    # runs inside this server, so the runner's next frame can arrive before the
    # stream belongs to its control channel. The stub holds the connect open
    # until that frame has been read.
    @tag :capture_log
    test "reaches the runner's control channel, so an admin call made then is answered" do
      server_ref = :sys.get_state(Server.name(:uds)).server_ref
      known = stream_ids(server_ref)
      test_pid = self()

      stub(FleetManager, :connect_handler, fn stream, namespace, payload, transport ->
        send(test_pid, {:connecting, self()})

        receive do
          :release -> :ok
        after
          15_000 -> :ok
        end

        call_original(FleetManager, :connect_handler, [stream, namespace, payload, transport])
      end)

      ns = "handover_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        try do
          FleetManager.stop_namespace(ns)
        catch
          _, _ -> :ok
        end
      end)

      # A runner played by this process, dialling the controller's own socket:
      # its connect frame and an admin request go out back to back, as an SDK's
      # start and first admin call do.
      runner = ControlFrameTestHelper.dial()

      connect =
        Jason.encode!(%{
          "namespace" => ns,
          "factories" => [],
          "stable_id" => "handover-#{System.unique_integer([:positive])}"
        })

      request =
        ControlFrameTestHelper.encode_request(
          "admin-during-connect",
          {:admin_request, %RunnerCommands.AdminRequest{verb: "Relay.list_relays", args: "[]"}}
        )

      :ok = ControlFrameTestHelper.send_frame(runner, "connect_control_channel", connect)
      :ok = ControlFrameTestHelper.send_frame(runner, "control_command", request)

      assert_receive {:connecting, server}, 5_000
      on_exit(fn -> send(server, :release) end)

      read_past_connect = fn -> frames_read_since(server_ref, known) >= 2 end

      assert NodeTestHelper.wait_until(read_past_connect, 5_000),
             "the runner's frame after its connect never reached the driver"

      send(server, :release)

      assert {:admin_response, %{error: nil}} =
               ControlFrameTestHelper.await_answering_handshake(runner, "admin-during-connect")

      :gen_tcp.close(runner)
    end
  end

  defp stream_ids(server_ref),
    do: server_ref |> Native.driver_stats() |> Map.fetch!(:streams) |> MapSet.new(& &1.stream_id)

  defp frames_read_since(server_ref, known) do
    server_ref
    |> Native.driver_stats()
    |> Map.fetch!(:streams)
    |> Enum.reject(&(&1.stream_id in known))
    |> Enum.map(& &1.frames_read)
    |> Enum.max(fn -> 0 end)
  end

  describe "a data stream carries no credential of its own" do
    @tag :capture_log
    test "a QUIC data stream on a connection with no admitted control channel binds nothing" do
      state = %{
        transport: :quic,
        pending_streams: %{},
        quic_conn_runners: %{},
        quic_conn_monitors: Flezha.Process.Watch.new()
      }

      # The routing key is the control stream's own nif id, and it is written
      # only once `connect_handler/4` admits that control channel. A data stream
      # whose connection was refused therefore reaches no StreamPool at all,
      # which is why the namespace credential is presented on the control
      # channel and nowhere else.
      assert {:noreply, ^state} = Server.handle_info({:flz_stream, make_ref(), 1, 0, 42}, state)
    end
  end

  # A supervisor restarts a listener by binding the same address at once.
  describe "a stopped listener" do
    test "has released its QUIC port when the supervisor's stop returns" do
      port = Config.get(:quic_listen_address) |> String.split(":") |> List.last()
      child = {Server, :quic}
      # Held here, the handle outlives the listener's heap, so only the
      # listener's own stop can free the port, never the collector.
      %{server_ref: held} = :sys.get_state(Server.name(:quic))

      :ok = Supervisor.terminate_child(Flezha.Supervisor, child)
      on_exit(fn -> Supervisor.restart_child(Flezha.Supervisor, child) end)

      assert {:ok, socket} = :gen_udp.open(String.to_integer(port))
      :ok = :gen_udp.close(socket)
      assert is_reference(held)
    end
  end
end
