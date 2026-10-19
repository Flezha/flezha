defmodule Flezha.Runner.ControlChannelSendControlTest do
  @moduledoc """
  Fails when: `ControlChannel.send_control/2` swallows transport failures and
  always returns `{:ok, ref}`, so a relay talking to a reset/dead stream
  registers a reply ref that never resolves, then blocks and crashes instead
  of receiving a clean error and healing.

  Also fails when a channel that dies or stalls *mid-call* takes its caller down
  with it: the `ControlUnit` is the caller, and an exiting unit loses every
  waiter in its `response_refs` and is reported `shut down abnormally`.
  """
  # async: false: registered runner channels.
  use ExUnit.Case, async: false

  alias Flezha.Runner.ControlChannel

  # fails when: a control frame offered to a full send buffer is answered
  # `send_buffer_full` instead of held, so a bind's restore never reaches the
  # runner and its Cell is bound on empty state; or a watermark crossing logs
  # "Unexpected message at control channel", thousands of times a day.
  test "a frame refused at a full send buffer is written once the buffer drains" do
    on_exit(FlezhaTest.Helper.NodeTestHelper.override_log_level(:warning))
    test_pid = self()
    writes = :counters.new(1, [])

    write_fun = fn _ref, _payload_id, _message, _metadata ->
      :counters.add(writes, 1, 1)
      send(test_pid, :written)
      if :counters.get(writes, 1) == 1, do: {:error, :send_buffer_full}, else: :ok
    end

    {:ok, pid} = ControlChannel.start_link({"ns_gate", "exec_gate", :stream, write_fun})
    command = {:ping, %RunnerCommands.Ping{nonce: "n1"}}

    log =
      ExUnit.CaptureLog.capture_log([level: :warning], fn ->
        sender = Task.async(fn -> ControlChannel.send_control("exec_gate", command) end)
        assert_receive :written
        refute Task.yield(sender, 200), "the refused frame must be held, not answered"

        send(pid, {:flz_send_gate, 7, false})
        assert {:ok, _ref} = Task.await(sender)
        assert :counters.get(writes, 1) == 2
      end)

    refute log =~ "Unexpected message"
    GenServer.stop(pid)
  end

  test "send_control surfaces a transport error when no stream is bound" do
    write_fun = fn _ref, _payload_id, _message, _metadata -> :ok end

    {:ok, pid} =
      ControlChannel.start_link({"ns_sc", "exec_sc", nil, write_fun})

    command = {:ping, %RunnerCommands.Ping{nonce: "n1"}}

    assert {:error, :no_connection_available} =
             ControlChannel.send_control("exec_sc", command)

    GenServer.stop(pid)
  end

  # fails when: a channel with no stream asks the transport anyway, or a runner
  # with no channel crashes the list instead of showing no process id.
  test "peer_pid answers nil for a channel with no stream and for a runner with no channel" do
    write_fun = fn _ref, _payload_id, _message, _metadata -> :ok end
    {:ok, pid} = ControlChannel.start_link({"ns_pp", "exec_pp", nil, write_fun})

    assert ControlChannel.peer_pid("exec_pp") == nil
    assert ControlChannel.peer_pid("no_such_runner") == nil

    GenServer.stop(pid)
  end

  test "a channel that dies mid-call answers its caller instead of exiting it" do
    # The teardown race of KI-48: the supervisor's reverse-order sweep stops the
    # channel while a ControlUnit is still blocked in this call.
    Process.flag(:trap_exit, true)

    {:ok, pid} = start_stuck_channel("exec_sc_die")

    caller =
      Task.async(fn ->
        ControlChannel.send_control("exec_sc_die", {:ping, %RunnerCommands.Ping{nonce: "n2"}})
      end)

    assert_receive :writing, 1_000
    Process.exit(pid, :kill)

    # `Task.await` returning a value at all is the survival proof: an exiting
    # caller propagates its exit here instead of answering.
    assert {:error, {:control_channel_exit, {:killed, {GenServer, :call, _}}}} =
             Task.await(caller, 1_000)
  end

  test "a channel that does not reply in time answers :timeout instead of exiting its caller" do
    Process.flag(:trap_exit, true)

    {:ok, pid} = start_stuck_channel("exec_sc_slow")

    assert {:error, :timeout} =
             ControlChannel.send_control(
               "exec_sc_slow",
               {:ping, %RunnerCommands.Ping{nonce: "n3"}},
               50
             )

    # The caller reached this line, so it survived the timeout; the channel is
    # still alive and merely slow, which must not kill the unit.
    assert_received :writing
    assert Process.alive?(pid)

    Process.exit(pid, :kill)
  end

  # A channel whose write never returns, so the caller stays blocked in
  # `GenServer.call` until the test decides how the call ends.
  defp start_stuck_channel(runner_id) do
    test_pid = self()

    write_fun = fn _ref, _payload_id, _message, _metadata ->
      send(test_pid, :writing)
      receive do: (:never -> :ok)
    end

    ControlChannel.start_link({"ns_" <> runner_id, runner_id, :stuck_stream, write_fun})
  end
end

defmodule FlezhaTest.Runner.ControlChannelDisconnectTest do
  alias Flezha.Identificator
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.EventManager
  alias FlezhaTest.Helper.NodeTestHelper

  # async: false: the live controller and its fleet; registered runner channels.

  use ExUnit.Case, async: false

  @namespace "test_temp_namespace"

  setup do
    on_exit(fn -> FleetManager.stop_namespace(@namespace) end)
    :ok
  end

  test "{:flz_closed, ...} on the ControlChannel triggers FleetManager disconnect handling" do
    runner_id = NodeTestHelper.start_runner(@namespace)
    assert runner_id in FleetManager.active_runners(@namespace)

    EventManager.add_handler(:runner_disconnected_event)

    on_exit(fn -> EventManager.rem_handler(:runner_disconnected_event) end)

    control_channel_pid = GenServer.whereis(Identificator.control_channel(runner_id))
    assert is_pid(control_channel_pid)

    send(control_channel_pid, {:flz_closed, make_ref(), :test_eof})

    assert_receive {:runner_disconnected_event, {@namespace, ^runner_id}}, 5_000

    refute runner_id in FleetManager.active_runners(@namespace)
  end
end

defmodule FlezhaTest.Runner.ControlChannelUpdateFactoriesTest do
  @moduledoc """
  SDK -> Controller `UpdateFactories` dispatch path
  (`sdk-runner-entrypoint-redesign`): the ControlChannel decodes the frame on
  the SDK-initiated-command seam and casts a full-set `{:sync_factories, ...}`
  to FleetManager — fire-and-forget, no reply frame, never blocking the
  channel.

  Uses the 4-tuple ControlChannel constructor with a capturing `write_fun`,
  mirroring `ControlChannelTaskCommitTest`.
  """
  # async: false: the live controller and its fleet; registered runner channels.
  use ExUnit.Case, async: false

  alias Flezha.Runner.{ControlChannel, FleetManager}
  alias FlezhaTest.Helper.ControlFrameTestHelper
  alias FlezhaTest.Helper.FleetStateHelper
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "ctrl-chan-update-factories-test"

  setup do
    original = :sys.get_state(FleetManager)

    on_exit(fn ->
      :sys.replace_state(FleetManager, fn _ -> original end)
      FleetStateHelper.forget_established(@namespace, ["alpha", "beta"])
    end)

    :ok
  end

  test "UpdateFactories is applied as a full-set FleetManager sync with no reply frame" do
    stream_ref = make_ref()
    runner_id = "uf_runner_#{System.unique_integer([:positive])}"
    test_pid = self()

    write_fun = fn ref, _payload_id, message, _metadata ->
      send(test_pid, {:test_writer_called, ref, message})
      :ok
    end

    {:ok, control_channel} =
      ControlChannel.start_link({@namespace, runner_id, stream_ref, write_fun})

    on_exit(fn ->
      try do
        NodeTestHelper.ensure_stopped(control_channel, :normal, 1_000)
      catch
        :exit, _ -> :ok
      end
    end)

    payload =
      ControlFrameTestHelper.encode_request(
        "ref-update-factories",
        {:update_factories,
         %RunnerCommands.UpdateFactories{
           factories: [
             %RunnerCommands.FactoryDecl{
               id: "alpha",
               description: "the alpha factory",
               run: "steps"
             },
             %RunnerCommands.FactoryDecl{id: "beta", description: "", run: "flow"}
           ]
         }}
      )

    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    # Dispatch completed (the cast is on its way); fire-and-forget = no frame
    # written back.
    _ = :sys.get_state(control_channel)
    refute_received {:test_writer_called, _, _}

    # The cast crosses a process boundary, so poll for the applied set.
    assert NodeTestHelper.wait_until(
             fn ->
               {:ok, factories} = FleetManager.list_factories(@namespace)

               Enum.map(factories, &Map.take(&1, [:id, :description, :run])) == [
                 %{id: "alpha", description: "the alpha factory", run: :steps},
                 %{id: "beta", description: nil, run: :flow}
               ]
             end,
             500,
             10
           ),
           "FleetManager must store the full factory set (empty description as nil)"

    assert FleetManager.runners_for_factory(@namespace, "alpha") == []
  end
end

defmodule FlezhaTest.Runner.ControlChannelDirectionTest do
  @moduledoc """
  Every control payload is one `ControlFrame` whose `oneof` says its direction:
  the channel decodes it once, dispatches a `request` by its tag and hands a
  `response` to the runner's `ControlUnit` already decoded.
  """
  # async: false: the warnings asserted here need the global log level lowered.
  use ExUnit.Case, async: false

  alias Flezha.Identificator
  alias Flezha.Runner.ControlChannel
  alias FlezhaTest.Helper.ControlFrameTestHelper

  import ExUnit.CaptureLog

  # The test process stands in for the runner's ControlUnit.
  setup do
    on_exit(FlezhaTest.Helper.NodeTestHelper.override_log_level(:warning))
    runner_id = "direction_#{System.unique_integer([:positive])}"
    {:via, Registry, {registry, key}} = Identificator.control_unit(runner_id)
    {:ok, _} = Registry.register(registry, key, nil)
    test_pid = self()

    write_fun = fn _ref, _payload_id, message, _metadata ->
      send(test_pid, {:written, message})
    end

    {:ok, channel} = ControlChannel.start_link({"ns_direction", runner_id, :stream, write_fun})
    on_exit(fn -> if Process.alive?(channel), do: GenServer.stop(channel) end)
    %{channel: channel, runner_id: runner_id}
  end

  # fails when: the ControlUnit decodes the response a second time, so it is
  # handed the frame's bytes instead of the `CommandResponse` they carry.
  test "a response reaches the ControlUnit decoded", %{channel: channel} do
    answer = ControlFrameTestHelper.encode_response("ref-add", "success")
    send(channel, {:flz_data, :stream_id, "control_command", answer, %{"k" => "v"}})

    assert_receive {:"$gen_cast",
                    {:handle_payload,
                     %RunnerCommands.CommandResponse{reference_id: "ref-add", status: "success"},
                     %{"k" => "v"}}}
  end

  # fails when: a list of runner-initiated tags decides a request's direction,
  # so a request whose tag the list does not name is taken for a response and
  # handed to the ControlUnit.
  test "a request whose tag no handler names is logged and dropped, never taken for a response",
       %{channel: channel, runner_id: runner_id} do
    frame = ControlFrameTestHelper.encode_request("ref-ping", {:ping, %RunnerCommands.Ping{}})

    log =
      capture_log([level: :warning], fn ->
        send(channel, {:flz_data, :stream_id, "control_command", frame, %{}})
        _ = :sys.get_state(channel)
      end)

    assert log =~
             "[control_channel] #{runner_id} sent ping (ref=ref-ping), " <>
               "a command only the controller sends; dropped"

    refute_received {:"$gen_cast", {:handle_payload, _, _}}
    refute_received {:written, _}
  end

  test "an unreadable frame is logged and dropped", %{channel: channel, runner_id: runner_id} do
    log =
      capture_log([level: :warning], fn ->
        send(channel, {:flz_data, :stream_id, "control_command", <<0xFF>>, %{}})
        _ = :sys.get_state(channel)
      end)

    assert log =~ "[control_channel] #{runner_id} sent an unreadable control frame: {:error,"
    refute_received {:"$gen_cast", {:handle_payload, _, _}}
  end
end
