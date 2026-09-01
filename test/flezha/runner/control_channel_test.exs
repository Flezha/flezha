defmodule Flezha.Runner.ControlChannelSendControlTest do
  @moduledoc """
  Fails when: `ControlChannel.send_control/2` swallows transport failures and
  always returns `{:ok, ref}`, so a operative talking to a reset/dead stream
  registers a reply ref that never resolves, then blocks and crashes instead
  of receiving a clean error and healing.

  Also fails when a channel that dies or stalls *mid-call* takes its caller down
  with it: the `ControlUnit` is the caller, and an exiting unit loses every
  waiter in its `response_refs` and is reported `shut down abnormally`.
  """
  # async: false: registered runner channels.
  use ExUnit.Case, async: false

  alias Flezha.Runner.ControlChannel

  test "send_control surfaces a transport error when no stream is bound" do
    write_fun = fn _ref, _payload_id, _message, _metadata -> :ok end

    {:ok, pid} =
      ControlChannel.start_link({"ns_sc", "exec_sc", nil, write_fun})

    command = {:ping, %RunnerCommands.Ping{nonce: "n1"}}

    assert {:error, :no_connection_available} =
             ControlChannel.send_control("exec_sc", command)

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
  mirroring `ControlChannelProcCommitTest`.
  """
  # async: false: the live controller and its fleet; registered runner channels.
  use ExUnit.Case, async: false

  alias Flezha.Runner.{ControlChannel, FleetManager}
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "ctrl-chan-update-factories-test"

  setup do
    original = :sys.get_state(FleetManager)
    on_exit(fn -> :sys.replace_state(FleetManager, fn _ -> original end) end)
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

    request = %RunnerCommands.CommandRequest{
      reference_id: "ref-update-factories",
      command:
        {:update_factories,
         %RunnerCommands.UpdateFactories{
           factories: [
             %RunnerCommands.FactoryDecl{id: "alpha", description: "the alpha factory"},
             %RunnerCommands.FactoryDecl{id: "beta", description: ""}
           ]
         }}
    }

    {:ok, iodata, _size} = Protox.encode(request)
    payload = IO.iodata_to_binary(iodata)

    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    # Dispatch completed (the cast is on its way); fire-and-forget = no frame
    # written back.
    _ = :sys.get_state(control_channel)
    refute_received {:test_writer_called, _, _}

    # The cast crosses a process boundary, so poll for the applied set.
    assert NodeTestHelper.wait_until(
             fn ->
               {:ok, factories} = FleetManager.list_factories(@namespace)

               factories == [
                 %{id: "alpha", description: "the alpha factory"},
                 %{id: "beta", description: nil}
               ]
             end,
             500,
             10
           ),
           "FleetManager must store the full factory set (empty description as nil)"

    assert FleetManager.runners_for_factory(@namespace, "alpha") == []
  end
end
