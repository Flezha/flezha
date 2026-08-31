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
