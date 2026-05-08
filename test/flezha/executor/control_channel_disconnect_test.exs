defmodule FlezhaTest.Executor.ControlChannelDisconnectTest do
  alias Flezha.Executor.FleetManager
  alias Flezha.Service.EventManager
  alias Flezha.Service.Identificator
  alias FlezhaTest.Helper.NodeTestHelper

  use ExUnit.Case, async: false

  @namespace "test_temp_namespace"

  setup do
    on_exit(fn -> FleetManager.stop_namespace(@namespace) end)
    :ok
  end

  test "{:flz_closed, ...} on the ControlChannel triggers FleetManager disconnect handling" do
    executor_id = NodeTestHelper.start_executor(@namespace)
    assert executor_id in FleetManager.active_executors(@namespace)

    EventManager.add_handler(:executor_disconnected_event)

    on_exit(fn -> EventManager.rem_handler(:executor_disconnected_event) end)

    control_channel_pid = GenServer.whereis(Identificator.control_channel(executor_id))
    assert is_pid(control_channel_pid)

    send(control_channel_pid, {:flz_closed, make_ref(), :test_eof})

    assert_receive {:executor_disconnected_event, {@namespace, ^executor_id}}, 5_000

    refute executor_id in FleetManager.active_executors(@namespace)
  end
end
