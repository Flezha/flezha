defmodule FlezhaTest.ComputeEngine.ControlChannelDisconnectTest do
  alias Flezha.ComputeEngine.FleetManager
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
    compute_engine_id = NodeTestHelper.start_compute_engine(@namespace)
    assert compute_engine_id in FleetManager.active_compute_engines(@namespace)

    EventManager.add_handler(:compute_engine_disconnected_event)

    on_exit(fn -> EventManager.rem_handler(:compute_engine_disconnected_event) end)

    control_channel_pid = GenServer.whereis(Identificator.control_channel(compute_engine_id))
    assert is_pid(control_channel_pid)

    send(control_channel_pid, {:flz_closed, make_ref(), :test_eof})

    assert_receive {:compute_engine_disconnected_event, {@namespace, ^compute_engine_id}}, 5_000

    refute compute_engine_id in FleetManager.active_compute_engines(@namespace)
  end
end
