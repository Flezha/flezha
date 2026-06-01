defmodule FlezhaTest.Executor.ControlChannelIdleWatchdogTest do
  @moduledoc """
  Fails when: an executor whose control-plane transport has gone silent
  (no `control_ping`/command frames) is never demoted, so its relays keep
  binding to the wedged corpse and market data freezes until a manual restart.
  """
  alias Flezha.Executor.FleetManager
  alias Flezha.Service.EventManager
  alias Flezha.Service.Identificator
  alias FlezhaTest.Helper.NodeTestHelper

  use ExUnit.Case, async: false

  @namespace "test_temp_namespace"

  setup do
    on_exit(fn ->
      Application.delete_env(:flezha, :executor_control_idle_timeout_ms)
      FleetManager.stop_namespace(@namespace)
    end)

    :ok
  end

  test "idle watchdog synthesizes a disconnect once the silence threshold elapses" do
    executor_id = NodeTestHelper.start_executor(@namespace)
    assert executor_id in FleetManager.active_executors(@namespace)

    EventManager.add_handler(:executor_disconnected_event)
    on_exit(fn -> EventManager.rem_handler(:executor_disconnected_event) end)

    control_channel_pid = GenServer.whereis(Identificator.control_channel(executor_id))
    assert is_pid(control_channel_pid)

    # Age the last-inbound stamp well past the threshold so the watchdog tick
    # fires deterministically — independent of the live executor's keepalive
    # pings, which would otherwise refresh the stamp and race the assertion.
    :sys.replace_state(control_channel_pid, fn state ->
      %{state | last_inbound_at: state.last_inbound_at - 10_000_000}
    end)

    Application.put_env(:flezha, :executor_control_idle_timeout_ms, 60_000)
    send(control_channel_pid, :control_idle_check)

    assert_receive {:executor_disconnected_event, {@namespace, ^executor_id}}, 5_000
    refute executor_id in FleetManager.active_executors(@namespace)
  end

  test "idle watchdog leaves a recently-active channel bound" do
    executor_id = NodeTestHelper.start_executor(@namespace)
    assert executor_id in FleetManager.active_executors(@namespace)

    EventManager.add_handler(:executor_disconnected_event)
    on_exit(fn -> EventManager.rem_handler(:executor_disconnected_event) end)

    control_channel_pid = GenServer.whereis(Identificator.control_channel(executor_id))
    assert is_pid(control_channel_pid)

    # Far above any plausible silence: a watchdog tick must be a no-op.
    Application.put_env(:flezha, :executor_control_idle_timeout_ms, 600_000)
    send(control_channel_pid, :control_idle_check)

    refute_receive {:executor_disconnected_event, {@namespace, ^executor_id}}, 1_500
    assert executor_id in FleetManager.active_executors(@namespace)
  end
end
