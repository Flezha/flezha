defmodule FlezhaTest.Runner.SpawnParentTest do
  @moduledoc """
  A spawn request's `parent_relay_id` is kept only when the requesting session
  hosts that instance; a parent it merely names, or none, leaves the spawn's
  origin without one and refuses nothing, and judging it reports nothing.
  """

  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Relay.ClassRegistry
  alias Flezha.Runner.ControlChannel
  alias Flezha.Service.RelayManager
  alias FlezhaTest.Helper.ControlFrameTestHelper, as: ControlFrame
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.Telemetry

  @namespace "spawn-parent-test"

  setup :set_mimic_global_until_exit

  setup do
    test_pid = self()
    runner_id = "parent_runner_#{System.unique_integer([:positive])}"
    write_fun = fn _ref, _payload_id, _message, _metadata -> :ok end
    {:ok, channel} = ControlChannel.start_link({@namespace, runner_id, make_ref(), write_fun})
    on_exit(fn -> NodeTestHelper.ensure_stopped(channel, :normal, 1_000) end)

    identity = :sys.get_state(channel).identity

    stub(ClassRegistry, :owner, fn
      "hosted" -> {:ok, self(), identity}
      "elsewhere" -> {:ok, self(), "another-runner"}
      _ -> :error
    end)

    stub(RelayManager, :spawn_or_deliver, fn _ns, id, _f, _p, _ir, _caller, _identity, opts ->
      send(test_pid, {:parent, Keyword.get(opts, :parent_relay)})
      {:spawned, id}
    end)

    Telemetry.capture([:flezha, :runner, :foreign_frame_refused])
    %{channel: channel}
  end

  for {named, kept} <- [{"hosted", "hosted"}, {"elsewhere", nil}, {"", nil}] do
    test "a parent named #{inspect(named)} is recorded as #{inspect(kept)}", %{channel: channel} do
      request = %RunnerCommands.SpawnStepsRequest{
        instance_id: "child",
        factory_id: "worker",
        parent_relay_id: unquote(named)
      }

      payload = ControlFrame.encode_request("ref-parent", {:spawn_steps_request, request})
      send(channel, {:flz_data, :stream_id, "control_command", payload, %{}})

      assert_receive {:parent, unquote(kept)}, 2_000
      refute_received {:telemetry, [:flezha, :runner, :foreign_frame_refused], _, _}
    end
  end
end
