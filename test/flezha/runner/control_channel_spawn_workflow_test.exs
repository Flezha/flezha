defmodule FlezhaTest.Runner.ControlChannelSpawnWorkflowTest do
  @moduledoc """
  SDK -> Controller `SpawnWorkflowRequest` dispatch
  (`procedure-requested-spawn`): the ControlChannel decodes the frame on the
  SDK-initiated-command seam, resolves it off-process against the channel's own
  namespace, and writes one `SpawnWorkflowResponse` back under the request's
  reference id.

  Uses the 4-tuple ControlChannel constructor with a capturing `write_fun`,
  mirroring `ControlChannelUpdateFactoriesTest`.
  """
  # async: false: Mimic global mode; Mimic stubs; registered runner channels.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Runner.ControlChannel
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.ControlFrameTestHelper, as: ControlFrame

  @namespace "ctrl-chan-spawn-workflow-test"

  setup :set_mimic_global_until_exit

  setup do
    stream_ref = make_ref()
    runner_id = "spawn_runner_#{System.unique_integer([:positive])}"
    test_pid = self()

    write_fun = fn ref, _payload_id, message, _metadata ->
      send(test_pid, {:written, ref, message})
      :ok
    end

    {:ok, channel} = ControlChannel.start_link({@namespace, runner_id, stream_ref, write_fun})

    on_exit(fn ->
      try do
        FlezhaTest.Helper.NodeTestHelper.ensure_stopped(channel, :normal, 1_000)
      catch
        :exit, _ -> :ok
      end
    end)

    %{channel: channel}
  end

  defp send_request(channel, fields) do
    request = %RunnerCommands.CommandRequest{
      reference_id: "ref-spawn",
      command: {:spawn_workflow_request, struct(RunnerCommands.SpawnWorkflowRequest, fields)}
    }

    {:ok, iodata, _size} = Protox.encode(request)
    send(channel, {:flz_data, :stream_id, "control_command", IO.iodata_to_binary(iodata), %{}})
  end

  defp await_response do
    assert_receive {:written, _ref, framed}, 2_000

    %RunnerCommands.CommandRequest{
      reference_id: reference_id,
      command: {:spawn_workflow_response, response}
    } = ControlFrame.decode_request(framed)

    {reference_id, response}
  end

  test "a request resolves against the channel's namespace and answers its outcome", %{
    channel: channel
  } do
    test_pid = self()

    expect(OperativeManager, :spawn_or_deliver, fn ns, id, factory, payload, if_running ->
      send(test_pid, {:resolved, ns, id, factory, payload, if_running})
      :spawned
    end)

    send_request(channel,
      instance_id: "w1",
      factory_id: "worker",
      payload: "p1",
      if_running: "deliver"
    )

    assert_receive {:resolved, @namespace, "w1", "worker", "p1", :deliver}
    assert {"ref-spawn", %{outcome: "spawned", error: ""}} = await_response()
  end

  test "the error policy is carried through verbatim", %{channel: channel} do
    expect(OperativeManager, :spawn_or_deliver, fn _ns, _id, _factory, _payload, :error ->
      :already_running
    end)

    send_request(channel, instance_id: "w1", factory_id: "worker", if_running: "error")

    assert {"ref-spawn", %{outcome: "already_running", error: ""}} = await_response()
  end

  test "an unset policy reads as deliver rather than refusing the payload", %{channel: channel} do
    expect(OperativeManager, :spawn_or_deliver, fn _ns, _id, _factory, _payload, if_running ->
      assert if_running == :deliver
      :delivered
    end)

    send_request(channel, instance_id: "w1", factory_id: "worker")

    assert {"ref-spawn", %{outcome: "delivered", error: ""}} = await_response()
  end

  test "a refusal ships its reason so the requester can log something useful", %{channel: channel} do
    expect(OperativeManager, :spawn_or_deliver, fn _ns, _id, _factory, _payload, _policy ->
      {:refused, "quota_exceeded"}
    end)

    send_request(channel, instance_id: "w1", factory_id: "worker")

    assert {"ref-spawn", %{outcome: "refused", error: "quota_exceeded"}} = await_response()
  end

  test "resolution never blocks the channel", %{channel: channel} do
    parent = self()

    expect(OperativeManager, :spawn_or_deliver, fn _ns, _id, _factory, _payload, _policy ->
      send(parent, {:resolving, self()})
      FlezhaTest.Helper.NodeTestHelper.park()
      :spawned
    end)

    send_request(channel, instance_id: "w1", factory_id: "worker")

    assert_receive {:resolving, resolver}, 1_000
    # The channel answers its own calls while the resolution is still running.
    assert %{} = :sys.get_state(channel)
    send(resolver, :unpark)
    assert {"ref-spawn", %{outcome: "spawned"}} = await_response()
  end
end
