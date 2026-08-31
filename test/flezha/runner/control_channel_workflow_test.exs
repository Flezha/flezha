defmodule FlezhaTest.Runner.ControlChannelWorkflowCompletedTest do
  @moduledoc """
  Exercises the SDK -> Controller `WorkflowCompleted` dispatch path added by
  `operative-task-kind` to `Flezha.Runner.ControlChannel`.

  A task-kind unit emits `WorkflowCompleted` when its run-once function returns.
  The controller decodes it off the `"control_command"` payload_id (same
  envelope as `RunnerShutdownNotification`) and
  drives deregister-on-completion via `OperativeManager.stop_operative/2`. A
  terminal failure (`status = "error"`) is torn down identically and
  surfaced via `[:flezha, :workflow, :failed]` telemetry — no retry.
  """

  # async: false: writes Application env; Mimic global mode; Mimic stubs.

  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Runner.ControlChannel
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Completion
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "ctrl-chan-task-completed-test"

  setup :set_mimic_global_until_exit

  setup do
    # Isolate the completion record `handle_workflow_completed/2` now writes.
    base = TestHelpers.tmp_app_data_dir()
    test_pid = self()
    handler_id = "task-completed-test-#{System.unique_integer([:positive])}"

    # Every exit trigger emits `:completed`; only a terminal failure also emits
    # `:failed`. Attach both so a test can assert convergence + the result payload.
    :telemetry.attach_many(
      handler_id,
      [[:flezha, :workflow, :completed], [:flezha, :workflow, :failed]],
      fn event, measurements, metadata, _ ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    :ok
  end

  test "WorkflowCompleted ok → drives stop_operative teardown + result, no failure telemetry" do
    test_pid = self()

    OperativeManager
    |> stub(:stop_operative, fn @namespace, operative_id ->
      send(test_pid, {:stop_operative_called, operative_id})
      :ok
    end)

    {control_channel, _stream_ref} = start_control_channel("exec-task-a")

    payload = build_workflow_completed_payload("task-1", "ok", "", "the-answer", "ref-1")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:stop_operative_called, "task-1"}, 500

    # The terminal outcome + result payload is surfaced on `:completed`.
    assert_receive {:telemetry, [:flezha, :workflow, :completed], %{count: 1}, meta}, 500
    assert meta.operative_id == "task-1"
    assert meta.status == "ok"
    assert meta.result == "the-answer"

    refute_received {:telemetry, [:flezha, :workflow, :failed], _m, _meta}
  end

  test "WorkflowCompleted error → failure telemetry + identical teardown" do
    test_pid = self()

    OperativeManager
    |> stub(:stop_operative, fn @namespace, operative_id ->
      send(test_pid, {:stop_operative_called, operative_id})
      :ok
    end)

    {control_channel, _stream_ref} = start_control_channel("exec-task-b")

    payload = build_workflow_completed_payload("task-2", "error", "boom", "", "ref-2")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:telemetry, [:flezha, :workflow, :failed], %{count: 1}, metadata}, 500
    assert metadata.operative_id == "task-2"
    assert metadata.error == "boom"

    # Errors also emit the converged `:completed`.
    assert_receive {:telemetry, [:flezha, :workflow, :completed], %{count: 1},
                    %{status: "error"}},
                   500

    # Terminal failure deregisters exactly as success — no retry.
    assert_receive {:stop_operative_called, "task-2"}, 500
  end

  test "WorkflowCompleted cancelled (cooperative stop) → same converged teardown + result" do
    test_pid = self()

    OperativeManager
    |> stub(:stop_operative, fn @namespace, operative_id ->
      send(test_pid, {:stop_operative_called, operative_id})
      :ok
    end)

    {control_channel, _stream_ref} = start_control_channel("exec-task-c")

    payload = build_workflow_completed_payload("task-3", "cancelled", "", "partial", "ref-3")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    # A cooperative external stop converges on the identical teardown path.
    assert_receive {:stop_operative_called, "task-3"}, 500

    assert_receive {:telemetry, [:flezha, :workflow, :completed], %{count: 1}, meta}, 500
    assert meta.status == "cancelled"
    assert meta.result == "partial"

    # Cancellation is not a failure.
    refute_received {:telemetry, [:flezha, :workflow, :failed], _m, _meta}
  end

  test "the teardown waits until the Overwatch has run the completion's direct outputs" do
    test_pid = self()

    # A stand-in Overwatch: answers `:get_overwatch_state` with how many
    # direct outputs it has run, bumped on demand.
    overwatch =
      spawn_link(fn ->
        Stream.iterate(0, & &1)
        |> Enum.reduce_while(0, fn _, seen ->
          receive do
            {:"$gen_call", from, :get_overwatch_state} ->
              GenServer.reply(from, %{direct_outputs_seen: seen})
              {:cont, seen}

            :output_ran ->
              {:cont, seen + 1}

            :stop ->
              {:halt, seen}
          end
        end)
      end)

    OperativeManager
    |> stub(:get_operative_pid, fn @namespace, "task-direct" -> {:ok, overwatch} end)
    |> stub(:stop_operative, fn @namespace, operative_id ->
      send(test_pid, {:stop_operative_called, operative_id})
      :ok
    end)

    {control_channel, _stream_ref} = start_control_channel("exec-task-direct")
    # The instance wrote one output to the data stream before completing.
    payload = build_workflow_completed_payload("task-direct", "ok", "", "", "ref-direct", 1)
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    # Not torn down while that output is still on its way: the channel has
    # handled the completion once it answers a call.
    _ = :sys.get_state(control_channel)
    refute_received {:stop_operative_called, "task-direct"}

    # ...and torn down once the Overwatch has run it.
    send(overwatch, :output_ran)
    assert_receive {:stop_operative_called, "task-direct"}, 1_000
    send(overwatch, :stop)
  end

  test "WorkflowCompleted writes a durable completion record for finished-job retention" do
    test_pid = self()

    OperativeManager
    |> stub(:stop_operative, fn @namespace, operative_id ->
      send(test_pid, {:stop_operative_called, operative_id})
      :ok
    end)

    {control_channel, _stream_ref} = start_control_channel("exec-task-rec")

    before = System.system_time(:second)
    payload = build_workflow_completed_payload("task-rec", "ok", "", "the-answer", "ref-rec")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    # The record is written synchronously in `handle_workflow_completed` BEFORE the
    # off-process teardown, so once teardown is observed the record must exist.
    assert_receive {:stop_operative_called, "task-rec"}, 500

    assert {:ok, rec} = Completion.read("task-rec")
    assert rec.namespace == @namespace
    assert rec.status == "ok"
    assert rec.result == "the-answer"
    assert is_integer(rec.finished_at) and rec.finished_at >= before
  end

  defp start_control_channel(suffix) do
    stream_ref = make_ref()
    runner_id = suffix
    test_pid = self()

    write_fun = fn ref, _payload_id, message, _metadata ->
      send(test_pid, {:test_writer_called, ref, message})
      :ok
    end

    {:ok, pid} =
      ControlChannel.start_link({@namespace, runner_id, stream_ref, write_fun})

    on_exit(fn ->
      NodeTestHelper.ensure_stopped(pid, :normal, 1_000)
    end)

    {pid, stream_ref}
  end

  defp build_workflow_completed_payload(
         operative_id,
         status,
         error,
         result,
         reference_id,
         direct_outputs \\ 0
       ) do
    request = %RunnerCommands.CommandRequest{
      reference_id: reference_id,
      command:
        {:workflow_completed,
         %RunnerCommands.WorkflowCompleted{
           operative_id: operative_id,
           status: status,
           error: error,
           result: result,
           direct_outputs: direct_outputs
         }}
    }

    {:ok, iodata, _size} = Protox.encode(request)
    IO.iodata_to_binary(iodata)
  end
end

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
  alias FlezhaTest.Helper.NodeTestHelper

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
        NodeTestHelper.ensure_stopped(channel, :normal, 1_000)
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
      NodeTestHelper.park()
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

defmodule FlezhaTest.Runner.ControlChannelShutdownNotificationTest do
  @moduledoc """
  Exercises the SDK -> Controller `RunnerShutdownNotification` dispatch path
  added by ship-fresh-state-on-sdk-shutdown to
  `Flezha.Runner.ControlChannel`.

  The notification reuses the existing `CommandRequest` envelope and the
  `"control_command"` payload_id, so the controller's dispatcher peeks at the
  oneof tag to recognise it. Two scenarios:

    * Present Overwatch — `HubManager.get_pid/2` returns `{:ok, pid}`. The
      ControlChannel SHALL cast `{:apply_external_snapshot, binary}` to
      that pid and SHALL emit `RunnerShutdownAck` on the stream.
    * Missing Overwatch — `HubManager.get_pid/2` returns `{:error, _}`
      (the operative migrated to a peer controller). The ControlChannel SHALL
      log a warning and SHALL still emit the ack so the SDK never blocks
      on a non-existent target.

  Because `Flezha.Transport.Native` is a NIF (Mimic can't copy NIF
  modules — the renamed `@on_load` fails to find the library), the test
  uses ControlChannel's 4-tuple constructor to inject a capturing
  `write_fun` instead of mocking `Native.send_data/2`.
  """

  # async: false: Mimic global mode; Mimic stubs; the live controller and its fleet.

  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Runner.ControlChannel
  alias Flezha.Service.HubManager
  alias FlezhaTest.Helper.ControlFrameTestHelper
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "ctrl-chan-shutdown-test"

  setup :set_mimic_global_until_exit

  setup do
    # `config/test.exs` pins the global Logger level to `:error`. The
    # ControlChannel emits the missing-Overwatch path at `:warning`, so
    # without lowering the per-module level here the message would be
    # filtered before any backend (including ExUnit.CaptureLog's) saw
    # it. Restore on exit so other tests are not affected.
    Logger.put_module_level(Flezha.Runner.ControlChannel, :warning)

    on_exit(fn ->
      Logger.delete_module_level(Flezha.Runner.ControlChannel)
    end)

    :ok
  end

  defmodule FakeOverwatch do
    use GenServer

    def start_link(forward_to), do: GenServer.start_link(__MODULE__, forward_to)

    @impl true
    def init(forward_to), do: {:ok, %{forward_to: forward_to}}

    @impl true
    def handle_cast({:apply_external_snapshot, binary}, state) do
      send(state.forward_to, {:overwatch_received_snapshot, binary})
      {:noreply, state}
    end
  end

  test "present Overwatch: cast is delivered and ack is emitted" do
    {:ok, fake_pid} = FakeOverwatch.start_link(self())

    HubManager
    |> stub(:get_pid, fn @namespace, "p1" -> {:ok, fake_pid} end)

    {control_channel, stream_ref} = start_control_channel("ce_present")

    payload = build_notification_payload("p1", "fresh_state_binary", "ref-present")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:overwatch_received_snapshot, "fresh_state_binary"}, 500

    assert_receive {:test_writer_called, ^stream_ref, ack_payload}, 500
    assert_ack_for(ack_payload, "p1", "ref-present")
  end

  test "missing Overwatch: logs warning, no cast delivered, ack still emitted" do
    HubManager
    |> stub(:get_pid, fn @namespace, "p_missing" -> {:error, "no pid found"} end)

    {control_channel, stream_ref} = start_control_channel("ce_missing")

    payload = build_notification_payload("p_missing", "stale_state", "ref-missing")

    # `config/test.exs` pins the global Logger level to `:error`, so a
    # `Logger.warning/1` call is filtered out by default. Override here
    # so the capture sees the "Overwatch missing" warning we assert on.
    log =
      ExUnit.CaptureLog.capture_log([level: :warning], fn ->
        send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})
        assert_receive {:test_writer_called, ^stream_ref, ack_payload}, 500
        assert_ack_for(ack_payload, "p_missing", "ref-missing")
      end)

    assert log =~ "Overwatch missing for shutdown notification"
    assert log =~ "p_missing"

    refute_received {:overwatch_received_snapshot, _}
  end

  defp start_control_channel(suffix) do
    stream_ref = make_ref()
    runner_id = "#{@namespace}_#{suffix}_#{System.unique_integer([:positive])}"
    test_pid = self()

    write_fun = fn ref, _payload_id, message, _metadata ->
      send(test_pid, {:test_writer_called, ref, message})
      :ok
    end

    {:ok, pid} =
      ControlChannel.start_link({@namespace, runner_id, stream_ref, write_fun})

    on_exit(fn ->
      NodeTestHelper.ensure_stopped(pid, :normal, 1_000)
    end)

    {pid, stream_ref}
  end

  defp build_notification_payload(operative_id, serialized_state, reference_id) do
    request = %RunnerCommands.CommandRequest{
      reference_id: reference_id,
      command:
        {:runner_shutdown_notification,
         %RunnerCommands.RunnerShutdownNotification{
           operative_id: operative_id,
           serialized_state: serialized_state
         }}
    }

    {:ok, iodata, _size} = Protox.encode(request)
    IO.iodata_to_binary(iodata)
  end

  defp assert_ack_for(framed_payload, expected_operative_id, expected_reference_id) do
    req = ControlFrameTestHelper.decode_request(framed_payload)

    assert req.reference_id == expected_reference_id

    assert {:runner_shutdown_ack,
            %RunnerCommands.RunnerShutdownAck{
              operative_id: ^expected_operative_id
            }} = req.command
  end
end
