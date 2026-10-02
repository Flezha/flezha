defmodule FlezhaTest.Runner.ControlChannelOperatorCompletedTest do
  @moduledoc """
  Exercises the SDK -> Controller `OperatorCompleted` dispatch path added by
  `operator-task-kind` to `Flezha.Runner.ControlChannel`.

  A task-kind unit emits `OperatorCompleted` when its run-once function returns.
  The controller decodes it off the `"control_command"` payload_id (same
  envelope as `RunnerShutdownNotification`) and
  drives deregister-on-completion via `OperatorManager.stop_operator/3`. A
  terminal failure (`status = "error"`) is torn down identically and
  surfaced via `[:flezha, :steps, :failed]` telemetry — no retry.
  """

  # async: false: writes Application env; Mimic global mode; Mimic stubs.

  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.StreamPool
  alias Flezha.Service.OperatorManager
  alias Flezha.State.Completion
  alias FlezhaTest.Helper.ControlFrameTestHelper
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "ctrl-chan-task-completed-test"

  setup :set_mimic_global_until_exit

  setup do
    test_pid = self()
    handler_id = "task-completed-test-#{System.unique_integer([:positive])}"

    # Every exit trigger emits `:completed`; only a terminal failure also emits
    # `:failed`. Attach both so a test can assert convergence + the result payload.
    :telemetry.attach_many(
      handler_id,
      [[:flezha, :steps, :completed], [:flezha, :steps, :failed]],
      fn event, measurements, metadata, _ ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok
  end

  test "OperatorCompleted ok → drives stop_operator teardown + result, no failure telemetry" do
    test_pid = self()

    OperatorManager
    |> stub(:stop_operator, fn @namespace, operator_id, [reason: :completed] ->
      send(test_pid, {:stop_operator_called, operator_id})
      :ok
    end)

    {control_channel, _stream_ref} = start_control_channel("exec-task-a")

    payload = build_operator_completed_payload("task-1", "ok", "", "the-answer", "ref-1")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:stop_operator_called, "task-1"}, 500

    # The terminal outcome + result payload is surfaced on `:completed`.
    assert_receive {:telemetry, [:flezha, :steps, :completed], %{count: 1}, meta}, 500
    assert meta.operator_id == "task-1"
    assert meta.status == "ok"
    assert meta.result == "the-answer"

    refute_received {:telemetry, [:flezha, :steps, :failed], _m, _meta}
  end

  test "OperatorCompleted error → failure telemetry + identical teardown" do
    test_pid = self()

    OperatorManager
    |> stub(:stop_operator, fn @namespace, operator_id, [reason: :completed] ->
      send(test_pid, {:stop_operator_called, operator_id})
      :ok
    end)

    {control_channel, _stream_ref} = start_control_channel("exec-task-b")

    payload = build_operator_completed_payload("task-2", "error", "boom", "", "ref-2")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:telemetry, [:flezha, :steps, :failed], %{count: 1}, metadata}, 500
    assert metadata.operator_id == "task-2"
    assert metadata.error == "boom"

    # Errors also emit the converged `:completed`.
    assert_receive {:telemetry, [:flezha, :steps, :completed], %{count: 1}, %{status: "error"}},
                   500

    # Terminal failure deregisters exactly as success — no retry.
    assert_receive {:stop_operator_called, "task-2"}, 500
  end

  test "OperatorCompleted cancelled (cooperative stop) → same converged teardown + result" do
    test_pid = self()

    OperatorManager
    |> stub(:stop_operator, fn @namespace, operator_id, [reason: :completed] ->
      send(test_pid, {:stop_operator_called, operator_id})
      :ok
    end)

    {control_channel, _stream_ref} = start_control_channel("exec-task-c")

    payload = build_operator_completed_payload("task-3", "cancelled", "", "partial", "ref-3")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    # A cooperative external stop converges on the identical teardown path.
    assert_receive {:stop_operator_called, "task-3"}, 500

    assert_receive {:telemetry, [:flezha, :steps, :completed], %{count: 1}, meta}, 500
    assert meta.status == "cancelled"
    assert meta.result == "partial"

    # Cancellation is not a failure.
    refute_received {:telemetry, [:flezha, :steps, :failed], _m, _meta}
  end

  test "the teardown flushes the runner's stream channels before it deregisters" do
    test_pid = self()

    # The barrier, held open: nothing may be deregistered while an output the
    # transport has already delivered could still be queued at a channel.
    StreamPool
    |> stub(:flush_channels, fn runner_id ->
      send(test_pid, {:flushing, runner_id, self()})

      receive do
        :release -> :ok
      end
    end)

    OperatorManager
    |> stub(:stop_operator, fn @namespace, operator_id, [reason: :completed] ->
      send(test_pid, {:stop_operator_called, operator_id})
      :ok
    end)

    {control_channel, _stream_ref} = start_control_channel("exec-task-direct")

    payload = build_operator_completed_payload("task-direct", "ok", "", "", "ref-direct")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    # The barrier is driven for the runner this completion arrived on...
    assert_receive {:flushing, "exec-task-direct", teardown}, 1_000
    refute_received {:stop_operator_called, "task-direct"}

    # ...and the teardown proceeds only once it answers.
    send(teardown, :release)
    assert_receive {:stop_operator_called, "task-direct"}, 1_000
  end

  test "OperatorCompleted writes a durable completion record for finished-job retention" do
    test_pid = self()

    OperatorManager
    |> stub(:stop_operator, fn @namespace, operator_id, [reason: :completed] ->
      send(test_pid, {:stop_operator_called, operator_id})
      :ok
    end)

    {control_channel, _stream_ref} = start_control_channel("exec-task-rec")

    before = System.system_time(:second)
    payload = build_operator_completed_payload("task-rec", "ok", "", "the-answer", "ref-rec")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    # The record is written synchronously in `handle_operator_completed` BEFORE the
    # off-process teardown, so once teardown is observed the record must exist.
    assert_receive {:stop_operator_called, "task-rec"}, 500

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

  defp build_operator_completed_payload(operator_id, status, error, result, reference_id) do
    ControlFrameTestHelper.encode_request(
      reference_id,
      {:operator_completed,
       %RunnerCommands.OperatorCompleted{
         operator_id: operator_id,
         status: status,
         error: error,
         result: result
       }}
    )
  end
end

defmodule FlezhaTest.Runner.ControlChannelSpawnStepsTest do
  @moduledoc """
  SDK -> Controller `SpawnStepsRequest` dispatch
  (`tasklet-requested-spawn`): the ControlChannel decodes the frame on the
  SDK-initiated-command seam, resolves it off-process against the channel's own
  namespace, and writes one `SpawnStepsResponse` back under the request's
  reference id.

  Uses the 4-tuple ControlChannel constructor with a capturing `write_fun`,
  mirroring `ControlChannelUpdateFactoriesTest`.
  """
  # async: false: Mimic global mode; Mimic stubs; registered runner channels.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Runner.ControlChannel
  alias Flezha.Service.OperatorManager
  alias FlezhaTest.Helper.ControlFrameTestHelper, as: ControlFrame
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "ctrl-chan-spawn-steps-test"

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
    command = {:spawn_steps_request, struct(RunnerCommands.SpawnStepsRequest, fields)}
    payload = ControlFrame.encode_request("ref-spawn", command)
    send(channel, {:flz_data, :stream_id, "control_command", payload, %{}})
  end

  defp await_response do
    assert_receive {:written, _ref, framed}, 2_000

    %RunnerCommands.CommandRequest{
      reference_id: reference_id,
      command: {:spawn_steps_response, response}
    } = ControlFrame.decode_request(framed)

    {reference_id, response}
  end

  test "a request resolves against the channel's namespace and answers its outcome", %{
    channel: channel
  } do
    test_pid = self()

    expect(OperatorManager, :spawn_or_deliver, fn ns,
                                                  id,
                                                  factory,
                                                  payload,
                                                  if_running,
                                                  caller,
                                                  _identity ->
      send(test_pid, {:resolved, ns, id, factory, payload, if_running, caller})
      {:spawned, id}
    end)

    send_request(channel,
      instance_id: "w1",
      factory_id: "worker",
      payload: "p1",
      if_running: "deliver"
    )

    assert_receive {:resolved, @namespace, "w1", "worker", "p1", :deliver, caller}
    # The identity is the connection's, never anything the frame said.
    assert %{source: :sdk, namespace: @namespace, principal: "runner@" <> @namespace} = caller
    assert {"ref-spawn", %{outcome: "spawned", error: nil, instance_id: "w1"}} = await_response()
  end

  test "a request without an id answers the id the controller generated", %{channel: channel} do
    expect(OperatorManager, :spawn_or_deliver, fn _ns,
                                                  "",
                                                  _factory,
                                                  _payload,
                                                  _policy,
                                                  _,
                                                  _identity ->
      {:spawned, "worker-k7m2qzxw4pd3fn2a"}
    end)

    send_request(channel, instance_id: "", factory_id: "worker")

    assert {"ref-spawn", %{outcome: "spawned", instance_id: "worker-k7m2qzxw4pd3fn2a"}} =
             await_response()
  end

  test "the error policy is carried through verbatim", %{channel: channel} do
    expect(OperatorManager, :spawn_or_deliver, fn _ns,
                                                  id,
                                                  _factory,
                                                  _payload,
                                                  :error,
                                                  _,
                                                  _identity ->
      {:already_running, id}
    end)

    send_request(channel, instance_id: "w1", factory_id: "worker", if_running: "error")

    assert {"ref-spawn", %{outcome: "already_running", error: nil, instance_id: "w1"}} =
             await_response()
  end

  test "an unset policy reads as deliver rather than refusing the payload", %{channel: channel} do
    expect(OperatorManager, :spawn_or_deliver, fn _ns,
                                                  _id,
                                                  _factory,
                                                  _payload,
                                                  if_running,
                                                  _,
                                                  _identity ->
      assert if_running == :deliver
      {:delivered, "w1"}
    end)

    send_request(channel, instance_id: "w1", factory_id: "worker")

    assert {"ref-spawn", %{outcome: "delivered", error: nil}} = await_response()
  end

  test "a refusal ships its reason so the requester can log something useful", %{channel: channel} do
    expect(OperatorManager, :spawn_or_deliver, fn _ns,
                                                  id,
                                                  _factory,
                                                  _payload,
                                                  _policy,
                                                  _,
                                                  _identity ->
      {{:refused,
        Flezha.Error.new("quota.exceeded", kind: "max_operators", current: 1, limit: 1)}, id}
    end)

    send_request(channel, instance_id: "w1", factory_id: "worker")

    assert {"ref-spawn", %{outcome: "", error: %{code: "quota.exceeded", details_json: details}}} =
             await_response()

    assert Jason.decode!(details) == %{"kind" => "max_operators", "current" => 1, "limit" => 1}
  end

  test "resolution never blocks the channel", %{channel: channel} do
    parent = self()

    expect(OperatorManager, :spawn_or_deliver, fn _ns,
                                                  _id,
                                                  _factory,
                                                  _payload,
                                                  _policy,
                                                  _,
                                                  _identity ->
      send(parent, {:resolving, self()})
      NodeTestHelper.park()
      {:spawned, "w1"}
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
      (the operator migrated to a peer controller). The ControlChannel SHALL
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
    |> stub(:get_pid, fn @namespace, "p_missing" ->
      {:error, {:operator_not_found, @namespace, "p_missing"}}
    end)

    {control_channel, stream_ref} = start_control_channel("ce_missing")

    payload = build_notification_payload("p_missing", "stale_state", "ref-missing")

    # `config/test.exs` pins the primary Logger level to `:error`, which drops
    # the warning before any capture handler sees it, so it is lowered here.
    on_exit(NodeTestHelper.override_log_level(:warning))

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

  defp build_notification_payload(operator_id, serialized_state, reference_id) do
    ControlFrameTestHelper.encode_request(
      reference_id,
      {:runner_shutdown_notification,
       %RunnerCommands.RunnerShutdownNotification{
         operator_id: operator_id,
         serialized_state: serialized_state
       }}
    )
  end

  defp assert_ack_for(framed_payload, expected_operator_id, expected_reference_id) do
    req = ControlFrameTestHelper.decode_request(framed_payload)

    assert req.reference_id == expected_reference_id

    assert {:runner_shutdown_ack,
            %RunnerCommands.RunnerShutdownAck{
              operator_id: ^expected_operator_id
            }} = req.command
  end
end
