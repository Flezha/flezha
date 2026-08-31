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
