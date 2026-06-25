defmodule FlezhaTest.Executor.ControlChannelTaskCompletedTest do
  @moduledoc """
  Exercises the SDK -> Controller `TaskCompleted` dispatch path added by
  `relay-task-kind` to `Flezha.Executor.ControlChannel`.

  A task-kind unit emits `TaskCompleted` when its run-once function returns.
  The controller decodes it off the `"control_command"` payload_id (same
  envelope as `ExecutorReconnected` / `ExecutorShutdownNotification`) and
  drives deregister-on-completion via `RelayManager.stop_relay/2`. A
  terminal failure (`status = "error"`) is torn down identically and
  surfaced via `[:flezha, :task, :failed]` telemetry — no retry.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Executor.ControlChannel
  alias Flezha.Service.RelayManager

  @namespace "ctrl-chan-task-completed-test"

  setup :set_mimic_global

  setup do
    test_pid = self()
    handler_id = "task-completed-test-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:flezha, :task, :failed],
      fn event, measurements, metadata, _ ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok
  end

  test "TaskCompleted ok → drives stop_relay teardown, no failure telemetry" do
    test_pid = self()

    RelayManager
    |> stub(:stop_relay, fn @namespace, relay_id ->
      send(test_pid, {:stop_relay_called, relay_id})
      :ok
    end)

    {control_channel, _stream_ref} = start_control_channel("exec-task-a")

    payload = build_task_completed_payload("task-1", "ok", "", "ref-1")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:stop_relay_called, "task-1"}, 500
    refute_receive {:telemetry, [:flezha, :task, :failed], _m, _meta}, 200
  end

  test "TaskCompleted error → failure telemetry + identical teardown" do
    test_pid = self()

    RelayManager
    |> stub(:stop_relay, fn @namespace, relay_id ->
      send(test_pid, {:stop_relay_called, relay_id})
      :ok
    end)

    {control_channel, _stream_ref} = start_control_channel("exec-task-b")

    payload = build_task_completed_payload("task-2", "error", "boom", "ref-2")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:telemetry, [:flezha, :task, :failed], %{count: 1}, metadata}, 500
    assert metadata.relay_id == "task-2"
    assert metadata.error == "boom"

    # Terminal failure deregisters exactly as success — no retry.
    assert_receive {:stop_relay_called, "task-2"}, 500
  end

  defp start_control_channel(suffix) do
    stream_ref = make_ref()
    executor_id = suffix
    test_pid = self()

    write_fun = fn ref, payload ->
      send(test_pid, {:test_writer_called, ref, payload})
      :ok
    end

    {:ok, pid} =
      ControlChannel.start_link({@namespace, executor_id, stream_ref, write_fun})

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid, :normal, 1_000)
    end)

    {pid, stream_ref}
  end

  defp build_task_completed_payload(relay_id, status, error, reference_id) do
    request = %ExecutorCommands.CommandRequest{
      reference_id: reference_id,
      command:
        {:task_completed,
         %ExecutorCommands.TaskCompleted{
           relay_id: relay_id,
           status: status,
           error: error
         }}
    }

    {:ok, iodata, _size} = Protox.encode(request)
    IO.iodata_to_binary(iodata)
  end
end
