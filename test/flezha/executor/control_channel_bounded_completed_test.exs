defmodule FlezhaTest.Executor.ControlChannelBoundedCompletedTest do
  @moduledoc """
  Exercises the SDK -> Controller `BoundedCompleted` dispatch path added by
  `operative-task-kind` to `Flezha.Executor.ControlChannel`.

  A task-kind unit emits `BoundedCompleted` when its run-once function returns.
  The controller decodes it off the `"control_command"` payload_id (same
  envelope as `ExecutorShutdownNotification`) and
  drives deregister-on-completion via `OperativeManager.stop_operative/2`. A
  terminal failure (`status = "error"`) is torn down identically and
  surfaced via `[:flezha, :bounded, :failed]` telemetry — no retry.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Executor.ControlChannel
  alias Flezha.Service.OperativeManager

  @namespace "ctrl-chan-task-completed-test"

  setup :set_mimic_global

  setup do
    test_pid = self()
    handler_id = "task-completed-test-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:flezha, :bounded, :failed],
      fn event, measurements, metadata, _ ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok
  end

  test "BoundedCompleted ok → drives stop_operative teardown, no failure telemetry" do
    test_pid = self()

    OperativeManager
    |> stub(:stop_operative, fn @namespace, operative_id ->
      send(test_pid, {:stop_operative_called, operative_id})
      :ok
    end)

    {control_channel, _stream_ref} = start_control_channel("exec-task-a")

    payload = build_bounded_completed_payload("task-1", "ok", "", "ref-1")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:stop_operative_called, "task-1"}, 500
    refute_receive {:telemetry, [:flezha, :bounded, :failed], _m, _meta}, 200
  end

  test "BoundedCompleted error → failure telemetry + identical teardown" do
    test_pid = self()

    OperativeManager
    |> stub(:stop_operative, fn @namespace, operative_id ->
      send(test_pid, {:stop_operative_called, operative_id})
      :ok
    end)

    {control_channel, _stream_ref} = start_control_channel("exec-task-b")

    payload = build_bounded_completed_payload("task-2", "error", "boom", "ref-2")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:telemetry, [:flezha, :bounded, :failed], %{count: 1}, metadata}, 500
    assert metadata.operative_id == "task-2"
    assert metadata.error == "boom"

    # Terminal failure deregisters exactly as success — no retry.
    assert_receive {:stop_operative_called, "task-2"}, 500
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

  defp build_bounded_completed_payload(operative_id, status, error, reference_id) do
    request = %ExecutorCommands.CommandRequest{
      reference_id: reference_id,
      command:
        {:bounded_completed,
         %ExecutorCommands.BoundedCompleted{
           operative_id: operative_id,
           status: status,
           error: error
         }}
    }

    {:ok, iodata, _size} = Protox.encode(request)
    IO.iodata_to_binary(iodata)
  end
end
