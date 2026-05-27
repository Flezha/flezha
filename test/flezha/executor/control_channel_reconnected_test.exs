defmodule FlezhaTest.Executor.ControlChannelReconnectedTest do
  @moduledoc """
  Exercises the SDK -> Controller `ExecutorReconnected` dispatch path
  added by `consume-recovery-mode-and-self-heal` §2 to
  `Flezha.Executor.ControlChannel`.

  The frame reuses the existing `CommandRequest` envelope and the
  `"control_command"` payload_id; the controller's dispatcher
  recognises it by the oneof tag and computes either a binding-delta
  (common case: SDK preserved its registry, controller's view still
  agrees) or a re-issue with a fresh executor id (rare: controller
  reassigned mid-disconnect).

  Two scenarios:

    * Bindings exist for the SDK's announced `previous_id` → the
      controller SHALL emit `[:flezha, :executor, :reconnected_delta]`
      and `[:flezha, :executor, :reconnected]` with `mode: :delta`.

    * Bindings are empty for `previous_id` (the controller reassigned
      during the disconnect window) → the controller SHALL emit
      `[:flezha, :executor, :reconnected_reassigned]` and
      `[:flezha, :executor, :reconnected]` with `mode: :reassigned`,
      mint a fresh id, and send `ReassignExecutorId` to the SDK.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Executor.ControlChannel
  alias Flezha.Service.HubManager

  @namespace "ctrl-chan-reconnected-test"

  setup :set_mimic_global

  setup do
    test_pid = self()
    handler_id = "reconnected-test-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler_id,
      [
        [:flezha, :executor, :reconnected],
        [:flezha, :executor, :reconnected_delta],
        [:flezha, :executor, :reconnected_reassigned]
      ],
      fn event, measurements, metadata, _ ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok
  end

  test "bindings exist → delta path emits zero re-issue frames" do
    HubManager
    |> stub(:executor_relays, fn @namespace, "exec-a" ->
      [{"r1", [{node(), self()}]}, {"r2", [{node(), self()}]}]
    end)

    {control_channel, _stream_ref} = start_control_channel("exec-a")

    payload = build_reconnected_payload("exec-a", "exec-a", "control_stream_broken", "ref-1")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:telemetry, [:flezha, :executor, :reconnected_delta], measurements,
                    metadata},
                   500

    assert measurements.added == 0
    assert measurements.removed == 0
    assert measurements.bound == 2
    assert metadata.executor_id == "exec-a"
    assert metadata.reason == "control_stream_broken"

    assert_receive {:telemetry, [:flezha, :executor, :reconnected], _m, %{mode: :delta}}, 500
  end

  test "bindings empty → reassigned path mints new id + emits reassigned telemetry" do
    HubManager
    |> stub(:executor_relays, fn
      @namespace, "exec-b" -> []
    end)

    {control_channel, _stream_ref} = start_control_channel("exec-b")

    payload = build_reconnected_payload("exec-b", "exec-b", "keepalive_timeout", "ref-2")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:telemetry, [:flezha, :executor, :reconnected_reassigned], _m, metadata},
                   500

    assert metadata.executor_id == "exec-b"
    assert metadata.previous_id == "exec-b"
    assert is_binary(metadata.new_id)
    assert metadata.reason == "keepalive_timeout"

    assert_receive {:telemetry, [:flezha, :executor, :reconnected], _m, %{mode: :reassigned}},
                   500
  end

  test "previous_id mismatch with channel's executor_id → reassigned path" do
    HubManager
    |> stub(:executor_relays, fn @namespace, _ -> [] end)

    {control_channel, _stream_ref} = start_control_channel("exec-c")

    # SDK announces a previous_id different from what the
    # ControlChannel is registered under — controller treats this
    # defensively as if it had already reassigned.
    payload = build_reconnected_payload("ghost-id", "ghost-id", "read_error", "ref-3")
    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    assert_receive {:telemetry, [:flezha, :executor, :reconnected_reassigned], _m, metadata},
                   500

    assert metadata.previous_id == "ghost-id"
    assert metadata.executor_id == "exec-c"
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

  defp build_reconnected_payload(previous_id, new_id, reason, reference_id) do
    request = %ExecutorCommands.CommandRequest{
      reference_id: reference_id,
      command:
        {:executor_reconnected,
         %ExecutorCommands.ExecutorReconnected{
           previous_id: previous_id,
           new_id: new_id,
           reason: reason
         }}
    }

    {:ok, iodata, _size} = Protox.encode(request)
    IO.iodata_to_binary(iodata)
  end
end
