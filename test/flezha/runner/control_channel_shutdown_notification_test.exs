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
      FlezhaTest.Helper.NodeTestHelper.ensure_stopped(pid, :normal, 1_000)
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
    req = FlezhaTest.Helper.ControlFrameTestHelper.decode_request(framed_payload)

    assert req.reference_id == expected_reference_id

    assert {:runner_shutdown_ack,
            %RunnerCommands.RunnerShutdownAck{
              operative_id: ^expected_operative_id
            }} = req.command
  end
end
