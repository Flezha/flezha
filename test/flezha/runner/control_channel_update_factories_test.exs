defmodule FlezhaTest.Runner.ControlChannelUpdateFactoriesTest do
  @moduledoc """
  SDK -> Controller `UpdateFactories` dispatch path
  (`sdk-runner-entrypoint-redesign`): the ControlChannel decodes the frame on
  the SDK-initiated-command seam and casts a full-set `{:sync_factories, ...}`
  to FleetManager — fire-and-forget, no reply frame, never blocking the
  channel.

  Uses the 4-tuple ControlChannel constructor with a capturing `write_fun`,
  mirroring `ControlChannelOpCommitTest`.
  """
  use ExUnit.Case, async: false

  alias Flezha.Runner.{ControlChannel, FleetManager}

  @namespace "ctrl-chan-update-factories-test"

  setup do
    original = :sys.get_state(FleetManager)
    on_exit(fn -> :sys.replace_state(FleetManager, fn _ -> original end) end)
    :ok
  end

  test "UpdateFactories is applied as a full-set FleetManager sync with no reply frame" do
    stream_ref = make_ref()
    runner_id = "uf_runner_#{System.unique_integer([:positive])}"
    test_pid = self()

    write_fun = fn ref, payload ->
      send(test_pid, {:test_writer_called, ref, payload})
      :ok
    end

    {:ok, control_channel} =
      ControlChannel.start_link({@namespace, runner_id, stream_ref, write_fun})

    on_exit(fn ->
      try do
        if Process.alive?(control_channel), do: GenServer.stop(control_channel, :normal, 1_000)
      catch
        :exit, _ -> :ok
      end
    end)

    request = %RunnerCommands.CommandRequest{
      reference_id: "ref-update-factories",
      command:
        {:update_factories,
         %RunnerCommands.UpdateFactories{
           factories: [
             %RunnerCommands.FactoryDecl{id: "alpha", description: "the alpha factory"},
             %RunnerCommands.FactoryDecl{id: "beta", description: ""}
           ]
         }}
    }

    {:ok, iodata, _size} = Protox.encode(request)
    payload = IO.iodata_to_binary(iodata)

    send(control_channel, {:flz_data, :stream_id, "control_command", payload, %{}})

    # Dispatch completed (the cast is on its way); fire-and-forget = no frame
    # written back.
    _ = :sys.get_state(control_channel)
    refute_receive {:test_writer_called, _, _}, 200

    # The cast crosses a process boundary, so poll for the applied set.
    assert eventually(fn ->
             {:ok, factories} = FleetManager.list_factories(@namespace)

             factories == [
               %{id: "alpha", description: "the alpha factory"},
               %{id: "beta", description: nil}
             ]
           end),
           "FleetManager must store the full factory set (empty description as nil)"

    assert FleetManager.runners_for_factory(@namespace, "alpha") == []
  end

  defp eventually(fun, attempts \\ 50) do
    cond do
      fun.() ->
        true

      attempts == 0 ->
        false

      true ->
        Process.sleep(10)
        eventually(fun, attempts - 1)
    end
  end
end
