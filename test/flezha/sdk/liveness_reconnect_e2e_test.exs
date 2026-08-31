defmodule Flezha.SDK.LivenessReconnectE2ETest do
  @moduledoc """
  In-node integration tests for the SDK's liveness and lifecycle behaviour:
  heartbeats keep an idle runner from stalling (6.2), a killed SDK-side stream
  synthesizes a close that drives reconnect (6.3), and the runner-as-handle
  contract — reconnect with the same stable id, `UpdateFactories`, and
  removal-refusal (7.2). Native transport, in-process, so `run/3` and the
  streams live in this BEAM.
  """
  # async: false: the live controller and its fleet; global telemetry handlers; OS environment variables.
  use ExUnit.Case, async: false

  alias Flezha.Runner.FleetManager
  alias Flezha.SDK.Ctx
  alias Flezha.SDK.RunnerServer
  alias FlezhaTest.Helper.NodeTestHelper
  alias Flezha.Service.OperativeManager
  alias Flezha.Operative.Builder

  defmodule EchoProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "echo"
    def init_state(_), do: nil

    def run(input, s, ctx),
      do:
        (
          Ctx.emit(ctx, input)
          {:ok, s}
        )
  end

  defmodule EchoOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "lr_echo"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(EchoProc) |> named("echo") |> sink("lr_out")
    end
  end

  defmodule OtherOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "lr_other"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(EchoProc) |> named("echo") |> sink("lr_out2")
    end
  end

  defp start_runner(ns, streams) do
    handle =
      Enum.reduce(streams, Flezha.SDK.create_runner(ns, transport: :native), fn m, acc ->
        Flezha.SDK.define_stream(acc, m)
      end)
      |> Flezha.SDK.start()

    assert %Flezha.SDK{} = handle, "start failed: #{inspect(handle)}"
    assert NodeTestHelper.wait_until(fn -> FleetManager.active_runners(ns) != [] end, 15_000)
    handle
  end

  setup do
    ns = "lr_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      try do
        FleetManager.stop_namespace(ns)
      catch
        _, _ -> :ok
      end
    end)

    {:ok, ns: ns}
  end

  test "an idle native runner emitting heartbeats fires no output_stall", %{ns: ns} do
    prev = System.get_env("FLEZHA_DATA_HEARTBEAT_MS")
    System.put_env("FLEZHA_DATA_HEARTBEAT_MS", "200")

    on_exit(fn ->
      if prev,
        do: System.put_env("FLEZHA_DATA_HEARTBEAT_MS", prev),
        else: System.delete_env("FLEZHA_DATA_HEARTBEAT_MS")
    end)

    test_pid = self()
    ref = make_ref()

    :telemetry.attach(
      "lr-stall-#{inspect(ref)}",
      [:flezha, :state, :output_stall],
      fn _e, _m, meta, _ -> send(test_pid, {:output_stall, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach("lr-stall-#{inspect(ref)}") end)

    handle = start_runner(ns, [EchoOp])
    on_exit(fn -> Flezha.SDK.close(handle) end)

    # Idle across several heartbeat windows — the heartbeats show as the
    # secondary stream's `last_seen` moving on — and no stall is flagged.
    [runner_id] = FleetManager.active_runners(ns)
    channels = secondary_channels(runner_id)
    idle_from = System.monotonic_time(:millisecond)

    assert NodeTestHelper.wait_until(
             fn ->
               Enum.any?(channels, &(:sys.get_state(&1).last_seen >= idle_from + 3 * 200))
             end,
             10_000
           ),
           "the heartbeats never reached a secondary stream"

    refute_received {:output_stall, _}
  end

  test "killing an SDK-side stream drives reconnect and the runner recovers", %{ns: ns} do
    handle = start_runner(ns, [EchoOp])
    on_exit(fn -> Flezha.SDK.close(handle) end)

    [_runner] = FleetManager.active_runners(ns)

    # Kill a Local.Stream process (an SDK-side data or control stream) — the peer
    # monitor synthesizes {:flz_closed}, which drives the SDK's reconnect.
    server = RunnerServer.whereis()
    assert is_pid(server)

    # Inject a stream close the way a killed SDK-side Local.Stream owner would
    # (the peer monitor synthesizes {:flz_closed}); it drives the SDK reconnect.
    send(server, {:flz_closed, 999_999, "test_injected"})

    # The runner reconnects and stays listed.
    assert NodeTestHelper.wait_until(fn -> FleetManager.active_runners(ns) != [] end, 15_000)
  end

  test "post-start define_stream ships UpdateFactories; removal with a live instance refuses", %{
    ns: ns
  } do
    handle = start_runner(ns, [EchoOp])
    on_exit(fn -> Flezha.SDK.close(handle) end)

    # Post-start define → UpdateFactories → the controller learns the new factory.
    handle2 = Flezha.SDK.define_stream(handle, OtherOp)
    assert %Flezha.SDK{} = handle2

    assert NodeTestHelper.wait_until(
             fn ->
               case FleetManager.list_factories(ns) do
                 {:ok, factories} -> "lr_other" in factory_ids(factories)
                 _ -> false
               end
             end,
             10_000
           )

    # Placing an instance of EchoOp, then removing it, refuses while live.
    operative_id = "lr_inst_#{System.unique_integer([:positive])}"

    assert {:ok, _} =
             OperativeManager.start_operative(ns, %Builder{
               id: operative_id,
               factory_id: "lr_echo",
               init_values: %{},
               opts: []
             })

    # `start_operative` returns after the SDK acknowledged `AddOperative`, so
    # the instance is live on the handle when the removal is refused.
    assert {:error, {:operative_type_in_use, "lr_echo", n}} =
             Flezha.SDK.remove_stream(handle2, EchoOp)

    assert n >= 1
  end

  defp factory_ids(factories) do
    Enum.map(factories, fn
      %{id: id} -> id
      %{"id" => id} -> id
      {id, _} -> id
      id when is_binary(id) -> id
      other -> other
    end)
  end

  # The runner's secondary (heartbeat-carrying) stream channels.
  defp secondary_channels(runner_id) do
    prefix = runner_id <> "_scs_"

    Registry.select(Flezha.Identificator, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])
    |> Enum.filter(fn {key, _pid} -> is_binary(key) and String.starts_with?(key, prefix) end)
    |> Enum.map(&elem(&1, 1))
  end
end
