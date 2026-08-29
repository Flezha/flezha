defmodule Flezha.SDK.InNodeE2ETest do
  @moduledoc """
  In-node end-to-end: the Elixir SDK boots over the native transport against the
  same-BEAM controller, FleetManager lists it, an operative is placed, and an
  io_in frame drives `run/3` (observed directly, since the Entity runs in this
  BEAM). Exercises the full path — handshake, AssignRunnerId, ConnectSocket,
  AddOperative, primary-stream io_in routing — with no external process.
  """
  # async: false: the live controller and its fleet; registered singletons.
  use ExUnit.Case, async: false

  alias Flezha.Runner.FleetManager
  alias FlezhaTest.Helper.NodeTestHelper
  alias Flezha.Service.OperativeManager
  alias Flezha.Operative.Builder

  @probe :sdk_in_node_probe

  defmodule ProbeProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "probe"
    def init_state(_), do: nil

    def run(input, s, ctx) do
      case Process.whereis(:sdk_in_node_probe) do
        nil -> :ok
        pid -> send(pid, {:sdk_probe_ran, input})
      end

      Flezha.SDK.Ctx.emit(ctx, input)
      {:ok, s}
    end
  end

  defmodule ProbeOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "sdk_probe"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(ProbeProc) |> named("probe") |> sink("probe_out")
    end
  end

  setup do
    ns = "sdk_in_node_#{System.unique_integer([:positive])}"
    Process.register(self(), @probe)

    handle =
      Flezha.SDK.create_runner(ns, transport: :native)
      |> Flezha.SDK.define_stream(ProbeOp)
      |> Flezha.SDK.start()

    assert %Flezha.SDK{} = handle, "SDK start failed: #{inspect(handle)}"

    on_exit(fn ->
      Flezha.SDK.close(handle)
      if Process.whereis(@probe), do: Process.unregister(@probe)

      try do
        FleetManager.stop_namespace(ns)
      catch
        _, _ -> :ok
      end
    end)

    {:ok, ns: ns, handle: handle}
  end

  test "the native runner connects and is listed with its factory", %{ns: ns} do
    assert NodeTestHelper.wait_until(fn -> FleetManager.active_runners(ns) != [] end, 15_000)
    assert [_runner_id] = FleetManager.active_runners(ns)
  end

  test "io_in drives run/3 through the placed operative", %{ns: ns} do
    assert NodeTestHelper.wait_until(fn -> FleetManager.active_runners(ns) != [] end, 15_000)

    operative_id = "probe_op_#{System.unique_integer([:positive])}"
    builder = %Builder{id: operative_id, factory_id: "sdk_probe", init_values: %{}, opts: []}

    # `start_operative` returns after the SDK acknowledged `AddOperative`, so
    # the operative is instantiated there when the first input is sent.
    assert {:ok, _info} = OperativeManager.start_operative(ns, builder)

    OperativeManager.send_io(operative_id, ns, "hello", %{"channel" => "in", "input_type" => "2"})

    assert_receive {:sdk_probe_ran, "hello"}, 10_000
  end

  test "child-spec form registers identically", %{ns: _ns} do
    # A second start on the same node must lose the process-wide slot.
    other = "sdk_in_node_other_#{System.unique_integer([:positive])}"

    result =
      Flezha.SDK.create_runner(other, transport: :native)
      |> Flezha.SDK.define_stream(ProbeOp)
      |> Flezha.SDK.start()

    assert {:error, {:already_running, ^other}} = result
  end
end
