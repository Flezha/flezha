defmodule Flezha.SDK.SocketRoundtripTest do
  @moduledoc """
  Exercises the standalone-mode client transport (the `flz_nif` client entry
  points) end to end within a single node: the SDK dials the controller's own
  UDS listener over the binary protocol — the same handshake, per-runner data
  sockets and framing a rust_runner uses — and drives an io_in through to
  `run/3`. Proves the client NIFs, the reader/writer threads and the `:socket`
  transport seam without a second OS process.
  """
  # async: false: the live controller and its fleet; registered singletons; OS environment variables.
  use ExUnit.Case, async: false

  alias Flezha.Runner.FleetManager
  alias FlezhaTest.Helper.NodeTestHelper
  alias Flezha.Service.OperativeManager
  alias Flezha.Operative.Builder

  @probe :sdk_socket_probe

  defmodule ProbeProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "probe"
    def init_state(_), do: nil

    def run(input, s, ctx) do
      case Process.whereis(:sdk_socket_probe) do
        nil -> :ok
        pid -> send(pid, {:sdk_socket_ran, input})
      end

      Flezha.SDK.Ctx.emit(ctx, input)
      {:ok, s}
    end
  end

  defmodule ProbeOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "sdk_socket_probe_factory"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(ProbeProc) |> named("probe") |> sink("probe_out")
    end
  end

  test "the SDK and the controller resolve one directory, from the one ladder" do
    # The Elixir SDK derives no path of its own: both roles ask
    # `flz_transport` through the NIF, which is the same code a Rust, Java or
    # Node runner reaches through `flz_core`. That a Rust runner started in the
    # same environment lands on the same directory is proven end to end by
    # `test/system/connect_discovery_e2e_test.exs`, under every runtime.
    configured = Flezha.Socket.Support.socket_dir()

    assert {:ok, runner_dir, _quic} =
             Flezha.Transport.Native.resolve_connect("runner", configured, nil)

    assert {:ok, controller_dir, _quic} =
             Flezha.Transport.Native.resolve_connect("controller", configured, nil)

    assert runner_dir == controller_dir
    assert runner_dir == configured
    assert Path.type(runner_dir) == :absolute
  end

  setup do
    # The standalone SDK reads FLEZHA_SOCKET_FILES_PATH; point it at the
    # controller's own listener path so the same-node dial finds it.
    prev = System.get_env("FLEZHA_SOCKET_FILES_PATH")

    System.put_env("FLEZHA_SOCKET_FILES_PATH", Flezha.Socket.Support.socket_dir())

    ns = "sdk_socket_#{System.unique_integer([:positive])}"
    Process.register(self(), @probe)

    handle =
      Flezha.SDK.create_runner(ns, transport: :uds)
      |> Flezha.SDK.define_stream(ProbeOp)
      |> Flezha.SDK.start()

    assert %Flezha.SDK{} = handle, "SDK (uds) start failed: #{inspect(handle)}"

    on_exit(fn ->
      Flezha.SDK.close(handle)
      if Process.whereis(@probe), do: Process.unregister(@probe)

      if prev,
        do: System.put_env("FLEZHA_SOCKET_FILES_PATH", prev),
        else: System.delete_env("FLEZHA_SOCKET_FILES_PATH")

      try do
        FleetManager.stop_namespace(ns)
      catch
        _, _ -> :ok
      end
    end)

    {:ok, ns: ns}
  end

  test "a standalone UDS runner connects, places, and dispatches io_in to run/3", %{ns: ns} do
    assert NodeTestHelper.wait_until(fn -> FleetManager.active_runners(ns) != [] end, 15_000),
           "runner never connected over UDS"

    operative_id = "socket_probe_op_#{System.unique_integer([:positive])}"

    builder = %Builder{
      id: operative_id,
      factory_id: ProbeOp.factory_id(),
      init_values: %{},
      opts: []
    }

    # `start_operative` returns after the SDK acknowledged `AddOperative`; the
    # SDK opens its data stream separately, and a cast into an unbound one drops.
    assert {:ok, _info} = OperativeManager.start_operative(ns, builder)
    assert NodeTestHelper.await_stream_bound(ns, operative_id), "the data stream never bound"

    OperativeManager.send_io(operative_id, ns, "over-the-wire", %{
      "channel" => "in",
      "input_type" => "2"
    })

    assert_receive {:sdk_socket_ran, "over-the-wire"}, 10_000
  end
end
