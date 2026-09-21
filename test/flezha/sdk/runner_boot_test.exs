defmodule Flezha.SDK.RunnerBootTest do
  @moduledoc """
  A runner boots and carries a `%State{}` its own later clauses can match on.

  `RunnerServer.init/1` builds that state and hands it to the GenServer, so a
  boot that yields any other shape dies on the first message rather than at
  `start/1`. The `system` tier would catch it, and `test/test_helper.exs`
  excludes that tier by default, so the unit tree asks the question here.
  """
  # async: false: the runner slot is a registered singleton and the controller's
  # fleet is live.
  use ExUnit.Case, async: false

  alias Flezha.Runner.FleetManager
  alias Flezha.SDK.RunnerServer

  defmodule BootTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "boot_probe"
    def init_state(_key), do: nil

    def run(value, state, ctx) do
      Flezha.SDK.Ctx.emit(ctx, value)
      {:ok, state}
    end
  end

  defmodule BootOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "boot_probe_op"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(BootTask) |> sink("probe")
    end
  end

  setup do
    ns = "sdk_runner_boot_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      try do
        FleetManager.stop_namespace(ns)
      catch
        _, _ -> :ok
      end
    end)

    {:ok, ns: ns}
  end

  test "a runner boots and stays alive", %{ns: ns} do
    handle =
      Flezha.SDK.create_runner(ns, transport: :native)
      |> Flezha.SDK.register_factory(BootOp)
      |> Flezha.SDK.start()

    assert %Flezha.SDK{server: server} = handle, "start failed: #{inspect(handle)}"
    on_exit(fn -> Flezha.SDK.close(handle) end)

    # The state `init/1` left is what every later clause matches on, so a runner
    # that boots with anything else dies on the first message rather than here.
    # Ask it something only a `%State{}` answers.
    assert RunnerServer.namespace(server) == ns
    assert Process.alive?(server)

    # And it survives its own scheduled work, which is where a malformed state
    # raises first.
    send(server, :data_heartbeat)
    assert RunnerServer.namespace(server) == ns
  end
end
