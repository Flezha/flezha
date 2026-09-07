defmodule Flezha.SDK.RunnerBootTest do
  @moduledoc """
  A runner boots holding the idle observers it was declared with.

  `RunnerServer.init/1` arms them by folding `register_idle/4` over its own
  state, so that function's return shape is load-bearing at boot as well as at
  the `on_entry_idle` call. Nothing else in the unit tree starts a RunnerServer
  with `idle_observers`, so a shape change there was invisible until the
  `system` tier ran — which `test/test_helper.exs` excludes by default.

  One observer and two, because the two failures differ: with one the fold
  yields a value the GenServer then carries as its state, and with two the
  second iteration is handed something that is no longer a `%State{}`.
  """
  # async: false: the runner slot is a registered singleton and the controller's
  # fleet is live.
  use ExUnit.Case, async: false

  alias Flezha.Runner.FleetManager
  alias Flezha.SDK.RunnerServer

  defmodule BootProc do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
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
      c |> input("in") |> into(BootProc) |> named("probe") |> sink("probe_out")
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

  for {label, channels} <- [{"one declared observer", ["in"]}, {"two", ["in", "second"]}] do
    test "a runner boots and stays alive with #{label}", %{ns: ns} do
      handle =
        Enum.reduce(unquote(channels), Flezha.SDK.create_runner(ns, transport: :native), fn
          channel, acc -> Flezha.SDK.on_entry_idle(acc, channel, 60_000, & &1)
        end)

      handle = Flezha.SDK.define_stream(handle, BootOp) |> Flezha.SDK.start()

      assert %Flezha.SDK{server: server} = handle, "start failed: #{inspect(handle)}"
      on_exit(fn -> Flezha.SDK.close(handle) end)

      # The state the fold left is what every later clause matches on, so a
      # runner that boots with a tuple in place of its state dies on the first
      # message rather than here. Ask it something only a `%State{}` answers.
      assert RunnerServer.namespace(server) == ns
      assert Process.alive?(server)

      # And it survives its own scheduled work, which is where a malformed
      # state raises first.
      send(server, :data_heartbeat)
      assert RunnerServer.namespace(server) == ns
    end
  end
end
